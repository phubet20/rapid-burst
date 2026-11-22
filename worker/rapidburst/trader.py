"""Autonomous trading loop: connection management, validation, entries, exits,
risk enforcement, STOP handling and restart recovery.

Runs in a single dedicated thread; every MetaTrader call happens on that thread.
Commands from the backend are queued and executed between iterations.
"""
from __future__ import annotations

import json
import logging
import math
import os
import queue
import socket
import threading
import time
import uuid
from collections import Counter, deque
from concurrent.futures import Future
from typing import Any, Optional

from . import __version__
from .config import EngineConfig
from .engine import MomentumEngine, PositionState
from .risk import AccountView, EntryPacer, OpenPos, RiskGuard, size_position
from .trend import Trend, compute_trend, relation

log = logging.getLogger("rapidburst")

SYMBOL_TRADE_MODE_FULL = 4
ACCOUNT_MODE = {0: "DEMO", 1: "CONTEST", 2: "REAL"}
MARGIN_HEDGING = 2
DEAL_ENTRY_IN = 0


class Trader:
    def __init__(self, broker, store, cfg: EngineConfig, data_dir: str, allow_live: bool = False):
        self.broker = broker
        self.store = store
        self.cfg = cfg
        self.data_dir = data_dir
        self.allow_live = bool(allow_live)  # server-side flag only, never from the UI
        self.cmdq: "queue.Queue[tuple[str, dict, Future]]" = queue.Queue()
        self.lock = threading.Lock()
        self.logs: deque = deque(maxlen=300)
        self.log_seq = 0
        self.snapshot: dict = {}
        self.stop_event = threading.Event()

        self.conn_state = "NO_CREDENTIALS"
        self.conn_msg = "Enter MT5 demo credentials with CONNECT MT5"
        self.next_reconnect = 0.0
        self.reconnect_backoff = 5.0
        self.validation: dict = {"ok": False, "checks": []}
        self.account: dict = {}
        self.terminal: dict = {}
        self.sym_info: dict[str, dict] = {}
        self.engines: dict[str, MomentumEngine] = {}
        self.last_msc: dict[str, int] = {}
        self.last_receipt: dict[str, float] = {}
        self.sym_status: dict[str, dict] = {}
        self.slippage: dict[str, deque] = {}
        self.pos_state: dict[int, PositionState] = {}
        self.own_positions: list[dict] = []
        self.foreign_positions: list[dict] = []
        self.risk = RiskGuard(cfg)
        self.pacer = EntryPacer(cfg)
        self.daily_realized = 0.0
        self.batch_realized = 0.0
        self.last_deals_check = 0.0
        self.last_snapshot = 0.0
        self.last_trade_check = 0.0
        self.stopping = False
        self.flatten_pending = False
        self.unverified_entry: Optional[str] = None
        # higher-timeframe trend per symbol (from real broker M1/M5 bars)
        self.trends: dict[str, Trend] = {}
        self.trend_ts: dict[str, float] = {}
        # burst batches: id -> {symbol, side, opened, peak_units, mode, realized}
        self.bursts: dict[str, dict] = {}
        self.burst_cooldown: dict[str, float] = {}
        # "why no trade" visibility
        self.skip_counts: dict[str, Counter] = {}
        self.skip_window: dict[str, Counter] = {}
        self.skip_last: dict[str, float] = {}
        self.decisions: deque = deque(maxlen=40)
        self.last_why_log = 0.0

        avail, why = broker.available()
        if not avail:
            self.conn_state, self.conn_msg = "UNAVAILABLE", why
        st = self._load_state()
        self.selected: list[str] = st.get("selected", [])
        # Never resume trading from persisted state: trading starts only on an explicit START.
        self.running: bool = False
        self.was_running_before_restart = bool(st.get("running", False))
        self.batch: dict = st.get("batch", {})
        self.server_offset: float = float(st.get("server_offset", 0.0))
        self.saved_pos_meta: dict = st.get("positions", {})
        self.bursts = st.get("bursts", {}) or {}
        self.skip_counts = {k: Counter(v) for k, v in (st.get("skip_counts") or {}).items()}
        self.decisions.extend(st.get("decisions") or [])
        self._validate()

    # ---------------------------------------------------------------- state
    def _state_path(self) -> str:
        return os.path.join(self.data_dir, "state.json")

    def _load_state(self) -> dict:
        try:
            with open(self._state_path(), "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:  # noqa: BLE001
            return {}

    def _save_state(self) -> None:
        data = {
            "selected": self.selected, "running": self.running, "batch": self.batch,
            "server_offset": self.server_offset,
            "positions": {str(k): {"entry_vu": v.entry_vu, "peak_units": v.peak_units, "open_time": v.open_time,
                                   "reason": v.reason, "batch_id": v.batch_id} for k, v in self.pos_state.items()},
            "bursts": self.bursts,
            "skip_counts": {k: dict(v) for k, v in self.skip_counts.items()},
            "decisions": list(self.decisions),
        }
        tmp = self._state_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp, self._state_path())

    def add_log(self, level: str, msg: str) -> None:
        with self.lock:
            self.log_seq += 1
            self.logs.append({"seq": self.log_seq, "ts": time.time(), "level": level, "msg": msg})
        getattr(log, level.lower() if level.lower() in ("info", "warning", "error") else "info")(msg)

    # ------------------------------------------------------------- commands
    def submit(self, cmd: str, args: Optional[dict] = None) -> Future:
        fut: Future = Future()
        self.cmdq.put((cmd, args or {}, fut))
        return fut

    def _process_commands(self) -> None:
        while True:
            try:
                cmd, args, fut = self.cmdq.get_nowait()
            except queue.Empty:
                return
            try:
                handler = getattr(self, f"cmd_{cmd}", None)
                if handler is None:
                    raise ValueError(f"unknown command {cmd}")
                fut.set_result(handler(**args))
            except Exception as exc:  # noqa: BLE001
                self.add_log("ERROR", f"command {cmd} failed: {exc}")
                fut.set_result({"ok": False, "msg": str(exc)})

    def cmd_connect(self, enc: str) -> dict:
        try:
            creds = self.store.decrypt_credentials(enc)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "msg": f"could not decrypt credentials ({type(exc).__name__})"}
        self.add_log("INFO", f"CONNECT MT5 requested for account {creds['login']} on {creds['server']}")
        ok, msg = self._connect(creds)
        if ok:
            where = self.store.save_credentials(creds)
            self.add_log("INFO", f"Credentials {where}")
        return {"ok": ok, "msg": msg, "validation": self.validation}

    def cmd_start(self) -> dict:
        """Idempotent START. Re-reads everything from the broker before trading and
        rebuilds tick buffers from live ticks only (no stale signals)."""
        if self.running:
            return {"ok": True, "msg": "already running", "batch": self.batch.get("id")}
        if self.conn_state != "CONNECTED" or not self.broker.is_connected():
            creds = self.store.load_credentials()
            if not creds:
                return {"ok": False, "msg": "MT5 not connected - use CONNECT MT5 first"}
            ok, msg = self._connect(creds)
            if not ok:
                return {"ok": False, "msg": f"MT5 connect failed: {msg}"}
        # fresh re-read of account, positions and orders; re-adopt own positions by magic
        self._refresh_account()
        self._adopt_positions()
        own_orders = [o for o in self.broker.orders() if int(o.get("magic", 0)) == self.cfg.magic]
        self._prepare_symbols()
        self._validate(wait_quotes=8.0)
        if not self.validation["ok"]:
            bad = [c["name"] for c in self.validation["checks"] if not c["ok"]]
            return {"ok": False, "msg": "validation failed: " + ", ".join(bad)}
        if self.risk.halted:
            return {"ok": False, "msg": f"halted for today: {self.risk.halt_reason}"}
        if self.unverified_entry:
            return {"ok": False, "msg": f"blocked: {self.unverified_entry}"}
        self._reset_engines()
        self.running = True
        self.stopping = False
        self.start_wall = time.time()
        self.batch = {"id": uuid.uuid4().hex[:8], "started_wall": time.time(),
                      "started_server": time.time() + self.server_offset}
        self.batch_realized = 0.0
        self.last_deals_check = 0.0
        self._save_state()
        self.add_log("INFO", f"START - session {self.batch['id']} on {', '.join(self.selected)}: account re-read "
                             f"(equity {self.account.get('equity')}), {len(self.own_positions)} bot position(s) adopted, "
                             f"{len(own_orders)} bot pending order(s); warming up on fresh live ticks "
                             f"({self.cfg.warmup_seconds:.0f}s) before the first entry")
        return {"ok": True, "msg": "running", "batch": self.batch["id"]}

    def _reset_engines(self) -> None:
        """Discard any previous tick history: the engine only uses ticks that arrive after START."""
        self.sym_status = {}
        self.trend_ts = {}
        self.skip_window = {}
        for s in list(dict.fromkeys(list(self.selected) + list(self.engines))):
            info = self.sym_info.get(s)
            if not info:
                continue
            self.engines[s] = MomentumEngine(s, float(info.get("point", 0.0)), self.cfg)
            lt = self.broker.last_tick(s) or {}
            msc = int(lt.get("time_msc") or 0)
            self.last_msc[s] = msc if msc > 0 else int((time.time() + self.server_offset) * 1000)
            self.last_receipt.pop(s, None)

    def cmd_set_credentials(self, creds: dict) -> dict:
        """Launcher-only (local pipe): plain credentials, kept in memory by the engine process."""
        self.store.save_credentials({"login": int(creds["login"]), "password": str(creds["password"]),
                                     "server": str(creds["server"])})
        return {"ok": True}

    def cmd_connect_stored(self) -> dict:
        creds = self.store.load_credentials()
        if not creds:
            return {"ok": False, "msg": "no credentials"}
        self.add_log("INFO", f"CONNECT MT5 for account {creds['login']} on {creds['server']}")
        ok, msg = self._connect(creds)
        return {"ok": ok, "msg": msg, "validation": self.validation, "account": self._account_brief()}

    def _account_brief(self) -> Optional[dict]:
        a = self.account if self.conn_state == "CONNECTED" else {}
        if not a:
            return None
        return {"login": a.get("login"), "server": a.get("server"), "company": a.get("company"),
                "currency": a.get("currency"),
                "mode": ACCOUNT_MODE.get(int(a.get("trade_mode", -1)), "UNKNOWN")}

    def cmd_shutdown(self, force: bool = False) -> dict:
        """Launcher-only: release MT5 and end this engine process (only when flat/stopped)."""
        if self.running:
            return {"ok": False, "msg": "engine is running - STOP first"}
        if self.flatten_pending and not force:
            return {"ok": False, "msg": "bot positions still open - STOP retrying"}
        self.broker.shutdown()
        self.conn_state, self.conn_msg = "SHUTDOWN", "engine shut down"
        self.add_log("INFO", "Engine shut down - MT5 connection released")
        self.stop_event.set()
        return {"ok": True}

    def cmd_stop(self) -> dict:
        was = self.running
        self.running = False
        self.stopping = True
        self._save_state()
        if self.conn_state != "CONNECTED" or not self.broker.is_connected():
            creds = self.store.load_credentials()
            if creds:
                self._connect(creds)
        self.add_log("INFO", "STOP - new entries blocked" + ("" if was else " (was not running)"))
        result = self._flatten("STOP")
        self.flatten_pending = not result["ok"]
        self.stopping = False
        return {"ok": result["ok"], "msg": result["msg"]}

    def cmd_search(self, q: str = "", offset: int = 0, limit: int = 50) -> dict:
        """Full broker symbol list (name/description search), paged."""
        if self.conn_state != "CONNECTED":
            return {"ok": False, "msg": "MT5 not connected", "symbols": []}
        allr = self.broker.search_symbols(q, 100000)
        offset, limit = max(0, int(offset)), max(1, min(200, int(limit)))
        page = allr[offset:offset + limit]
        return {"ok": True, "symbols": page, "total": len(allr), "offset": offset,
                "has_more": offset + limit < len(allr)}

    def cmd_select(self, symbols: list) -> dict:
        syms = [str(s).strip() for s in symbols if str(s).strip()][:6]
        if not syms:
            return {"ok": False, "msg": "select at least one symbol"}
        if self.running:
            return {"ok": False, "msg": "STOP before changing symbols"}
        if self.conn_state == "CONNECTED":
            for s in syms:
                if not self.broker.select_symbol(s):
                    return {"ok": False, "msg": f"symbol {s} not available on this account"}
        self.selected = syms
        self._save_state()
        self._prepare_symbols()
        self._validate(wait_quotes=6.0)
        self.add_log("INFO", f"Symbols selected: {', '.join(syms)}")
        return {"ok": True, "selected": syms, "validation": self.validation, "account": self._account_brief()}

    def cmd_refresh(self) -> dict:
        if self.conn_state != "CONNECTED":
            creds = self.store.load_credentials()
            if creds:
                ok, msg = self._connect(creds)
                return {"ok": ok, "msg": msg}
            return {"ok": False, "msg": self.conn_msg}
        self._prepare_symbols()
        self._validate(wait_quotes=6.0)
        return {"ok": True, "validation": self.validation}

    def cmd_forget(self) -> dict:
        if self.running:
            return {"ok": False, "msg": "STOP first"}
        self.store.forget_credentials()
        self.broker.shutdown()
        self.conn_state, self.conn_msg = "NO_CREDENTIALS", "Credentials removed"
        self.add_log("INFO", "MT5 credentials removed from worker")
        return {"ok": True}

    # ----------------------------------------------------------- connection
    def _connect(self, creds: dict) -> tuple[bool, str]:
        avail, why = self.broker.available()
        if not avail:
            self.conn_state, self.conn_msg = "UNAVAILABLE", why
            self.add_log("ERROR", why)
            return False, why
        self.conn_state, self.conn_msg = "CONNECTING", f"Logging in {creds['login']}@{creds['server']}"
        ok, msg = self.broker.connect(creds["login"], creds["password"], creds["server"])
        if not ok:
            self.conn_state, self.conn_msg = "ERROR", msg
            self.add_log("ERROR", msg)
            return False, msg
        self.conn_state, self.conn_msg = "CONNECTED", "MT5 connected"
        self.reconnect_backoff = 5.0
        self.account = self.broker.account()
        self.terminal = self.broker.terminal()
        mode = ACCOUNT_MODE.get(int(self.account.get("trade_mode", -1)), "UNKNOWN")
        self.add_log("INFO", f"MT5 connected: {self.account.get('login')} {self.account.get('server')} "
                             f"[{mode}] {self.account.get('company', '')}")
        self._prepare_symbols()
        self._refresh_account()
        self._adopt_positions(initial=True)
        self._validate(wait_quotes=6.0)
        return True, "connected"

    def _ensure_connection(self, now: float) -> None:
        if self.conn_state == "CONNECTED":
            if self.broker.is_connected():
                return
            self.conn_state, self.conn_msg = "DISCONNECTED", "MT5 terminal lost connection - reconnecting"
            self.add_log("WARNING", self.conn_msg + " (entries paused; server-side SL protects positions)")
            self.next_reconnect = now + 2.0
            return
        if self.conn_state in ("DISCONNECTED", "ERROR", "NO_CREDENTIALS", "UNAVAILABLE") and now >= self.next_reconnect:
            creds = self.store.load_credentials()
            if not creds:
                self.next_reconnect = now + 10.0
                return
            ok, _ = self._connect(creds)
            if not ok:
                self.next_reconnect = now + (300.0 if self.conn_state == "UNAVAILABLE" else self.reconnect_backoff)
                self.reconnect_backoff = min(self.reconnect_backoff * 2, 120.0)

    def _prepare_symbols(self) -> None:
        for s in self.selected:
            if self.conn_state == "CONNECTED":
                self.broker.select_symbol(s)
                info = self.broker.symbol_info(s)
                if info:
                    self.sym_info[s] = info
            if s not in self.engines and s in self.sym_info:
                self.engines[s] = MomentumEngine(s, float(self.sym_info[s].get("point", 0.0)), self.cfg)
                self.last_msc[s] = 0
        for s in list(self.engines):
            if s not in self.selected and not any(p["symbol"] == s for p in self.own_positions):
                del self.engines[s]

    # ---------------------------------------------------- per-symbol facts
    def _tradable(self, s: str) -> tuple[bool, str]:
        """Can the bot trade this symbol safely? (reason is plain words)"""
        info = self.sym_info.get(s) or {}
        if not info:
            return False, "not offered by this account (or no contract spec)"
        if int(info.get("trade_mode", 0)) != SYMBOL_TRADE_MODE_FULL:
            return False, "trading disabled for this symbol"
        tv = float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0) or 0)
        if tv <= 0 or float(info.get("trade_tick_size", 0) or 0) <= 0:
            return False, "tick value unknown - cannot size risk safely"
        if float(info.get("volume_step", 0) or 0) <= 0 or float(info.get("volume_max", 0) or 0) <= 0:
            return False, "volume limits unknown"
        return True, "ok"

    def _quote_state(self, s: str) -> str:
        """live | closed | no_stream | unknown (never guessed: 'live' means a fresh non-stale tick)."""
        fn = getattr(self.broker, "quote_state", None)
        if fn:
            return fn(s)
        return "live" if self.broker.last_tick(s) else "closed"

    def _quoting(self, s: str) -> bool:
        return self._quote_state(s) in ("live", "unknown")

    def _notional(self, s: str, volume: float, price: float) -> float:
        info = self.sym_info.get(s) or {}
        cs = float(info.get("trade_contract_size", 0) or 0)
        cur = self.account.get("currency")
        if not cs or not cur:
            return 0.0
        if info.get("currency_profit") == cur:
            return cs * volume * price
        if info.get("currency_base") == cur:
            return cs * volume
        return 0.0

    def _deviation(self, s: str, spread: float = 0.0, vol_unit: float = 0.0) -> int:
        point = float((self.sym_info.get(s) or {}).get("point", 0) or 0)
        if point <= 0:
            return self.cfg.deviation_points
        return int(max(self.cfg.deviation_points, math.ceil(1.5 * spread / point), math.ceil(0.5 * vol_unit / point)))

    def _close_dev(self, symbol: str) -> int:
        eng = self.engines.get(symbol)
        f = eng.last if eng else None
        return self._deviation(symbol, f.spread if f else 0.0, f.vol_unit if f else 0.0)

    def _validate(self, wait_quotes: float = 0.0) -> None:
        checks = []

        def add(name, ok, detail=""):
            checks.append({"name": name, "ok": bool(ok), "detail": detail})

        avail, why = self.broker.available()
        add(getattr(self.broker, "avail_label", "MetaTrader5 package"), avail,
            why if (not avail or self.broker.name != "mt5") else "official package loaded")
        connected = self.conn_state == "CONNECTED" and self.broker.is_connected()
        add("Terminal connected to broker", connected, self.conn_msg)
        acct = self.account
        if connected:
            mode = ACCOUNT_MODE.get(int(acct.get("trade_mode", -1)), "UNKNOWN")
            demo_ok = mode == "DEMO" or (mode == "REAL" and self.allow_live)
            add("Account type", demo_ok,
                f"{mode}" + ("" if demo_ok else " - LIVE trading is disabled (DEMO only)"))
            term = self.terminal
            algo = bool(term.get("trade_allowed")) and not bool(term.get("tradeapi_disabled"))
            add("Algo trading enabled in terminal", algo,
                "ok" if algo else ("read-only (investor) login - use the trading password"
                                   if self.broker.name == "tickerall" else "enable 'Algo Trading' in the MT5 terminal"))
            acc_ok = bool(acct.get("trade_allowed")) and bool(acct.get("trade_expert"))
            add("Account allows expert trading", acc_ok, "ok" if acc_ok else "broker disallows automated trading")
            soft = []
            if wait_quotes > 0 and self.selected and hasattr(self.broker, "wait_quotes"):
                self.broker.wait_quotes(list(self.selected), wait_quotes)
            good, quoting = [], []
            for s in self.selected:
                ok_t, why_t = self._tradable(s)
                if not ok_t:
                    soft.append({"name": f"Symbol {s}", "ok": False, "detail": why_t, "soft": True})
                    continue
                good.append(s)
                qs = self._quote_state(s)
                if qs in ("live", "unknown"):
                    quoting.append(s)
                    soft.append({"name": f"Symbol {s}", "ok": True, "soft": True,
                                 "detail": "tradable, live quotes" if qs == "live" else "tradable, quote state unknown"})
                else:
                    why_q = ("price stream not connected" if qs == "no_stream"
                             else "no live quotes - market closed or symbol not trading now; skipped until quotes arrive")
                    soft.append({"name": f"Symbol {s}", "ok": False, "soft": True, "detail": "tradable, " + why_q})
            if not self.selected:
                detail = "no symbol selected - open Symbols, search, tick the assets you want (up to 6) and press APPLY"
            elif good:
                detail = ", ".join(good)
            else:
                detail = "; ".join(f"{s}: {self._tradable(s)[1]}" for s in self.selected)
                try:
                    similar = []
                    for s in self.selected:
                        base = "".join(ch for ch in s if ch.isalnum())[:6]
                        similar += [x["name"] for x in self.broker.search_symbols(base, 8)]
                    if similar:
                        detail += (f" - this broker offers: {', '.join(dict.fromkeys(similar))[:120]}; "
                                   "open Symbols, pick one and press APPLY")
                except Exception:  # noqa: BLE001
                    pass
            add("Tradable symbol selected", bool(good), detail)
            if good:
                nq = ", ".join(quoting) if quoting else "none"
                hard = not self.running      # once running, closed symbols are just skipped
                add("A selected symbol is quoting now", bool(quoting) or not hard,
                    f"quoting: {nq}" + ("" if quoting else " - every selected symbol is closed/not quoting; "
                                                          "pick an open one (e.g. a crypto on weekends)"))
            checks.extend(soft)
            hedging = int(acct.get("margin_mode", 0)) == MARGIN_HEDGING
            add("Account margin mode", True, "hedging" if hedging else "netting (1 position per symbol)")
        self.validation = {"ok": all(c["ok"] for c in checks if not c.get("soft")) and connected, "checks": checks,
                           "ts": time.time()}

    # ----------------------------------------------------------- positions
    def _refresh_account(self) -> None:
        self.account = self.broker.account() or self.account
        self.terminal = self.broker.terminal() or self.terminal
        allp = self.broker.positions()
        self.own_positions = [p for p in allp if int(p.get("magic", 0)) == self.cfg.magic]
        self.foreign_positions = [p for p in allp if int(p.get("magic", 0)) != self.cfg.magic]

    @staticmethod
    def _side(p: dict) -> str:
        return "BUY" if int(p.get("type", 0)) == 0 else "SELL"

    def _adopt_positions(self, initial: bool = False) -> None:
        live = {int(p["ticket"]): p for p in self.own_positions}
        for t, p in live.items():
            if t not in self.pos_state:
                meta = self.saved_pos_meta.get(str(t), {})
                eng = self.engines.get(p["symbol"])
                vu = meta.get("entry_vu") or (eng.last.vol_unit if eng and eng.last.vol_unit else 0.0)
                if not vu:
                    sl_dist = abs(p["price_open"] - p["sl"]) if p.get("sl") else 0.0
                    vu = sl_dist / self.cfg.sl_vol_units if sl_dist else 0.0
                self.pos_state[t] = PositionState(
                    ticket=t, symbol=p["symbol"], side=self._side(p), volume=float(p["volume"]),
                    open_price=float(p["price_open"]), open_time=float(meta.get("open_time", time.time())),
                    sl=float(p.get("sl", 0.0)), tp=float(p.get("tp", 0.0)), entry_vu=float(vu or 0.0),
                    peak_units=float(meta.get("peak_units", 0.0)), reason=meta.get("reason", "adopted"),
                    batch_id=str(meta.get("batch_id", "")))
                self.add_log("INFO", f"Re-adopted bot position #{t} {self._side(p)} {p['volume']} {p['symbol']}")
                if p["symbol"] not in self.engines:
                    info = self.broker.symbol_info(p["symbol"])
                    if info:
                        self.sym_info[p["symbol"]] = info
                        self.engines[p["symbol"]] = MomentumEngine(p["symbol"], float(info["point"]), self.cfg)
                        self.last_msc[p["symbol"]] = 0
        for t in [t for t in self.pos_state if t not in live]:
            ps = self.pos_state.pop(t)
            profit = self._closed_profit(t)
            self.add_log("INFO", f"Position #{t} {ps.side} {ps.symbol} closed"
                                 + (f" [batch {ps.batch_id}]" if ps.batch_id else "")
                                 + (f", P/L {profit:+.2f}" if profit is not None else ""))
            self.last_deals_check = 0.0
            b = self.bursts.get(ps.batch_id) if ps.batch_id else None
            if b is None:
                self.pacer.on_close(ps.symbol, profit if profit is not None else 0.0, time.time())
                continue
            b["realized"] = float(b.get("realized", 0.0)) + (profit or 0.0)
            if not any(x.batch_id == ps.batch_id for x in self.pos_state.values()):
                # whole batch is flat: one win/loss for the pacer, then the post-batch cooldown
                now = getattr(self, "now_wall", None) or time.time()
                self.pacer.on_close(ps.symbol, b["realized"], now)
                self.burst_cooldown[ps.symbol] = now + self.cfg.batch_cooldown_seconds
                self.add_log("INFO", f"BATCH {ps.batch_id} {b['side']} {ps.symbol} fully closed: "
                                     f"P/L {b['realized']:+.2f} over {b.get('legs', '?')} order(s); "
                                     f"cooldown {self.cfg.batch_cooldown_seconds:.0f}s")
                self._decision(ps.symbol, b["side"], None, f"batch closed P/L {b['realized']:+.2f}")
                self.bursts.pop(ps.batch_id, None)
        if initial and self.own_positions:
            self.add_log("INFO", f"Managing {len(self.own_positions)} existing bot position(s) after reconnect")
        self._save_state()

    def _closed_profit(self, ticket: int) -> Optional[float]:
        now = time.time() + self.server_offset
        deals = self.broker.deals(now - 3 * 86400, now + 86400)
        rel = [d for d in deals if int(d.get("position_id", 0)) == ticket]
        if not rel:
            return None
        return sum(float(d.get(k, 0.0)) for d in rel for k in ("profit", "commission", "swap", "fee"))

    def _update_realized(self, now_wall: float) -> None:
        if now_wall - self.last_deals_check < 5.0:
            return
        self.last_deals_check = now_wall
        server_now = now_wall + self.server_offset
        day_start = server_now - (server_now % 86400)
        deals = self.broker.deals(day_start - 7 * 86400, server_now + 86400)
        bot_positions = {int(d["position_id"]) for d in deals
                         if int(d.get("magic", 0)) == self.cfg.magic and int(d.get("entry", -1)) == DEAL_ENTRY_IN}
        bot_positions |= {int(p["ticket"]) for p in self.own_positions}

        def pnl(d):
            return sum(float(d.get(k, 0.0)) for k in ("profit", "commission", "swap", "fee"))

        mine = [d for d in deals if int(d.get("position_id", 0)) in bot_positions]
        self.daily_realized = sum(pnl(d) for d in mine if float(d.get("time", 0)) >= day_start)
        bs = float(self.batch.get("started_server", 0.0)) if self.batch else 0.0
        self.batch_realized = sum(pnl(d) for d in mine if bs and float(d.get("time", 0)) >= bs)

    # ------------------------------------------------------------- flatten
    def _flatten(self, why: str) -> dict:
        if self.conn_state != "CONNECTED":
            msg = f"{why}: MT5 not connected - entries blocked; positions keep server-side SL"
            self.add_log("WARNING", msg)
            return {"ok": False, "msg": msg}
        cancelled = 0
        for o in self.broker.orders():
            if int(o.get("magic", 0)) == self.cfg.magic:
                r = self.broker.cancel_order(int(o["ticket"]))
                cancelled += 1 if r["ok"] else 0
                self.add_log("INFO" if r["ok"] else "ERROR", f"{why}: cancel pending #{o['ticket']}: {r['msg']}")
        self._refresh_account()
        if not self.cfg.stop_closes_positions:
            return {"ok": True, "msg": f"{cancelled} pending cancelled; positions left with SL"}
        failed = []
        for p in list(self.own_positions):
            r = self.broker.close_position(int(p["ticket"]), self._close_dev(p["symbol"]), self.cfg.magic)
            if r.get("verified") and r["ok"]:
                self.add_log("INFO", f"{why}: closed #{p['ticket']} {self._side(p)} {p['volume']} {p['symbol']} (verified)")
            else:
                failed.append(int(p["ticket"]))
                self.add_log("ERROR", f"{why}: close #{p['ticket']} NOT verified: {r['msg']}")
        self._refresh_account()
        self._adopt_positions()
        still = [int(p["ticket"]) for p in self.own_positions]
        ok = not still
        msg = (f"{why}: {cancelled} pending cancelled, all bot positions closed (verified)" if ok
               else f"{why}: positions still open {still} - retrying each loop")
        self.add_log("INFO" if ok else "ERROR", msg)
        return {"ok": ok, "msg": msg}

    # --------------------------------------------------------------- ticks
    def _pull_ticks(self, now_wall: float) -> None:
        for s, eng in self.engines.items():
            ticks = self.broker.ticks_since(s, self.last_msc.get(s, 0))
            if ticks:
                for t, bid, ask, msc in ticks:
                    eng.on_tick(t, bid, ask)
                self.last_msc[s] = ticks[-1][3]
                self.last_receipt[s] = now_wall
                if eng.last.tick_rate >= 0.5 and len(ticks) < 50:
                    off = round((ticks[-1][0] - now_wall) / 900.0) * 900.0
                    if off != self.server_offset:
                        self.server_offset = off
                        self._save_state()

    def _server_now(self, symbol: str, now_wall: float) -> float:
        eng = self.engines[symbol]
        if not eng.buf.ticks:
            return now_wall + self.server_offset
        return eng.buf.last_t + (now_wall - self.last_receipt.get(symbol, now_wall))

    # ---------------------------------------------------------------- exits
    def _manage_exits(self, now_wall: float) -> None:
        live = {int(p["ticket"]): p for p in self.own_positions}
        for t, ps in list(self.pos_state.items()):
            p = live.get(t)
            if p is None:
                continue
            # mandatory protective stop
            if not float(p.get("sl", 0.0)):
                eng = self.engines.get(ps.symbol)
                vu = ps.entry_vu or (eng.last.vol_unit if eng else 0.0)
                if vu:
                    d = 1 if ps.side == "BUY" else -1
                    sl = ps.open_price - d * self.cfg.sl_vol_units * vu
                    r = self.broker.modify_sl(t, sl, float(p.get("tp", 0.0)))
                    if not r["ok"]:
                        cr = self.broker.close_position(t, self._close_dev(ps.symbol), self.cfg.magic)
                        self.add_log("ERROR", f"#{t} had no SL and SL could not be set - closed: {cr['msg']}")
                        continue
                    self.add_log("WARNING", f"#{t} protective SL attached at {sl}")
        if not self.running and not self.risk.halted:
            return            # exits are managed only while STARTed (server-side SL/TP protect otherwise)
        groups: dict[str, list[PositionState]] = {}
        for t, ps in self.pos_state.items():
            if t in live:
                groups.setdefault(ps.batch_id or f"#{t}", []).append(ps)
        for key, legs in groups.items():
            sym = legs[0].symbol
            eng = self.engines.get(sym)
            if eng is None:
                continue
            f = eng.features(self._server_now(sym, now_wall))
            reason = None
            if self.risk.halted:
                reason = f"risk halt: {self.risk.halt_reason}"
            elif self.running:
                reason = self._group_exit(key, legs, f, now_wall)
            if not reason:
                continue
            closed = []
            for ps in legs:
                r = self.broker.close_position(ps.ticket, self._close_dev(sym), self.cfg.magic)
                ok = r["ok"] and r.get("verified")
                closed.append(f"#{ps.ticket} {'ok' if ok else 'FAIL ' + str(r['msg'])}")
                if not ok:
                    ps.closing_attempts += 1
            label = f"BATCH {key}" if not key.startswith("#") else f"#{legs[0].ticket}"
            self.add_log("INFO" if all(c.endswith("ok") for c in closed) else "ERROR",
                         f"EXIT {label} {legs[0].side} {sum(p.volume for p in legs):g} {sym}: {reason} -> "
                         + ", ".join(closed))
            self._refresh_account()

    def _group_exit(self, key: str, legs: list, f, now_wall: float) -> Optional[str]:
        """Exit decision for a burst batch (all legs together) or a single position."""
        vol = sum(p.volume for p in legs)
        agg = PositionState(ticket=legs[0].ticket, symbol=legs[0].symbol, side=legs[0].side, volume=vol,
                            open_price=sum(p.open_price * p.volume for p in legs) / vol,
                            open_time=min(p.open_time for p in legs),
                            entry_vu=max(p.entry_vu for p in legs) or 0.0,
                            peak_units=max(p.peak_units for p in legs), reason=legs[0].reason)
        eng = self.engines[legs[0].symbol]
        is_batch = not key.startswith("#")
        if is_batch and agg.entry_vu > 0 and f.bid and f.ask:
            d = 1 if agg.side == "BUY" else -1
            exit_px = f.bid if agg.side == "BUY" else f.ask          # executable price: spread already paid
            units = (exit_px - agg.open_price) * d / agg.entry_vu
            if units >= self.cfg.batch_tp_units:
                return f"batch target +{units:.2f}vu reached"
            if now_wall - agg.open_time >= self.cfg.batch_max_hold_seconds:
                return f"batch time stop {self.cfg.batch_max_hold_seconds:.0f}s ({units:+.2f}vu)"
        reason = eng.exit_decision(agg, f, now_wall)
        for p in legs:
            p.peak_units = agg.peak_units
        return reason

    # -------------------------------------------------------------- entries
    def _try_entries(self, now_wall: float) -> None:
        cfg = self.cfg
        acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                           float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
        hedging = int(self.account.get("margin_mode", 0)) == MARGIN_HEDGING
        open_pos = [OpenPos(p["symbol"], self._side(p), float(p["volume"]), float(p["profit"]),
                            self._pos_risk(p), self._notional(p["symbol"], float(p["volume"]),
                                                              float(p.get("price_current") or p["price_open"])))
                    for p in self.own_positions]
        for s in self.selected:
            eng = self.engines.get(s)
            info = self.sym_info.get(s)
            if eng is None or not info:
                continue
            now_srv = self._server_now(s, now_wall)
            sig = eng.signal(now_srv)
            status = {"signal": sig.action, "score": round(sig.score, 3), "reason": sig.reason,
                      "features": sig.features.brief(),
                      "spread_pts": round(sig.features.spread / float(info["point"]), 1) if info.get("point") else None,
                      "tick_age": round(now_wall - self.last_receipt[s], 1) if s in self.last_receipt else None}
            self.sym_status[s] = status
            tr = self.trends.get(s)
            status["trend"] = tr.brief() if tr else None
            if sig.action == "HOLD":
                self._skip(s, status, sig.reason, now_wall)
                continue
            if not hedging and (any(p["symbol"] == s for p in self.foreign_positions)
                                or any(p["symbol"] == s for p in self.own_positions)):
                self._skip(s, status, "netting account: symbol already has a position", now_wall, sig)
                continue
            ok, why = self.pacer.can_enter(s, now_wall)
            if not ok:
                self._skip(s, status, f"{sig.action} blocked: {why}", now_wall, sig)
                continue
            sl_hist = self.slippage.get(s)
            if sl_hist and len(sl_hist) >= 3 and sum(sl_hist) / len(sl_hist) > cfg.max_avg_slippage_vu:
                self._skip(s, status, "paused: poor execution quality (slippage)", now_wall, sig)
                continue
            cd = self.burst_cooldown.get(s, 0.0) - now_wall
            if cd > 0:
                self._skip(s, status, f"{sig.action} blocked: batch cooldown {cd:.0f}s", now_wall, sig)
                continue
            tr = self.trends.get(s)
            rel = relation(tr, sig.action)
            status["trend_rel"] = rel
            mine = [ps for ps in self.pos_state.values() if ps.symbol == s]
            risk_mult, batch_id, mode = 1.0, uuid.uuid4().hex[:6], "batch"
            if mine:
                # pyramiding into an open batch: only same side, only with the trend; RiskGuard
                # additionally requires every open leg in profit (never adds to a loser)
                if rel != "aligned":
                    self._skip(s, status, f"{sig.action} skipped: batch open, adding only when trend aligned "
                                          f"(trend {tr.label if tr else 'unknown'})", now_wall, sig)
                    continue
                n, batch_id, mode = 1, mine[0].batch_id or batch_id, "pyramid"
            elif rel == "counter":
                if abs(sig.score) < cfg.countertrend_min_score:
                    self._skip(s, status, f"{sig.action} skipped: counter-trend ({tr.label}) needs "
                                          f"|score| >= {cfg.countertrend_min_score:.2f}", now_wall, sig)
                    continue
                n, risk_mult, mode = 1, 0.5, "counter-trend"
            else:
                n = self._batch_size(abs(sig.score))
            self._enter_batch(s, sig, info, acct, open_pos, now_wall, status, n, risk_mult, batch_id, mode)

    def _batch_size(self, score: float) -> int:
        c = self.cfg
        lo, hi = max(1, c.batch_min), max(c.batch_min, c.batch_max)
        span = max(1e-9, c.batch_full_score - c.entry_threshold)
        frac = min(1.0, max(0.0, (score - c.entry_threshold) / span))
        return int(lo + round(frac * (hi - lo)))

    # ------------------------------------------------- "why no trade" ledger
    @staticmethod
    def _category(reason: str) -> str:
        r = (reason or "").lower()
        table = [("warming", "warming up"), ("no ticks", "no live ticks"), ("idle", "no live ticks"),
                 ("stale", "no live ticks"), ("thin tick", "thin tick flow"), ("tick rate", "thin tick flow"),
                 ("spread", "spread filter"), ("no momentum", "no momentum edge"), ("extended", "move extended"),
                 ("batch cooldown", "batch cooldown"), ("cooldown", "cooldown"), ("per minute", "entry rate cap"),
                 ("/min", "entry rate cap"), ("counter-trend", "counter-trend filter"),
                 ("trend aligned", "batch open (no add)"), ("min lot", "sizing refused"),
                 ("averaging", "no adding to losers"), ("add requires", "add needs stronger score"),
                 ("margin", "margin"), ("risk", "risk cap"), ("max ", "risk cap"), ("opposite", "no hedge vs open"),
                 ("slippage", "execution quality"), ("halt", "risk halt"), ("netting", "netting position"),
                 ("loss streak", "loss-streak pause"), ("pause", "loss-streak pause")]
        for k, v in table:
            if k in r:
                return v
        return "".join(ch for ch in r.split(":")[0] if not ch.isdigit())[:32].strip() or "other"

    def _skip(self, s: str, status: dict, reason: str, now_wall: float, sig=None) -> None:
        status["reason"] = reason
        if now_wall - self.skip_last.get(s, 0.0) < 1.0:      # sample at most once per second per symbol
            return
        self.skip_last[s] = now_wall
        cat = self._category(reason)
        self.skip_counts.setdefault(s, Counter())[cat] += 1
        self.skip_window.setdefault(s, Counter())[cat] += 1
        if sig is not None and sig.action != "HOLD":
            self._decision(s, sig.action, sig.score, reason)

    def _decision(self, s: str, side: str, score, outcome: str) -> None:
        tr = self.trends.get(s)
        self.decisions.append({"ts": round(getattr(self, "now_wall", None) or time.time(), 1), "symbol": s, "side": side,
                               "score": None if score is None else round(float(score), 2),
                               "trend": tr.label if tr else "unknown", "outcome": outcome[:160]})

    def _log_why(self, now_wall: float) -> None:
        if now_wall - self.last_why_log < 60.0:
            return
        self.last_why_log = now_wall
        parts = []
        for s, c in self.skip_window.items():
            if c:
                parts.append(f"{s}: " + ", ".join(f"{k} {v}" for k, v in c.most_common(4)))
        self.skip_window = {}
        if parts:
            self.add_log("INFO", "WHY NO TRADE (last 60s, samples/s) " + " | ".join(parts))
            self._save_state()

    # ------------------------------------------------------------- trend
    def _update_trends(self, now_wall: float) -> None:
        fn = getattr(self.broker, "candles", None)
        for s in self.selected:
            if now_wall - self.trend_ts.get(s, 0.0) < self.cfg.trend_refresh_seconds:
                continue
            self.trend_ts[s] = now_wall
            if fn is None or not self._tradable(s)[0]:
                self.trends[s] = Trend(label="unknown (no bar history from this broker)")
                continue
            try:
                m1 = fn(s, "M1", 0.75)
                m5 = fn(s, "M5", 1.5)
            except Exception as e:  # noqa: BLE001
                m1, m5 = [], []
                log.debug("candles %s failed: %s", s, e)
            prev = self.trends.get(s)
            tr = compute_trend(m1, m5, self.cfg.trend_min_strength)
            tr.detail["m1_bars"], tr.detail["m5_bars"] = float(len(m1)), float(len(m5))
            self.trends[s] = tr
            if prev is None or prev.label.split(" ")[0] != tr.label.split(" ")[0]:
                self.add_log("INFO", f"TREND {s}: {tr.label} (M1 {len(m1)} bars, M5 {len(m5)} bars)")

    def _pos_risk(self, p: dict) -> float:
        info = self.sym_info.get(p["symbol"]) or {}
        sl = float(p.get("sl", 0.0))
        ts, tv = float(info.get("trade_tick_size", 0)), float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
        if not sl or not ts or not tv:
            return float(self.account.get("equity", 0)) * self.cfg.risk_per_trade_pct / 100.0
        return max(0.0, abs(float(p["price_open"]) - sl) / ts * tv * float(p["volume"]))

    def _enter_batch(self, s, sig, info, acct, open_pos, now_wall, status, n, risk_mult, batch_id, mode) -> None:
        """Open up to n same-direction market orders sharing batch_id. Each leg carries its own SL;
        each leg is re-checked by RiskGuard (positions, lots, notional, open risk, margin) and the batch
        is cut short at the first refusal. Never martingale: every leg has the same risk-based size."""
        cfg = self.cfg
        f = sig.features
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl_dist = max(cfg.sl_vol_units * f.vol_unit, cfg.min_sl_spread_mult * f.spread, stops)
        tp_dist = max(cfg.tp_vol_units * f.vol_unit, 2 * sl_dist)
        tv = float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
        size = size_position(acct.equity * risk_mult, cfg, sl_dist, float(info.get("trade_tick_size", 0)), tv,
                             float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"]))
        if size.volume <= 0:
            self._skip(s, status, f"{sig.action} skipped: {size.note}", now_wall, sig)
            return
        d = 1 if sig.action == "BUY" else -1
        add_ok = abs(sig.score) >= cfg.add_threshold
        placed, why_stop = [], ""
        self.pacer.on_entry(s, now_wall)          # one batch = one entry for the rate limiter
        for i in range(max(1, n)):
            f = self.engines[s].features(self._server_now(s, now_wall)) if i else f
            price = f.ask if sig.action == "BUY" else f.bid
            margin = self.broker.calc_margin(sig.action, s, size.volume, price)
            if margin is None:
                why_stop = "margin calc unavailable"
                break
            notional = self._notional(s, size.volume, price)
            ok, why = self.risk.check_entry(s, sig.action, open_pos, acct, size.volume, size.risk_money, margin,
                                            add_ok, notional, batch_leg=bool(placed))
            if not ok:
                why_stop = why
                break
            sl = price - d * sl_dist
            tp = price + d * tp_dist
            comment = f"{cfg.comment_prefix}-{batch_id}-{uuid.uuid4().hex[:4]}"[:31]   # unique per leg
            self.pacer.inflight.add(s)
            try:
                r = self.broker.market_order(s, sig.action, size.volume, sl, tp,
                                             self._deviation(s, f.spread, f.vol_unit), cfg.magic, comment)
            finally:
                self.pacer.inflight.discard(s)
            if not r.get("ok"):
                why_stop = f"rejected: {r.get('msg')}"
                self.add_log("WARNING", f"ENTRY {sig.action} {size.volume} {s} rejected: {r.get('msg')}")
                if "not found" in str(r.get("msg")):
                    self.unverified_entry = r["msg"]
                    self.running = False
                    self._save_state()
                break
            fill = float(r["price"])
            slip = (fill - float(r["req_price"])) * d / point
            self.slippage.setdefault(s, deque(maxlen=10)).append(max(0.0, slip * point / max(f.vol_unit, point)))
            ticket = int(r["ticket"])
            self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=sig.action, volume=float(r["volume"]),
                                                   open_price=fill, open_time=now_wall, sl=sl, tp=tp,
                                                   entry_vu=f.vol_unit, reason=sig.reason, batch_id=batch_id)
            placed.append(ticket)
            self.add_log("INFO", f"ENTRY {sig.action} {r['volume']} {s} @ {fill} #{ticket} batch {batch_id} "
                                 f"leg {len(placed)}/{n} ({mode}) score {sig.score:+.2f} [{sig.reason}] "
                                 f"SL {sl:.{int(info['digits'])}f} slip {slip:+.0f}pt risk {size.risk_money:.2f} "
                                 f"{r.get('latency', 0)*1000:.0f}ms")
            open_pos.append(OpenPos(s, sig.action, float(r["volume"]), 0.0, size.risk_money, notional))
            self._refresh_account()
            acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                               float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
        if placed:
            b = self.bursts.setdefault(batch_id, {"symbol": s, "side": sig.action, "opened": now_wall,
                                                  "mode": mode, "realized": 0.0, "legs": 0})
            b["legs"] = int(b.get("legs", 0)) + len(placed)
            tr = self.trends.get(s)
            self._decision(s, sig.action, sig.score,
                           f"OPENED {len(placed)}/{n} ({mode}, size {size.volume:g}, {size.note})"
                           + (f"; stopped: {why_stop}" if why_stop else ""))
            status["reason"] = f"batch {batch_id}: {len(placed)} order(s) {sig.action} (trend {tr.label if tr else 'unknown'})"
            self._save_state()
        else:
            self._skip(s, status, f"{sig.action} blocked: {why_stop}", now_wall, sig)

    # ----------------------------------------------------------------- loop
    def step(self, now_wall: Optional[float] = None) -> None:
        now_wall = time.time() if now_wall is None else now_wall
        self.now_wall = now_wall
        self._process_commands()
        self._ensure_connection(now_wall)
        if self.conn_state == "CONNECTED":
            if self.running:
                self._pull_ticks(now_wall)
            self._refresh_account()
            self._adopt_positions()
            self._update_realized(now_wall)
            floating = sum(float(p.get("profit", 0)) + float(p.get("swap", 0)) for p in self.own_positions)
            server_now = now_wall + self.server_offset
            day_key = time.strftime("%Y-%m-%d", time.gmtime(server_now))
            acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                               float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
            halt = self.risk.update(day_key, acct, self.daily_realized + floating)
            if halt:
                self.add_log("ERROR", f"RISK HALT: {halt} - trading stopped for today, closing bot positions")
                self.running = False
                self._save_state()
            if now_wall - self.last_trade_check > 10.0:
                self.last_trade_check = now_wall
                self._validate()
            self._update_trends(now_wall)
            self._manage_exits(now_wall)
            if self.running:
                self._log_why(now_wall)
            if self.risk.halted and self.own_positions:
                self._flatten("RISK HALT")
            if self.flatten_pending:
                self.flatten_pending = not self._flatten("STOP retry")["ok"]
            if self.running and not self.stopping and self.validation.get("ok") and not self.risk.halted:
                self._try_entries(now_wall)
            elif self.engines:
                for s, eng in self.engines.items():
                    sig = eng.signal(self._server_now(s, now_wall))
                    reason = sig.reason if self.running else "stopped"
                    if self.running and not self.validation.get("ok"):
                        reason = "validation failing - entries paused"
                    if self.risk.halted:
                        reason = f"halted: {self.risk.halt_reason}"
                    tr = self.trends.get(s)
                    self.sym_status[s] = {"signal": "HOLD" if not self.running else sig.action, "score": round(sig.score, 3),
                                          "reason": reason, "trend": tr.brief() if tr else None, "features": sig.features.brief(),
                                          "tick_age": round(now_wall - self.last_receipt[s], 1) if s in self.last_receipt else None}
        if now_wall - self.last_snapshot >= self.cfg.snapshot_interval:
            self.last_snapshot = now_wall
            self._build_snapshot(now_wall)

    def _symbol_view(self) -> dict:
        """Per selected symbol: tradable?, market state, and why it is (not) being traded - never blocks the others."""
        out = {k: dict(v) for k, v in self.sym_status.items()}
        if self.conn_state != "CONNECTED":
            return out
        for s in self.selected:
            ok_t, why_t = self._tradable(s)
            st = out.setdefault(s, {"signal": "HOLD", "score": 0.0, "reason": "", "features": {}})
            tr = self.trends.get(s)
            st["trend"] = tr.brief() if tr else None
            st["skips"] = dict(self.skip_counts.get(s, Counter()).most_common(6))
            st["tradable"] = ok_t
            if not ok_t:
                st["market"], st["signal"], st["reason"] = "n/a", "HOLD", f"not tradable: {why_t}"
                continue
            qs = self._quote_state(s)
            st["market"] = qs
            if qs in ("closed", "no_stream") and s not in self.last_receipt:
                st["reason"] = ("no live quotes - market closed or not trading now; skipped, resumes automatically"
                                if qs == "closed" else "price stream not connected - retrying")
                st["signal"] = "HOLD"
        return out

    def _risk_view(self) -> dict:
        c = self.cfg
        return {"risk_per_trade_pct": c.risk_per_trade_pct, "small_account_max_loss_pct": c.small_account_max_loss_pct,
                "max_total_risk_pct": c.max_total_risk_pct, "daily_loss_limit_pct": c.daily_loss_limit_pct,
                "equity_drawdown_pct": c.equity_drawdown_pct, "batch": f"{c.batch_min}-{c.batch_max}",
                "batch_tp_units": c.batch_tp_units, "batch_max_hold_s": c.batch_max_hold_seconds,
                "batch_cooldown_s": c.batch_cooldown_seconds, "entry_threshold": c.entry_threshold,
                "countertrend_min_score": c.countertrend_min_score, "min_tick_rate": c.min_tick_rate,
                "max_spread_bps": c.max_spread_bps, "max_positions_total": c.max_positions_total,
                "max_positions_per_symbol": c.max_positions_per_symbol, "sl_vol_units": c.sl_vol_units}

    def _build_snapshot(self, now_wall: float) -> None:
        acct = self.account if self.conn_state == "CONNECTED" else {}
        floating = sum(float(p.get("profit", 0)) + float(p.get("swap", 0)) for p in self.own_positions)
        mode = ACCOUNT_MODE.get(int(acct["trade_mode"]), "UNKNOWN") if acct.get("trade_mode") is not None else None
        positions = []
        for p in self.own_positions if self.conn_state == "CONNECTED" else []:
            ps = self.pos_state.get(int(p["ticket"]))
            positions.append({"ticket": int(p["ticket"]), "symbol": p["symbol"], "side": self._side(p),
                              "volume": float(p["volume"]), "price_open": p["price_open"],
                              "price_current": p.get("price_current"), "sl": p.get("sl"), "tp": p.get("tp"),
                              "profit": round(float(p.get("profit", 0)) + float(p.get("swap", 0)), 2),
                              "age_s": round(now_wall - ps.open_time) if ps else None,
                              "reason": ps.reason if ps else "", "batch": ps.batch_id if ps else ""})
        snap = {
            "ts": now_wall,
            "worker": {"version": __version__, "host": socket.gethostname(),
                       "mt5_package": self.broker.available()[0]},
            "conn": {"state": self.conn_state, "msg": self.conn_msg,
                     "terminal_connected": bool(self.terminal.get("connected")) if self.conn_state == "CONNECTED" else False,
                     "algo_trading": bool(self.terminal.get("trade_allowed")) if self.conn_state == "CONNECTED" else False,
                     "ping_ms": (round(self.terminal["ping_last"] / 1000.0, 1)
                                 if self.conn_state == "CONNECTED" and self.terminal.get("ping_last") is not None else None)},
            "account": ({"login": acct.get("login"), "server": acct.get("server"), "name": acct.get("name"),
                         "company": acct.get("company"), "currency": acct.get("currency"),
                         "leverage": acct.get("leverage"), "mode": mode,
                         "balance": acct.get("balance"), "equity": acct.get("equity"),
                         "margin_free": acct.get("margin_free"), "margin_level": acct.get("margin_level"),
                         "profit": acct.get("profit")} if acct else None),
            "validation": self.validation,
            "engine": {"running": self.running, "start_wall": getattr(self, "start_wall", None), "halted": self.risk.halted, "halt_reason": self.risk.halt_reason,
                       "blocked": self.unverified_entry, "batch": self.batch, "symbols": self._symbol_view(),
                       "selected": self.selected, "live_allowed": self.allow_live,
                       "bursts": {k: {kk: v.get(kk) for kk in ("symbol", "side", "mode", "legs")}
                                  for k, v in self.bursts.items()},
                       "decisions": list(self.decisions)[-15:],
                       "risk_config": self._risk_view()},
            "pnl": ({"floating": round(floating, 2), "daily": round(self.daily_realized + floating, 2),
                     "daily_realized": round(self.daily_realized, 2),
                     "batch": round(self.batch_realized + floating, 2) if self.batch else None}
                    if acct else None),
            "positions": positions,
            "foreign_positions": len(self.foreign_positions) if acct else None,
            "pubkey": getattr(self.store, "public_key_spki_b64", lambda: None)(),
            "flatten_pending": self.flatten_pending,
        }
        with self.lock:
            self.snapshot = snap

    def get_snapshot(self) -> dict:
        with self.lock:
            return dict(self.snapshot)

    def logs_after(self, seq: int) -> list[dict]:
        with self.lock:
            return [x for x in self.logs if x["seq"] > seq]

    def run_forever(self) -> None:
        self.add_log("INFO", f"Trading engine {__version__} started on {socket.gethostname()} (pid {os.getpid()})")
        while not self.stop_event.is_set():
            t0 = time.time()
            try:
                self.step(t0)
            except Exception as exc:  # noqa: BLE001
                self.add_log("ERROR", f"engine loop error: {type(exc).__name__}: {exc}")
                time.sleep(1.0)
            dt = time.time() - t0
            time.sleep(max(0.0, self.cfg.loop_interval - dt))
