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
from .impulse import Impulse, detect as detect_impulse, detect_early, detect_reversal
from .learner import Learner, regime as vol_regime
from .risk import normalize_volume
from .engine import Signal

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
        self.atr_m1: dict[str, float] = {}          # M1 ATR from real broker bars
        self.impulse: dict[str, Impulse] = {}
        self.no_up_after_loss: dict[str, float] = {}  # symbol -> max lot until a winning batch (no martingale)
        self.learner = Learner(os.path.join(data_dir, "learning.json"), min_samples=self.cfg.learn_min_samples,
                               half_life_h=self.cfg.learn_half_life_hours, max_mult=self.cfg.learn_max_mult)
        self.learn_view: dict[str, str] = {}
        self._xq: dict[str, tuple] = {}              # per-loop executable quote cache
        self._rest_mode: dict[str, float] = {}       # symbol -> use REST quote until this time
        self._rest_check: dict[str, float] = {}
        self._lag_log: dict[str, float] = {}
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
        self.scalp_rev: dict = {}
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
                                   "reason": v.reason, "batch_id": v.batch_id,
                                   "hard_dist": v.hard_dist, "sl_state": v.sl_state} for k, v in self.pos_state.items()},
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
            self.engines[s].seed_vu = 0.15 * self.atr_m1.get(s, 0.0)
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
                    batch_id=str(meta.get("batch_id", "")),
                    hard_dist=float(meta.get("hard_dist") or (abs(float(p["price_open"]) - float(p["sl"])) * self.cfg.hard_close_mult
                                                              if p.get("sl") else float(vu or 0.0) * self.cfg.sl_vol_units)),
                    sl_state=str(meta.get("sl_state", "initial")))
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
                if b["realized"] < 0:
                    self.no_up_after_loss[ps.symbol] = float(b.get("lot", ps.volume))   # never size up after a loss
                else:
                    self.no_up_after_loss.pop(ps.symbol, None)
                self.burst_cooldown[ps.symbol] = now + self.cfg.batch_cooldown_seconds
                self._learn_from(ps, b, now)
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
    def _check_price_sync(self, live: dict, now_wall: float) -> None:
        """Diagnostic only: log when the engine's tick price disagrees with the broker's own current
        price on an open position (stale/lagging stream). Rate-limited, never changes behaviour."""
        last = getattr(self, "_sync_log", {})
        self._sync_log = last
        for p in live.values():
            s = p["symbol"]
            eng = self.engines.get(s)
            cur = float(p.get("price_current") or 0.0)
            if eng is None or not eng.buf.ticks or cur <= 0 or now_wall - last.get(s, 0.0) < 15.0:
                continue
            tk = eng.buf.ticks[-1]
            gap = abs(tk.mid - cur)
            if gap > max(4.0 * tk.spread, 1e-9):
                last[s] = now_wall
                self.add_log("WARNING", f"PRICE MISMATCH {s}: engine tick mid {tk.mid:.2f} vs broker position price "
                                        f"{cur:.2f} (diff {gap:.2f} = {gap / max(tk.spread, 1e-9):.0f}x spread, "
                                        f"tick age {now_wall - self.last_receipt.get(s, now_wall):.1f}s) - feed may lag")

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
            # optional broker-side protective stop (only when RB_BROKER_SL is on; default OFF)
            if self.cfg.broker_sl and not float(p.get("sl", 0.0)):
                eng = self.engines.get(ps.symbol)
                vu = ps.entry_vu or (eng.last.vol_unit if eng else 0.0)
                dist = (ps.hard_dist / self.cfg.hard_close_mult) if ps.hard_dist else self.cfg.sl_vol_units * vu
                if dist:
                    d = 1 if ps.side == "BUY" else -1
                    sl = ps.open_price - d * dist
                    r = self.broker.modify_sl(t, sl, float(p.get("tp", 0.0)))
                    if not r["ok"]:
                        cr = self.broker.close_position(t, self._close_dev(ps.symbol), self.cfg.magic)
                        self.add_log("ERROR", f"#{t} had no SL and SL could not be set - closed: {cr['msg']}")
                        continue
                    self.add_log("WARNING", f"#{t} protective SL attached at {sl}")
        if not self.running and not self.risk.halted:
            bare = [p for p in self.own_positions
                    if not float(p.get("sl", 0.0)) or (self.cfg.emergency_sl and not self.cfg.broker_sl)]
            if bare and not self.stopping:
                # nothing manages these positions (restart / reconnect / not STARTed) and no working broker SL
                # protects them: never leave them unprotected - flatten immediately
                self.add_log("WARNING", f"{len(bare)} bot position(s) without a working broker SL and engine "
                                        f"not running - flattening immediately")
                self._flatten("UNPROTECTED (no SL, engine not running)")
            return            # exits are managed only while STARTed (broker SL/TP protect otherwise)
        self._check_price_sync(live, now_wall)
        self._manage_stops(live, now_wall)
        groups: dict[str, list[PositionState]] = {}
        for t, ps in self.pos_state.items():
            if t in live:
                groups.setdefault(ps.batch_id or f"#{t}", []).append(ps)
        for key, legs in groups.items():
            sym = legs[0].symbol
            eng = self.engines.get(sym)
            f = eng.features(self._server_now(sym, now_wall)) if eng is not None else None
            reason = None
            if self.risk.halted:
                reason = f"risk halt: {self.risk.halt_reason}"
            elif self._is_scalp(key):
                reason = self._hard_close(legs, live, f)          # far SL mirror / locked-stop mirror / group loss stop
                if not reason and self.running and eng is not None:
                    reason = self._scalp_exit(key, legs, live, f, now_wall)
            elif self._is_basket(key):
                reason = self._basket_exit(key, legs, live)       # whole basket together: target+trail / loss stop
            else:
                reason = self._hard_close(legs, live, f)          # software stop: needs no warm-up
                if not reason and self.running and eng is not None:
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
            if key in self.bursts:
                self.bursts[key]["exit_reason"] = reason
            label = f"BATCH {key}" if not key.startswith("#") else f"#{legs[0].ticket}"
            self.add_log("INFO" if all(c.endswith("ok") for c in closed) else "ERROR",
                         f"EXIT {label} {legs[0].side} {sum(p.volume for p in legs):g} {sym}: {reason} -> "
                         + ", ".join(closed))
            self._refresh_account()

    # ------------------------------------------------- protective stops
    def _atr(self, s: str) -> tuple[float, str]:
        a = self.atr_m1.get(s, 0.0)
        if a > 0:
            return a, "M1"
        eng = self.engines.get(s)
        vu = eng.last.vol_unit if eng and eng.last.vol_unit else 0.0
        return (4.0 * vu, "est") if vu else (0.0, "none")

    def _exec_quote(self, s: str, now_wall: float) -> Optional[tuple]:
        """(bid, ask, source) used for EXITS and TRAILING. The WS tick is used normally; the broker's REST
        quote replaces it when the WS tick is stale (> 2 s) or disagrees with the broker's own position price
        by more than 2x spread (checked every 5 s too, to catch a lagging stream); REST then stays in use for
        10 s. Only polled while a bot position is open."""
        c = self._xq.get(s)
        if c and c[0] == now_wall:
            return c[1]
        eng = self.engines.get(s)
        ws = None
        if eng is not None and eng.buf.ticks:
            tk = eng.buf.ticks[-1]
            ws = (tk.bid, tk.ask, tk.spread)
        res = (ws[0], ws[1], "ws") if ws else None
        mine = [p for p in self.own_positions if p["symbol"] == s]
        qfn = getattr(self.broker, "quote", None)
        if mine and qfn is not None:
            age = now_wall - self.last_receipt[s] if s in self.last_receipt else None
            info = self.sym_info.get(s) or {}
            spread = max(ws[2] if ws else 0.0, 2 * float(info.get("point", 0) or 0))
            dev = False
            if ws:
                for p in mine:
                    cur = float(p.get("price_current") or 0.0)
                    wx = ws[0] if self._side(p) == "BUY" else ws[1]
                    if cur and abs(wx - cur) > 2 * spread:
                        dev = True
            check = (ws is None or age is None or age > 2.0 or dev or self._rest_mode.get(s, 0.0) > now_wall
                     or now_wall - self._rest_check.get(s, 0.0) > 5.0)
            if check:
                self._rest_check[s] = now_wall
                q = qfn(s)
                if q:
                    if ws:
                        lag = abs((q["bid"] + q["ask"]) / 2 - (ws[0] + ws[1]) / 2)
                        if dev or lag > 2 * spread:
                            self._rest_mode[s] = now_wall + 10.0
                            if now_wall - self._lag_log.get(s, 0.0) > 15.0:
                                self._lag_log[s] = now_wall
                                self.add_log("WARNING", f"FEED LAG {s}: WS tick mid {(ws[0] + ws[1]) / 2:.2f} vs broker "
                                                        f"quote {(q['bid'] + q['ask']) / 2:.2f} (diff {lag:.2f} = "
                                                        f"{lag / spread:.1f}x spread, WS age "
                                                        f"{(age if age is not None else -1):.1f}s) - exits use the broker price")
                    if self._rest_mode.get(s, 0.0) > now_wall or ws is None or (age is not None and age > 2.0):
                        res = (q["bid"], q["ask"], "rest")
        self._xq[s] = (now_wall, res)
        return res

    def _exec_px(self, ps, p: dict, f, now_wall: Optional[float] = None) -> Optional[float]:
        """Worst (most conservative) executable price we know: bid for BUY, ask for SELL, from the broker's
        position price and the executable quote (WS tick or REST quote, see _exec_quote)."""
        d = 1 if ps.side == "BUY" else -1
        prices = []
        if p.get("price_current"):
            prices.append(float(p["price_current"]))
        xq = self._exec_quote(ps.symbol, now_wall if now_wall is not None else getattr(self, "now_wall", 0.0))
        if xq:
            prices.append(float(xq[0] if d > 0 else xq[1]))
        elif f is not None and f.bid and f.ask:
            prices.append(float(f.bid if d > 0 else f.ask))
        if not prices:
            return None
        return min(prices) if d > 0 else max(prices)

    def _manage_stops(self, live: dict, now_wall: float) -> None:
        """Breakeven+ then trailing: move the broker SL forward as the bar continues in our favour.
        A software mirror (vsl) is always kept, so if the broker refuses a modify the same level is
        still enforced by the engine."""
        cfg = self.cfg
        for t, ps in self.pos_state.items():
            p = live.get(t)
            if p is None or self._is_basket(ps.batch_id):
                continue                  # basket legs have no per-order BE/trail: the basket is trailed as a whole
            eng = self.engines.get(ps.symbol)
            f = eng.features(self._server_now(ps.symbol, now_wall)) if eng is not None else None
            px = self._exec_px(ps, p, f, now_wall)
            if px is None:
                continue
            d = 1 if ps.side == "BUY" else -1
            pl = (px - ps.open_price) * d
            ps.peak_pl = max(ps.peak_pl, pl)
            info = self.sym_info.get(ps.symbol) or {}
            point = float(info.get("point", 0.0)) or 1e-9
            spread = (f.spread if f is not None and f.spread else 0.0) or abs(float(p.get("price_current", px)) - px)
            stops = (int(info.get("trade_stops_level", 0)) + 3) * point
            atr, _ = self._atr(ps.symbol)
            trail = max((cfg.scalp_trail_atr if self._is_scalp(ps.batch_id) else cfg.trail_atr_frac) * atr,
                        cfg.trail_spread_mult * spread, stops)
            be_lock = max(cfg.be_lock_spread_mult * spread, 2 * point)
            cur = float(p.get("sl", 0.0)) or ps.sl
            new, state = None, ps.sl_state
            if ps.sl_state == "initial":
                if pl >= be_lock + trail:             # room to lock BE+ while staying >= trail behind price
                    new, state = ps.open_price + d * be_lock, "BE+"
            else:
                cand = px - d * trail
                step = max(0.25 * trail, 5 * point)
                if (cand - (cur or ps.vsl)) * d >= step:
                    new, state = cand, "trail"
            if new is None:
                continue
            if cur and (new - cur) * d <= 0:
                continue                               # never move a stop backwards
            ps.vsl = new if not ps.vsl else (max(ps.vsl, new) if d > 0 else min(ps.vsl, new))
            ps.sl_state = state
            digits = int(info.get("digits", 5))
            if not cfg.broker_sl or now_wall - ps.last_mod < cfg.sl_modify_min_interval:
                continue
            ps.last_mod = now_wall
            r = self.broker.modify_sl(t, new, float(p.get("tp", 0.0)) or ps.tp)
            if r.get("ok"):
                sent = float(r.get("sl") or new)
                ps.sl = sent
                self.add_log("INFO", f"SL #{t} {ps.side} {ps.symbol} -> {sent:.{digits}f} ({state}, wanted "
                                     f"{new:.{digits}f}, P/L {pl:+.{digits}f} px, trail {trail:.{digits}f})")
            elif r.get("closed"):
                self.add_log("INFO", f"SL #{t}: position already closed - modify skipped")
            elif r.get("soft"):
                log.debug("SL #%s not placeable yet: %s", t, r.get("msg"))   # software stop still enforced
            else:
                self.add_log("WARNING", f"SL modify #{t} refused (retcode {r.get('retcode')}, {r.get('code')}: "
                                        f"{r.get('msg')}) - software {state} stop {new:.{digits}f} enforced by the "
                                        f"engine on the BROKER price")

    def _hard_close(self, legs: list, live: dict, f) -> Optional[str]:
        """Engine-side stops (backup to the broker SL): software mirror of the BE+/trailing level,
        per-leg hard distance, and the whole group's floating loss in % of equity."""
        cfg = self.cfg
        total = 0.0
        for ps in legs:
            p = live.get(ps.ticket) or {}
            total += float(p.get("profit", 0.0)) + float(p.get("swap", 0.0))
            hd = ps.hard_dist or (ps.entry_vu * cfg.sl_vol_units)
            if hd <= 0:
                return f"hard close: #{ps.ticket} has no known risk distance (unprotected)"
            d = 1 if ps.side == "BUY" else -1
            px = self._exec_px(ps, p, f)
            if px is None:
                continue
            if ps.vsl and (px - ps.vsl) * d <= 0:
                return f"{ps.sl_state} stop hit #{ps.ticket} at {px} (locked {(ps.vsl - ps.open_price) * d:+.2f} px)"
            adverse = (ps.open_price - px) * d
            if adverse >= hd:
                return (f"hard close: #{ps.ticket} adverse {adverse:.2f} >= {hd:.2f} px "
                        f"(software backup, broker SL {'on' if cfg.broker_sl else 'OFF'})")
        eq = float(self.account.get("equity", 0.0))
        loss_pct = cfg.scalp_group_loss_pct if legs and self._is_scalp(legs[0].batch_id) else cfg.batch_max_loss_pct
        limit = eq * loss_pct / 100.0
        if f is not None and f.spread and legs:
            info = self.sym_info.get(legs[0].symbol) or {}
            ts = float(info.get("trade_tick_size", 0))
            tv = float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
            if ts and tv:       # never let the % cap sit inside the round-trip spread cost (tiny accounts)
                limit = max(limit, 4.0 * f.spread / ts * tv * sum(p.volume for p in legs))
        if eq > 0 and total <= -limit and not any(ps.sl_state != "initial" for ps in legs):
            return f"hard close: group loss {total:+.2f} <= -{limit:.2f} ({loss_pct:g}% of equity {eq:.2f})"
        return None

    def _group_exit(self, key: str, legs: list, f, now_wall: float) -> Optional[str]:
        """Exit decision for a burst (all legs together) or a single position.
        Wrong way -> cut fast. Right way -> the BE+/trailing stop governs; short ATR target takes profit."""
        cfg = self.cfg
        vol = sum(p.volume for p in legs)
        agg = PositionState(ticket=legs[0].ticket, symbol=legs[0].symbol, side=legs[0].side, volume=vol,
                            open_price=sum(p.open_price * p.volume for p in legs) / vol,
                            open_time=min(p.open_time for p in legs),
                            entry_vu=max(p.entry_vu for p in legs) or 0.0,
                            peak_units=max(p.peak_units for p in legs), reason=legs[0].reason)
        eng = self.engines[legs[0].symbol]
        locked = any(p.sl_state != "initial" for p in legs)
        d = 1 if agg.side == "BUY" else -1
        livemap = {int(p["ticket"]): p for p in self.own_positions}
        pxs = [x for x in (self._exec_px(ps, livemap.get(ps.ticket, {}), f, now_wall) for ps in legs) if x]
        if pxs and f is not None and f.spread:
            exit_px = min(pxs) if d > 0 else max(pxs)        # broker executable price (not just the WS tick)
            pl = (exit_px - agg.open_price) * d
            held = now_wall - max(p.open_time for p in legs)
            atr, src = self._atr(agg.symbol)
            target = cfg.batch_tp_atr * atr if atr else cfg.batch_tp_units * agg.entry_vu
            if target > 0 and pl >= target:
                return f"short take-profit +{pl:.2f} px (target {target:.2f})"
            if not locked:
                peak = max(p.peak_pl for p in legs)
                fast = 0.6 if (self.bursts.get(key) or {}).get("fast_cut") else 1.0   # learning cold start
                if held >= cfg.wrong_way_seconds * fast and peak < -cfg.wrong_way_follow_spread * f.spread:
                    return f"wrong way: no follow-through in {held:.0f}s (best {peak:+.2f} px)"
                sl_d = min((p.hard_dist / cfg.hard_close_mult) for p in legs if p.hard_dist) if any(
                    p.hard_dist for p in legs) else 0.0
                if sl_d and pl <= -cfg.wrong_way_loss_frac * fast * sl_d and d * f.d_fast < 0:
                    return f"wrong way: {pl:+.2f} px against with momentum against"
            if now_wall - agg.open_time >= cfg.batch_max_hold_seconds:
                return f"time stop {cfg.batch_max_hold_seconds:.0f}s ({pl:+.2f} px)"
        reason = eng.exit_decision(agg, f, now_wall)
        for p in legs:
            p.peak_units = agg.peak_units
        if reason and locked and not (reason.startswith("reversal") or reason.startswith("time stop")):
            return None        # in profit with a locked stop: the trailing SL governs, not early exits
        if reason and reason.startswith("adverse fade") and cfg.adverse_fade_units >= 99:
            return None
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
            sig = self._entry_signal(s, eng, now_srv)
            status = {"signal": sig.action, "score": round(sig.score, 3), "reason": sig.reason,
                      "features": sig.features.brief(),
                      "spread_pts": round(sig.features.spread / float(info["point"]), 1) if info.get("point") else None,
                      "tick_age": round(now_wall - self.last_receipt[s], 1) if s in self.last_receipt else None}
            self.sym_status[s] = status
            tr = self.trends.get(s)
            status["trend"] = tr.brief() if tr else None
            if self._scalp_mode() and self._scalp_step(s, eng, info, sig, status, now_wall):
                continue
            if cfg.basket_enabled and self._basket_step(s, eng, info, sig, status, now_wall):
                continue
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
            sl_hist = [v for (ts_, v) in self.slippage.get(s, ()) if now_wall - ts_ <= cfg.slippage_window_s]
            if len(sl_hist) >= 3 and sum(sl_hist) / len(sl_hist) > cfg.max_avg_slippage_vu:
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
            is_imp = sig.reason.startswith("impulse")
            if mine:
                # pyramid: only same side, only when EVERY open order's SL is already locked in profit
                # (BE+/trailing), never against the trend, capped per burst by the equity tier
                if any(ps.side != sig.action for ps in mine):
                    self._skip(s, status, f"{sig.action} skipped: opposite burst open", now_wall, sig)
                    continue
                if any(ps.sl_state == "initial" for ps in mine):
                    self._skip(s, status, f"{sig.action} skipped: burst open, adding only after its SL is "
                                          f"locked in profit", now_wall, sig)
                    continue
                if rel == "counter":
                    self._skip(s, status, f"{sig.action} skipped: no pyramiding against the trend "
                                          f"({tr.label if tr else 'unknown'})", now_wall, sig)
                    continue
                batch_id = mine[0].batch_id or batch_id
                adds = int((self.bursts.get(batch_id) or {}).get("adds", 0))
                max_adds = self._tier_legs(acct.equity)
                if adds >= max_adds:
                    self._skip(s, status, f"{sig.action} skipped: equity {acct.equity:.2f} tier allows "
                                          f"{max_adds} add(s) per burst (done {adds})", now_wall, sig)
                    continue
                n, mode = 1, "pyramid"
            elif is_imp:
                if (rel == "counter" and abs(sig.score) < cfg.impulse_counter_min
                        and (not self._scalp_mode() or cfg.scalp_trend_filter)):
                    self._skip(s, status, f"{sig.action} skipped: counter-trend impulse ({tr.label}) needs "
                                          f"strength >= {cfg.impulse_counter_min:.2f}", now_wall, sig)
                    continue
                n = 1 if (rel == "counter" or abs(sig.score) < 0.85) else min(2, self._tier_legs(acct.equity))
                mode = "impulse" + ("-counter" if rel == "counter" else "")
            elif rel == "counter":
                if abs(sig.score) < cfg.countertrend_min_score:
                    self._skip(s, status, f"{sig.action} skipped: counter-trend ({tr.label}) needs "
                                          f"|score| >= {cfg.countertrend_min_score:.2f}", now_wall, sig)
                    continue
                n, risk_mult, mode = 1, 0.5, "counter-trend"
            else:
                n = min(self._batch_size(abs(sig.score)), self._tier_legs(acct.equity), 2)
            learn = None
            if cfg.learning_enabled:
                atr, _ = self._atr(s)
                bucket = self.learner.key("impulse" if is_imp else "momentum", rel, vol_regime(sig.features.spread, atr))
                learn = self.learner.decide(bucket)
                learn.bucket = bucket
                self.learn_view[s] = learn.why
                if not learn.allow and mode != "pyramid":      # adds to an SL-locked winner are never gated
                    self._skip(s, status, learn.why, now_wall, sig)
                    continue
            if self._scalp_mode():
                self._scalp_order(s, sig.action, "open", sig, info, now_wall, status, batch_id, learn)
                continue
            if cfg.basket_enabled:
                self._basket_order(s, sig.action, "open", sig, info, now_wall, status, batch_id, learn)
                continue
            self._enter_batch(s, sig, info, acct, open_pos, now_wall, status, n, risk_mult, batch_id, mode, learn)

    # -------------------------------------------------------------- SCALP mode
    def _scalp_mode(self) -> bool:
        return bool(self.cfg.scalp_enabled and not self.cfg.basket_enabled)

    def _is_scalp(self, key: str) -> bool:
        return bool((self.bursts.get(key) or {}).get("scalp"))

    def _scalp_signal(self, s: str, eng, now: float, sig):
        """Early-impulse entry only (the momentum-score fallback is too late for scalps)."""
        cfg = self.cfg
        f = sig.features
        hold = lambda why: Signal("HOLD", 0.0, why, f)      # noqa: E731
        if not f.ready:
            return hold(f.reason)
        if f.tick_rate < cfg.min_tick_rate:
            return hold(f"tick rate {f.tick_rate:.2f}/s < {cfg.min_tick_rate}")
        ok, why = eng.spread_ok(f)
        if not ok:
            return hold(why)
        atr, src = self._atr(s)
        ticks = [tk for tk in eng.buf.ticks if tk.t >= now - 70.0]
        imp = detect_early(ticks, now, atr, f.spread, cfg)
        imp.status["atr_src"] = src
        self.impulse[s] = imp
        if imp.dir:
            return Signal("BUY" if imp.dir > 0 else "SELL", imp.dir * imp.strength, imp.reason, f)
        return hold(imp.reason)

    def _scalp_step(self, s: str, eng, info: dict, sig, status: dict, now_wall: float) -> bool:
        """Adds to an open scalp group while the impulse continues (same lot, ~1 s apart, never into a loser).
        Returns True when a group exists on s (the normal entry path is then skipped)."""
        cfg = self.cfg
        mine = [ps for ps in self.pos_state.values() if ps.symbol == s]
        if not mine:
            return False
        if not self.running or self.risk.halted:
            return True
        side = mine[0].side
        bid_ = mine[0].batch_id
        b = self.bursts.get(bid_)
        if b is None or not b.get("scalp"):
            return True                       # adopted / old-style batch: legacy exit logic only
        if sig.action != side:
            status["reason"] = f"scalp {side} x{len(mine)}: no continuing {side} impulse ({sig.reason})"
            return True
        if len(mine) >= cfg.scalp_max_orders:
            self._skip(s, status, f"scalp group full: {len(mine)}/{cfg.scalp_max_orders}", now_wall, sig)
            return True
        if now_wall - float(b.get("last_add", 0.0)) < cfg.scalp_add_interval:
            return True
        f = eng.features(self._server_now(s, now_wall))
        xq = self._exec_quote(s, now_wall)
        mid = (xq[0] + xq[1]) / 2.0 if xq else f.mid
        d = 1 if side == "BUY" else -1
        adverse = (float(b.get("last_mid") or mid) - mid) * d
        if adverse > cfg.scalp_add_adverse_spread * f.spread:
            self._skip(s, status, "scalp add skipped: price against the last order (no averaging)", now_wall, sig)
            return True
        self._scalp_order(s, side, "add", sig, info, now_wall, status, batch_id=bid_)
        return True

    def _scalp_order(self, s: str, side: str, kind: str, sig, info: dict, now_wall: float, status: dict,
                     batch_id: str, learn=None) -> bool:
        """One small market order of a scalp group (kind open|add). Same lot for every order of the group (no
        martingale). Total used margin <= max_margin_use_pct of equity AND free margin stays >= the guard, using
        the broker's own margin numbers. Far emergency SL + quick small TP."""
        cfg = self.cfg
        eng = self.engines[s]
        f = eng.features(self._server_now(s, now_wall))
        d = 1 if side == "BUY" else -1
        point = float(info["point"])
        vmin, vmax, vstep = float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"])
        ts, tv = self._tv(s)
        atr, _ = self._atr(s)
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl_dist = max(stops, cfg.scalp_sl_atr * atr if atr else 2.0 * cfg.sl_vol_units * f.vol_unit, 3.0 * f.spread)
        tp_dist = max(stops, cfg.scalp_tp_atr * atr if atr else 0.0, cfg.scalp_tp_spread_mult * f.spread)
        price = f.ask if side == "BUY" else f.bid
        eq = float(self.account.get("equity", 0) or 0)
        b = self.bursts.get(batch_id)
        cap = eq * cfg.max_margin_use_pct / 100.0
        used = self._margin_used()
        free = float(self.account.get("margin_free", 0) or 0)
        n_live = sum(1 for ps in self.pos_state.values() if ps.batch_id == batch_id)

        def refuse(why: str) -> bool:
            if kind == "open":
                self.pacer.on_entry(s, now_wall)
            elif b is not None:
                b["last_add"] = now_wall
            self._skip(s, status, f"{sig.action} {kind} blocked: {why}", now_wall, sig)
            return False

        if b is not None and b.get("lot0") and kind != "open":
            lot0 = float(b["lot0"])
        else:        # many small orders: the lot is what lets scalp_max_orders orders fill the margin cap (compounds with equity)
            m_min = self.broker.calc_margin(side, s, vmin, price)
            if m_min is None:
                return refuse("margin calc unavailable")
            lot = cap / (cfg.scalp_max_orders * m_min / vmin) if m_min > 0 else vmin
            lot0 = normalize_volume(min(max(lot, vmin), cfg.max_lot_per_order, vmax), vmin, vmax, vstep) or vmin
        margin = self.broker.calc_margin(side, s, lot0, price)
        if margin is None:
            return refuse("margin calc unavailable")
        guard = eq * cfg.scalp_min_free_margin_pct / 100.0
        if used + margin > cap + 1e-9:
            return refuse(f"margin use cap: {used:.2f} + {margin:.2f} > {cfg.max_margin_use_pct:g}% of equity ({cap:.2f})")
        if free - margin < guard:
            return refuse(f"free margin guard: {free:.2f} - {margin:.2f} < {guard:.2f} ({cfg.scalp_min_free_margin_pct:g}% of equity)")
        sl = price - d * sl_dist if cfg.broker_sl else 0.0
        tp = price + d * tp_dist
        comment = f"{cfg.comment_prefix}-{batch_id}-{uuid.uuid4().hex[:4]}"[:31]
        self.pacer.inflight.add(s)
        try:
            r = self.broker.market_order(s, side, lot0, sl, tp, self._deviation(s, f.spread, f.vol_unit), cfg.magic, comment)
        finally:
            self.pacer.inflight.discard(s)
        if not r.get("ok"):
            self.add_log("WARNING", f"SCALP {kind.upper()} {side} {lot0} {s} rejected: {r.get('msg')}")
            if "not found" in str(r.get("msg")):
                self.unverified_entry = r["msg"]
                self.running = False
                self._save_state()
            return refuse(f"rejected: {r.get('msg')}")
        fill = float(r["price"])
        slip = (fill - float(r["req_price"])) * d / point
        self.slippage.setdefault(s, deque(maxlen=10)).append((now_wall, max(0.0, slip * point / max(f.vol_unit, point))))
        ticket = int(r["ticket"])
        self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=side, volume=float(r["volume"]),
                                               open_price=fill, open_time=now_wall, sl=sl, tp=tp, entry_vu=f.vol_unit,
                                               reason=f"scalp-{kind}: {sig.reason}", batch_id=batch_id,
                                               hard_dist=sl_dist * cfg.hard_close_mult, peak_pl=-abs(f.spread))
        cut = max(cfg.scalp_cut_atr * atr if atr else 0.0, 2.0 * f.spread)
        risk_money = float(r["volume"]) * (cut / ts * tv if ts and tv else 0.0)
        if b is None:
            b = self.bursts.setdefault(batch_id, {"symbol": s, "side": side, "opened": now_wall, "mode": "scalp",
                                                  "realized": 0.0, "legs": 0, "scalp": True, "lot0": lot0, "adds": 0,
                                                  "peak_px": fill - d * f.spread / 2.0, "peak_v": 0.0, "best_pl": 0.0})
        b["legs"] = int(b.get("legs", 0)) + 1
        b["lot"] = float(r["volume"])
        b["risk"] = float(b.get("risk", 0.0)) + risk_money
        b["last_mid"] = fill - d * f.spread / 2.0
        b["last_add"] = now_wall
        if kind != "open":
            b["adds"] = int(b.get("adds", 0)) + 1
        else:
            self.pacer.on_entry(s, now_wall)
            if learn is not None:
                tr_ = self.trends.get(s)
                imp_ = self.impulse.get(s)
                atr_, _ = self._atr(s)
                b.update(bucket=getattr(learn, "bucket", ""), type="impulse", rel=status.get("trend_rel"),
                         regime=vol_regime(f.spread, atr_), hour=time.gmtime(now_wall).tm_hour, learn_state=learn.state,
                         comps={"score": round(sig.score, 3), "impulse": round(imp_.strength, 3) if imp_ and imp_.dir else 0.0,
                                "pressure": round(f.pressure, 3), "vel": round(f.d_fast, 3), "breakout": f.breakout,
                                "trend": round(tr_.dir * tr_.strength, 2) if tr_ and tr_.known else 0.0,
                                "spread_atr": round(f.spread / atr_, 3) if atr_ else None})
        dg = int(info["digits"])
        self.add_log("INFO", f"SCALP {kind.upper()} {side} {r['volume']} {s} @ {fill} #{ticket} group {batch_id} "
                             f"order {n_live + 1}/{cfg.scalp_max_orders} [{sig.reason}] TP {tp:.{dg}f} "
                             f"far SL {format(sl, '.%df' % dg) if sl else 'OFF'} margin used {used + margin:.2f}/{cap:.2f} "
                             f"({cfg.max_margin_use_pct:g}% of equity) free {free - margin:.2f} "
                             f"slip {slip:+.0f}pt {r.get('latency', 0)*1000:.0f}ms")
        self._decision(s, side, sig.score, f"SCALP {kind.upper()} order {n_live + 1} (lot {r['volume']})")
        status["reason"] = f"scalp {batch_id}: {n_live + 1} order(s) {side}"
        self._refresh_account()
        self._save_state()
        return True

    def _scalp_exit(self, key: str, legs: list, live: dict, f, now_wall: float) -> Optional[str]:
        """Exit the whole group the moment the bar's force reverses: tick velocity flips / decays with opposing
        pressure / a wick forms against. Winners are taken short, losers cut fast when the reversal says wrong."""
        cfg = self.cfg
        b = self.bursts.get(key)
        eng = self.engines.get(legs[0].symbol)
        if b is None or eng is None or f is None or not f.spread:
            return None
        sym = legs[0].symbol
        d = 1 if legs[0].side == "BUY" else -1
        vol = sum(p.volume for p in legs)
        avg = sum(p.open_price * p.volume for p in legs) / vol
        pxs = [x for x in (self._exec_px(ps, live.get(ps.ticket, {}), f, now_wall) for ps in legs) if x]
        if not pxs:
            return None
        exit_px = min(pxs) if d > 0 else max(pxs)            # broker executable price
        pl = (exit_px - avg) * d
        now_srv = self._server_now(sym, now_wall)
        atr, _ = self._atr(sym)
        spread = f.spread
        pk = b.get("peak_px")
        b["peak_px"] = f.mid if pk is None else (max(pk, f.mid) if d > 0 else min(pk, f.mid))
        ticks = [tk for tk in eng.buf.ticks if tk.t >= now_srv - 10.0]
        rev = detect_reversal(ticks, now_srv, d, atr, spread, b["peak_px"], float(b.get("peak_v", 0.0)), cfg)
        b["peak_v"] = max(float(b.get("peak_v", 0.0)), rev["v"])
        b["best_pl"] = max(float(b.get("best_pl", 0.0)), pl)
        self.scalp_rev[sym] = {k: (round(v, 4) if isinstance(v, float) else v) for k, v in rev.items()}
        tp_dist = max(cfg.scalp_tp_atr * atr if atr else 0.0, cfg.scalp_tp_spread_mult * spread)
        min_profit = cfg.scalp_min_profit_spread * spread
        why = [k for k in ("vel_flip", "decay", "press", "wick") if rev[k]]
        if tp_dist > 0 and pl >= tp_dist:
            return f"quick take-profit +{pl:.2f} px (target {tp_dist:.2f})"
        if pl >= min_profit:
            if rev["vel_flip"] or rev["wick"] or (rev["decay"] and rev["press"]):
                return f"reversal take-profit +{pl:.2f} px: {'+'.join(why)}"
        else:
            cut = cfg.scalp_cut_spread * spread
            if pl <= -cut and (rev["vel_flip"] or rev["press"]):
                return f"reversal cut {pl:+.2f} px: {'+'.join(why)} - trade is wrong"
            if atr and pl <= -cfg.scalp_cut_atr * atr:
                return f"hard cut {pl:+.2f} px (>= {cfg.scalp_cut_atr:g} ATR against)"
        first = min(p.open_time for p in legs)
        if now_wall - first >= cfg.scalp_wrong_way_s and b["best_pl"] < min_profit:
            return f"wrong way: no profit in {now_wall - first:.0f}s (best {b['best_pl']:+.2f} px, now {pl:+.2f})"
        if now_wall - first >= cfg.scalp_max_hold_s:
            return f"scalp time stop {cfg.scalp_max_hold_s:.0f}s ({pl:+.2f} px)"
        return None

    def _scalp_view(self) -> dict:
        c = self.cfg
        eq = float(self.account.get("equity", 0) or 0) if self.conn_state == "CONNECTED" else 0.0
        live = {int(p["ticket"]): p for p in self.own_positions} if self.conn_state == "CONNECTED" else {}
        items = {}
        for k, b in self.bursts.items():
            if not b.get("scalp"):
                continue
            legs = [ps for ps in self.pos_state.values() if ps.batch_id == k and ps.ticket in live]
            if legs:
                net = sum(float(live[ps.ticket].get("profit", 0)) + float(live[ps.ticket].get("swap", 0)) for ps in legs)
                items[k] = {"symbol": b.get("symbol"), "side": b.get("side"), "orders": len(legs),
                            "volume": round(sum(ps.volume for ps in legs), 4), "net": round(net, 2),
                            "adds": int(b.get("adds", 0)), "rev": self.scalp_rev.get(b.get("symbol"))}
        used = self._margin_used() if eq else 0.0
        return {"enabled": self._scalp_mode(), "max_orders": c.scalp_max_orders, "add_interval": c.scalp_add_interval,
                "max_margin_use_pct": c.max_margin_use_pct, "margin_used": round(used, 2),
                "margin_use_pct": round(used / eq * 100, 1) if eq else None,
                "max_bar_atr": c.scalp_max_bar_atr, "max_swing_atr": c.scalp_max_swing_atr,
                "tp_atr": c.scalp_tp_atr, "group_loss_pct": c.scalp_group_loss_pct, "items": items}

    # ------------------------------------------------------------- BASKET mode
    def _tv(self, s: str) -> tuple[float, float]:
        info = self.sym_info.get(s) or {}
        return (float(info.get("trade_tick_size", 0) or 0),
                float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0) or 0))

    def _is_basket(self, key: str) -> bool:
        return bool((self.bursts.get(key) or {}).get("basket"))

    def _margin_used(self) -> float:
        eq = float(self.account.get("equity", 0) or 0)
        m = float(self.account.get("margin", 0) or 0)
        return m if m > 0 else max(0.0, eq - float(self.account.get("margin_free", 0) or 0))

    def _basket_metrics(self, key: str, legs: list, live: dict) -> dict:
        """Net floating P/L of the whole basket, its profit target, trail floor and loss-stop level."""
        cfg = self.cfg
        b = self.bursts.get(key) or {}
        sym = legs[0].symbol
        net = 0.0
        for ps in legs:
            p = live.get(ps.ticket) or {}
            net += float(p.get("profit", 0.0)) + float(p.get("swap", 0.0)) + float(p.get("commission", 0.0) or 0.0)
        eq0 = float(b.get("eq0") or self.account.get("equity", 0) or 0)
        vol = sum(ps.volume for ps in legs)
        ts, tv = self._tv(sym)
        eng = self.engines.get(sym)
        spread = float(eng.last.spread) if eng is not None and getattr(eng, "last", None) is not None else 0.0
        spread_cost = spread / ts * tv * vol if ts and tv else 0.0
        target = max(eq0 * cfg.basket_target_pct / 100.0, cfg.basket_target_spread_mult * spread_cost)
        loss_limit = max(eq0 * cfg.basket_loss_pct / 100.0, 4.0 * spread_cost)
        peak = float(b.get("peak", 0.0))
        floor = max(peak * (1.0 - cfg.basket_trail_frac), 0.5 * target) if b.get("armed") else None
        return {"net": round(net, 2), "eq0": round(eq0, 2), "target": round(target, 2), "loss_limit": round(loss_limit, 2),
                "peak": round(peak, 2), "armed": bool(b.get("armed")), "trail_floor": round(floor, 2) if floor is not None else None,
                "orders": len(legs), "volume": round(vol, 4), "spread_cost": round(spread_cost, 4)}

    def _basket_exit(self, key: str, legs: list, live: dict) -> Optional[str]:
        """Close the whole basket together: profit target with a trailed profit, or the moderate basket loss stop."""
        b = self.bursts.get(key)
        if b is None:
            return None
        m = self._basket_metrics(key, legs, live)
        net = m["net"]
        if net <= -m["loss_limit"]:
            return (f"basket loss stop: {net:+.2f} <= -{m['loss_limit']:.2f} "
                    f"({self.cfg.basket_loss_pct:g}% of equity at basket start {m['eq0']:.2f})")
        if net >= m["target"]:
            b["armed"] = True
        if b.get("armed"):
            b["peak"] = max(float(b.get("peak", 0.0)), net)
            floor = max(b["peak"] * (1.0 - self.cfg.basket_trail_frac), 0.5 * m["target"])
            if net <= floor:
                return (f"basket profit trail: {net:+.2f} fell back to <= {floor:.2f} "
                        f"(peak {b['peak']:+.2f}, target {m['target']:.2f})")
        return None

    def _basket_step(self, s: str, eng, info: dict, sig, status: dict, now_wall: float) -> bool:
        """Manage adds of an existing basket on s. Returns True when a basket exists (normal entry path skipped)."""
        cfg = self.cfg
        mine = [ps for ps in self.pos_state.values() if ps.symbol == s]
        if not mine:
            return False
        if not self.running or self.risk.halted:
            return True
        side = mine[0].side
        if any(ps.side != side for ps in mine):
            self._skip(s, status, f"{sig.action} skipped: opposite order open", now_wall, sig)
            return True
        bid_ = mine[0].batch_id or uuid.uuid4().hex[:6]
        b = self.bursts.get(bid_)
        if b is None or not b.get("basket"):
            return True                      # an old-style batch (adopted): handled by the legacy exit logic
        if len(mine) >= cfg.basket_max_orders:
            self._skip(s, status, f"basket full: {len(mine)}/{cfg.basket_max_orders} orders", now_wall, sig)
            return True
        wait = float(b.get("last_add", 0.0)) + cfg.basket_add_cooldown - now_wall
        if wait > 0:
            self._skip(s, status, f"basket add cooldown {wait:.0f}s", now_wall, sig)
            return True
        atr, _ = self._atr(s)
        f = eng.features(self._server_now(s, now_wall))
        xq = self._exec_quote(s, now_wall)
        mid = (xq[0] + xq[1]) / 2.0 if xq else f.mid
        d = 1 if side == "BUY" else -1
        last_mid = float(b.get("last_mid") or mine[-1].open_price)
        adverse = (last_mid - mid) * d                      # > 0: price is against the basket
        spacing = lambda mult: max(mult * atr, 3.0 * f.spread)   # noqa: E731
        if not atr:
            self._skip(s, status, "basket: no ATR yet", now_wall, sig)
            return True
        kind = None
        if adverse >= spacing(cfg.basket_avg_atr):
            kind = "avg"
        elif -adverse >= spacing(cfg.basket_pyr_atr) and sig.action == side:
            tr = self.trends.get(s)
            if relation(tr, side) == "counter":
                self._skip(s, status, f"pyramid skipped: against the trend", now_wall, sig)
                return True
            kind = "pyramid"
        if kind is None:
            status["reason"] = (f"basket {side} {len(mine)} order(s): waiting (adverse {adverse:+.2f}, "
                                f"avg at {spacing(cfg.basket_avg_atr):.2f}, pyramid at -{spacing(cfg.basket_pyr_atr):.2f})")
            return True
        self._basket_order(s, side, kind, sig, info, now_wall, status, batch_id=bid_)
        return True

    def _basket_order(self, s: str, side: str, kind: str, sig, info: dict, now_wall: float, status: dict,
                      batch_id: str, learn=None) -> bool:
        """One market order of a basket (kind open|avg|pyramid): compounding base lot, far broker SL, no TP.
        Refused when the total used margin would exceed max_margin_use_pct of equity (or margin level < floor)."""
        cfg = self.cfg
        eng = self.engines[s]
        f = eng.features(self._server_now(s, now_wall))
        d = 1 if side == "BUY" else -1
        point = float(info["point"])
        vmin, vmax, vstep = float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"])
        ts, tv = self._tv(s)
        atr, _ = self._atr(s)
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl_dist = max(stops, cfg.basket_sl_atr * atr if atr else 2.0 * cfg.sl_vol_units * f.vol_unit, 3.0 * f.spread)
        price = f.ask if side == "BUY" else f.bid
        eq = float(self.account.get("equity", 0) or 0)
        bal = float(self.account.get("balance", 0) or 0) or eq
        b = self.bursts.get(batch_id)
        cap = eq * cfg.max_margin_use_pct / 100.0
        used = self._margin_used()
        ml = float(self.account.get("margin_level", 0) or 0)
        n_live = sum(1 for ps in self.pos_state.values() if ps.batch_id == batch_id)

        def refuse(why: str) -> bool:
            if kind == "open":
                self.pacer.on_entry(s, now_wall)
            self._skip(s, status, f"{sig.action} {kind} blocked: {why}", now_wall, sig)
            return False

        if cfg.basket_min_margin_level and 0 < ml < cfg.basket_min_margin_level:
            return refuse(f"margin level {ml:.0f}% < {cfg.basket_min_margin_level:.0f}%")
        if b is not None and b.get("lot0") and kind != "open":
            lot0 = float(b["lot0"])
        else:                                      # compounding: lot follows the balance, planned basket fits the margin cap
            lot = (bal * cfg.basket_risk_pct / 100.0) / (sl_dist / ts * tv) if ts and tv and sl_dist else vmin
            m_min = self.broker.calc_margin(side, s, vmin, price)
            if m_min is None:
                return refuse("margin calc unavailable")
            if m_min > 0:
                lot = min(lot, cap / (cfg.basket_max_orders * m_min / vmin))
            lot0 = normalize_volume(min(max(lot, vmin), cfg.max_lot_per_order, vmax), vmin, vmax, vstep) or vmin
        vol = lot0 * (cfg.basket_avg_mult if kind == "avg" else 1.0)
        vol = normalize_volume(min(max(vol, vmin), cfg.max_lot_per_order, vmax), vmin, vmax, vstep) or vmin
        margin = self.broker.calc_margin(side, s, vol, price)
        if margin is None:
            return refuse("margin calc unavailable")
        if used + margin > cap and vol > vmin:      # shrink to what the cap allows (never below the minimum lot)
            room = max(0.0, cap - used)
            vol = normalize_volume(max(vmin, vol * room / margin), vmin, vmax, vstep) or vmin
            margin = self.broker.calc_margin(side, s, vol, price)
            if margin is None:
                return refuse("margin calc unavailable")
        if used + margin > cap:
            return refuse(f"margin use cap: {used:.2f} + {margin:.2f} > {cfg.max_margin_use_pct:g}% of equity ({cap:.2f})")
        sl = price - d * sl_dist if cfg.broker_sl else 0.0
        comment = f"{cfg.comment_prefix}-{batch_id}-{uuid.uuid4().hex[:4]}"[:31]
        self.pacer.inflight.add(s)
        try:
            r = self.broker.market_order(s, side, vol, sl, 0.0, self._deviation(s, f.spread, f.vol_unit), cfg.magic, comment)
        finally:
            self.pacer.inflight.discard(s)
        if not r.get("ok"):
            self.add_log("WARNING", f"BASKET {kind.upper()} {side} {vol} {s} rejected: {r.get('msg')}")
            if "not found" in str(r.get("msg")):
                self.unverified_entry = r["msg"]
                self.running = False
                self._save_state()
            if kind == "open":
                self.pacer.on_entry(s, now_wall)
            self._skip(s, status, f"{sig.action} {kind} blocked: rejected: {r.get('msg')}", now_wall, sig)
            if b is not None:
                b["last_add"] = now_wall
            return False
        fill = float(r["price"])
        slip = (fill - float(r["req_price"])) * d / point
        self.slippage.setdefault(s, deque(maxlen=10)).append((now_wall, max(0.0, slip * point / max(f.vol_unit, point))))
        ticket = int(r["ticket"])
        self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=side, volume=float(r["volume"]),
                                               open_price=fill, open_time=now_wall, sl=sl, tp=0.0, entry_vu=f.vol_unit,
                                               reason=f"basket-{kind}: {sig.reason}", batch_id=batch_id,
                                               hard_dist=sl_dist * cfg.hard_close_mult, peak_pl=-abs(f.spread))
        risk_money = float(r["volume"]) / 1.0 * (sl_dist / ts * tv if ts and tv else 0.0)
        if b is None:
            b = self.bursts.setdefault(batch_id, {"symbol": s, "side": side, "opened": now_wall, "mode": "basket",
                                                  "realized": 0.0, "legs": 0, "basket": True, "eq0": eq, "peak": 0.0,
                                                  "armed": False, "adds": 0, "lot0": lot0})
        b["legs"] = int(b.get("legs", 0)) + 1
        b["lot"] = float(r["volume"])
        b["risk"] = float(b.get("risk", 0.0)) + risk_money
        b["last_mid"] = fill - d * f.spread / 2.0
        b["last_add"] = now_wall
        if kind != "open":
            b["adds"] = int(b.get("adds", 0)) + 1
        else:
            self.pacer.on_entry(s, now_wall)
            if learn is not None:
                tr_ = self.trends.get(s)
                imp_ = self.impulse.get(s)
                atr_, _ = self._atr(s)
                b.update(bucket=getattr(learn, "bucket", ""), type=("impulse" if sig.reason.startswith("impulse") else "momentum"),
                         rel=status.get("trend_rel"), regime=vol_regime(f.spread, atr_), hour=time.gmtime(now_wall).tm_hour,
                         learn_state=learn.state,
                         comps={"score": round(sig.score, 3), "impulse": round(imp_.strength, 3) if imp_ and imp_.dir else 0.0,
                                "pressure": round(f.pressure, 3), "vel": round(f.d_fast, 3), "breakout": f.breakout,
                                "trend": round(tr_.dir * tr_.strength, 2) if tr_ and tr_.known else 0.0,
                                "spread_atr": round(f.spread / atr_, 3) if atr_ else None})
        dg = int(info["digits"])
        self.add_log("INFO", f"BASKET {kind.upper()} {side} {r['volume']} {s} @ {fill} #{ticket} basket {batch_id} "
                             f"order {n_live + 1}/{cfg.basket_max_orders} score {sig.score:+.2f} [{sig.reason}] "
                             f"broker SL {format(sl, '.%df' % dg) if sl else 'OFF'} (far, {sl_dist:.{dg}f}) no TP; "
                             f"margin used {used + margin:.2f}/{cap:.2f} ({cfg.max_margin_use_pct:g}% of equity) "
                             f"slip {slip:+.0f}pt {r.get('latency', 0)*1000:.0f}ms")
        self._decision(s, side, sig.score, f"BASKET {kind.upper()} order {n_live + 1} (lot {r['volume']}, margin {used + margin:.2f}/{cap:.2f})")
        status["reason"] = f"basket {batch_id}: {n_live + 1} order(s) {side}"
        self._refresh_account()
        self._save_state()
        return True

    def _entry_signal(self, s: str, eng, now: float):
        """Impulse detector first (fastest), the momentum score as fallback. Same safety gates."""
        cfg = self.cfg
        sig = eng.signal(now)
        f = sig.features
        if self._scalp_mode():
            return self._scalp_signal(s, eng, now, sig)
        if not cfg.impulse_enabled or not f.ready:
            return sig
        if f.tick_rate < cfg.min_tick_rate or not eng.spread_ok(f)[0]:
            return sig
        atr, src = self._atr(s)
        ticks = [tk for tk in eng.buf.ticks if tk.t >= now - 70.0]
        imp = detect_impulse(ticks, now, atr, f.spread, cfg)
        imp.status["atr_src"] = src
        self.impulse[s] = imp
        if imp.dir:
            return Signal("BUY" if imp.dir > 0 else "SELL", imp.dir * imp.strength, imp.reason, f)
        return sig

    def _tier_legs(self, equity: float) -> int:
        """Max orders per batch for this equity (tiny accounts: 1; grows automatically with equity)."""
        for part in str(self.cfg.batch_tiers).split(","):
            try:
                lim, legs = part.split(":")
                if equity < float(lim):
                    return max(1, int(legs))
            except ValueError:
                continue
        return max(1, self.cfg.batch_max)

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
                 ("trend aligned", "batch open (no add)"), ("tier allows", "equity tier cap"), ("locked in profit", "waiting SL lock to add"),
                 ("opposite burst", "opposite burst open"), ("no pyramiding", "counter-trend filter"), ("min lot", "sizing refused"),
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
        cat = self._category(reason)
        if now_wall - self.skip_last.get((s, cat), 0.0) < 1.0:   # sample each reason at most 1/s per symbol
            return
        self.skip_last[(s, cat)] = now_wall
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
            rngs = [b["high"] - b["low"] for b in m1[-15:-1] if b.get("high") is not None and b["high"] > b["low"]]
            if rngs:
                self.atr_m1[s] = sum(rngs) / len(rngs)
                if s in self.engines:
                    self.engines[s].seed_vu = 0.15 * self.atr_m1[s]
            tr.detail["m1_bars"], tr.detail["m5_bars"] = float(len(m1)), float(len(m5))
            self.trends[s] = tr
            if prev is None or prev.label.split(" ")[0] != tr.label.split(" ")[0]:
                self.add_log("INFO", f"TREND {s}: {tr.label} (M1 {len(m1)} bars, M5 {len(m5)} bars)")

    def _pos_risk(self, p: dict) -> float:
        info = self.sym_info.get(p["symbol"]) or {}
        sl = float(p.get("sl", 0.0))
        ps = self.pos_state.get(int(p["ticket"]))
        d = 1 if self._side(p) == "BUY" else -1
        if sl and (sl - float(p["price_open"])) * d >= 0:
            return 0.0                                  # SL locked at breakeven+ : no open risk
        dist = ps.hard_dist if ps and ps.hard_dist else (abs(float(p["price_open"]) - sl) if sl else 0.0)
        ts, tv = float(info.get("trade_tick_size", 0)), float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
        if not dist or not ts or not tv:
            return float(self.account.get("equity", 0)) * self.cfg.risk_per_trade_pct / 100.0
        return max(0.0, dist / ts * tv * float(p["volume"]))

    def _learn_from(self, ps, b: dict, now: float) -> None:
        """Feed one fully closed burst (real broker P/L) to the online learner."""
        if not b.get("bucket"):
            return
        risk = float(b.get("risk", 0.0))
        entry = {"symbol": ps.symbol, "side": b.get("side"), "bucket": b["bucket"], "type": b.get("type"),
                 "rel": b.get("rel"), "regime": b.get("regime"), "hour": b.get("hour"), "comps": b.get("comps", {}),
                 "hold_s": round(now - float(b.get("opened", now)), 1), "exit": b.get("exit_reason", "broker SL/TP"),
                 "pl": round(float(b.get("realized", 0.0)), 2), "legs": b.get("legs"),
                 "r": (float(b.get("realized", 0.0)) / risk) if risk > 0 else 0.0}
        try:
            self.learner.record(entry)
        except Exception as e:  # noqa: BLE001  learning must never break trading
            log.warning("learner record failed: %s", e)

    def _enter_batch(self, s, sig, info, acct, open_pos, now_wall, status, n, risk_mult, batch_id, mode,
                     learn=None) -> None:
        """Open up to n same-direction market orders sharing batch_id. Each leg carries its own SL;
        each leg is re-checked by RiskGuard (positions, lots, notional, open risk, margin) and the batch
        is cut short at the first refusal. Never martingale: every leg has the same risk-based size."""
        cfg = self.cfg
        f = sig.features
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        atr, src = self._atr(s)
        sl_dist = max(stops, cfg.sl_spread_mult * f.spread, cfg.sl_atr_frac * atr if atr else
                      cfg.sl_vol_units * f.vol_unit)         # tight protective stop: spread + M1 micro-ATR
        tp_dist = max(cfg.tp_vol_units * f.vol_unit, 2 * sl_dist)
        tv = float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
        size = size_position(acct.equity * risk_mult, cfg, sl_dist, float(info.get("trade_tick_size", 0)), tv,
                             float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"]))
        if size.volume <= 0:
            self._skip(s, status, f"{sig.action} skipped: {size.note}", now_wall, sig)
            return
        vmin, vmax, vstep = float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"])
        if learn is not None and learn.min_lot and size.volume > vmin:      # cold/unproven bucket: smallest lot
            size = type(size)(vmin, size.risk_money * vmin / size.volume, size.note + "; learning: min lot")
        elif learn is not None and learn.mult > 1.0 and size.volume > 0:                        # proven positive expectancy only
            v = normalize_volume(min(size.volume * learn.mult, cfg.max_lot_per_order), vmin, vmax, vstep)
            if v > size.volume:
                size = type(size)(v, size.risk_money * v / size.volume, size.note + f"; learning x{learn.mult:.2f}")
        cap = self.no_up_after_loss.get(s)
        if cap and size.volume > cap:                 # no martingale: never size up after a losing burst
            size = type(size)(cap, size.risk_money * cap / size.volume, size.note + f"; capped at {cap:g} after loss")
        d = 1 if sig.action == "BUY" else -1
        add_ok = abs(sig.score) >= cfg.add_threshold or mode == "pyramid"
        placed, why_stop = [], ""
        for i in range(max(1, n)):
            f = self.engines[s].features(self._server_now(s, now_wall)) if i else f
            price = f.ask if sig.action == "BUY" else f.bid
            margin = self.broker.calc_margin(sig.action, s, size.volume, price)
            if margin is None:
                why_stop = "margin calc unavailable"
                break
            notional = self._notional(s, size.volume, price)
            ok, why = self.risk.check_entry(s, sig.action, open_pos, acct, size.volume, size.risk_money, margin,
                                            add_ok, notional, batch_leg=bool(placed),
                                            floor_mode=size.note.startswith("min lot floor"))
            if not ok:
                why_stop = why
                break
            sl_virtual = price - d * sl_dist            # engine hard-close level (also the sizing risk distance)
            if cfg.broker_sl:
                sl = sl_virtual
            elif cfg.emergency_sl:
                sl = price - d * max(cfg.emergency_sl_units * f.vol_unit, 3 * sl_dist)   # far disaster stop only
            else:
                sl = 0.0                                 # NO broker-side SL (default)
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
            self.slippage.setdefault(s, deque(maxlen=10)).append((now_wall, max(0.0, slip * point / max(f.vol_unit, point))))
            ticket = int(r["ticket"])
            self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=sig.action, volume=float(r["volume"]),
                                                   open_price=fill, open_time=now_wall, sl=sl, tp=tp,
                                                   entry_vu=f.vol_unit, reason=sig.reason, batch_id=batch_id,
                                                   hard_dist=sl_dist * cfg.hard_close_mult,
                                                   peak_pl=-abs(f.spread))
            if not placed:
                self.pacer.on_entry(s, now_wall)      # one batch = one entry for the rate limiter
            placed.append(ticket)
            self.add_log("INFO", f"ENTRY {sig.action} {r['volume']} {s} @ {fill} #{ticket} batch {batch_id} "
                                 f"leg {len(placed)}/{n} ({mode}) score {sig.score:+.2f} [{sig.reason}] "
                                 f"SL {format(sl, '.%df' % int(info['digits'])) if sl else 'OFF (broker)'} "
                                 f"(stop dist {sl_dist:.{int(info['digits'])}f}, software backup "
                                 f"{sl_dist * cfg.hard_close_mult:.{int(info['digits'])}f}) "
                                 f"slip {slip:+.0f}pt risk {size.risk_money:.2f} "
                                 f"{r.get('latency', 0)*1000:.0f}ms")
            open_pos.append(OpenPos(s, sig.action, float(r["volume"]), 0.0, size.risk_money, notional))
            self._refresh_account()
            acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                               float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
        if placed:
            b = self.bursts.setdefault(batch_id, {"symbol": s, "side": sig.action, "opened": now_wall,
                                                  "mode": mode, "realized": 0.0, "legs": 0})
            b["legs"] = int(b.get("legs", 0)) + len(placed)
            b["lot"] = size.volume
            b["risk"] = float(b.get("risk", 0.0)) + size.risk_money * len(placed)
            if learn is not None:
                tr_ = self.trends.get(s)
                imp_ = self.impulse.get(s)
                atr_, _ = self._atr(s)
                b.update(bucket=getattr(learn, "bucket", ""), type=mode.split("-")[0], rel=status.get("trend_rel"),
                         regime=vol_regime(f.spread, atr_), hour=time.gmtime(now_wall).tm_hour,
                         fast_cut=learn.fast_cut, learn_state=learn.state,
                         comps={"score": round(sig.score, 3), "impulse": round(imp_.strength, 3) if imp_ and imp_.dir else 0.0,
                                "pressure": round(f.pressure, 3), "vel": round(f.d_fast, 3), "breakout": f.breakout,
                                "trend": round(tr_.dir * tr_.strength, 2) if tr_ and tr_.known else 0.0,
                                "spread_atr": round(f.spread / atr_, 3) if atr_ else None})
            if mode == "pyramid":
                b["adds"] = int(b.get("adds", 0)) + len(placed)
            tr = self.trends.get(s)
            self._decision(s, sig.action, sig.score,
                           f"OPENED {len(placed)}/{n} ({mode}, size {size.volume:g}, {size.note})"
                           + (f"; stopped: {why_stop}" if why_stop else ""))
            status["reason"] = f"batch {batch_id}: {len(placed)} order(s) {sig.action} (trend {tr.label if tr else 'unknown'})"
            self._save_state()
        else:
            self.pacer.on_entry(s, now_wall)          # failed attempt: retry only after the normal cooldown
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
            imp = self.impulse.get(s)
            st["impulse"] = {"reason": imp.reason, **imp.status} if imp else None
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
                "max_positions_per_symbol": c.max_positions_per_symbol, "sl_vol_units": c.sl_vol_units,
                "broker_sl": "ON" if c.broker_sl else ("EMERGENCY ONLY" if c.emergency_sl else "OFF"),
                "batch_max_loss_pct": c.batch_max_loss_pct, "max_total_notional_x": c.max_total_notional_x,
                "tier_legs_now": self._tier_legs(float(self.account.get("equity", 0) or 0)),
                "sl_rule": f"SL max({c.sl_spread_mult:g}x spread, {c.sl_atr_frac:g}x M1 ATR) -> BE+ "
                           f"{c.be_lock_spread_mult:g}x spread -> trail max({c.trail_atr_frac:g}x ATR, "
                           f"{c.trail_spread_mult:g}x spread); wrong-way cut {c.wrong_way_seconds:g}s",
                "batch_tiers": c.batch_tiers}

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
                              "reason": ps.reason if ps else "", "batch": ps.batch_id if ps else "",
                              "sl_state": ps.sl_state if ps else ("broker" if p.get("sl") else "none")})
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
                       "baskets": self._basket_view(),
                       "scalp": self._scalp_view(),
                       "decisions": list(self.decisions)[-15:],
                       "learning": dict(self.learner.summary(), last=self.learn_view),
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

    def _basket_view(self) -> dict:
        c = self.cfg
        eq = float(self.account.get("equity", 0) or 0) if self.conn_state == "CONNECTED" else 0.0
        live = {int(p["ticket"]): p for p in self.own_positions} if self.conn_state == "CONNECTED" else {}
        items = {}
        for k, b in self.bursts.items():
            if not b.get("basket"):
                continue
            legs = [ps for ps in self.pos_state.values() if ps.batch_id == k and ps.ticket in live]
            if legs:
                items[k] = dict(self._basket_metrics(k, legs, live), symbol=b.get("symbol"), side=b.get("side"),
                                adds=int(b.get("adds", 0)))
        used = self._margin_used() if eq else 0.0
        return {"enabled": c.basket_enabled, "max_margin_use_pct": c.max_margin_use_pct, "loss_pct": c.basket_loss_pct,
                "target_pct": c.basket_target_pct, "trail_frac": c.basket_trail_frac, "max_orders": c.basket_max_orders,
                "margin_used": round(used, 2), "margin_use_pct": round(used / eq * 100, 1) if eq else None,
                "margin_cap": round(eq * c.max_margin_use_pct / 100, 2) if eq else None, "items": items}

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
