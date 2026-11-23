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
from dataclasses import replace as dc_replace
from typing import Optional

from . import __version__
from .config import EngineConfig
from .engine import MomentumEngine, PositionState
from .risk import AccountView, EntryPacer, OpenPos, RiskGuard, size_position
from .trend import Trend, compute_trend
from .microburst import Micro, Strength, decide, exit_reason
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
        self.group_cooldown: dict[str, float] = {}
        self.atr_m1: dict[str, float] = {}          # M1 ATR from real broker bars
        self.m1_bars: dict[str, list] = {}          # completed broker M1 bars (candle context)
        self._mcache: dict[str, tuple] = {}          # per-loop micro analysis cache
        self.last_burst: dict[str, dict] = {}        # duplicate prevention: last traded burst per symbol
        self._recv_wall: dict[str, float] = {}       # real wall time the latest tick of a symbol reached the worker
        self._feed_ms: dict[str, float] = {}         # broker tick time -> worker receipt (when the API provides it)
        self.lat_entries: deque = deque(maxlen=200)  # measured entry latency per order (real clocks)
        self.lat_exits: deque = deque(maxlen=200)
        self.perf: list = []                         # closed batches with real broker P/L (after costs)
        self._closed_tickets: dict[int, float] = {}  # verified-closed tickets: never closed twice
        self._deals_cache: tuple = (0.0, [])
        self.exec_fails: dict[str, deque] = {}
        self.exec_pause: dict[str, float] = {}
        self.exec_halt = False                       # close failures => no new entries
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
        self.risk.reset_session(float(self.account.get("equity", 0) or 0))
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
        # a ticket verified closed may linger for <1 s in a cached broker list: never re-adopt / re-close it
        live = {int(p["ticket"]): p for p in self.own_positions if int(p["ticket"]) not in self._closed_tickets}
        for t, p in live.items():
            if t not in self.pos_state:
                meta = self.saved_pos_meta.get(str(t), {})
                eng = self.engines.get(p["symbol"])
                vu = meta.get("entry_vu") or (eng.last.vol_unit if eng and eng.last.vol_unit else 0.0)
                if not vu:
                    sl_dist = abs(p["price_open"] - p["sl"]) if p.get("sl") else 0.0
                    vu = sl_dist / 4.0 if sl_dist else 0.0
                self.pos_state[t] = PositionState(
                    ticket=t, symbol=p["symbol"], side=self._side(p), volume=float(p["volume"]),
                    open_price=float(p["price_open"]), open_time=float(meta.get("open_time", time.time())),
                    sl=float(p.get("sl", 0.0)), tp=float(p.get("tp", 0.0)), entry_vu=float(vu or 0.0),
                    peak_units=float(meta.get("peak_units", 0.0)), reason=meta.get("reason", "adopted"),
                    batch_id=str(meta.get("batch_id", "")),
                    hard_dist=float(meta.get("hard_dist") or (abs(float(p["price_open"]) - float(p["sl"])) * self.cfg.soft_stop_mult
                                                              if p.get("sl") else 0.0)),
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
                self.pacer.on_close(ps.symbol, profit if profit is not None else 0.0,
                                    getattr(self, "now_wall", None) or time.time())
                continue
            b["realized"] = float(b.get("realized", 0.0)) + (profit or 0.0)
            if not any(x.batch_id == ps.batch_id for x in self.pos_state.values()):
                # whole group is flat: one win/loss for the pacer, then the short post-group cooldown
                now = getattr(self, "now_wall", None) or time.time()
                self.pacer.on_close(ps.symbol, b["realized"], now)
                if b["realized"] < 0:
                    self.no_up_after_loss[ps.symbol] = float(b.get("lot", ps.volume))   # never size up after a loss
                else:
                    self.no_up_after_loss.pop(ps.symbol, None)
                self.group_cooldown[ps.symbol] = now + self.cfg.group_cooldown_seconds
                self._learn_from(ps, b, now)
                self.perf.append({"ts": now, "symbol": ps.symbol, "side": b["side"], "pl": round(b["realized"], 2),
                                  "legs": b.get("legs"), "hold_s": round(now - float(b.get("opened", now)), 1),
                                  "exit": b.get("exit_reason", "broker SL/close")})
                self.add_log("INFO", f"GROUP {ps.batch_id} {b['side']} {ps.symbol} fully closed: "
                                     f"P/L {b['realized']:+.2f} over {b.get('legs', '?')} order(s) "
                                     f"[{b.get('exit_reason', 'broker SL/close')}]")
                self._decision(ps.symbol, b["side"], None, f"group closed P/L {b['realized']:+.2f}")
                self.bursts.pop(ps.batch_id, None)
        if initial and self.own_positions:
            self.add_log("INFO", f"Managing {len(self.own_positions)} existing bot position(s) after reconnect")
        self._save_state()

    def _deals(self, max_age: float = 1.0) -> list:
        """Broker deal history (last 2 days), cached so a batch close needs ONE history call, not one per order."""
        ts, deals = self._deals_cache
        if time.time() - ts > max_age:
            now = time.time() + self.server_offset
            deals = self.broker.deals(now - 2 * 86400, now + 86400)
            self._deals_cache = (time.time(), deals)
        return deals

    def _closed_profit(self, ticket: int) -> Optional[float]:
        rel = [d for d in self._deals() if int(d.get("position_id", 0)) == ticket]
        if not rel:
            return None
        return sum(float(d.get(k, 0.0)) for d in rel for k in ("profit", "commission", "swap", "fee"))

    def _update_realized(self, now_wall: float) -> None:
        if now_wall - self.last_deals_check < 5.0:
            return
        self.last_deals_check = now_wall
        server_now = now_wall + self.server_offset
        day_start = server_now - (server_now % 86400)
        deals = self._deals(max_age=4.0)
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
                meta = self._tick_meta(s)
                self._recv_wall[s] = meta.get("recv") or time.time()
                if meta.get("feed_ms") is not None:
                    self._feed_ms[s] = meta["feed_ms"]
                if eng.last.tick_rate >= 0.5 and len(ticks) < 50:
                    off = round((ticks[-1][0] - now_wall) / 900.0) * 900.0
                    if off != self.server_offset:
                        self.server_offset = off
                        self._save_state()

    def _tick_meta(self, s: str) -> dict:
        fn = getattr(self.broker, "tick_meta", None)
        try:
            return (fn(s) or {}) if fn else {}
        except Exception:  # noqa: BLE001
            return {}

    def _server_now(self, symbol: str, now_wall: float) -> float:
        eng = self.engines[symbol]
        if not eng.buf.ticks:
            return now_wall + self.server_offset
        return eng.buf.last_t + (now_wall - self.last_receipt.get(symbol, now_wall))

    # ================================================================ ONE ENGINE
    # ticks -> micro analysis -> BUY/SELL strength -> validation & risk -> entry -> tick management
    # -> hard risk exits (always first) / profit capture / momentum & micro-reversal exits
    # -> position verification against the broker (_adopt_positions every loop) -> next scan

    def _micro(self, s: str, now_wall: float):
        """Real-tick micro analysis + dynamic BUY/SELL strength for one symbol (cached per loop)."""
        c = self._mcache.get(s)
        if c and c[0] == now_wall:
            return c[1], c[2]
        eng = self.engines.get(s)
        cfg = self.cfg
        if eng is None:
            m = Micro(reason="no tick engine")
            st = Strength(reason=m.reason)
        else:
            now = self._server_now(s, now_wall)
            f = eng.features(now)
            atr, src = self._atr(s)
            slips = [v for (ts_, v) in self.slippage.get(s, ()) if now_wall - ts_ <= cfg.slippage_window_s]
            m = eng.scanner.snapshot(now, atr, f.vol_unit, self.m1_bars.get(s, []), cfg, slips)
            st = decide(m, cfg)
            if st.side != "NO TRADE" and src != "M1" and atr <= 0:
                # no broker M1 history and < 60 s of live ticks: volatility scale unknown, entries wait
                st.side, st.reason = "NO TRADE", "warming up: measuring volatility (no M1 history yet)"
            if m.ready and f.ready and st.side != "NO TRADE":
                ok, why = eng.spread_ok(f)
                if not ok:
                    st.side, st.reason = "NO TRADE", why
            f.score = st.score
        self._mcache[s] = (now_wall, m, st)
        return m, st

    def _status(self, s: str, m, st, now_wall: float) -> dict:
        info = self.sym_info.get(s) or {}
        tr = self.trends.get(s)
        return {"signal": st.side if st.side in ("BUY", "SELL") else "HOLD", "score": round(st.score, 3),
                "reason": st.reason, "features": m.brief() if m.ready else {"reason": m.reason},
                "strength": {"buy": st.buy, "sell": st.sell, "threshold": round(st.threshold, 3),
                             "scan": st.scan, "vetoes": st.vetoes[:3], "regime": m.regime if m.ready else None,
                             "comps": st.comps},
                "spread_pts": round(m.spread / float(info["point"]), 1) if info.get("point") and m.spread else None,
                "tick_age": round(now_wall - self.last_receipt[s], 1) if s in self.last_receipt else None,
                "trend": tr.brief() if tr else None}

    def _acct(self) -> AccountView:
        return AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                           float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))

    # -------------------------------------------------------------- entries
    def _try_entries(self, now_wall: float) -> None:
        """Signal detection -> risk validation -> execution, per symbol. A symbol with an open basket is only
        managed (no adds: one batch per signal); a flat symbol may open ONE new batch."""
        cfg = self.cfg
        hedging = int(self.account.get("margin_mode", 0)) == MARGIN_HEDGING
        for s in self.selected:
            eng = self.engines.get(s)
            info = self.sym_info.get(s)
            if eng is None or not info:
                continue
            m, st = self._micro(s, now_wall)
            t_dec = time.time()
            status = self._status(s, m, st, now_wall)
            self.sym_status[s] = status
            mine = [ps for ps in self.pos_state.values() if ps.symbol == s]
            if mine:
                g = self.bursts.get(mine[0].batch_id) or {}
                status["reason"] = (f"managing {mine[0].side} basket {mine[0].batch_id} ({len(mine)} order(s), "
                                    f"net {g.get('net_px', 0.0):+.5g}) - no adds: one batch per signal")
                continue
            if st.side == "NO TRADE":
                self._skip(s, status, st.reason, now_wall)
                continue
            if self.exec_halt:
                self._skip(s, status, "execution protection: close failures - new entries halted", now_wall)
                continue
            side = st.side
            sig = Signal(side, st.score, st.reason, eng.last)
            if not hedging and any(p["symbol"] == s for p in self.foreign_positions):
                self._skip(s, status, "netting account: symbol already has a (manual) position", now_wall, sig)
                continue
            ok, why = self.pacer.can_enter(s, now_wall)
            if not ok:
                self._skip(s, status, f"{side} blocked: {why}", now_wall, sig)
                continue
            if self.exec_pause.get(s, 0.0) > now_wall:
                self._skip(s, status, f"{side} blocked: execution failures - paused "
                                      f"{self.exec_pause[s] - now_wall:.0f}s", now_wall, sig)
                continue
            sl_hist = [v for (ts_, v) in self.slippage.get(s, ()) if now_wall - ts_ <= cfg.slippage_window_s]
            if len(sl_hist) >= 3 and sum(sl_hist) / len(sl_hist) > cfg.max_avg_slippage_vu:
                self._skip(s, status, "paused: poor execution quality (slippage)", now_wall, sig)
                continue
            cd = self.group_cooldown.get(s, 0.0) - now_wall
            if cd > 0:
                self._skip(s, status, f"{side} blocked: group cooldown {cd:.1f}s", now_wall, sig)
                continue
            last = self.last_burst.get(s)
            if last and last.get("side") == side and abs(float(last.get("origin", 0.0)) - st.origin) < 0.5:
                self._skip(s, status, f"{side} skipped: duplicate - this burst was already traded", now_wall, sig)
                continue
            if last and float(last.get("tick_t", -1.0)) == m.t:
                self._skip(s, status, f"{side} skipped: duplicate - same tick", now_wall, sig)
                continue
            learn = None
            if cfg.learning_enabled:
                bucket = self.learner.key("micro", m.regime, vol_regime(m.spread, m.atr))
                learn = self.learner.decide(bucket, now_wall)
                learn.bucket = bucket
                self.learn_view[s] = learn.why
                if not learn.allow:
                    self._skip(s, status, learn.why, now_wall, sig)
                    continue
            self._open_batch(s, side, m, st, info, status, now_wall, t_dec, learn)

    def _open_batch(self, s: str, side: str, m, st, info: dict, status: dict, now_wall: float, t_dec: float,
                    learn=None) -> bool:
        """ONE burst batch for ONE signal: n same-lot market orders sent back to back, each fill verified.
        n grows with the signal's edge over the threshold (capped); batch risk/lot caps apply; the remaining
        orders are cancelled when price has run past the edge, the spread widens or an order fails.
        Never martingale, never averaging down, never added to later."""
        cfg = self.cfg
        d = 1 if side == "BUY" else -1
        vmin, vmax, vstep = float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"])
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl_dist = max(stops, cfg.sl_atr * m.atr, cfg.sl_spread_mult * m.spread)
        ts, tv = self._tv(s)
        acct = self._acct()
        sig = Signal(side, st.score, st.reason, self.engines[s].last)

        def refuse(why: str) -> bool:
            self.pacer.on_entry(s, now_wall)
            self._skip(s, status, f"{side} batch blocked: {why}", now_wall, sig)
            return False

        strength = st.buy if side == "BUY" else st.sell
        n = 1 + int(max(0.0, strength - st.threshold) / max(cfg.batch_step_strength, 1e-6))
        n = max(1, min(n, cfg.max_orders_per_batch, cfg.max_positions_per_symbol))
        if m.regime in ("quiet", "chop"):
            n = 1
        probe = self.risk.probe_left > 0
        if probe:
            n = 1
        per_pct = min(cfg.risk_per_trade_pct, cfg.risk_per_batch_pct / n)
        size = size_position(acct.equity, dc_replace(cfg, risk_per_trade_pct=per_pct), sl_dist, ts, tv, vmin, vmax, vstep)
        if size.volume <= 0:
            return refuse(size.note)
        floor = size.note.startswith("min lot floor")
        if (probe or (learn is not None and learn.min_lot)) and size.volume > vmin:
            size = type(size)(vmin, size.risk_money * vmin / size.volume,
                              size.note + ("; safe-mode probe: min lot" if probe else "; learning: min lot"))
        elif learn is not None and learn.mult > 1.0:
            v = normalize_volume(min(size.volume * learn.mult, cfg.max_lot_per_order), vmin, vmax, vstep)
            if v > size.volume:
                size = type(size)(v, size.risk_money * v / size.volume, size.note + f"; learning x{learn.mult:.2f}")
        cap_lot = self.no_up_after_loss.get(s)
        if cap_lot and size.volume > cap_lot:
            size = type(size)(cap_lot, size.risk_money * cap_lot / size.volume, size.note + f"; capped {cap_lot:g} after a loss")
        budget = acct.equity * cfg.risk_per_batch_pct / 100.0
        while n > 1 and (n * size.volume > cfg.max_batch_lots + 1e-9 or (not floor and n * size.risk_money > budget + 1e-9)):
            n -= 1
        if size.volume > cfg.max_batch_lots + 1e-9:
            v = normalize_volume(cfg.max_batch_lots, vmin, vmax, vstep)
            if v <= 0:
                return refuse(f"min lot {vmin:g} > max batch lots {cfg.max_batch_lots:g}")
            size = type(size)(v, size.risk_money * v / size.volume, size.note + "; batch lot cap")
        gid = uuid.uuid4().hex[:6]
        ref = m.ask if side == "BUY" else m.bid
        run_cap = cfg.batch_run_cancel_frac * m.target
        opened, why_stop = 0, ""
        for i in range(n):
            if i > 0:
                q = self.broker.last_tick(s) or {}
                px = float(q.get("ask") if side == "BUY" else q.get("bid") or 0.0)
                sp = float(q.get("ask") or 0.0) - float(q.get("bid") or 0.0)
                typ = max(m.spread, self.engines[s].scanner.spread_ema)
                if px <= 0:
                    why_stop = "no fresh quote"
                elif (px - ref) * d > run_cap:
                    why_stop = f"price ran {(px - ref) * d:.5g} past the edge (> {run_cap:.5g})"
                elif (px - ref) * d < -run_cap:
                    why_stop = f"price moved {(px - ref) * d:.5g} against the signal"
                elif sp > cfg.spread_spike_ratio * typ:
                    why_stop = f"spread widened to {sp:.5g}"
                if why_stop:
                    break
            r = self._send_one(s, side, i, n, gid, size, sl_dist, m, st, info, status, now_wall, t_dec, floor)
            if not r:
                why_stop = why_stop or "order not filled"
                break
            opened += 1
        if opened == 0:
            self.pacer.on_entry(s, now_wall)
            return False
        g = self.bursts[gid]
        g["planned"] = n
        self.pacer.on_entry(s, now_wall)
        self.last_burst[s] = {"side": side, "origin": st.origin, "tick_t": m.t}
        if probe:
            self.risk.probe_left -= 1
        if learn is not None:
            comps = dict(st.comps.get(side, {}))
            comps["spread_atr"] = round(m.spread / m.atr, 3) if m.atr else None
            g.update(bucket=getattr(learn, "bucket", ""), type="micro", rel=m.regime,
                     regime=vol_regime(m.spread, m.atr), hour=time.gmtime(now_wall).tm_hour,
                     learn_state=learn.state, comps=comps)
        msg = f"BATCH {gid} {side} {s}: {opened}/{n} order(s) filled, {opened * size.volume:g} lots"
        if opened < n:
            msg += f" - rest cancelled: {why_stop}"
        self.add_log("INFO", msg)
        self._decision(s, side, st.score, f"OPENED batch {gid} {opened}/{n} x {size.volume:g} ({m.regime})")
        status["reason"] = f"basket {gid}: {opened} order(s) {side}"
        self._refresh_account()
        self._save_state()
        return True

    def _send_one(self, s, side, i, n, gid, size, sl_dist, m, st, info, status, now_wall, t_dec, floor):
        """Validate and send one order of a batch; verify the fill; measure latency on real clocks."""
        cfg = self.cfg
        d = 1 if side == "BUY" else -1
        point = float(info["point"])
        acct = self._acct()
        price = m.ask if side == "BUY" else m.bid
        margin = self.broker.calc_margin(side, s, size.volume, price)
        if margin is None:
            self._skip(s, status, f"{side} order {i + 1}/{n} blocked: margin calc unavailable", now_wall)
            return None
        open_pos = [OpenPos(p["symbol"], self._side(p), float(p["volume"]), float(p["profit"]), self._pos_risk(p),
                            self._notional(p["symbol"], float(p["volume"]), float(p.get("price_current") or p["price_open"])))
                    for p in self.own_positions]
        ok, why = self.risk.check_entry(s, side, open_pos, acct, size.volume, size.risk_money, margin, True,
                                        self._notional(s, size.volume, price), batch_leg=i > 0, floor_mode=floor)
        if ok:
            cap = acct.equity * cfg.max_margin_use_pct / 100.0
            used = self._margin_used()
            if used + margin > cap + 1e-9:
                ok, why = False, (f"margin use cap: {used:.2f} + {margin:.2f} > {cfg.max_margin_use_pct:g}% "
                                  f"of equity ({cap:.2f})")
        if not ok:
            self._skip(s, status, f"{side} order {i + 1}/{n} blocked: {why}", now_wall)
            return None
        sl = price - d * sl_dist if cfg.broker_sl else 0.0
        comment = f"{cfg.comment_prefix}-{gid}-{i + 1}"[:31]
        self.pacer.inflight.add(s)
        t_send = time.time()
        try:
            r = self.broker.market_order(s, side, size.volume, sl, 0.0, self._deviation(s, m.spread, m.vu), cfg.magic, comment)
        finally:
            self.pacer.inflight.discard(s)
        t_fill = time.time()
        if not r.get("ok"):
            q = self.exec_fails.setdefault(s, deque(maxlen=20))
            q.append(now_wall)
            recent = [x for x in q if now_wall - x <= 60.0]
            self.add_log("WARNING", f"ENTRY {side} {size.volume} {s} rejected: {r.get('msg')} "
                                    f"({len(recent)} failure(s) in 60s, {(t_fill - t_send) * 1000:.0f}ms)")
            if len(recent) >= cfg.exec_fail_max:
                self.exec_pause[s] = now_wall + cfg.exec_fail_pause_s
                self.add_log("ERROR", f"EXECUTION PROTECTION {s}: {len(recent)} rejected orders in 60s - entries on "
                                      f"{s} paused {cfg.exec_fail_pause_s:.0f}s")
            if "not found" in str(r.get("msg")):
                self.unverified_entry = r["msg"]
                self.running = False
                self._save_state()
            return None
        fill = float(r["price"])
        slip = (fill - float(r["req_price"])) * d / point
        self.slippage.setdefault(s, deque(maxlen=10)).append((now_wall, max(0.0, slip * point / max(m.vu, point))))
        ack = float(r.get("latency") or 0.0)
        t_recv = self._recv_wall.get(s, t_dec)
        lat = {"ts": now_wall, "symbol": s, "side": side, "order": f"{i + 1}/{n}",
               "tick_to_decision_ms": round(max(0.0, t_dec - t_recv) * 1000, 1),
               "decision_to_send_ms": round(max(0.0, t_send - t_dec) * 1000, 1),
               "send_to_ack_ms": round(ack * 1000, 1) if ack else None,
               "send_to_fill_verified_ms": round((t_fill - t_send) * 1000, 1),
               "tick_to_fill_ms": round(max(0.0, t_fill - t_recv) * 1000, 1),
               "feed_ms": self._feed_ms.get(s), "slip_pts": round(slip, 1)}
        self.lat_entries.append(lat)
        ticket = int(r["ticket"])
        self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=side, volume=float(r["volume"]),
                                               open_price=fill, open_time=now_wall, sl=sl, tp=0.0, entry_vu=m.vu,
                                               reason=st.reason, batch_id=gid,
                                               hard_dist=sl_dist * cfg.soft_stop_mult, peak_pl=-abs(m.spread))
        g = self.bursts.get(gid)
        if g is None:
            g = self.bursts[gid] = {"symbol": s, "side": side, "opened": now_wall, "mode": "micro", "micro": True,
                                    "realized": 0.0, "legs": 0, "target": m.target, "peak_pl": 0.0,
                                    "peak_vel": max(0.0, d * m.vel), "ext_px": m.mid, "last_ext_t": m.t,
                                    "origin": st.origin, "regime": m.regime, "stop_dist": sl_dist, "adds": 0}
        g["legs"] = int(g.get("legs", 0)) + 1
        g.setdefault("lot", float(r["volume"]))
        g["risk"] = float(g.get("risk", 0.0)) + size.risk_money
        dg = int(info["digits"])
        strength = st.buy if side == "BUY" else st.sell
        self.add_log("INFO", f"ENTRY {side} {r['volume']} {s} @ {fill} #{ticket} batch {gid} order {i + 1}/{n} "
                             f"strength {strength:.2f}/{st.threshold:.2f} [{st.reason}] "
                             f"SL {format(sl, '.%df' % dg) if sl else 'OFF'} (stop {sl_dist:.{dg}f}) "
                             f"target {m.target:.{dg}f} risk {size.risk_money:.2f} ({size.note}) slip {slip:+.0f}pt | "
                             f"latency tick->decision {lat['tick_to_decision_ms']:.0f}ms, decision->send "
                             f"{lat['decision_to_send_ms']:.0f}ms, send->fill verified {lat['send_to_fill_verified_ms']:.0f}ms"
                             + (f" (ack {lat['send_to_ack_ms']:.0f}ms)" if lat["send_to_ack_ms"] else "")
                             + (f", feed {lat['feed_ms']:.0f}ms" if lat["feed_ms"] is not None else ""))
        return r

    # ---------------------------------------------------------------- exits
    def _manage_exits(self, now_wall: float) -> None:
        cfg = self.cfg
        live = {int(p["ticket"]): p for p in self.own_positions}
        for t, ps in list(self.pos_state.items()):
            p = live.get(t)
            if p is None:
                continue
            if cfg.broker_sl and not float(p.get("sl", 0.0)):       # adopted/legacy order without a stop
                dist = (ps.hard_dist / cfg.soft_stop_mult) if ps.hard_dist else 0.0
                if not dist:
                    atr, _ = self._atr(ps.symbol)
                    dist = cfg.sl_atr * atr
                if dist:
                    d = 1 if ps.side == "BUY" else -1
                    r = self.broker.modify_sl(t, ps.open_price - d * dist, float(p.get("tp", 0.0)))
                    if not r["ok"] and not r.get("closed"):
                        cr = self.broker.close_position(t, self._close_dev(ps.symbol), cfg.magic)
                        self.add_log("ERROR", f"#{t} had no SL and SL could not be set - closed: {cr['msg']}")
                        continue
                    ps.hard_dist = ps.hard_dist or dist * cfg.soft_stop_mult
                    self.add_log("WARNING", f"#{t} protective SL attached at {ps.open_price - d * dist}")
        if not self.running and not self.risk.halted:
            bare = [p for p in self.own_positions if not float(p.get("sl", 0.0))]
            if bare and not self.stopping:
                self.add_log("WARNING", f"{len(bare)} bot position(s) without a broker SL and engine not running - "
                                        f"flattening immediately")
                self._flatten("UNPROTECTED (no SL, engine not running)")
            return            # exits are managed only while STARTed (broker SL protects otherwise)
        self._check_price_sync(live, now_wall)
        self._manage_stops(live, now_wall)
        groups: dict[str, list[PositionState]] = {}
        self._closed_tickets = {k: v for k, v in self._closed_tickets.items() if now_wall - v < 60.0}
        for t, ps in self.pos_state.items():
            p = live.get(t)
            # only live, bot-owned legs of THIS basket (same symbol/side); a ticket verified closed is never re-closed
            if (p is None or t in self._closed_tickets or p.get("symbol") != ps.symbol
                    or int(p.get("magic", self.cfg.magic)) != self.cfg.magic or self._side(p) != ps.side):
                continue
            groups.setdefault(ps.batch_id or f"#{t}", []).append(ps)
        for key, legs in groups.items():
            sym = legs[0].symbol
            m, st = self._micro(sym, now_wall)
            reason = self._hard_risk_exit(key, legs, live, m, now_wall)       # hard risk ALWAYS first
            if not reason and self.running:
                reason = self._micro_exit(key, legs, live, m, st, now_wall)
            if not reason:
                continue
            closed = []
            t_dec = time.time()
            for ps in legs:
                if ps.ticket in self._closed_tickets:
                    continue
                t0 = time.time()
                r = self.broker.close_position(ps.ticket, self._close_dev(sym), cfg.magic)
                t1 = time.time()
                ok = r["ok"] and r.get("verified")
                closed.append(f"#{ps.ticket} {'ok' if ok else 'FAIL ' + str(r['msg'])} {(t1 - t0) * 1000:.0f}ms")
                if ok:
                    self._closed_tickets[ps.ticket] = now_wall
                    self.lat_exits.append({"ts": now_wall, "symbol": sym, "ticket": ps.ticket,
                                           "close_ms": round((t1 - t0) * 1000, 1),
                                           "decision_to_close_ms": round((t1 - t_dec) * 1000, 1)})
                if not ok:
                    ps.closing_attempts += 1
                    if ps.closing_attempts >= cfg.close_fail_halt and not self.exec_halt:
                        self.exec_halt = True
                        self.add_log("ERROR", f"EXECUTION PROTECTION: #{ps.ticket} could not be closed "
                                              f"{ps.closing_attempts}x - new entries halted, still retrying the close")
            if key in self.bursts:
                self.bursts[key]["exit_reason"] = reason
            self.add_log("INFO" if not any(" FAIL " in f" {c}" for c in closed) else "ERROR",
                         f"EXIT GROUP {key} {legs[0].side} {sum(p.volume for p in legs):g} {sym}: {reason} -> "
                         + ", ".join(closed))
            self._refresh_account()

    def _group_pl(self, legs: list, live: dict, m, now_wall: float):
        """(executable P/L per unit in price, floating money) for a group, from the broker's prices."""
        d = 1 if legs[0].side == "BUY" else -1
        vol = sum(p.volume for p in legs)
        avg = sum(p.open_price * p.volume for p in legs) / vol
        pxs = [x for x in (self._exec_px(ps, live.get(ps.ticket, {}), m if getattr(m, "bid", 0) else None, now_wall)
                           for ps in legs) if x]
        money = sum(float((live.get(ps.ticket) or {}).get("profit", 0.0)) + float((live.get(ps.ticket) or {}).get("swap", 0.0))
                    for ps in legs)
        if not pxs:
            return None, money
        exit_px = min(pxs) if d > 0 else max(pxs)
        return (exit_px - avg) * d, money

    def _hard_risk_exit(self, key: str, legs: list, live: dict, m, now_wall: float) -> Optional[str]:
        """Hard risk exits override everything: safe mode (equity drawdown), hard stop mirror and
        locked-stop mirror, group loss cap, exposure breach, emergency (no price)."""
        cfg = self.cfg
        if self.risk.halted:
            return self.risk.halt_reason
        sym = legs[0].symbol
        for ps in legs:
            p = live.get(ps.ticket) or {}
            d = 1 if ps.side == "BUY" else -1
            px = self._exec_px(ps, p, m if getattr(m, "bid", 0) else None, now_wall)
            if px is None:
                continue
            if ps.vsl and (px - ps.vsl) * d <= 0:
                return f"{ps.sl_state} stop hit #{ps.ticket} at {px} (engine mirror of the broker stop)"
            if ps.hard_dist <= 0:
                return f"unprotected #{ps.ticket}: no known stop distance"
            adverse = (ps.open_price - px) * d
            if adverse >= ps.hard_dist:
                return f"hard stop #{ps.ticket}: adverse {adverse:.5g} >= {ps.hard_dist:.5g} (engine mirror)"
        eq = float(self.account.get("equity", 0.0))
        money = sum(float((live.get(ps.ticket) or {}).get("profit", 0.0)) + float((live.get(ps.ticket) or {}).get("swap", 0.0))
                    for ps in legs)
        lim = eq * cfg.group_loss_pct / 100.0
        ts, tv = self._tv(sym)
        if ts and tv and m is not None and getattr(m, "spread", 0):
            lim = max(lim, 3.0 * m.spread / ts * tv * sum(p.volume for p in legs))   # never inside the spread cost
        if eq > 0 and money <= -lim:
            return f"hard group loss {money:+.2f} <= -{lim:.2f} ({cfg.group_loss_pct:g}% of equity)"
        if eq > 0 and cfg.max_total_risk_pct > 0:
            open_risk = sum(self._pos_risk(p) for p in self.own_positions)
            if open_risk > eq * cfg.max_total_risk_pct / 100.0 * 1.5:
                newest = max(self.pos_state.values(), key=lambda x: x.open_time, default=None)
                if newest is not None and newest.batch_id == legs[0].batch_id:
                    return f"max exposure breach: open risk {open_risk:.2f} > 1.5x cap - closing newest group"
        age = now_wall - self.last_receipt.get(sym, now_wall)
        if age > cfg.emergency_stale_s:
            xq = self._exec_quote(sym, now_wall)
            if xq is None or xq[2] != "rest":
                return f"emergency: no fresh price for {age:.0f}s"
        return None

    def _micro_exit(self, key: str, legs: list, live: dict, m, st, now_wall: float) -> Optional[str]:
        """The single tick-based exit: profit capture net of cost, momentum exit, micro-reversal / failure."""
        cfg = self.cfg
        d = 1 if legs[0].side == "BUY" else -1
        g = self.bursts.get(key)
        if g is None:                       # adopted position(s): manage them with the same engine
            g = self.bursts[key] = {"symbol": legs[0].symbol, "side": legs[0].side, "opened": min(p.open_time for p in legs),
                                    "mode": "micro", "micro": True, "realized": 0.0, "legs": len(legs),
                                    "target": m.target if m.ready else 0.0, "peak_pl": 0.0, "peak_vel": 0.0,
                                    "ext_px": m.mid, "last_ext_t": m.t, "stop_dist": 0.0}
        pl, money = self._group_pl(legs, live, m, now_wall)
        if pl is None or not m.ready:
            return None
        ts, tv = self._tv(legs[0].symbol)
        cost = cfg.commission_per_lot / (tv / ts) if (cfg.commission_per_lot and ts and tv) else 0.0
        net = pl - cost
        g["net_px"] = net
        g["money"] = money
        g["peak_pl"] = max(float(g.get("peak_pl", 0.0)), net)
        if d * (m.mid - float(g.get("ext_px", m.mid))) > 0 or "ext_px" not in g:
            g["ext_px"], g["last_ext_t"] = m.mid, m.t
        g["peak_vel"] = max(float(g.get("peak_vel", 0.0)), d * m.vel)
        held = now_wall - min(p.open_time for p in legs)
        opp = st.sell if d > 0 else st.buy
        stop_dist = float(g.get("stop_dist") or 0.0) or min((p.hard_dist for p in legs if p.hard_dist), default=0.0) / cfg.soft_stop_mult
        return exit_reason(g, m, d, pl, held, opp, st.threshold, stop_dist, cost, cfg)


    # ------------------------------------------------- data, protection, ledger helpers
    def _atr(self, s: str) -> tuple[float, str]:
        a = self.atr_m1.get(s, 0.0)
        if a > 0:
            return a, "M1"
        eng = self.engines.get(s)
        vu = eng.last.vol_unit if eng and eng.last.vol_unit else 0.0
        if not vu:
            return 0.0, "none"
        # no broker M1 history: the scanner's rolling 60 s range (incremental), floor 6x vol unit; 0 until 60 s
        r60 = eng.scanner.atr_estimate(eng.scanner.last[0]) if eng.scanner.last else 0.0
        return (max(6.0 * vu, r60), "est") if r60 > 0 else (0.0, "none")

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
            if p is None:
                continue
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
            trail = max(cfg.trail_atr_frac * atr,
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

    def _tv(self, s: str) -> tuple[float, float]:
        info = self.sym_info.get(s) or {}
        return (float(info.get("trade_tick_size", 0) or 0),
                float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0) or 0))

    def _margin_used(self) -> float:
        eq = float(self.account.get("equity", 0) or 0)
        m = float(self.account.get("margin", 0) or 0)
        return m if m > 0 else max(0.0, eq - float(self.account.get("margin_free", 0) or 0))

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
            self.learner.record(entry, now)
        except Exception as e:  # noqa: BLE001  learning must never break trading
            log.warning("learner record failed: %s", e)

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
                m1 = fn(s, "M1", 0.75)     # one history call: M5 fetch removed (trend is display-only)
                m5 = []
            except Exception as e:  # noqa: BLE001
                m1, m5 = [], []
                log.debug("candles %s failed: %s", s, e)
            prev = self.trends.get(s)
            tr = compute_trend(m1, m5, self.cfg.trend_min_strength)     # context/display only, never an entry gate
            if m1:
                self.m1_bars[s] = m1
            rngs = [b["high"] - b["low"] for b in m1[-15:-1] if b.get("high") is not None and b["high"] > b["low"]]
            if rngs:
                self.atr_m1[s] = sum(rngs) / len(rngs)
                if s in self.engines:
                    self.engines[s].seed_vu = 0.15 * self.atr_m1[s]
            tr.detail["m1_bars"], tr.detail["m5_bars"] = float(len(m1)), float(len(m5))
            self.trends[s] = tr
            if prev is None or prev.label.split(" ")[0] != tr.label.split(" ")[0]:
                self.add_log("INFO", f"TREND {s}: {tr.label} (M1 {len(m1)} bars, M5 {len(m5)} bars)")

    @staticmethod
    def _category(reason: str) -> str:
        r = (reason or "").lower()
        table = [("safe mode", "safe mode"), ("exhaustion", "exhaustion"), ("speed-only", "speed-only (B/C weak)"),
                 ("structure-only", "no tick force (scanner A)"), ("managing", "managing basket"),
                 ("duplicate", "duplicate burst"), ("warming", "warming up"), ("no ticks", "no live ticks"),
                 ("idle", "no live ticks"), ("stale", "no live ticks"), ("feed unstable", "feed unstable"),
                 ("thin tick", "thin tick flow"), ("tick rate", "thin tick flow"), ("anti-chase", "anti-chase"),
                 ("no live force", "no live force"), ("no edge", "below threshold"), ("two-sided", "two-sided"),
                 ("supporting signal", "weak support"), ("spread", "spread / cost filter"),
                 ("group cooldown", "group cooldown"), ("cooldown", "cooldown"), ("per minute", "entry rate cap"),
                 ("/min", "entry rate cap"), ("min lot", "sizing refused"), ("averaging", "no adding to losers"),
                 ("not in net profit", "no adding to losers"), ("margin", "margin"),
                 ("execution failure", "execution failures"), ("slippage", "execution quality"),
                 ("halt", "execution protection"), ("netting", "netting position"), ("loss streak", "loss-streak pause"),
                 ("pause", "loss-streak pause"), ("learning", "learner"), ("risk", "risk cap"), ("max ", "risk cap"),
                 ("opposite", "no hedge vs open")]
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
            acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                               float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
            ev = self.risk.update(now_wall, acct) if self.running else None
            if ev and ev[0] == "safe":
                self.add_log("ERROR", f"{ev[1]} - closing bot positions; the bot keeps running (no all-day stop)")
            elif ev:
                self.add_log("WARNING", ev[1])
            if now_wall - self.last_trade_check > 10.0:
                self.last_trade_check = now_wall
                self._validate()
            self._update_trends(now_wall)
            self._manage_exits(now_wall)
            if self.running:
                self._log_why(now_wall)
            if self.risk.halted and self.own_positions:
                self._flatten("SAFE MODE")
            if self.flatten_pending:
                self.flatten_pending = not self._flatten("STOP retry")["ok"]
            if self.running and not self.stopping and self.validation.get("ok") and not self.risk.halted:
                self._try_entries(now_wall)
            elif self.running and self.risk.halted:
                for s in list(self.engines):
                    m, st = self._micro(s, now_wall)
                    status = self._status(s, m, st, now_wall)
                    status["signal"] = "HOLD"
                    status["reason"] = (f"{self.risk.halt_reason} (resumes in "
                                        f"{max(0.0, self.risk.safe_until - now_wall):.0f}s)")
                    self.sym_status[s] = status
            elif self.engines:
                for s in list(self.engines):
                    m, st = self._micro(s, now_wall)
                    status = self._status(s, m, st, now_wall)
                    if not self.running:
                        status["signal"], status["reason"] = "HOLD", "stopped"
                    elif not self.validation.get("ok"):
                        status["signal"], status["reason"] = "HOLD", "validation failing - entries paused"
                    if self.risk.halted:
                        status["signal"], status["reason"] = "HOLD", self.risk.halt_reason
                    self.sym_status[s] = status
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
        eq = float(self.account.get("equity", 0) or 0) if self.conn_state == "CONNECTED" else 0.0
        used = self._margin_used() if eq else 0.0
        return {"engine": "Ultra-Fast Pre-Burst Short Scalping (single engine, scanners A/B/C)",
                "risk_per_trade_pct": c.risk_per_trade_pct, "max_total_risk_pct": c.max_total_risk_pct,
                "daily_loss_limit": "none (removed - no all-day stop)", "equity_drawdown_pct": c.equity_drawdown_pct,
                "safe_mode": self.risk.halted, "safe_reason": self.risk.halt_reason if self.risk.halted else "",
                "safe_resume_in_s": (round(max(0.0, self.risk.safe_until - (getattr(self, "now_wall", 0) or time.time())))
                                     if self.risk.halted else None),
                "safe_pause_s": c.safe_pause_s, "probe_batches_left": self.risk.probe_left,
                "group_loss_pct": c.group_loss_pct, "max_orders_per_batch": c.max_orders_per_batch,
                "risk_per_batch_pct": c.risk_per_batch_pct, "max_batch_lots": c.max_batch_lots,
                "max_positions_total": c.max_positions_total, "max_positions_per_symbol": c.max_positions_per_symbol,
                "max_margin_use_pct": c.max_margin_use_pct, "margin_used": round(used, 2),
                "margin_use_pct": round(used / eq * 100, 1) if eq else None, "max_total_notional_x": c.max_total_notional_x,
                "max_spread_bps": c.max_spread_bps, "min_tick_rate": c.min_tick_rate,
                "threshold": c.mb_threshold, "scan_support": c.mb_scan_support, "max_chase_atr": c.mb_max_chase_atr,
                "max_bar_atr": c.mb_max_bar_atr, "target_atr": c.mb_target_atr, "target_spread": c.mb_target_spread,
                "follow_s": c.mb_follow_seconds, "max_hold_s": c.mb_max_hold_seconds,
                "broker_sl": "ON" if c.broker_sl else "OFF", "exec_halt": self.exec_halt,
                "sl_rule": f"hard stop max({c.sl_atr:g}x M1 ATR, {c.sl_spread_mult:g}x spread) at the broker -> BE+ "
                           f"{c.be_lock_spread_mult:g}x spread -> trail max({c.trail_atr_frac:g}x ATR, "
                           f"{c.trail_spread_mult:g}x spread); engine mirror {c.soft_stop_mult:g}x"}

    def _groups_view(self) -> dict:
        live = {int(p["ticket"]): p for p in self.own_positions} if self.conn_state == "CONNECTED" else {}
        out = {}
        for k, g in self.bursts.items():
            legs = [ps for ps in self.pos_state.values() if (ps.batch_id or f"#{ps.ticket}") == k and ps.ticket in live]
            if not legs:
                continue
            out[k] = {"symbol": g.get("symbol"), "side": g.get("side"), "orders": len(legs),
                      "volume": round(sum(ps.volume for ps in legs), 4),
                      "money": round(sum(float(live[ps.ticket].get("profit", 0)) + float(live[ps.ticket].get("swap", 0))
                                         for ps in legs), 2),
                      "net_px": g.get("net_px"), "peak_px": g.get("peak_pl"), "target_px": g.get("target"),
                      "stop_px": g.get("stop_dist"), "regime": g.get("regime"), "adds": g.get("adds", 0),
                      "age_s": round((getattr(self, "now_wall", 0) or time.time()) - min(ps.open_time for ps in legs))}
        return out

    @staticmethod
    def _pct(vals: list, q: float) -> Optional[float]:
        v = sorted(x for x in vals if x is not None)
        if not v:
            return None
        return round(v[min(len(v) - 1, int(round(q * (len(v) - 1))))], 1)

    def _latency_view(self) -> dict:
        """Measured on real wall clocks per order (not estimated). Medians/p90 over the last 200 orders."""
        e, x = list(self.lat_entries), list(self.lat_exits)
        out = {"entries": len(e), "exits": len(x), "last_entry": e[-1] if e else None, "last_exit": x[-1] if x else None}
        for k in ("tick_to_decision_ms", "decision_to_send_ms", "send_to_ack_ms", "send_to_fill_verified_ms",
                  "tick_to_fill_ms", "feed_ms"):
            vals = [r.get(k) for r in e]
            out[k] = {"p50": self._pct(vals, 0.5), "p90": self._pct(vals, 0.9)}
        out["close_ms"] = {"p50": self._pct([r.get("close_ms") for r in x], 0.5),
                           "p90": self._pct([r.get("close_ms") for r in x], 0.9)}
        return out

    def _perf_view(self) -> dict:
        """Results of closed batches from broker deals (profit + commission + swap + fee) in this process."""
        pls = [float(r["pl"]) for r in self.perf]
        n = len(pls)
        if not n:
            return {"batches": 0, "source": "real broker deals"}
        wins = [x for x in pls if x > 0]
        losses = [x for x in pls if x <= 0]
        eq = peak = dd = 0.0
        for x in pls:
            eq += x
            peak = max(peak, eq)
            dd = max(dd, peak - eq)
        gl = -sum(losses)
        return {"batches": n, "source": "real broker deals", "wins": len(wins),
                "win_rate": round(len(wins) / n * 100, 1), "net": round(sum(pls), 2),
                "expectancy": round(sum(pls) / n, 2),
                "profit_factor": round(sum(wins) / gl, 2) if gl > 0 else None,
                "max_drawdown": round(dd, 2), "last": self.perf[-5:]}

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
                       "groups": self._groups_view(),
                       "latency": self._latency_view(), "performance": self._perf_view(),
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
