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
from types import SimpleNamespace
from concurrent.futures import Future
from dataclasses import replace as dc_replace
from typing import Optional

from . import __version__
from . import precision, regime, zones
from .guardian import LossGuardian
from .config import TUNABLE, EngineConfig, validate_tune
from .engine import MomentumEngine, PositionState
from .risk import AccountView, EntryPacer, OpenPos, RiskGuard, SizeResult, size_position
from .mtf import MTF
from . import indicators as ind
from concurrent.futures import ThreadPoolExecutor
from .microburst import Micro, Strength, decide, exit_reason, exit_signals
from .kvstore import from_env as kv_from_env
from .learner import Learner, regime as vol_regime
from .risk import normalize_volume
from .engine import Signal

log = logging.getLogger("rapidburst")

SYMBOL_TRADE_MODE_FULL = 4
ACCOUNT_MODE = {0: "DEMO", 1: "CONTEST", 2: "REAL"}
MARGIN_HEDGING = 2
DEAL_ENTRY_IN = 0


class Trader:
    def __init__(self, broker, store, cfg: EngineConfig, data_dir: str, allow_live: bool = True):
        self.broker = broker
        self.store = store
        self.cfg = cfg
        self.data_dir = data_dir
        self.allow_live = bool(allow_live)  # t46u: any account may be connected; only a server-side RB_ALLOW_LIVE=0 locks REAL out (never from the UI)
        self.cmdq: "queue.Queue[tuple[str, dict, Future]]" = queue.Queue()
        self.lock = threading.Lock()
        self.logs: deque = deque(maxlen=300)
        self.log_seq = 0
        self.snapshot: dict = {}
        self.stop_event = threading.Event()

        self.conn_state = "NO_CREDENTIALS"
        self.conn_msg = "Enter MT5 login / password / server with CONNECT MT5 (demo or real)"
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
        self._handoff_done = False
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
        # M5 direction bias + M1 timing per symbol (real broker candles + forming bar from live ticks)
        self.mtf: dict[str, MTF] = {}
        self._candle_jobs: dict[tuple, Future] = {}
        self._candle_log: dict[str, str] = {}
        self._io = ThreadPoolExecutor(max_workers=6, thread_name_prefix="rb-io")
        self._last_close_wall: dict[str, float] = {}  # symbol -> when its last basket was fully closed
        self.reentry: deque = deque(maxlen=200)      # measured basket close -> next entry (s), real clocks
        # burst batches: id -> {symbol, side, opened, peak_units, mode, realized}
        self.bursts: dict[str, dict] = {}
        self.atr_m1: dict[str, float] = {}          # M1 ATR from real broker bars
        self.m1_bars: dict[str, list] = {}          # completed broker M1 bars (candle context)
        self._mcache: dict[str, tuple] = {}          # per-loop micro analysis cache
        self._ind_cache: dict = {}
        self._reverse_to: dict = {}
        self.reversals = 0
        self._exit_pending: dict = {}     # t36u: basket -> soft exit awaiting confirmation (anti-jitter)
        self._mid_hist: dict = {}         # t36u: symbol -> recent (tick time, mid) for the outlier filter
        self._exit_held_log: dict = {}
        self.exits_filtered = 0
        self._prec = precision.Analyzer()  # t45u: structure / OB / liquidity cache per closed M5 bar
        self._ip_used: dict = {}           # t48u: (symbol, impulse key) -> wall time: one entry per pullback
        self._pe_used: dict = {}           # t45u: (symbol, setup key) -> wall time of its entry (one entry per structure event)
        self._pe_log: dict = {}
        self._regime_last: dict = {}      # t40u: symbol -> (state, phase) for the REGIME change log
        self.recoveries = {"hedges": 0, "layers": 0, "cycle_tp": 0}
        self.guardian = LossGuardian(self)
        self.last_burst: dict[str, dict] = {}        # duplicate prevention: last traded burst per symbol
        self._recv_wall: dict[str, float] = {}       # real wall time the latest tick of a symbol reached the worker
        self._feed_ms: dict[str, float] = {}         # broker tick time -> worker receipt (when the API provides it)
        self.lat_entries: deque = deque(maxlen=200)  # measured entry latency per order (real clocks)
        self.lat_exits: deque = deque(maxlen=200)
        self.perf: list = []                         # closed batches with real broker P/L (after costs)
        self._closed_tickets: dict[int, float] = {}  # verified-closed tickets: never closed twice
        self._floor_tickets: set = set()             # opened at the broker minimum lot (risk above the % caps)
        self._await_pl: dict = {}
        self._pl_poll_wall = 0.0                    # basket id -> (leg, basket, close time): exit deal pending
        # runtime tuning (no redeploy): validated overrides persisted in the data dir, re-applied on every start
        self.tune: dict = {}
        try:
            with open(os.path.join(data_dir, "tune.json"), "r", encoding="utf-8") as fh:
                ok, _bad = validate_tune(json.load(fh))
            for k, v in ok.items():
                setattr(self.cfg, k, v)
            self.tune = ok
        except (OSError, ValueError):
            pass
        # basket protection events (real modifies verified on the MT5 position) and intrabar cycles per M1 bar
        self.prot_stats = {"be": 0, "trail": 0, "modify_ok": 0, "verified": 0, "modify_fail": 0, "unverified": 0,
                           "protect_close": 0}
        self.bar_cycles: dict = {}                   # symbol -> {"bar": M1 bar time, "opened": n, "closed": n}
        self._deals_cache: tuple = (0.0, [])
        self.exec_fails: dict[str, deque] = {}
        self.exec_pause: dict[str, float] = {}
        self.exec_halt = False                       # close failures => no new entries
        self.no_up_after_loss: dict[str, float] = {}  # symbol -> max lot until a winning batch (no martingale)
        self.win_streak: dict[str, int] = {}          # t47u: symbol -> consecutive winning baskets (lot compounding, anti-martingale)
        self.learner = Learner(os.path.join(data_dir, "learning.json"), min_samples=self.cfg.learn_min_samples,
                               half_life_h=self.cfg.learn_half_life_hours, max_mult=self.cfg.learn_max_mult,
                               remote=kv_from_env())
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
        self.last_loc: dict[str, str] = {}
        self._bar_entry: dict[str, int] = {}    # t46u: symbol -> M1 bar (minute) that already got an entry

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
        self.saved_login = st.get("login")      # t46u: account the persisted state belongs to
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
            "login": self.saved_login, "server_offset": self.server_offset,
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

    def cmd_tune(self, values: Optional[dict] = None, reset: bool = False) -> dict:
        """Change whitelisted engine settings at runtime (bounded, see config.TUNABLE); persisted so an engine
        restart keeps them; takes effect on the next loop. No redeploy, the MT5 login is untouched."""
        base = EngineConfig.load()
        if reset:
            for k in list(self.tune):
                setattr(self.cfg, k, getattr(base, k))
            changed, self.tune = dict(self.tune), {}
            bad: list = []
            self.add_log("WARNING", f"TUNE reset to defaults: {sorted(changed)}")
        else:
            ok, bad = validate_tune(values or {})
            changed = {}
            for k, v in ok.items():
                old = getattr(self.cfg, k)
                setattr(self.cfg, k, v)
                self.tune[k] = v
                changed[k] = {"old": old, "new": v}
            if self.cfg.m1_only:                       # t46u: the M1 fast mode owns these (never no-SL, no M5 precision plan)
                self.cfg.precision_enabled = False
                self.cfg.no_stop_mode = False
            if changed:
                self.add_log("WARNING", "TUNE (runtime, no redeploy): " + ", ".join(
                    f"{k} {c['old']} -> {c['new']}" for k, c in changed.items()))
        try:
            path = os.path.join(self.data_dir, "tune.json")
            with open(path + ".tmp", "w", encoding="utf-8") as fh:
                json.dump(self.tune, fh)
            os.replace(path + ".tmp", path)
        except OSError as exc:
            bad = list(bad) + [f"not persisted: {exc}"]
        return {"ok": not bad or bool(changed), "changed": changed, "rejected": bad, "active": dict(self.tune)}

    def cmd_connect(self, enc: str) -> dict:
        try:
            creds = self.store.decrypt_credentials(enc)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "msg": f"could not decrypt credentials ({type(exc).__name__})"}
        self.add_log("INFO", f"CONNECT MT5 requested for account {creds['login']} on {creds['server']}")
        why = self._switch_block(creds)
        if why:
            self.add_log("WARNING", why)
            return {"ok": False, "msg": why, "validation": self.validation}
        ok, msg = self._connect(creds)
        if ok:
            where = self.store.save_credentials(creds)
            self.add_log("INFO", f"Credentials {where}")
        return {"ok": ok, "msg": msg, "validation": self.validation}

    def _switch_block(self, creds: dict) -> str:
        """t49u: switching to a DIFFERENT account is never refused any more. If the bot is trading (or still holds its
        own positions) on the current account, it is STOPPED and its positions are closed there first - so nothing is
        orphaned and two accounts never run at once. Only a failed close blocks the switch (a position that cannot be
        closed must not be left behind). Same account re-connect is always allowed. Returns "" when the way is clear."""
        cur = self.account.get("login") if (self.conn_state == "CONNECTED" and self.account) else None
        try:
            if cur is None or int(cur) == int(creds["login"]):
                return ""
        except (TypeError, ValueError):
            return ""
        if not (self.running or self.own_positions or self.flatten_pending):
            return ""
        self.add_log("WARNING", f"switching account {cur} -> {creds['login']}: STOP and close the bot's positions on {cur} first")
        r = self.cmd_stop()
        self._handoff_done = bool(r.get("ok"))
        if not r.get("ok"):
            return (f"could not close the bot's positions on account {cur} ({r.get('msg')}) - "
                    f"not switching to {creds['login']} so nothing is left orphaned; press STOP again")
        return ""

    def _account_changed(self, new_login) -> None:
        """t46u: persisted per-account state (position meta, bursts, one-entry-per-event memory, guard/risk session)
        must never leak from one account into another."""
        old = self.saved_login
        if old is not None and str(old) != str(new_login):
            self.add_log("WARNING", f"account changed {old} -> {new_login}: per-account state reset "
                                    f"(positions meta, bursts, risk session)")
            self.pos_state.clear()
            self.bursts = {}
            self.saved_pos_meta = {}
            self._closed_tickets = {}
            self._pe_used = {}
            self.skip_counts = {}
            self.guardian.view = {}
            self.risk.reset_session(float(self.account.get("equity", 0) or 0))
            self.batch = {}
        self.saved_login = new_login
        try:
            self._save_state()
        except OSError:
            pass

    def cmd_start(self) -> dict:
        """Idempotent START. Re-reads everything from the broker before trading and
        rebuilds tick buffers from live ticks only (no stale signals)."""
        if self.running:
            return {"ok": True, "msg": "already running", "batch": self.batch.get("id")}
        if self.stopping or self.flatten_pending:
            return {"ok": False, "msg": "STOP is still closing the bot's positions - wait until flat, then START "
                                        "(two actions must never run against each other)"}
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
        self.mtf = {}
        self._candle_jobs = {}
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
        self._handoff_done = False
        why = self._switch_block(creds)
        if why:
            self.add_log("WARNING", why)
            return {"ok": False, "msg": why, "validation": self.validation, "account": self._account_brief()}
        ok, msg = self._connect(creds)
        return {"ok": ok, "msg": msg, "validation": self.validation, "account": self._account_brief(),
                "handoff": bool(self._handoff_done)}

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
        self._account_changed(self.account.get("login", creds.get("login")))
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
            add("Account type", True, f"{mode} - any account type is accepted (no demo/real lock)")
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
            if profit is None:
                profit = self._closed_profit(t, fresh=True)     # a broker-side SL close: re-read the history once
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
            if profit is None:
                b.setdefault("pl_pending", []).append(t)        # exit deal not in the history yet: resolve later
            if not any(x.batch_id == ps.batch_id for x in self.pos_state.values()):
                now = getattr(self, "now_wall", None) or time.time()
                self._last_close_wall[ps.symbol] = now          # the symbol is flat: re-evaluated in this loop
                if b.get("pl_pending"):
                    self._await_pl[ps.batch_id] = (ps, b, now)
                    self.bursts.pop(ps.batch_id, None)
                    continue
                self._finish_basket(ps, b, now)
        if initial and self.own_positions:
            self.add_log("INFO", f"Managing {len(self.own_positions)} existing bot position(s) after reconnect")
        self._save_state()

    def _resolve_pending_pl(self, now_wall: float) -> None:
        """Baskets closed by the broker (SL/trail) whose exit deals were not in the history yet: book the REAL
        P/L as soon as the deals appear (never a guessed number); after 120 s book what is known and flag it."""
        if not self._await_pl or now_wall - self._pl_poll_wall < 0.5:
            return
        self._pl_poll_wall = now_wall
        self._deals_cache = (0.0, self._deals_cache[1])      # re-read the history (max 2 calls/s while pending)
        for gid, (ps, b, t_close) in list(self._await_pl.items()):
            left = []
            for t in b.get("pl_pending", []):
                pr = self._closed_profit(t)
                if pr is None:
                    left.append(t)
                else:
                    b["realized"] = float(b.get("realized", 0.0)) + pr
            b["pl_pending"] = left
            if not left or now_wall - t_close > 120.0:
                if left:
                    b["pl_incomplete"] = list(left)
                del self._await_pl[gid]
                self._finish_basket(ps, b, t_close)

    def _finish_basket(self, ps, b: dict, now: float) -> None:
        """Whole basket is flat and its real P/L known: book it (pacer, stats, learning). NO cooldown."""
        self.pacer.on_close(ps.symbol, b["realized"], now)
        if b["realized"] < 0:
            self.no_up_after_loss[ps.symbol] = float(b.get("lot", ps.volume))   # never size up after a loss
            self.win_streak[ps.symbol] = 0                                      # t47u: a loss resets the compounding
        else:
            self.no_up_after_loss.pop(ps.symbol, None)
            if b["realized"] > 0:
                self.win_streak[ps.symbol] = self.win_streak.get(ps.symbol, 0) + 1
        lb = self.last_burst.get(ps.symbol)
        if lb and lb.get("gid") == ps.batch_id:
            # a failed signal is not re-fired; a winning one - or one stopped at break-even / by the trail - may
            # be re-entered at once while the direction is still valid
            lb["lost"] = b["realized"] < 0 and not b.get("prot")
        self._learn_from(ps, b, now)
        exit_txt = b.get("exit_reason") or (f"broker {b['prot']} stop" if b.get("prot") else "broker SL/close")
        self.perf.append({"ts": now, "symbol": ps.symbol, "side": b["side"], "pl": round(b["realized"], 2),
                          "legs": b.get("legs"), "hold_s": round(now - float(b.get("opened", now)), 1),
                          "exit": exit_txt, "attr": b.get("attr") or {},
                          "spread_pts": b.get("spread_pts"), "slip_pts": b.get("slip_pts") or [],
                          "reentry_s": b.get("reentry_s"), "tickets": b.get("tickets") or [],
                          "m1_bar": b.get("m1_bar"), "planned": b.get("planned"), "score_q": b.get("score_q"),
                          "risk": round(float(b.get("risk", 0.0)), 2),
                          "protect": b.get("prot") or "none", "trail_moves": int(b.get("trail_moves", 0)),
                          "pl_incomplete": b.get("pl_incomplete"), "precision": b.get("plan")})
        c = self.bar_cycles.get(ps.symbol)
        if c is not None and b.get("m1_bar") == c.get("bar"):
            c["closed"] += 1
        self.add_log("INFO", f"GROUP {ps.batch_id} {b['side']} {ps.symbol} fully closed: "
                             f"P/L {b['realized']:+.2f} over {b.get('legs', '?')} order(s) [{exit_txt}]"
                             + (f" - exit deal(s) {b['pl_incomplete']} not in the broker history yet (P/L incomplete)"
                                if b.get("pl_incomplete") else ""))
        self._decision(ps.symbol, b["side"], None, f"group closed P/L {b['realized']:+.2f}")
        self.bursts.pop(ps.batch_id, None)

    def _deals(self, max_age: float = 1.0) -> list:
        """Broker deal history (last 2 days), cached so a batch close needs ONE history call, not one per order."""
        ts, deals = self._deals_cache
        if time.time() - ts > max_age:
            now = time.time() + self.server_offset
            deals = self.broker.deals(now - 2 * 86400, now + 86400)
            self._deals_cache = (time.time(), deals)
        return deals

    def _closed_profit(self, ticket: int, fresh: bool = False) -> Optional[float]:
        """Real closed P/L of a position from the broker deals, or None while its EXIT deal is not there yet."""
        rel = [d for d in self._deals(max_age=0.3 if fresh else 1.0) if int(d.get("position_id", 0)) == ticket]
        if not rel or not any(int(d.get("entry", -1)) != DEAL_ENTRY_IN for d in rel):
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
                mt = self.mtf.setdefault(s, MTF())
                off = self._candle_off()
                for t, bid, ask, msc in ticks:
                    eng.on_tick(t, bid, ask)
                    mt.on_tick(t - off, bid)          # candles are bid bars on the UTC clock
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

    def _candle_off(self) -> float:
        fn = getattr(self.broker, "candle_offset", None)
        try:
            return float(fn()) if fn else 0.0
        except Exception:  # noqa: BLE001
            return 0.0

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

    def _ind_view(self, s: str, mt, price: float):
        """Indicator confluence (indicators.py) from the real closed M1 bars, recomputed once per new closed bar
        (cached), plus the live price."""
        bars = mt.m5.bars if len(mt.m5.bars) >= 30 else mt.m1.bars      # t34u: indicators on the M5 frame
        if not bars:
            return None
        key = (len(bars), bars[-1]["time"], bars[-1]["close"])
        c = self._ind_cache.get(s)
        if c is None or c[0] != key:
            c = (key, ind.compute(list(bars)))
            self._ind_cache[s] = c
        return ind.view(c[1], price, getattr(mt.m1, "live", None))

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
            mt = self.mtf.get(s)
            if mt is not None:
                m.mtf = mt.view(now - self._candle_off())
                if cfg.ind_enabled and m.ready:
                    m.ind = self._ind_view(s, mt, m.mid)
                    m.ind_w = self.learner.ind_weights(s) if cfg.learning_enabled else None
                if cfg.zones_enabled:
                    try:
                        m.zones = zones.analyze(list(mt.m5.bars), list(mt.m1.bars), getattr(mt.m1, "live", None),
                                                m.mid, m.atr, cfg)
                    except Exception:  # noqa: BLE001 - context only, never blocks the engine
                        m.zones = None
                if cfg.regime_enabled:
                    try:
                        adx = float(getattr(m.ind, "adx", 0.0) or 0.0) if getattr(m, "ind", None) is not None else 0.0
                        m.trend = regime.analyze(list(mt.m5.bars), getattr(mt.m5, "live", None), adx, cfg)
                        key = (m.trend.state, m.trend.phase)
                        if m.trend.ready and self._regime_last.get(s) != key:
                            self._regime_last[s] = key
                            self.add_log("INFO", f"REGIME {s}: {m.trend.label()} [{m.trend.why}]")
                    except Exception:  # noqa: BLE001
                        m.trend = None
                if cfg.precision_enabled:
                    try:
                        m.prec = self._prec.view(s, mt.m5.bars, getattr(mt.m5, "live", None), cfg)
                    except Exception:  # noqa: BLE001 - no structure view = no entry (decide() reports it), never a crash
                        log.exception("precision analysis failed for %s", s)
                        m.prec = None
            st = decide(m, cfg)
            add = self.pacer.threshold_add(s)
            if add and st.side != "NO TRADE":
                best = st.buy if st.side == "BUY" else st.sell
                if best < st.threshold + add:
                    st.side, st.reason = "NO TRADE", (f"loss streak raises the bar: {best:.2f} < "
                                                      f"{st.threshold:.2f}+{add:.2f}")
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
        return {"signal": st.side if st.side in ("BUY", "SELL") else "HOLD", "score": round(st.score, 3),
                "reason": st.reason, "features": m.brief() if m.ready else {"reason": m.reason},
                "strength": {"buy": st.buy, "sell": st.sell, "threshold": round(st.threshold, 3),
                             "scan": st.scan, "vetoes": st.vetoes[:3], "regime": m.regime if m.ready else None,
                             "comps": st.comps},
                "spread_pts": round(m.spread / float(info["point"]), 1) if info.get("point") and m.spread else None,
                "tick_age": round(now_wall - self.last_receipt[s], 1) if s in self.last_receipt else None,
                "mtf": m.mtf.brief() if getattr(m, "mtf", None) is not None else None,
                "ind": m.ind.brief() if getattr(m, "ind", None) is not None else None,
                "zones": m.zones.brief() if getattr(m, "zones", None) is not None else None,
                "trend": m.trend.brief() if getattr(m, "trend", None) is not None else None,
                "precision": ({"structure": m.prec.brief() if getattr(m, "prec", None) is not None else None,
                               "pe": st.pe, "plans": {k: v.brief() for k, v in st.plans.items()}}
                              if self.cfg.precision_enabled else None),
                "m1_cycles": self._cycles(s, now_wall)}

    def _m1_bar(self, s: str, now_wall: float) -> float:
        t = self._server_now(s, now_wall) - self._candle_off()
        return t - (t % 60.0)

    def _cycles(self, s: str, now_wall: float) -> dict:
        """Entry/exit cycles in the CURRENT M1 bar (no per-candle limit: shown, never used as a gate)."""
        bar = self._m1_bar(s, now_wall)
        c = self.bar_cycles.get(s)
        if not c or c["bar"] != bar:
            c = self.bar_cycles[s] = {"bar": bar, "opened": 0, "closed": 0}
        return c

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
            mine = [ps for ps in self.pos_state.values() if ps.symbol == s and ps.ticket not in self._closed_tickets]
            if mine and cfg.no_stop_mode and self._net(mine, {int(p["ticket"]): p for p in self.own_positions}) < 0:
                g0 = self.guardian.view.get(s) or {}
                status["reason"] = f"LOSS GUARDIAN: {g0.get('action', 'watching')} (net {g0.get('total', 0):+.2f})"
                continue                                   # t42u: the guardian manages red baskets
            if mine and cfg.no_stop_mode and len({ps.side for ps in mine}) > 1:
                status["reason"] = (f"recovery cycle: {len(mine)} legs on both sides - closes together once the "
                                    f"combined net is in profit")
                continue
            bar = int(float(getattr(m, "t", 0.0) or 0.0) // 60)
            bar_side = ""
            if cfg.m1_only and cfg.m1_every_bar and st.bar_side and bar and self._bar_entry.get(s) != bar:
                bar_side = st.bar_side                      # t46u: a new M1 bar without an entry yet
            if mine:
                g = self.bursts.get(mine[0].batch_id) or {}
                if self._ladder_step(s, mine, g, m, st, info, status, now_wall, t_dec):
                    continue
                why_not = self._add_check(s, mine, g, m, st, now_wall)
                status["reason"] = (f"managing {mine[0].side} basket {mine[0].batch_id} ({len(mine)} order(s), "
                                    f"net {g.get('net_px', 0.0):+.5g}, protection {g.get('prot') or 'hard stop'}, "
                                    f"momentum {g.get('momentum', '-')}) - add: {why_not or 'now'}")
                if why_not is None and not self.exec_halt and self.pacer.can_enter(s, now_wall)[0]:
                    if self._add_to_basket(s, mine, g, m, st, info, status, now_wall, t_dec):
                        self._bar_entry[s] = bar
                        continue
                # t46u: every-M1-bar entry on top of open baskets ONLY when they are all the same side as the trend side,
                # in net profit together (never averaging down, never a hedge); it is a new basket with its own SL
                live_now = {int(p["ticket"]): p for p in self.own_positions}
                if not (bar_side and hedging and {ps.side for ps in mine} == {bar_side}
                        and self._net(mine, live_now) > 0 and not self.exec_halt):
                    continue
            if st.side == "NO TRADE" and not bar_side:
                self._skip(s, status, st.reason, now_wall)
                continue
            if st.side == "NO TRADE" or (mine and st.side != mine[0].side):
                st = dc_replace(st, side=bar_side, reason=st.bar_reason)      # t46u: the M1 bar entry
            if self.exec_halt:
                self._skip(s, status, "execution protection: close failures - new entries halted", now_wall)
                continue
            side = st.side
            sig = Signal(side, st.score, st.reason, eng.last)
            if not hedging and any(p["symbol"] == s for p in self.foreign_positions):
                self._skip(s, status, "netting account: symbol already has a (manual) position", now_wall, sig)
                continue
            if cfg.avoid_foreign_conflict and hedging:
                opp = [p for p in self.foreign_positions if p["symbol"] == s and self._side(p) != side]
                if opp:
                    self._skip(s, status, f"{side} skipped: another bot / manual position ({self._side(opp[0])} #{opp[0].get('ticket')}) "
                                          f"is open on {s} - the bot never trades against positions that are not its own", now_wall, sig)
                    continue
            ok, why = self.pacer.can_enter(s, now_wall)
            if not ok:
                self._skip(s, status, f"{side} blocked: {why}", now_wall, sig)
                continue
            if getattr(st, "ip_key", ()) and (s, tuple(st.ip_key)) in self._ip_used:
                self._skip(s, status, f"{side} skipped: this pullback was already entered (one entry per pullback) - waiting for "
                                      f"the next impulse", now_wall, Signal(side, st.score, st.reason, eng.last))
                continue
            pl = st.plans.get(side) if cfg.precision_enabled else None
            if pl is not None and (s, pl.key) in self._pe_used:
                self._skip(s, status, f"{side} skipped: this setup ({pl.kind} {pl.key}) was already entered - one entry per "
                                      f"structure event, the next one needs a new break of structure", now_wall, sig)
                continue
            if self.exec_pause.get(s, 0.0) > now_wall:
                self._skip(s, status, f"{side} blocked: execution failures - paused "
                                      f"{self.exec_pause[s] - now_wall:.0f}s", now_wall, sig)
                continue
            sl_hist = [v for (ts_, v) in self.slippage.get(s, ()) if now_wall - ts_ <= cfg.slippage_window_s]
            if len(sl_hist) >= 3 and sum(sl_hist) / len(sl_hist) > cfg.max_avg_slippage_vu:
                self._skip(s, status, "paused: poor execution quality (slippage)", now_wall, sig)
                continue
            last = self.last_burst.get(s)
            if (last and last.get("rejected")
                    and now_wall - float(last.get("wall", 0.0)) < 3.0):     # t35u: any side - a reject is not retried
                # t34u: only a signal the BROKER REJECTED is not re-sent for 3 s (no blind resend). A losing basket's
                # signal no longer locks that side until the swing changes (that held re-entry back 25-40 s live)
                self._skip(s, status, f"{side} skipped: the broker rejected an order on {s} <3 s ago - not re-sent blindly",
                           now_wall, sig)
                continue
            if last and float(last.get("tick_t", -1.0)) == m.t:
                self._skip(s, status, f"{side} skipped: duplicate - same tick", now_wall, sig)
                continue
            learn = None
            if not cfg.learning_enabled:
                # t39u: the learner has NO influence on trading (no skip, no size change, no weights, no exits);
                # closed trades are still recorded for reporting only
                learn = SimpleNamespace(bucket=self._setup_key(s, side, m), allow=True, min_lot=False, mult=1.0, state="record-only",
                                        why="learning: record only (no influence)")
                self.learn_view[s] = learn.why
            else:
                bucket = self._setup_key(s, side, m)
                learn = self.learner.decide(bucket, now_wall)
                learn.bucket = bucket
                self.learn_view[s] = learn.why
                if not learn.allow:
                    # t34u: the learner sizes, it never locks entries - a losing setup trades the min lot
                    learn.allow, learn.min_lot = True, True
                    learn.why += " -> min lot (no skip)"
            rev = self._reverse_to.pop(s, None)
            if self._open_batch(s, side, m, st, info, status, now_wall, t_dec, learn):
                self._bar_entry[s] = bar
                if rev:
                    g = self.bursts.get(self.last_burst.get(s, {}).get("gid", ""), {})
                    g["reversal"] = True
                    self.reversals += 1

    def _ladder_step(self, s: str, legs: list, g: dict, m, st, info: dict, status: dict, now_wall: float,
                     t_dec: float) -> bool:
        """t46u PULLBACK LADDER: send the next tranche of the basket's reserved orders when price has pulled back the next
        step (same lot, the basket's own structural stop - never widened, so each later order risks less). Dropped when the M1
        trend turned against the basket, the stop is too close, protection already moved the stop (basket is in profit), the
        ladder expired, or the order caps say no. True = orders were sent."""
        lad = g.get("ladder")
        if not lad or lad.get("left", 0) <= 0 or not getattr(m, "ready", False):
            return False
        cfg = self.cfg
        side = legs[0].side
        d = 1 if side == "BUY" else -1

        def drop(why: str) -> bool:
            g.pop("ladder", None)
            self.add_log("INFO", f"LADDER basket {legs[0].batch_id} {side} {s} dropped ({lad.get('left', 0)} order(s) not sent): {why}")
            return False
        if lad.get("side") != side or not cfg.ladder_enabled:
            return drop("side / setting changed")
        if now_wall > lad["exp"]:
            return drop(f"expired after {cfg.ladder_ttl_s:g}s without the pullback")
        if g.get("prot"):
            return drop(f"basket already protected ({g['prot']}) - no more orders into it")
        v = m.mtf
        if v is not None and v.m1.ready and d * v.bias <= -cfg.hold_flip_bias:
            return drop(f"M1 trend turned against the basket (bias {v.bias:+.2f})")
        ref_px = m.ask if side == "BUY" else m.bid
        trigger = lad["ref"] - d * lad["step"] * (lad["done"] + 1)
        if (ref_px - trigger) * d > 0:
            return False                                      # not pulled back far enough yet
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl = float(lad["sl"])
        if (ref_px - sl) * d < stops + 1.5 * m.spread:
            return drop("price is too close to the stop")
        if self.exec_halt or not self.pacer.can_enter(s, now_wall)[0]:
            return False
        left_levels = max(1, lad["levels"] - lad["done"])
        n = max(1, -(-lad["left"] // left_levels))             # ceil: spread the rest over the remaining steps
        lot = float(lad["lot"])
        ts, tv = self._tv(s)
        sl_dist = abs(ref_px - sl)
        risk = sl_dist / ts * tv * lot if ts and tv else 0.0
        size = SizeResult(lot, risk, "pullback ladder (same lot, shared stop)")
        gid = legs[0].batch_id
        plans: list = []
        for i in range(n):
            ok, why, margin = self._check_order(s, side, len(legs) + i, size, m, False, plans, add=True, ladder=True)
            if not ok:
                if not plans:
                    status["reason"] = f"ladder blocked: {why}"
                    return False
                break
            plans.append((i, margin))
        dev = self._deviation(s, m.spread, m.vu)
        opened = 0
        self.pacer.inflight.add(s)
        try:
            for i, _ in plans:
                r, t_send, t_fill = self._send_raw(s, side, lot, sl, dev, f"{cfg.comment_prefix}-{gid}-l{len(legs) + i + 1}")
                if self._book_fill(s, side, len(legs) + i, len(legs) + len(plans), gid, size, sl, sl_dist, m, st, info,
                                   now_wall, t_dec, r, t_send, t_fill, False):
                    opened += 1
                else:
                    break
        finally:
            self.pacer.inflight.discard(s)
        self.pacer.on_entry(s, now_wall)
        lad["done"] += 1                                       # the step is consumed even if the caps trimmed it
        lad["left"] = max(0, lad["left"] - opened) if opened else lad["left"]
        if lad["done"] >= lad["levels"]:
            lad["left"] = 0
        if opened:
            g["peak_pl"] = 0.0                                 # the average price moved - restart the peak on the new basket
            self.add_log("INFO", f"LADDER fill basket {gid} {side} {s}: +{opened} order(s) x {lot:g} at {ref_px:.5g} "
                                 f"(step {lad['done']}/{lad['levels']}, stop {sl:.5g} unchanged, {lad['left']} left) "
                                 f"- {len(legs) + opened} order(s) now")
            self._decision(s, side, st.score, f"LADDER {opened} to basket {gid}")
            self._refresh_account()
            self._save_state()
        return opened > 0

    def _add_check(self, s: str, legs: list, g: dict, m, st, now_wall: float) -> Optional[str]:
        """Pyramid ADD (None = add now, else why not): only into a basket whose stop is already at break-even or
        trailing (never averaging down), after a shallow pullback from the best price when price RESUMES in the
        trade direction and the signal for that side is valid again. Same lot, no multiplier, legs <= the cap."""
        cfg = self.cfg
        if not cfg.add_enabled:
            return "adds disabled"
        side = legs[0].side
        d = 1 if side == "BUY" else -1
        if not getattr(m, "ready", False) or not m.atr:
            return "no data"
        ext = float(g.get("ext_px", m.mid) or m.mid)
        depth = d * (ext - m.mid)
        if depth >= cfg.add_pullback_atr * m.atr:
            g["pb_depth"] = max(float(g.get("pb_depth", 0.0)), depth)
        pb = float(g.get("pb_depth", 0.0))
        if not g.get("prot"):
            return "waits for the break-even stop (no averaging down)"
        if len(legs) >= min(cfg.max_orders_per_batch, cfg.max_positions_per_symbol):
            return f"basket full ({len(legs)} orders)"
        if pb <= 0:
            return "no pullback yet"
        if not (d * m.vel > 0 and depth <= 0.6 * pb):
            return f"pullback {pb:.5g} - waiting for price to resume"
        if st.side != side:
            return "signal not valid for that side now"
        if now_wall - float(g.get("last_add", 0.0)) < 1.0:
            return "just added"
        return None

    def _add_to_basket(self, s: str, legs: list, g: dict, m, st, info: dict, status: dict, now_wall: float,
                       t_dec: float) -> bool:
        cfg = self.cfg
        side = legs[0].side
        d = 1 if side == "BUY" else -1
        gid = legs[0].batch_id
        g["last_add"] = now_wall
        g["pb_depth"] = 0.0
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        ref = m.ask if side == "BUY" else m.bid
        sl_dist = max(stops, cfg.sl_atr * m.atr, cfg.sl_spread_mult * m.spread)
        sl = ref - d * sl_dist
        lvl = float(g.get("prot_level") or 0.0)
        if lvl and d * (lvl - sl) > 0 and d * (ref - lvl) >= stops + m.spread:
            sl = lvl                                      # the add shares the basket's break-even/trail stop
        else:
            # t32u (6): an add WITHOUT the shared BE/trail stop would be fresh risk on a winning basket -> skip
            status["reason"] = "add skipped: the shared break-even/trail stop cannot be set this close to price"
            return False
        if not cfg.broker_sl:
            sl = 0.0
        lot = float(g.get("lot") or legs[0].volume)
        ts, tv = self._tv(s)
        risk = abs(ref - (sl or ref - d * sl_dist)) / ts * tv * lot if ts and tv else 0.0
        size = SizeResult(lot, risk, "pyramid add (same lot)")
        n = max(1, min(cfg.add_max_per_step, min(cfg.max_orders_per_batch, cfg.max_positions_per_symbol) - len(legs)))
        plans: list = []
        for i in range(n):
            ok, why, margin = self._check_order(s, side, len(legs) + i, size, m, False, plans, add=True)
            if not ok:
                if not plans:
                    status["reason"] = f"add blocked: {why}"
                    return False
                break
            plans.append((i, margin))
        dev = self._deviation(s, m.spread, m.vu)
        opened = 0
        self.pacer.inflight.add(s)
        try:
            for i, _ in plans:
                r, t_send, t_fill = self._send_raw(s, side, lot, sl, dev, f"{cfg.comment_prefix}-{gid}-a{len(legs) + i + 1}")
                if self._book_fill(s, side, len(legs) + i, len(legs) + len(plans), gid, size, sl, abs(ref - sl) if sl else sl_dist,
                                   m, st, info, now_wall, t_dec, r, t_send, t_fill, False):
                    opened += 1
                else:
                    break
        finally:
            self.pacer.inflight.discard(s)
        self.pacer.on_entry(s, now_wall)
        if opened:
            g["adds"] = int(g.get("adds", 0)) + opened
            g["peak_pl"] = 0.0          # t32u (3): the average price moved - restart the peak on the new basket
            self.add_log("INFO", f"ADD basket {gid} {side} {s}: +{opened} order(s) x {lot:g} after a pullback, price "
                                 f"resumed with the M1 trend (stop shared BE/trail) "
                                 f"- {len(legs) + opened} order(s) now")
            self._decision(s, side, st.score, f"ADD {opened} to basket {gid}")
            self._refresh_account()
            self._save_state()
        return opened > 0

    def _sl_distance(self, side: str, m, stops: float) -> tuple:
        """t32u STRUCTURE stop distance: beyond the last M1 swing low (BUY) / swing high (SELL) + spread +
        buffer, clamped to [sl_min_atr, sl_max_atr] x ATR and never inside the stops level or 3x spread. Falls back
        to the ATR stop when no swing is on the right side of price."""
        cfg = self.cfg
        d = 1 if side == "BUY" else -1
        atr = m.atr or 0.0
        base = max(stops, cfg.sl_atr * atr, cfg.sl_spread_mult * m.spread)
        if not cfg.sl_structure or atr <= 0:
            return base, "ATR"
        ref = m.ask if d > 0 else m.bid
        v = getattr(m, "mtf", None)
        lvl = (v.m1.swing_lo if d > 0 else v.m1.swing_hi) if v is not None and v.m1.ready else 0.0
        lo = max(stops, 3.0 * m.spread, cfg.sl_min_atr * atr)
        hi = max(lo, cfg.sl_max_atr * atr)
        if lvl and d * (ref - lvl) > 0:
            dist = d * (ref - lvl) + m.spread + cfg.sl_struct_buffer_atr * atr
            if dist <= hi:
                return max(lo, dist), "structure (beyond the last M1 swing)"
            return hi, "ATR cap (last swing too far)"
        return max(lo, min(hi, base)), "ATR (no swing on that side)"

    @staticmethod
    def _setup_key(s: str, side: str, m) -> str:
        """Learning bucket = the SETUP: symbol | side vs the M1 main trend (+ swing structure) | tape | spread."""
        v = getattr(m, "mtf", None)
        d = 1 if side == "BUY" else -1
        if v is None or not v.m1.ready:
            rel = "trend?"
        else:
            b = d * v.bias
            rel = "with" if b >= 0.2 else "counter" if b <= -0.2 else "flat"
            rel += {1: "+swing", -1: "-swing", 0: ""}[d * v.m1.swing]
        return f"{s}|{rel}|{m.regime}|{vol_regime(m.spread, m.atr)}"

    def _open_batch(self, s: str, side: str, m, st, info: dict, status: dict, now_wall: float, t_dec: float,
                    learn=None) -> bool:
        """ONE burst batch for ONE signal: n same-lot market orders, each fill verified. n grows with the
        signal's edge over the threshold (capped); batch risk/lot caps apply; min-lot-floor accounts trade one
        order at a time. With a thread-safe broker the orders go out in parallel (one round trip for the
        batch); otherwise back to back, the rest cancelled when price runs past the edge or the spread widens.
        Never martingale, never averaging down, never added to later."""
        cfg = self.cfg
        d = 1 if side == "BUY" else -1
        vmin, vmax, vstep = float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"])
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl_dist, sl_src = self._sl_distance(side, m, stops)
        ip_stop = float(getattr(st, "ip_stop", 0.0) or 0.0)
        if ip_stop and cfg.ip_enabled:
            # t48u: the stop sits just beyond the PULLBACK extreme - if price takes that out the impulse failed (clamped like the
            # structure stop: never inside the broker stops level / 3x spread, never wider than sl_max_atr)
            ref_ip = m.ask if side == "BUY" else m.bid
            atr_ip = m.atr or self._atr(s)[0] or (sl_dist / max(cfg.sl_atr, 0.1))
            if atr_ip > 0 and d * (ref_ip - ip_stop) > 0:
                lo_ip = max(stops, 3.0 * m.spread, cfg.sl_min_atr * atr_ip)
                dist_ip = d * (ref_ip - ip_stop) + m.spread + cfg.sl_struct_buffer_atr * atr_ip
                sl_dist, sl_src = max(lo_ip, min(max(lo_ip, cfg.sl_max_atr * atr_ip), dist_ip)), "beyond the pullback extreme (impulse failed if taken out)"
        ts, tv = self._tv(s)
        acct = self._acct()
        sig = Signal(side, st.score, st.reason, self.engines[s].last)

        def refuse(why: str) -> bool:
            self._skip(s, status, f"{side} batch blocked: {why}", now_wall, sig)
            return False

        plan = st.plans.get(side) if (cfg.precision_enabled and getattr(st, "plans", None)) else None
        if plan is not None:
            # t45u: the stop is the STRUCTURAL INVALIDATION of this setup (never a fixed distance, never widened to
            # "give it room"); stop distance, lot and reward-to-risk are re-checked on the live price before sending
            ref0 = m.ask if side == "BUY" else m.bid
            atr5 = float(m.prec.core.atr) if getattr(m, "prec", None) is not None and m.prec.ready else (m.atr or 0.0)
            dist = (ref0 - plan.sl) * d
            if dist <= 0:
                return refuse("price is already beyond the structural invalidation level")
            dist = max(dist, stops, 3.0 * m.spread)          # broker minimum only: the nearest placeable stop beyond it
            if atr5 > 0 and dist > cfg.pe_max_sl_atr * atr5 * 1.05:
                return refuse(f"structural stop {dist:.5g} > {cfg.pe_max_sl_atr:g} ATR(M5) {atr5:.5g} - not widened, skipped")
            reward = (plan.target - ref0) * d - m.spread
            if reward <= 0 or reward / dist < cfg.pe_rr_min:
                return refuse(f"reward-to-risk {max(reward, 0.0) / dist:.2f} < {cfg.pe_rr_min:g} to the Target LIQ "
                              f"{plan.target:.5g} (stop {dist:.5g}) at the live price")
            sl_dist, sl_src = dist, f"structural invalidation ({'+'.join(plan.tags)}; {plan.kind})"

        # the SCORE drives the order count and the risk: sq = edge over the threshold (0..1)
        strength = st.buy if side == "BUY" else st.sell
        sq = max(0.0, min(1.0, (strength - st.threshold) / max(1e-6, 1.0 - st.threshold)))
        n_min = max(1, min(cfg.min_orders_per_batch, cfg.max_orders_per_batch))
        if cfg.full_batch:
            n = cfg.max_orders_per_batch            # t37u: FULL batch from the first entry - capped only by stop-risk,
        else:                                       # margin and exposure limits below (no score scaling)
            n = max(n_min, 1 + int(round(7 * sq)))
            if m.regime in ("quiet", "chop"):
                n = min(n, max(2, n_min))           # weak tape: small baskets only
        mult = float(getattr(learn, "mult", 1.0) or 1.0) if learn is not None else 1.0
        if mult > 1.0:
            n = int(round(n * mult))                # learning: a proven setup gets more orders
        n = max(1, min(n, cfg.max_orders_per_batch, cfg.max_positions_per_symbol))
        probe = self.risk.probe_left > 0
        if probe:
            n = n_min
        risk_scale = 1.0 if cfg.full_batch else min(1.0, (0.6 + 0.4 * sq) * mult)   # weaker edge -> less risk
        per_pct = min(cfg.risk_per_trade_pct, cfg.risk_per_batch_pct * risk_scale / n)
        size = size_position(acct.equity, dc_replace(cfg, risk_per_trade_pct=per_pct), sl_dist, ts, tv, vmin, vmax, vstep)
        if size.volume <= 0:
            return refuse(size.note)
        floor = False
        if cfg.no_stop_mode:
            # t38u: FULL batch at the signal, sized by MARGIN (no per-order stop, owner request): every order the
            # same lot, all sent at once, total margin of all bot positions <= nostop_margin_use_pct of equity
            price0 = m.ask if side == "BUY" else m.bid
            m_min = self.broker.calc_margin(side, s, vmin, price0)
            if not m_min:
                return refuse("margin calc unavailable")
            tot_room = acct.equity * cfg.nostop_margin_use_pct / 100.0 - self._margin_used()
            tot_room = min(tot_room, float(acct.margin_free) * 0.95) if acct.margin_free else tot_room
            room = min(tot_room, acct.equity * cfg.batch_margin_pct / 100.0)     # t40u: one batch = a share
            units = int(max(0.0, room) // m_min + 1e-9)
            if units < 1 and tot_room >= m_min:
                units, room = 1, tot_room                  # small account: one min lot if the TOTAL cap allows it
            if units < 1:
                return refuse(f"no-stop mode: margin room {room:.2f} < one min-lot margin {m_min:.2f} "
                              f"({cfg.nostop_margin_use_pct:g}% margin-use cap keeps a buffer)")
            n = max(1, min(cfg.max_orders_per_batch, cfg.max_positions_per_symbol, units))
            v = normalize_volume(min((units // n) * vmin, cfg.max_batch_lots / n, cfg.max_lot_per_order), vmin, vmax, vstep)
            if v < vmin:
                return refuse(f"no-stop mode: lot {v:g} below the minimum {vmin:g}")
            size = type(size)(v, 0.0, f"no-stop full batch: {n} x {v:g} lots, margin ~{n * v / vmin * m_min:.2f} of "
                                      f"room {room:.2f} (no per-order SL, owner request)")
            n_min = 1
        else:
            floor = size.note.startswith("min lot floor")
            open_risk = sum(self._pos_risk(p) for p in self.own_positions)
            if floor:
                # small account: the min lot already exceeds the per-order share -> as many min lots as fit in the
                # basket stop-risk budget (scaled by the score); at least min_orders_per_batch if the UNSCALED budget
                # holds them
                cap_money = acct.equity * cfg.small_account_batch_risk_pct / 100.0
                k = int((cap_money * risk_scale + 1e-9) // max(size.risk_money, 1e-9))
                k_full = int((cap_money + 1e-9) // max(size.risk_money, 1e-9))
                tot_room = int((acct.equity * cfg.max_total_risk_pct / 100.0 - open_risk + 1e-9) // max(size.risk_money, 1e-9))
                k_full = min(k_full, tot_room)
                if k_full < 1:
                    return refuse(f"min-lot stop risk {size.risk_money:.2f} ({size.risk_money / max(acct.equity, 1e-9):.1%} of "
                                  f"equity) does not fit the basket risk budget {cap_money:.2f} "
                                  f"({cfg.small_account_batch_risk_pct:g}% of equity, open risk {open_risk:.2f})")
                if k_full < n_min:
                    # t32u (5): never a single order - if two min lots do not fit the stop-risk budget, skip the symbol
                    return refuse(f"only {k_full} min lot(s) of stop risk {size.risk_money:.2f} fit the basket budget "
                                  f"{cap_money:.2f} - a basket needs >= {n_min} orders (skip until equity grows)")
                n = max(n_min, min(n, max(k, n_min), k_full))
            if learn is not None and learn.min_lot and size.volume > vmin:
                size = type(size)(vmin, size.risk_money * vmin / size.volume, size.note + "; learning: losing setup, min lot")
            if probe and size.volume > vmin:
                size = type(size)(vmin, size.risk_money * vmin / size.volume, size.note + "; safe-mode probe: min lot")
            if cfg.alloc_enabled and not floor and not probe and not (learn is not None and learn.min_lot):
                # t48u: the lot comes from the PORTFOLIO - the basket may use alloc_margin_pct of equity as margin, split into
                # equal orders; the loss if every stop is hit stays <= risk_per_batch_pct (the shared margin / risk caps below
                # still apply). Lots therefore grow with equity after wins and shrink after losses.
                price_a = m.ask if side == "BUY" else m.bid
                m_min_a = self.broker.calc_margin(side, s, vmin, price_a)
                if m_min_a and size.volume > 0:
                    room_a = min(acct.equity * cfg.alloc_margin_pct / 100.0,
                                 acct.equity * cfg.max_margin_use_pct / 100.0 - self._margin_used(),
                                 float(acct.margin_free) * 0.95 if acct.margin_free else 1e18)
                    v_m = (max(0.0, room_a) / (n * m_min_a)) * vmin
                    per_lot_risk = size.risk_money / size.volume
                    v_r = (acct.equity * cfg.risk_per_batch_pct / 100.0) / (n * per_lot_risk) if per_lot_risk > 0 else v_m
                    v_a = normalize_volume(min(v_m, v_r, cfg.max_lot_per_order, cfg.max_batch_lots / n), vmin, vmax, vstep)
                    if v_a >= vmin and v_a != size.volume:
                        size = type(size)(v_a, per_lot_risk * v_a, f"alloc {cfg.alloc_margin_pct:g}% of equity as margin: {n} x {v_a:g} lot "
                                                                    f"(margin cap {v_m:.3g}, stop-risk cap {v_r:.3g})")
            cap_lot = self.no_up_after_loss.get(s)
            if cap_lot and size.volume > cap_lot:
                size = type(size)(cap_lot, size.risk_money * cap_lot / size.volume, size.note + f"; capped {cap_lot:g} after a loss")
            budget = acct.equity * (cfg.small_account_batch_risk_pct if floor else cfg.risk_per_batch_pct) / 100.0
            streak = self.win_streak.get(s, 0)
            if cfg.compound_enabled and streak > 0 and not cap_lot and not probe and not (learn is not None and learn.min_lot):
                mult_c = min(cfg.compound_max_mult, 1.0 + cfg.compound_step * streak)
                v2 = normalize_volume(size.volume * mult_c, vmin, vmax, vstep)
                if v2 > size.volume:
                    r2 = size.risk_money * v2 / size.volume
                    if n_min * r2 <= budget + 1e-9:          # the stop-risk budget of the batch is never exceeded
                        size = type(size)(v2, r2, size.note + f"; compounded {size.volume:g} -> {v2:g} lot after "
                                                              f"{streak} winning basket(s)")
            while n > n_min and (n * size.volume > cfg.max_batch_lots + 1e-9 or n * size.risk_money > budget + 1e-9):
                n -= 1
            if size.volume > cfg.max_batch_lots + 1e-9:
                v = normalize_volume(cfg.max_batch_lots, vmin, vmax, vstep)
                if v <= 0:
                    return refuse(f"min lot {vmin:g} > max batch lots {cfg.max_batch_lots:g}")
                size = type(size)(v, size.risk_money * v / size.volume, size.note + "; batch lot cap")
            # margin: lots compound with equity up to the margin-use cap (shared by every symbol), keeping a buffer
            price0 = m.ask if side == "BUY" else m.bid
            m_one = self.broker.calc_margin(side, s, size.volume, price0)
            if m_one:
                room = acct.equity * cfg.max_margin_use_pct / 100.0 - self._margin_used()
                if n * m_one > room + 1e-9:
                    v = normalize_volume(size.volume * max(0.0, room) / (n * m_one), vmin, vmax, vstep)
                    if v >= vmin and v < size.volume:
                        size = type(size)(v, size.risk_money * v / size.volume, size.note + "; margin-use cap")
                    elif v < vmin:
                        m_min = m_one * vmin / size.volume
                        n = min(n, int(max(0.0, room) // max(m_min, 1e-12)))
                        if n < n_min:
                            return refuse(f"margin room {room:.2f} fits fewer than {n_min} min-lot orders")
                        if size.volume > vmin:
                            size = type(size)(vmin, size.risk_money * vmin / size.volume, size.note + "; margin-use cap")
        ref = m.ask if side == "BUY" else m.bid
        run_cap = cfg.batch_run_cancel_frac * m.target
        q = self.broker.last_tick(s) or {}
        px = float((q.get("ask") if side == "BUY" else q.get("bid")) or 0.0)
        if px > 0 and (px - ref) * d > run_cap:
            return refuse(f"price ran {(px - ref) * d:.5g} past the edge before send (> {run_cap:.5g}) - anti-chase")
        if px > 0 and (px - ref) * d < -run_cap:
            return refuse(f"price moved {(px - ref) * d:.5g} against the signal before send")
        # validate every order of the batch up front (risk, exposure, margin incl. the batch's own orders)
        plans: list = []
        for i in range(n):
            ok, why, margin = self._check_order(s, side, i, size, m, floor, plans)
            if not ok:
                if len(plans) < n_min:
                    return refuse(why if not plans else f"only {len(plans)} order(s) pass the checks ({why}) - "
                                                        f"no single-order baskets")
                break
            plans.append((i, margin))
        n = len(plans)
        gid = uuid.uuid4().hex[:6]
        sl = (ref - d * sl_dist) if (cfg.broker_sl and not cfg.no_stop_mode) else 0.0   # t38u: no per-order SL
        ladder = None
        atr_l = (m.atr or self._atr(s)[0] or (sl_dist / max(cfg.sl_atr, 0.1))) if cfg.ladder_enabled else 0.0
        if cfg.ladder_enabled and sl and n >= 2 and not floor and atr_l:
            # t46u: only the first tranche goes now; the rest waits for a pullback (same lot, same structural stop)
            step = cfg.ladder_step_atr * atr_l
            levels = min(cfg.ladder_levels, int((0.8 * sl_dist) // step)) if step > 0 else 0
            n_now = max(1, int(round(n * cfg.ladder_first_frac)))
            if levels >= 1 and n_now < n:
                ladder = {"side": side, "ref": ref, "step": step, "levels": levels, "done": 0, "left": n - n_now,
                          "lot": size.volume, "sl": sl, "exp": now_wall + cfg.ladder_ttl_s, "total": n}
                plans, n = plans[:n_now], n_now
        dev = self._deviation(s, m.spread, m.vu)
        opened, why_stop = 0, ""
        self.pacer.inflight.add(s)
        try:
            if bool(getattr(self.broker, "concurrent_io", False)) and n > 1:
                futs = [self._io.submit(self._send_raw, s, side, size.volume, sl, dev, f"{cfg.comment_prefix}-{gid}-{i + 1}")
                        for i, _ in plans]
                for (i, _), fu in zip(plans, futs):
                    r, t_send, t_fill = fu.result()
                    if self._book_fill(s, side, i, n, gid, size, sl, sl_dist, m, st, info, now_wall, t_dec, r, t_send, t_fill, floor):
                        opened += 1
                    else:
                        why_stop = why_stop or str(r.get("msg"))
            else:
                for i, _ in plans:
                    if i > 0:
                        q = self.broker.last_tick(s) or {}
                        px = float((q.get("ask") if side == "BUY" else q.get("bid")) or 0.0)
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
                    r, t_send, t_fill = self._send_raw(s, side, size.volume, sl, dev, f"{cfg.comment_prefix}-{gid}-{i + 1}")
                    if not self._book_fill(s, side, i, n, gid, size, sl, sl_dist, m, st, info, now_wall, t_dec, r, t_send, t_fill, floor):
                        why_stop = why_stop or "order not filled"
                        break
                    opened += 1
        finally:
            self.pacer.inflight.discard(s)
        self.pacer.on_entry(s, now_wall)
        if opened == 0:
            # the broker rejected this setup: never resend the SAME unchanged signal (no retry loop)
            self.last_burst[s] = {"side": side, "origin": st.origin, "tick_t": m.t, "rejected": True, "wall": now_wall,
                             "level": m.micro_hi if side == "BUY" else m.micro_lo}
            return False
        g = self.bursts[gid]
        g["planned"], g["score_q"] = (ladder["total"] if ladder else n), round(sq, 2)
        if ladder:
            g["ladder"] = ladder
            self.add_log("INFO", f"LADDER basket {gid} {side} {s}: {opened} order(s) now, {ladder['left']} more at "
                                 f"{ladder['levels']} pullback step(s) of {ladder['step']:.5g} below {ref:.5g} "
                                 f"(same lot {ladder['lot']:g}, same stop {sl:.5g}, total <= batch budget)")
        self.last_burst[s] = {"side": side, "origin": st.origin, "tick_t": m.t, "gid": gid,
                             "level": m.micro_hi if side == "BUY" else m.micro_lo}
        if probe:
            self.risk.probe_left -= 1
        if floor:
            self.risk.floor_risk = max(self.risk.floor_risk, opened * size.risk_money)
        v = m.mtf
        v1 = v.m1 if v is not None and v.m1.ready else None
        fam = st.scan.get(side, {})
        g["attr"] = {
            "trend": ("n/a" if v is None or not v.m5.ready else "aligned" if d * v.m5_bias >= 0.2
                      else "against" if d * v.m5_bias <= -0.2 else "neutral") if v is not None and v.m5.ready else
                     ("n/a" if v is None or not v.m1.ready else "aligned" if d * v.bias >= 0.2
                   else "against" if d * v.bias <= -0.2 else "neutral"),
            "macd": ("n/a" if v1 is None else "expanding with" if d * v1.hist_slope > 0.03
                     else "turning against" if d * v1.hist_slope < -0.03 else "flat"),
            "volume": ("n/a" if v1 is None or v1.vol_x is None else "expanding" if v1.vol_x >= 1.3
                       else "low" if v1.vol_x < 0.8 else "normal"),
            "tick": "strong" if (fam.get("A") or 0) >= 0.6 else "moderate",
            "agree": "".join(k for k, x in fam.items() if x is not None and x >= cfg.mb_scan_support),
            "regime": m.regime}
        g["sb0"] = v.m1.sbreak if v is not None and v.m1.ready else 0       # M1 swing state at entry
        g["spread_pts"] = round(m.spread / point, 1) if point else None
        g["m1_bar"] = self._m1_bar(s, now_wall)
        self._cycles(s, now_wall)["opened"] += 1
        last_close = self._last_close_wall.pop(s, None)
        if last_close is not None:
            g["reentry_s"] = round(now_wall - last_close, 2)
            self.reentry.append(g["reentry_s"])
        if learn is not None:
            comps = dict(st.comps.get(side, {}))
            comps["spread_atr"] = round(m.spread / m.atr, 3) if m.atr else None
            g.update(bucket=getattr(learn, "bucket", ""), type="micro", rel=m.regime,
                     regime=vol_regime(m.spread, m.atr), hour=time.gmtime(now_wall).tm_hour,
                     learn_state=learn.state, comps=comps)
        iv = getattr(m, "ind", None)
        if iv is not None and getattr(iv, "ready", False):
            g["ind_votes"] = {k: round(d * x, 3) for k, x in iv.votes.items()}      # toward the trade side
        g["sl_src"] = sl_src
        if getattr(st, "ip_key", ()) and cfg.ip_enabled:
            g["ip"] = {"key": list(st.ip_key), "stop": float(st.ip_stop), "start": float(st.ip_info.get("start", 0.0)),
                       "end": float(st.ip_info.get("end", 0.0)), "retr": round(float(st.ip_info.get("retr", 0.0)), 3),
                       "t0": now_wall, "best": 0.0}
            self._ip_used[(s, tuple(st.ip_key))] = now_wall
            if len(self._ip_used) > 500:
                for k_ in sorted(self._ip_used, key=self._ip_used.get)[:200]:
                    self._ip_used.pop(k_, None)
            self.add_log("INFO", f"IMPULSE-PULLBACK {gid} {side} {s}: leg {st.ip_info.get('start', 0):.5g} -> {st.ip_info.get('end', 0):.5g}, "
                                 f"pullback {float(st.ip_info.get('retr', 0)):.0%}, stop beyond {st.ip_stop:.5g}")
        if plan is not None:
            g["plan"] = {"key": plan.key, "kind": plan.kind, "tags": list(plan.tags), "pe": round(plan.score, 3),
                         "inval": plan.inval, "sl": plan.sl, "target": plan.target, "target_kind": plan.target_kind,
                         "rr": round(plan.rr, 2), "risk": plan.risk, "depth": round(plan.depth, 3),
                         "entry": plan.entry, "path": plan.path, "atr": float(m.prec.core.atr) if getattr(m, "prec", None) is not None else m.atr,
                         "zone": list(plan.zone), "origin": plan.origin, "extreme": plan.extreme,
                         "mae": 0.0, "mfe": 0.0, "t0": now_wall}
            self._pe_used[(s, plan.key)] = now_wall
            if len(self._pe_used) > 500:
                for k_ in sorted(self._pe_used, key=self._pe_used.get)[:200]:
                    self._pe_used.pop(k_, None)
            self.add_log("INFO", f"PRECISION {gid} {side} {s}: {plan.kind} pullback {plan.depth:.2f} of the leg [{'+'.join(plan.tags)}] "
                                 f"PE {plan.score:.2f} | stop {plan.sl:.5g} (invalidation {plan.inval:.5g}) | Target LIQ "
                                 f"{plan.target_kind} {plan.target:.5g} R:R {plan.rr:.1f}")
        tv_ = getattr(m, "trend", None)
        g["regime"] = tv_.label() if tv_ is not None else "-"
        msg = (f"BATCH {gid} {side} {s}: {opened}/{n} order(s) filled, {opened * size.volume:g} lots | {g['regime']} | M5 {g['attr']['trend']}, "
               f"M1 MACD {g['attr']['macd']}, volume {g['attr']['volume']}, agree {g['attr']['agree']}"
               + (f" | re-entry {g['reentry_s']:.2f}s after the last basket closed" if "reentry_s" in g else ""))
        if opened < n:
            msg += f" - rest cancelled: {why_stop}"
        self.add_log("INFO", msg)
        self._decision(s, side, st.score, f"OPENED batch {gid} {opened}/{n} x {size.volume:g} ({m.regime})")
        status["reason"] = f"basket {gid}: {opened} order(s) {side}"
        self._refresh_account()
        self._save_state()
        return True

    def _check_order(self, s, side, i, size, m, floor, plans, add: bool = False, ladder: bool = False) -> tuple:
        """Risk / exposure / margin check for order i of a batch, counting the batch's earlier orders."""
        cfg = self.cfg
        acct = self._acct()
        price = m.ask if side == "BUY" else m.bid
        margin = self.broker.calc_margin(side, s, size.volume, price)
        if margin is None:
            return False, "margin calc unavailable", 0.0
        open_pos = [OpenPos(p["symbol"], self._side(p), float(p["volume"]), float(p["profit"]), self._pos_risk(p),
                            self._notional(p["symbol"], float(p["volume"]), float(p.get("price_current") or p["price_open"])))
                    for p in self.own_positions]
        open_pos += [OpenPos(s, side, size.volume, 0.0, size.risk_money, self._notional(s, size.volume, price))
                     for _ in plans]
        ok, why = self.risk.check_entry(s, side, open_pos, acct, size.volume, size.risk_money, margin, True,
                                        self._notional(s, size.volume, price), batch_leg=(i > 0 and not add)
                                        or (add and bool(plans)) or ladder, floor_mode=floor)   # t46u: a ladder leg is a planned leg of its batch
        if ok:
            cap = acct.equity * cfg.max_margin_use_pct / 100.0
            used = self._margin_used() + sum(mg for _, mg in plans)
            if used + margin > cap + 1e-9:
                ok, why = False, (f"margin use cap: {used:.2f} + {margin:.2f} > {cfg.max_margin_use_pct:g}% "
                                  f"of equity ({cap:.2f})")
        return ok, why, margin

    def _send_raw(self, s, side, volume, sl, dev, comment):
        t_send = time.time()
        try:
            r = self.broker.market_order(s, side, volume, sl, 0.0, dev, self.cfg.magic, comment[:31])
        except Exception as exc:  # noqa: BLE001
            r = {"ok": False, "msg": f"{type(exc).__name__}: {exc}"}
        return r, t_send, time.time()

    def _book_fill(self, s, side, i, n, gid, size, sl, sl_dist, m, st, info, now_wall, t_dec, r, t_send, t_fill, floor) -> bool:
        """Register one verified fill (or count a rejection) and log its measured latency."""
        cfg = self.cfg
        d = 1 if side == "BUY" else -1
        point = float(info["point"])
        if not r.get("ok"):
            q = self.exec_fails.setdefault(s, deque(maxlen=20))
            q.append(now_wall)
            recent = [x for x in q if now_wall - x <= 60.0]
            self.add_log("WARNING", f"ENTRY {side} {size.volume} {s} rejected: {r.get('msg')} "
                                    f"({len(recent)} failure(s) in 60s, {(t_fill - t_send) * 1000:.0f}ms)")
            if len(recent) >= cfg.exec_fail_max and not cfg.no_stop_mode:     # t42u: no auto-pause in owner mode
                self.exec_pause[s] = now_wall + cfg.exec_fail_pause_s
                self.add_log("ERROR", f"EXECUTION PROTECTION {s}: {len(recent)} rejected orders in 60s - entries on "
                                      f"{s} paused {cfg.exec_fail_pause_s:.0f}s")
            if "not found" in str(r.get("msg")) and not cfg.no_stop_mode:   # t42u: owner mode never self-stops
                self.unverified_entry = r["msg"]
                self.running = False
                self._save_state()
            return False
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
        if floor:
            self._floor_tickets.add(ticket)
        self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=side, volume=float(r["volume"]),
                                               open_price=fill, open_time=now_wall, sl=sl, tp=0.0, entry_vu=m.vu,
                                               reason=st.reason, batch_id=gid,
                                               hard_dist=sl_dist * cfg.soft_stop_mult, peak_pl=-abs(m.spread))
        g = self.bursts.get(gid)
        if g is None:
            g = self.bursts[gid] = {"symbol": s, "side": side, "opened": now_wall, "mode": "micro", "micro": True,
                                    "realized": 0.0, "legs": 0, "target": m.target, "peak_pl": 0.0,
                                    "peak_vel": max(0.0, d * m.vel), "ext_px": m.mid, "last_ext_t": m.t,
                                    "origin": st.origin, "regime": m.regime, "stop_dist": sl_dist, "adds": 0,
                                    "slip_pts": [], "tickets": []}
        g["legs"] = int(g.get("legs", 0)) + 1
        g.setdefault("lot", float(r["volume"]))
        g["risk"] = float(g.get("risk", 0.0)) + size.risk_money
        g.setdefault("slip_pts", []).append(round(slip, 1))
        g.setdefault("tickets", []).append(ticket)
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
        return True

    # ---------------------------------------------------------------- exits
    def _manage_exits(self, now_wall: float) -> None:
        cfg = self.cfg
        live = {int(p["ticket"]): p for p in self.own_positions}
        for t, ps in list(self.pos_state.items()):
            p = live.get(t)
            if p is None:
                continue
            if cfg.broker_sl and not cfg.no_stop_mode and not float(p.get("sl", 0.0)):   # adopted order without a stop
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
            legs = [ps for ps in legs if ps.ticket not in self._closed_tickets]
            if not legs:
                continue
            sym = legs[0].symbol
            m, st = self._micro(sym, now_wall)
            reason = self._hard_risk_exit(key, legs, live, m, now_wall)       # hard risk ALWAYS first
            if not reason and cfg.no_stop_mode and self.running:
                reason, legs = self._profit_only_exits(key, legs, live, m, st, now_wall)
                if reason or legs is None:
                    legs = legs or []
                    if not reason:
                        continue
            if not reason and self.running and not cfg.no_stop_mode:
                # BE / trailing; its only exit is "protection failed" (stop not confirmed by MT5) = a RISK exit, never delayed
                reason = self._protect_basket(key, legs, live, m, st, now_wall)
            if not reason and self.running and not cfg.no_stop_mode and cfg.scalp_enabled:
                reason, part = self._scalp_exit(key, legs, live, m, now_wall)        # t47u: bank short, never give a winner back
                if part:
                    legs = part                                                      # partial take-profit: only these legs
            if not reason and self.running and not cfg.no_stop_mode and cfg.ip_enabled and (self.bursts.get(key) or {}).get("ip"):
                reason = self._ip_exit(key, legs, live, m, now_wall)                  # t48u: a loss leaves fast
            ip_owned = bool(cfg.ip_single_owner and cfg.ip_enabled and (self.bursts.get(key) or {}).get("ip"))
            if not reason and self.running and not cfg.no_stop_mode and not ip_owned:
                grp = self.bursts.get(key) or {}
                lq = self._liq_exit(key, legs, live, m, now_wall) if (cfg.precision_enabled and grp.get("plan")) else None
                if lq and lq.startswith("TARGET LIQ"):
                    reason = lq                                                 # the target is taken at once
                else:
                    # t45u: a precision basket is held to its Target LIQ (no cents-level zone target); the structural
                    # invalidation (confirmed against jitter) and the momentum / reversal exits still apply
                    soft = lq or ((None if grp.get("plan") else self._zone_target(legs, live, m))
                                  or self._micro_exit(key, legs, live, m, st, now_wall))
                    if not soft:
                        soft = self._reversal_check(legs, m, st)
                    if soft and not lq and self._pullback_hold(legs, soft, m, now_wall, key):
                        soft = None                                                 # t37u: pullback into own zone
                    if soft and not lq and self._hold_winner(key, legs, live, m, soft, now_wall):
                        soft = None                                                 # t46u: keep the winner
                    reason = self._confirm_soft_exit(key, sym, soft, m, now_wall)   # t36u: anti-jitter
            else:
                self._exit_pending.pop(key, None)
            if not reason:
                continue
            closed = []
            t_dec = time.time()
            todo = [ps for ps in legs if ps.ticket not in self._closed_tickets]
            dev = self._close_dev(sym)

            def _close(ticket):
                t0 = time.time()
                try:
                    r = self.broker.close_position(ticket, dev, cfg.magic)
                except Exception as exc:  # noqa: BLE001
                    r = {"ok": False, "verified": False, "msg": f"{type(exc).__name__}: {exc}"}
                return r, t0, time.time()
            if bool(getattr(self.broker, "concurrent_io", False)) and len(todo) > 1:
                res = [f.result() for f in [self._io.submit(_close, ps.ticket) for ps in todo]]   # one round trip
            else:
                res = [_close(ps.ticket) for ps in todo]
            for ps, (r, t0, t1) in zip(todo, res):
                ok = r["ok"] and r.get("verified")
                closed.append(f"#{ps.ticket} {'ok' if ok else 'FAIL ' + str(r['msg'])} {(t1 - t0) * 1000:.0f}ms")
                if ok:
                    self._closed_tickets[ps.ticket] = now_wall
                    self.lat_exits.append({"ts": now_wall, "symbol": sym, "ticket": ps.ticket,
                                           "close_ms": round((t1 - t0) * 1000, 1),
                                           "decision_to_close_ms": round((t1 - t_dec) * 1000, 1)})
                if not ok:
                    ps.closing_attempts += 1
                    if ps.closing_attempts >= cfg.close_fail_halt and not self.exec_halt and not cfg.no_stop_mode:
                        self.exec_halt = True
                        self.add_log("ERROR", f"EXECUTION PROTECTION: #{ps.ticket} could not be closed "
                                              f"{ps.closing_attempts}x - new entries halted, still retrying the close")
            partial = bool(reason.startswith("SCALP TP1"))
            if key in self.bursts:
                if partial:
                    if any(ps.ticket in self._closed_tickets for ps in todo):
                        self.bursts[key]["scalp_tp1"] = True        # the rest of the basket keeps running
                else:
                    self.bursts[key]["exit_reason"] = reason
            self.add_log("INFO" if not any(" FAIL " in f" {c}" for c in closed) else "ERROR",
                         f"EXIT GROUP {key} {legs[0].side} {sum(p.volume for p in legs):g} {sym}: {reason} -> "
                         + ", ".join(closed))
            self._refresh_account()
            self._adopt_positions()          # book the closed basket now: the same loop can re-enter on a fresh signal

    def _ip_exit(self, key: str, legs: list, live: dict, m, now_wall: float) -> Optional[str]:
        """t48u: exits of an impulse-pullback basket that cut a LOSS fast (a winner is only held / trailed, see _protect_basket):
        * IMPULSE FAILED: price took out the pullback extreme (the setup's invalidation) - leave now instead of waiting for the broker;
        * M5 TREND TURNED: the M5 bias is <= -ip_m5_flip against the basket;
        * NO FOLLOW-THROUGH: `ip_stale_s` after the entry the basket is still red and never made 0.2 ATR of profit.
        2 clean ticks, no time wait, own pending key (the regular soft exit of the same loop resets `key`)."""
        cfg = self.cfg
        g = self.bursts.get(key) or {}
        ip = g.get("ip")
        sym, side = legs[0].symbol, legs[0].side
        d = 1 if side == "BUY" else -1
        cand = None
        if ip and getattr(m, "ready", False):
            pl, money = self._group_pl(legs, live, m, now_wall)
            if pl is not None:
                vol = sum(p.volume for p in legs)
                avg = sum(p.open_price * p.volume for p in legs) / vol
                px = avg + d * pl
                atr, _ = self._atr(sym)
                atr = atr or float(getattr(m, "atr", 0.0) or 0.0)
                ip["best"] = max(float(ip.get("best", 0.0)), pl)
                v = getattr(m, "mtf", None)
                if ip.get("stop") and d * (px - float(ip["stop"])) < 0 and not g.get("prot"):
                    cand = (f"IMPULSE FAILED: price {px:.5g} took out the pullback extreme {float(ip['stop']):.5g} "
                            f"- the setup is wrong, leaving now ({money:+.2f})")
                elif v is not None and v.m5.ready and d * float(v.m5_bias) <= -cfg.ip_m5_flip:
                    cand = f"M5 TREND TURNED against the {side} basket (M5 bias {float(v.m5_bias):+.2f}) - leaving ({money:+.2f})"
                elif (money < 0 and not g.get("prot") and now_wall - float(ip.get("t0", now_wall)) >= cfg.ip_stale_s
                      and atr > 0 and float(ip["best"]) < 0.2 * atr):
                    cand = (f"NO FOLLOW-THROUGH: {now_wall - float(ip['t0']):.0f}s after the entry the basket is still red "
                            f"({money:+.2f}) and never made 0.2 ATR - cutting the loss")
        sk = key + "|ip"
        if not cand:
            self._confirm_soft_exit(sk, sym, None, m, now_wall)
            return None
        return self._confirm_soft_exit(sk, sym, cand, m, now_wall, confirm_s=0.0, ticks=2)

    def _scalp_exit(self, key: str, legs: list, live: dict, m, now_wall: float) -> tuple:
        """t47u: confirm (2 clean ticks, no time wait) the scalp candidate from `_scalp_pick`. Its own pending key - the regular soft
        exit path of the same loop resets `key`, which would otherwise wipe this confirmation every tick."""
        cand, part = self._scalp_pick(key, legs, live, m, now_wall)
        sk, sym = key + "|scalp", legs[0].symbol
        if not cand:
            self._confirm_soft_exit(sk, sym, None, m, now_wall)
            return None, None
        r = self._confirm_soft_exit(sk, sym, cand, m, now_wall, confirm_s=0.0, ticks=2)
        return r, (part if r else None)

    def _scalp_pick(self, key: str, legs: list, live: dict, m, now_wall: float) -> tuple:
        """t47u SCALP candidate: (reason, partial_legs). Two exits that bank a SHORT profit and never let a winner run back into a loss:
        * SCALP TP1: net basket profit >= scalp_target_atr x ATR -> close `scalp_partial_frac` of the orders in profit (the runner
          keeps going; _protect_basket then moves its stop to break-even+ at once);
        * SCALP GIVEBACK: before any break-even stop is in place, a basket that peaked >= scalp_giveback_min_atr x ATR is closed
          (in profit) once it gives back scalp_giveback_frac of the peak. hold_winners / pullback_hold never override it.
        Money-side checks use the net of the legs (broker profit + swap); price-side ones the executable exit price."""
        cfg = self.cfg
        g = self.bursts.get(key)
        if g is None or not getattr(m, "ready", False):
            return None, None
        d = 1 if legs[0].side == "BUY" else -1
        sym = legs[0].symbol
        pl, money = self._group_pl(legs, live, m, now_wall)
        if pl is None:
            return None, None
        ts, tv = self._tv(sym)
        cost = cfg.commission_per_lot / (tv / ts) if (cfg.commission_per_lot and ts and tv) else 0.0
        net = pl - cost
        atr, _ = self._atr(sym)
        atr = atr or float(getattr(m, "atr", 0.0) or 0.0)
        spread = float(getattr(m, "spread", 0.0) or 0.0)
        if atr <= 0:
            return None, None
        peak = max(float(g.get("scalp_peak", 0.0)), net)
        g["scalp_peak"] = peak
        if money <= 0 or net <= 0:
            return None, None                                    # only ever a PROFIT exit
        floor = cost + 1.5 * spread
        if not g.get("scalp_tp1") and len(legs) >= 2 and net >= max(cfg.scalp_target_atr * atr, floor + 2.0 * spread):
            k = max(1, min(len(legs) - 1, int(round(cfg.scalp_partial_frac * len(legs)))))
            part = sorted(legs, key=lambda ps: ps.ticket)[:k]
            return (f"SCALP TP1: net {net:+.5g} >= {cfg.scalp_target_atr:g} ATR - banking {k}/{len(legs)} order(s) in "
                    f"profit {money:+.2f}, the rest runs with a break-even+ stop"), part
        arm = max(cfg.scalp_giveback_min_atr * atr, 3.0 * spread + cost)
        if not g.get("prot") and peak >= arm:
            level = max(peak * (1.0 - cfg.scalp_giveback_frac), floor)
            if net <= level:
                return (f"SCALP GIVEBACK: peak {peak:+.5g} -> {net:+.5g} (gave back >= {cfg.scalp_giveback_frac:.0%}, "
                        f"no break-even stop yet) - closing in profit {money:+.2f}"), None
        return None, None

    def _liq_exit(self, key: str, legs: list, live: dict, m, now_wall: float) -> Optional[str]:
        """t45u: exits that belong to a precision basket - the Target LIQ (take it), the structural invalidation (the
        setup failed: leave now, do not wait for the wider broker stop) - plus MAE / MFE tracking for the report."""
        cfg = self.cfg
        g = self.bursts.get(key) or {}
        pl = g.get("plan")
        if not pl:
            return None
        d = 1 if legs[0].side == "BUY" else -1
        pxs = [x for x in (self._exec_px(ps, live.get(ps.ticket, {}), m if getattr(m, "bid", 0) else None, now_wall)
                           for ps in legs) if x]
        if not pxs:
            return None
        px = min(pxs) if d > 0 else max(pxs)
        fav = (px - pl["entry"]) * d
        pl["mfe"] = max(float(pl.get("mfe", 0.0)), fav)
        pl["mae"] = max(float(pl.get("mae", 0.0)), -fav)
        if cfg.pe_target_exit and (px - pl["target"]) * d >= 0:
            return (f"TARGET LIQ reached: {pl['target_kind']} {pl['target']:.5g} (price {px:.5g}, R:R planned "
                    f"{pl['rr']:.1f}, {pl['kind']} setup [{'+'.join(pl['tags'])}])")
        if cfg.pe_invalidation_exit and (px - pl["inval"]) * d <= 0:
            return (f"STRUCTURE INVALIDATED: price {px:.5g} took out {pl['inval']:.5g} ({pl['kind']} setup "
                    f"[{'+'.join(pl['tags'])}]) - the setup failed, leaving now instead of waiting for the stop")
        return None

    def _hold_winner(self, key: str, legs: list, live: dict, m, soft: str, now_wall: float) -> bool:
        """t46u HOLD WINNERS: a discretionary exit must not bank a small profit while the trend is still with the basket
        (and, with the whole bot portfolio still negative, not at all once the basket's stop is at break-even / trailing:
        that stop already protects the profit). True = do not close now. Hard risk exits, the BE / trailing stop and the
        broker SL never pass through here. Never applies to a basket that is not in net profit."""
        cfg = self.cfg
        if not cfg.hold_winners:
            return False
        money = self._net(legs, live)
        if money <= 0:
            return False
        d = 1 if legs[0].side == "BUY" else -1
        g = self.bursts.get(key) or {}
        v = getattr(m, "mtf", None)
        v1 = v.m1 if (v is not None and v.m1.ready) else None
        flipped = False
        if g.get("ip") and v is not None and v.m5.ready:
            flipped = d * v.m5_bias <= -cfg.ip_m5_flip          # t48u: an impulse-pullback winner is held until the M5 trend turns
        elif v1 is not None:
            flipped = (d * v.bias <= -cfg.hold_flip_bias and (not v1.rsi_ready or d * (v1.rsi - 50.0) < 0)) \
                or v1.sbreak == -d
        portfolio = sum(float(p.get("profit", 0.0)) + float(p.get("swap", 0.0)) for p in self.own_positions)
        protected = bool(g.get("prot"))
        if portfolio < 0 and protected:
            hold, why = True, f"portfolio {portfolio:+.2f} still negative, stop already {g.get('prot')} (profit protected)"
        elif not flipped:
            hold, why = True, ("M5 trend still with the basket" if g.get("ip") else "M1 trend still with the basket")
        else:
            hold, why = False, ""
        if hold and now_wall - self._exit_held_log.get("hw" + key, 0.0) > 10.0:
            self._exit_held_log["hw" + key] = now_wall
            self.add_log("INFO", f"HOLD WINNER basket {key} {legs[0].symbol} {legs[0].side} {money:+.2f}: not closing on "
                                 f"'{soft[:60]}' - {why}")
        if hold:
            self.exits_filtered += 1
        return hold

    def _pullback_hold(self, legs: list, soft: str, m, now_wall: float, key: str) -> bool:
        """t37u: a mere pullback INTO a demand zone / bullish FVG (BUY basket; supply / bearish FVG for SELL) does not
        close the basket. Real reversals (REVERSAL with the opposite side scoring, M1 swing broken) and every hard
        stop still close it; the broker SL protects the pullback."""
        cfg = self.cfg
        zv = getattr(m, "zones", None)
        if not cfg.pullback_hold or zv is None or not getattr(zv, "ready", False):
            return False
        if soft.startswith("REVERSAL") or "M1 swing" in soft or "in profit" in soft or "profit protection" in soft:
            return False                                  # reversal / swing break / profit-taking are not pullbacks
        d = 1 if legs[0].side == "BUY" else -1
        if not zv.holds(d) or zv.wick == -d or zv.confirm == -d:
            return False
        if now_wall - self._exit_held_log.get("pb" + key, 0.0) > 5.0:
            self._exit_held_log["pb" + key] = now_wall
            self.add_log("INFO", f"HOLD basket {key} {legs[0].symbol}: pullback into "
                                 f"{'/'.join(zv.tags['BUY' if d > 0 else 'SELL'])} - not closing on '{soft[:50]}' "
                                 f"(hard stop active)")
        return True

    def _exit_noise(self, sym: str, m) -> Optional[str]:
        """t36u: why the CURRENT tick must not count as confirmation of a soft exit (chart glitch / jitter)."""
        cfg = self.cfg
        mid = float(getattr(m, "mid", 0.0) or 0.0)
        t = float(getattr(m, "t", 0.0) or 0.0)
        h = self._mid_hist.setdefault(sym, deque(maxlen=7))
        if mid > 0 and (not h or h[-1][0] != t):
            h.append((t, mid))
        if not getattr(m, "ready", False) or mid <= 0:
            return "no clean quote"
        if not getattr(m, "stable", True):
            return f"feed gap {getattr(m, 'gap_max', 0.0):.1f}s"
        lat = float(getattr(m, "latency", 0.0) or 0.0)
        if lat > cfg.exit_stale_s:
            return f"stale quote ({lat:.1f}s old)"
        if float(getattr(m, "spread_ratio", 1.0) or 1.0) > cfg.spread_spike_ratio:
            return f"spread spike ({getattr(m, 'spread_ratio', 0.0):.1f}x normal)"
        atr = float(getattr(m, "atr", 0.0) or 0.0)
        if atr > 0 and len(h) >= 4:
            prev = sorted(x for _, x in list(h)[-6:-1])
            med = prev[len(prev) // 2]
            if abs(mid - med) > cfg.exit_outlier_atr * atr:
                return f"outlier tick ({abs(mid - med) / atr:.2f} ATR from the median)"
        return None

    def _confirm_soft_exit(self, key: str, sym: str, soft: Optional[str], m, now_wall: float,
                           confirm_s: Optional[float] = None, ticks: Optional[int] = None) -> Optional[str]:
        """t36u: a soft exit fires only when it PERSISTS: >= exit_confirm_s and >= exit_confirm_ticks distinct clean
        ticks. A flicker that disappears resets it; glitch ticks (gap, stale, spread spike, outlier) never count."""
        cfg = self.cfg
        if not soft:
            if self._exit_pending.pop(key, None):
                self.exits_filtered += 1                  # the signal vanished before confirmation = jitter avoided
            return None
        need_s = cfg.exit_confirm_s if confirm_s is None else confirm_s
        need_n = cfg.exit_confirm_ticks if ticks is None else ticks
        if need_s <= 0 and need_n <= 1:
            return soft
        noise = self._exit_noise(sym, m)
        c = self._exit_pending.get(key)
        if c is None:
            c = self._exit_pending[key] = {"t0": now_wall, "n": 0, "tick": None}
        if noise:
            if now_wall - self._exit_held_log.get(key, 0.0) > 5.0:
                self._exit_held_log[key] = now_wall
                self.add_log("INFO", f"EXIT HELD basket {key} {sym}: '{soft[:60]}' not confirmed - {noise} "
                                     f"(glitch/jitter filter; hard stop still active)")
        else:
            tick = float(getattr(m, "t", 0.0) or 0.0)
            if tick != c["tick"]:
                c["n"], c["tick"] = c["n"] + 1, tick
        age = now_wall - c["t0"]
        if age >= need_s and c["n"] >= need_n:
            self._exit_pending.pop(key, None)
            return f"{soft} [confirmed {age:.1f}s / {c['n']} clean ticks]"
        if soft.startswith("REVERSAL"):
            self._reverse_to.pop(sym, None)               # not reversed until the close is confirmed
        return None

    def _reversal_check(self, legs: list, m, st) -> Optional[str]:
        """t34u AUTO-REVERSAL: the OPPOSITE side passes its own entry check (positive evidence, threshold, families)
        and beats the held side's strength by `reverse_edge` -> close this basket now (every close verified on
        MT5); the same loop's entry pass then opens the opposite batch at once. A failed close leaves the legs
        open, so the opposite side is never opened on top of them (no hedge, no blind resend)."""
        cfg = self.cfg
        if not cfg.reverse_enabled or not getattr(m, "ready", False) or st.side in ("NO TRADE", legs[0].side):
            return None
        held = st.buy if legs[0].side == "BUY" else st.sell
        opp = st.sell if legs[0].side == "BUY" else st.buy
        if opp - held < cfg.reverse_edge:
            return None
        self._reverse_to[legs[0].symbol] = st.side
        return (f"REVERSAL: {st.side} momentum has the edge ({st.side} {opp:.2f} vs {legs[0].side} {held:.2f}) - "
                f"close {legs[0].side}, verify, open {st.side}")

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
        if cfg.no_stop_mode:
            return self._nostop_emergency(legs, live, m, now_wall)
        if self.risk.catastrophic:
            return (f"CATASTROPHIC drawdown: equity below {self.cfg.safe_flatten_pct:g}% under the closed-balance peak "
                    f"- flatten")
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
        g = self.bursts.get(legs[0].batch_id) or {}
        if g.get("risk"):
            lim = max(lim, 1.1 * float(g["risk"]))   # backup only: the basket's own hard stops (sized at entry) rule
        if eq > 0 and money <= -lim:
            return f"hard group loss {money:+.2f} <= -{lim:.2f} ({cfg.group_loss_pct:g}% of equity)"
        if eq > 0 and cfg.max_total_risk_pct > 0:
            open_risk = sum(self._pos_risk(p) for p in self.own_positions)
            # min-lot-floor orders were admitted under the small-account basket cap: measure them against it
            floor_open = any(int(p["ticket"]) in self._floor_tickets for p in self.own_positions)
            cap_pct = max(cfg.max_total_risk_pct, cfg.small_account_batch_risk_pct) if floor_open else cfg.max_total_risk_pct
            if open_risk > eq * cap_pct / 100.0 * 1.5:
                newest = max(self.pos_state.values(), key=lambda x: x.open_time, default=None)
                if newest is not None and newest.batch_id == legs[0].batch_id:
                    return (f"max exposure breach: open risk {open_risk:.2f} > 1.5x the {cap_pct:g}% cap - "
                            f"closing newest group")
        age = now_wall - self.last_receipt.get(sym, now_wall)
        if age > cfg.emergency_stale_s:
            xq = self._exec_quote(sym, now_wall)
            if xq is None or xq[2] != "rest":
                return f"emergency: no fresh price for {age:.0f}s"
        return None

    def _net(self, legs: list, live: dict) -> float:
        return sum(float((live.get(ps.ticket) or {}).get("profit", 0.0)) + float((live.get(ps.ticket) or {}).get("swap", 0.0))
                   for ps in legs)

    def _profit_only_exits(self, key: str, legs: list, live: dict, m, st, now_wall: float):
        """t40u (owner): CLOSE ONLY IN PROFIT. Returns (reason, legs_to_close) / (None, None) = nothing to do.
        1. hedged / recovery cycle on the symbol: combined net of ALL bot legs >= cycle_tp_pct of equity -> close all;
        2. this basket's own exits (BE/trail level, zone target, momentum/structure/reversal, anti-jitter confirmed)
           only while its net > tp_min_money. A losing basket is never closed here (recovery handles it)."""
        cfg = self.cfg
        sym = legs[0].symbol
        allsym = [ps for ps in self.pos_state.values() if ps.symbol == sym and ps.ticket in live
                  and ps.ticket not in self._closed_tickets]
        sides = {ps.side for ps in allsym}
        multi = len({ps.batch_id for ps in allsym}) > 1
        eq = float(self.account.get("equity", 0.0) or 0.0)
        if multi and (len(sides) > 1 or any(int((self.bursts.get(ps.batch_id) or {}).get("layers", 0)) for ps in allsym)):
            tot = self._net(allsym, live)
            tgt = max(cfg.tp_min_money, eq * cfg.cycle_tp_pct / 100.0)
            if tot > tgt:
                self.recoveries["cycle_tp"] += 1
                return (f"CYCLE TAKE PROFIT {sym}: hedge + recovery legs net {tot:+.2f} >= {tgt:.2f} "
                        f"({len(allsym)} legs, sides {'/'.join(sorted(sides))})"), allsym
            if multi and len(sides) > 1:
                # t44u (owner): both sides open on the symbol (an old hedged set) close TOGETHER, in profit. A green
                # basket is never taken alone while its opposite basket is red - that is what left one side + and the
                # other -; the set waits for its combined net to reach the cycle target above.
                self._reverse_to.pop(sym, None)
                return None, None
        money = self._net(legs, live)
        green = money > cfg.tp_min_money
        d = 1 if legs[0].side == "BUY" else -1
        soft = None
        # t41u fast PROFIT LOCK: take the profit as soon as it starts to fade, before the basket turns red
        g = self.bursts.get(legs[0].batch_id)
        if g is not None:
            ts, tv = self._tv(sym)
            cost = (m.spread / ts * tv * sum(ps.volume for ps in legs)) if ts and tv and getattr(m, "spread", 0) else 0.0
            lock = max(cfg.profit_lock_min_money, cfg.profit_lock_spread_x * cost)
            if money > lock:
                g["lock_peak"] = max(float(g.get("lock_peak", 0.0)), money)
            peak = float(g.get("lock_peak", 0.0))
            vol = sum(ps.volume for ps in legs)
            # t43u profit TARGET: a reasonable profit (>= tp_target_atr x M1 ATR on the basket's lots, not tiny)
            tgt = max(cfg.tp_target_money, lock,
                      (cfg.tp_target_atr * m.atr / ts * tv * vol) if ts and tv and getattr(m, "atr", 0) else 0.0)
            g["tp_target"] = round(tgt, 2)
            fast = None
            if green and money >= cfg.tp_grab_x * tgt:
                fast = f"TAKE PROFIT: basket net {money:+.2f} >= {cfg.tp_grab_x:g}x the target {tgt:.2f} - grab it now"
            elif green and peak >= tgt and money <= peak * (1.0 - cfg.tp_target_giveback):
                fast = (f"TAKE PROFIT: target {tgt:.2f} reached (peak {peak:+.2f}), net {money:+.2f} gave back "
                        f"{cfg.tp_target_giveback:.0%} - grab it before it comes back")
            elif green and peak > 0 and money <= max(cfg.tp_min_money, cfg.profit_lock_floor_frac * lock,
                                                     peak * (1.0 - cfg.profit_lock_giveback)):
                fast = (f"PROFIT LOCK: basket net {money:+.2f} fell from its peak {peak:+.2f} (lock {lock:.2f}) - "
                        f"taking the profit before it turns red")
            elif green and st.side == ("SELL" if d > 0 else "BUY"):
                # t43u: about to turn negative by the signal. t44u (owner: closes were too short): ONLY a real opposite
                # signal closes early - a momentary velocity flip (d * m.vel < 0) no longer does, and the opposite
                # signal must persist through the normal anti-jitter confirmation (not the 1-tick profit-grab one)
                fast = (f"EARLY CLOSE: opposite {st.side} signal while still green ({money:+.2f}) - "
                        f"close before it turns red")
            if fast:
                if fast.startswith("EARLY CLOSE"):
                    reason = self._confirm_soft_exit(key, sym, fast, m, now_wall)
                else:
                    reason = self._confirm_soft_exit(key, sym, fast, m, now_wall, cfg.profit_lock_confirm_s, 1)
                if reason and self._net(legs, live) > cfg.tp_min_money:
                    return reason, legs
                return None, None
        if green:
            # t43u: no break-even close (it ended at ~0 / slightly negative after costs) - profit exits only
            soft = self._zone_target(legs, live, m) or self._micro_exit(key, legs, live, m, st, now_wall)
            if not soft:
                soft = self._reversal_check(legs, m, st)
            if soft and self._pullback_hold(legs, soft, m, now_wall, key):
                soft = None
        else:
            self._reverse_to.pop(sym, None)
        reason = self._confirm_soft_exit(key, sym, soft, m, now_wall)
        if reason and self._net(legs, live) <= cfg.tp_min_money:
            return None, None                                               # turned red while confirming: hold
        return (reason, legs) if reason else (None, None)

    def _recovery(self, s: str, mine: list, m, st, info: dict, status: dict, now_wall: float, t_dec: float,
                  hedging: bool) -> bool:
        """t40u OWNER-REQUESTED recovery instead of loss stops:
        * HEDGE: a basket not green after recovery_after_s, or red beyond recovery_loss_pct of equity, while the
          OPPOSITE side has a valid signal -> open the opposite basket now (hedging accounts only);
        * AVERAGING LAYERS: a losing basket gets a layer at a better price every avg_spacing_atr ATR, up to
          avg_max_layers, lot x avg_lot_mult per layer (1.0 = same lot). Margin caps always apply."""
        cfg = self.cfg
        if not getattr(m, "ready", False) or not m.atr:
            return False
        live = {int(p["ticket"]): p for p in self.own_positions}
        eq = float(self.account.get("equity", 0.0) or 0.0)
        by_side: dict = {}
        for ps in mine:
            by_side.setdefault(ps.side, []).append(ps)
        if cfg.hedge_enabled and hedging and len(by_side) == 1:
            side = mine[0].side
            opp = "SELL" if side == "BUY" else "BUY"
            g = self.bursts.get(mine[0].batch_id) or {}
            net = self._net(mine, live)
            age = now_wall - float(g.get("opened", now_wall))
            red_long = net < 0 and age >= cfg.recovery_after_s
            red_deep = eq > 0 and net <= -eq * cfg.recovery_loss_pct / 100.0
            if (red_long or red_deep) and st.side == opp:
                self.risk.allow_opposite = True
                try:
                    ok = self._open_batch(s, opp, m, st, info, status, now_wall, t_dec, None)
                finally:
                    self.risk.allow_opposite = False
                if ok:
                    self.recoveries["hedges"] += 1
                    self.add_log("WARNING", f"RECOVERY HEDGE {s}: {side} basket {net:+.2f} "
                                            f"({'not green after %.0fs' % age if red_long else 'red beyond %g%% equity' % cfg.recovery_loss_pct})"
                                            f" and {opp} has the signal -> opposite basket opened (owner-requested, no loss close)")
                    return True
        if not cfg.avg_enabled or cfg.avg_max_layers <= 0:
            return False
        for side, legs in by_side.items():
            net = self._net(legs, live)
            if net >= 0:
                continue
            gid = legs[0].batch_id
            g = self.bursts.get(gid)
            if g is None:
                continue
            layers = int(g.get("layers", 0))
            if layers >= cfg.avg_max_layers or now_wall - float(g.get("last_layer", 0.0)) < 1.0:
                continue
            d = 1 if side == "BUY" else -1
            worst = min(ps.open_price for ps in legs) if d > 0 else max(ps.open_price for ps in legs)
            px = m.ask if d > 0 else m.bid
            if d * (worst - px) < cfg.avg_spacing_atr * m.atr:
                continue
            base = [ps for ps in legs if "-a" not in (getattr(ps, "comment", "") or "")] or legs
            n0 = max(1, int(g.get("planned") or len(base)))
            vmin = float(info.get("volume_min", 0.01) or 0.01)
            vmax = float(info.get("volume_max", 100.0) or 100.0)
            vstep = float(info.get("volume_step", vmin) or vmin)
            k = min(n0, 4)                                 # a layer = the batch's total lots in <= 4 orders
            lot = normalize_volume(float(g.get("lot") or legs[0].volume) * n0 / k * cfg.avg_lot_mult ** (layers + 1),
                                   vmin, vmax, vstep)
            n0 = k
            size = SizeResult(lot, 0.0, f"recovery layer {layers + 1}/{cfg.avg_max_layers} (owner-requested averaging)")
            plans: list = []
            self.risk.allow_averaging = True
            try:
                for i in range(n0):
                    ok, why, margin = self._check_order(s, side, len(legs) + i, size, m, False, plans, add=True)
                    if not ok:
                        break
                    plans.append((i, margin))
            finally:
                self.risk.allow_averaging = False
            if not plans:
                status["reason"] = f"recovery layer blocked: {why}"
                continue
            room = eq * cfg.batch_margin_pct / 100.0
            while len(plans) > 1 and sum(mg for _, mg in plans) > room + 1e-9:
                plans.pop()
            dev = self._deviation(s, m.spread, m.vu)
            opened = 0
            g["last_layer"] = now_wall
            self.pacer.inflight.add(s)
            try:
                for i, _ in plans:
                    r, t_send, t_fill = self._send_raw(s, side, lot, 0.0, dev, f"{cfg.comment_prefix}-{gid}-r{layers + 1}{i + 1}")
                    if self._book_fill(s, side, len(legs) + i, len(legs) + len(plans), gid, size, 0.0, cfg.sl_atr * m.atr,
                                       m, st, info, now_wall, t_dec, r, t_send, t_fill, False):
                        opened += 1
                    else:
                        break
            finally:
                self.pacer.inflight.discard(s)
            if opened:
                g["layers"] = layers + 1
                g["peak_pl"] = 0.0
                self.recoveries["layers"] += 1
                self.add_log("WARNING", f"RECOVERY ADD {s} basket {gid} {side}: layer {layers + 1}/{cfg.avg_max_layers} "
                                        f"+{opened} x {lot:g} at {px} ({d * (worst - px) / m.atr:.1f} ATR better than the "
                                        f"worst entry; basket {net:+.2f}) - owner-requested averaging, no loss close")
                self._refresh_account()
                self._save_state()
                return True
        return False

    def _nostop_emergency(self, legs: list, live: dict, m, now_wall: float) -> Optional[str]:
        """t38u no-stop mode: the ONLY hard exits - margin level near stop-out, data-feed loss, and the engine-side
        BE/trail level of a winning basket (profit protection, never a loss stop)."""
        cfg = self.cfg
        sym = legs[0].symbol
        ml = float(self.account.get("margin_level", 0.0) or 0.0)
        if ml and ml < cfg.emergency_margin_level_pct:
            return f"EMERGENCY margin level {ml:.0f}% < {cfg.emergency_margin_level_pct:g}% - close bot positions"
        age = now_wall - self.last_receipt.get(sym, now_wall)
        if age > cfg.emergency_stale_s:
            xq = self._exec_quote(sym, now_wall)
            if xq is None or xq[2] != "rest":
                return f"EMERGENCY data feed lost: no fresh price for {age:.0f}s"
        return None

    def _zone_target(self, legs: list, live: dict, m) -> Optional[str]:
        """t38u: take the basket's profit when price reaches an OPPOSING zone (supply / bearish FVG / resistance for a
        BUY basket; demand / bullish FVG / support for SELL)."""
        cfg = self.cfg
        if not cfg.zone_target_exit:
            return None
        money = sum(float((live.get(ps.ticket) or {}).get("profit", 0.0)) for ps in legs)
        if money <= 0:
            return None
        d = 1 if legs[0].side == "BUY" else -1
        zv, iv = getattr(m, "zones", None), getattr(m, "ind", None)
        hit = []
        if zv is not None and getattr(zv, "ready", False):
            hit += [t for t in zv.tags["SELL" if d > 0 else "BUY"]]
        lvl = getattr(iv, "res_above" if d > 0 else "sup_below", None) if iv is not None else None
        if lvl and m.atr and abs(float(lvl) - m.mid) <= 0.1 * m.atr:
            hit.append("resistance" if d > 0 else "support")
        if hit:
            return f"ZONE TARGET: price reached {'/'.join(hit)} with the basket {money:+.2f} in profit"
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
        vol = sum(p.volume for p in legs)
        if ts and tv and vol > 0 and all(p.ticket in live and "profit" in live[p.ticket] for p in legs):
            # t32u (3): exits use the basket's MONEY P/L (all legs incl. adds, broker profit + swap) in price units;
            # the more conservative of that and the executable price P/L
            pl = min(pl, money / (vol * tv / ts))
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

    def _protect_basket(self, key: str, legs: list, live: dict, m, st, now_wall: float) -> Optional[str]:
        """BASKET break-even + dynamic trailing, one stop level for every order of the basket.
        * BE: only after NET basket profit >= max(be_trigger_atr x M1 ATR, be_trigger_spread x spread, stops level
          + lock + spread) - late enough that noise does not take it; the BE level is the basket average price +
          costs (commission) + a small lock, i.e. net profit after costs.
        * Trail: price - d x distance, distance = ATR x (wide while momentum is strong, normal, tight once it fades),
          never closer than trail_spread_mult x spread or the broker stops/freeze level; only ever moves forward.
        * Every modify goes through the broker's clamped PATCH and is VERIFIED on the live MT5 position. Until it
          is, the leg counts as unprotected at the broker: the engine enforces the same level itself (vsl mirror,
          checked in _hard_risk_exit) and retries; after protect_fail_max failures in a row the basket is closed.
        The initial hard stop always stays in place (stops only move forward)."""
        cfg = self.cfg
        g = self.bursts.get(key)
        if g is None:
            return None
        sym, side = legs[0].symbol, legs[0].side
        d = 1 if side == "BUY" else -1
        info = self.sym_info.get(sym) or {}
        point = float(info.get("point", 0.0)) or 1e-9
        pl, _ = self._group_pl(legs, live, m, now_wall)
        if pl is None:
            return None
        vol = sum(p.volume for p in legs)
        avg = sum(p.open_price * p.volume for p in legs) / vol
        px = avg + d * pl                                        # worst executable exit price of the basket
        ts, tv = self._tv(sym)
        cost = cfg.commission_per_lot / (tv / ts) if (cfg.commission_per_lot and ts and tv) else 0.0
        net = pl - cost
        spread = float(getattr(m, "spread", 0.0) or 0.0)
        atr, _ = self._atr(sym)
        atr = atr or float(getattr(m, "atr", 0.0) or 0.0)
        if atr <= 0:
            return None
        stops = (max(int(info.get("trade_stops_level", 0)), int(info.get("trade_freeze_level", 0))) + 3) * point
        lock = cost + max(cfg.be_lock_spread_mult * spread, 2 * point)
        floor_trig = stops + lock + spread
        be_atr = cfg.scalp_be_atr if cfg.scalp_enabled else cfg.be_trigger_atr     # t47u: scalp = lock the profit early
        trigger = max(be_atr * atr, cfg.be_trigger_spread * spread, floor_trig)
        if cfg.scalp_enabled and g.get("scalp_tp1"):
            trigger = floor_trig                                  # the partial profit is banked: the runner is locked at once
        mom = "normal"
        if getattr(m, "ready", False):
            sig = exit_signals(g, m, d, st.sell if d > 0 else st.buy, st.threshold, cfg)
            cont = sig.pop("_continuing")
            nsig = sum(1 for v in sig.values() if v)
            mom = "strong" if cont and nsig == 0 else "fading" if (nsig >= 2 or not cont) else "normal"
        k = {"strong": cfg.trail_atr_strong, "fading": cfg.trail_atr_fade}.get(mom, cfg.trail_atr_frac)
        if cfg.scalp_enabled:
            k *= cfg.scalp_trail_mult
        if g.get("ip"):
            # t48u: let the profit run - break-even only after ip_be_atr ATR, then a trail that is ip_trail_atr ATR wide (never a
            # stop hugging the price); the M5 trend turning is the discretionary exit (_ip_exit)
            trigger = max(trigger, cfg.ip_be_atr * atr)
            k = max(k, cfg.ip_trail_atr)
        trail = max(k * atr, cfg.trail_spread_mult * spread, stops)
        level = float(g.get("prot_level") or 0.0)
        state = str(g.get("prot") or "")
        g["prot_trigger"], g["trail_dist"], g["momentum"] = trigger, trail, mom
        new = None
        if not state:
            if net >= trigger:
                new, state = avg + d * lock, "BE"
        else:
            cand = px - d * trail
            if (cand - level) * d >= max(0.3 * trail, 5 * point):
                new, state = cand, "trail"
        if new is not None:
            if level and (new - level) * d <= 0:
                new = None
            else:
                was = g.get("prot")
                g["prot"], g["prot_level"] = state, new
                if state == "BE" and not was:
                    g["be_at"] = now_wall
                    self.prot_stats["be"] += 1
                    self.add_log("INFO", f"BREAK-EVEN basket {key} {side} {sym}: net {net:+.5g} >= trigger {trigger:.5g} "
                                         f"-> stop {new:.{int(info.get('digits', 5))}f} for all {len(legs)} order(s)")
                elif state == "trail":
                    g["trail_moves"] = int(g.get("trail_moves", 0)) + 1
                    self.prot_stats["trail"] += 1
        level = float(g.get("prot_level") or 0.0)
        if not level:
            return None
        digits = int(info.get("digits", 5))
        tol = 0.5 * float(info.get("trade_tick_size") or point)
        todo = []
        # write budget: TickerAll allows 300 order writes/min per key -> one basket update at most every
        # max(sl_modify_min_interval, 0.25 s x legs) (8 legs: every 2 s)
        gap = max(cfg.sl_modify_min_interval, 0.25 * len(legs))
        for ps in legs:
            p = live.get(ps.ticket) or {}
            ps.vsl = level if not ps.vsl else (max(ps.vsl, level) if d > 0 else min(ps.vsl, level))  # engine mirror
            ps.sl_state = state
            live_sl = float(p.get("sl", 0.0) or 0.0)
            if live_sl and (live_sl - level) * d >= -tol:            # the REAL position shows the stop
                if not ps.sl_verified:
                    self.prot_stats["verified"] += 1
                ps.sl_verified, ps.prot_fail, ps.sl = True, 0, live_sl
                continue
            ps.sl_verified = False
            if cfg.broker_sl and not cfg.no_stop_mode and now_wall - ps.last_mod >= gap:
                ps.last_mod = now_wall
                todo.append((ps, float(p.get("tp", 0.0)) or ps.tp))
        if not todo:
            return None

        def _mod(ps, tp):
            try:
                return self.broker.modify_sl(ps.ticket, level, tp)
            except Exception as exc:  # noqa: BLE001
                return {"ok": False, "msg": f"{type(exc).__name__}: {exc}"}
        if bool(getattr(self.broker, "concurrent_io", False)) and len(todo) > 1:
            res = [f.result() for f in [self._io.submit(_mod, ps, tp) for ps, tp in todo]]   # one round trip
        else:
            res = [_mod(ps, tp) for ps, tp in todo]
        for (ps, _), r in zip(todo, res):
            sent = float(r.get("sl") or level)
            if r.get("ok") and (r.get("verified") or abs(sent - level) <= tol or (sent - level) * d > 0):
                ps.sl = sent
                ps.sl_verified, ps.prot_fail = bool(r.get("verified", True)), 0
                self.prot_stats["modify_ok"] += 1
                if r.get("verified"):
                    self.prot_stats["verified"] += 1
                self.add_log("INFO", f"SL #{ps.ticket} {side} {sym} -> {ps.sl:.{digits}f} ({state}, basket {key}, "
                                     f"momentum {mom}, trail {trail:.{digits}f}){' verified on MT5' if r.get('verified') else ''}")
            elif r.get("closed"):
                continue
            elif r.get("soft") or r.get("ok"):
                # the broker's nearest placeable level is behind the wanted one (stops/freeze level): the broker
                # keeps the best placeable stop, the engine enforces the wanted level on the broker price
                log.debug("SL #%s %s not placeable yet: %s", ps.ticket, level, r.get("msg"))
                if r.get("ok"):
                    ps.sl = sent
            else:
                ps.prot_fail += 1
                self.prot_stats["modify_fail" if not r.get("unverified") else "unverified"] += 1
                self.add_log("WARNING", f"SL modify #{ps.ticket} {state} {level:.{digits}f} NOT confirmed "
                                        f"({ps.prot_fail}/{cfg.protect_fail_max}): {r.get('msg')} - treated as "
                                        f"unprotected at the broker; the engine enforces the level and retries")
                if ps.prot_fail >= cfg.protect_fail_max:
                    self.prot_stats["protect_close"] += 1
                    return (f"protection failed: {state} stop for #{ps.ticket} not confirmed by MT5 "
                            f"{ps.prot_fail}x - closing the basket ({net:+.5g})")
        return None

    def _tv(self, s: str) -> tuple[float, float]:
        info = self.sym_info.get(s) or {}
        return (float(info.get("trade_tick_size", 0) or 0),
                float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0) or 0))

    def _margin_used(self) -> float:
        eq = float(self.account.get("equity", 0) or 0)
        m = float(self.account.get("margin", 0) or 0)
        return m if m > 0 else max(0.0, eq - float(self.account.get("margin_free", 0) or 0))

    def _pos_risk(self, p: dict) -> float:
        if self.cfg.no_stop_mode:
            return 0.0                                  # t38u: no stop -> stop-risk is not defined; margin governs
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
                 "r": (float(b.get("realized", 0.0)) / risk) if risk > 0 else 0.0,
                 "ind_votes": b.get("ind_votes") or {}}
        try:
            self.learner.record(entry, now)
        except Exception as e:  # noqa: BLE001  learning must never break trading
            log.warning("learner record failed: %s", e)

    def _update_candles(self, now_wall: float) -> None:
        """Refresh real broker M1 bars per symbol (t30u: M1 only, every ~8 s; the forming bar is live ticks). With a thread-safe
        broker the HTTP call runs in the background so the tick loop never waits on it."""
        fn = getattr(self.broker, "candles", None)
        if fn is None:
            return
        cfg = self.cfg
        conc = bool(getattr(self.broker, "concurrent_io", False))
        for s in self.selected:
            if not self._tradable(s)[0]:
                continue
            mt = self.mtf.setdefault(s, MTF())
            for tf, every, hours in (("M1", cfg.candle_poll_m1_s, 2), ("M5", cfg.candle_poll_m5_s, 8)):   # t34u: M5 main
                key = (s, tf)
                job = self._candle_jobs.get(key)
                if job is not None:
                    if not job.done():
                        continue
                    self._candle_jobs.pop(key, None)
                    try:
                        bars = job.result()
                    except Exception:  # noqa: BLE001
                        bars = []
                    self._apply_candles(s, tf, bars)
                    continue
                if now_wall - mt.polled[tf] < every:
                    continue
                mt.polled[tf] = now_wall
                if conc:
                    self._candle_jobs[key] = self._io.submit(fn, s, tf, hours)
                else:
                    try:
                        bars = fn(s, tf, hours)
                    except Exception:  # noqa: BLE001
                        bars = []
                    self._apply_candles(s, tf, bars)

    def _apply_candles(self, s: str, tf: str, bars: list) -> None:
        mt = self.mtf.setdefault(s, MTF())
        eng = self.engines.get(s)
        now = (eng.scanner.last[0] - self._candle_off()) if eng is not None and eng.scanner.last else time.time()
        if not bars:
            err = getattr(self.broker, "candle_error", "") or "no bars returned"
            if self._candle_log.get(f"{s}{tf}") != err:
                self._candle_log[f"{s}{tf}"] = err
                self.add_log("WARNING", f"CANDLES {s} {tf}: {err} - using bars built from live ticks only")
            return
        (mt.m1 if tf == "M1" else mt.m5).load(bars, now)
        if self._candle_log.get(f"{s}{tf}") != "ok":
            self._candle_log[f"{s}{tf}"] = "ok"
            vol = sum(1 for b in bars if b.get("tick_volume") is not None)
            self.add_log("INFO", f"CANDLES {s} {tf}: {len(bars)} broker bars ({vol} with tickVolume)")
        if tf == "M1":
            closed = mt.closed_m1()
            self.m1_bars[s] = closed
            rngs = [b["high"] - b["low"] for b in closed[-14:] if b["high"] > b["low"]]
            if rngs:
                self.atr_m1[s] = sum(rngs) / len(rngs)
                if eng is not None:
                    eng.seed_vu = 0.15 * self.atr_m1[s]

    _LOC_LABELS = (("m5 structure is", "location: M5 structure against this side"),
                   ("no displacement", "location: no M5 impulse yet"),
                   ("impulse leg too small", "location: M5 impulse too small"),
                   ("structure lost", "location: structure lost (pullback too deep)"),
                   ("no confluence", "location: no confluence in the zone"),
                   ("extreme of the m5 range", "location: premium/discount extreme"),
                   ("not in the pullback zone", "location: waiting for the pullback zone"),
                   ("waiting for the trigger", "location: in zone, waiting for the trigger"),
                   ("beyond the invalidation", "location: past the invalidation level"),
                   ("too wide", "location: structural stop too wide"),
                   ("of the stop risk", "location: spread too high vs the stop"),
                   ("at liquidity already", "location: no room to liquidity"),
                   ("no liquidity target", "location: no target with enough R:R"),
                   ("no m5 structure data", "location: no M5 structure data"))

    @staticmethod
    def _category(reason: str) -> str:
        r = (reason or "").lower()
        if "location rejected" in r:                      # t46u: name the exact precision veto instead of a garbled label
            tail = r.split("):", 1)[-1]
            for k, v in Trader._LOC_LABELS:
                if k in tail:
                    return v
            return "location: other"
        if "position score" in r and "<" in r:
            return "location: entry score below minimum"
        table = [("safe mode", "safe mode"), ("exhaustion", "exhaustion"), ("speed-only", "speed-only (B/C weak)"),
                 ("structure-only", "no tick force (scanner A)"), ("managing", "managing basket"),
                 ("duplicate", "duplicate burst"), ("warming", "warming up"), ("no ticks", "no live ticks"),
                 ("idle", "no live ticks"), ("stale", "no live ticks"), ("feed unstable", "feed unstable"),
                 ("thin tick", "thin tick flow"), ("tick rate", "thin tick flow"), ("anti-chase", "anti-chase"),
                 ("no live force", "no live force"), ("no edge", "below threshold"), ("two-sided", "two-sided"),
                 ("supporting signal", "weak support"), ("spread", "spread / cost filter"),
                 ("per minute", "entry rate cap"),
                 ("/min", "entry rate cap"), ("min lot", "sizing refused"), ("averaging", "no adding to losers"),
                 ("not in net profit", "no adding to losers"), ("margin", "margin"),
                 ("execution failure", "execution failures"), ("slippage", "execution quality"),
                 ("halt", "execution protection"), ("netting", "netting position"), ("loss streak", "loss streak raises the bar"),
                 ("paused", "execution pause"), ("learning", "learner"), ("risk", "risk cap"), ("max ", "risk cap"),
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
        if cat.startswith("location"):
            self.last_loc[s] = reason[:230]
        self.skip_counts.setdefault(s, Counter())[cat] += 1
        self.skip_window.setdefault(s, Counter())[cat] += 1
        if sig is not None and sig.action != "HOLD":
            self._decision(s, sig.action, sig.score, reason)

    def _decision(self, s: str, side: str, score, outcome: str) -> None:
        c = self._mcache.get(s)
        v = c[1].mtf if c and getattr(c[1], "mtf", None) is not None else None
        self.decisions.append({"ts": round(getattr(self, "now_wall", None) or time.time(), 1), "symbol": s, "side": side,
                               "score": None if score is None else round(float(score), 2),
                               "trend": v.bias_label if v else "unknown", "outcome": outcome[:160]})

    def _log_why(self, now_wall: float) -> None:
        if now_wall - self.last_why_log < 60.0:
            return
        self.last_why_log = now_wall
        parts = []
        for s, c in self.skip_window.items():
            if c:
                parts.append(f"{s}: " + ", ".join(f"{k} {v}" for k, v in c.most_common(5))
                             + (f" || last location: {self.last_loc[s]}" if s in self.last_loc else ""))
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
            self._resolve_pending_pl(now_wall)
            acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                               float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
            ev = self.risk.update(now_wall, acct) if self.running else None
            if ev and ev[0] == "safe":
                self.add_log("ERROR", f"{ev[1]} - the bot keeps running (no all-day stop)")
            elif ev:
                self.add_log("WARNING", ev[1])
            if now_wall - self.last_trade_check > 10.0:
                self.last_trade_check = now_wall
                self._validate()
            self._update_candles(now_wall)
            self._manage_exits(now_wall)
            if self.running:
                self._log_why(now_wall)
            if self.risk.catastrophic and self.own_positions and not self.cfg.no_stop_mode:
                self._flatten("CATASTROPHIC DRAWDOWN")      # t40u: no loss close in owner no-stop mode
            if self.flatten_pending:
                self.flatten_pending = not self._flatten("STOP retry")["ok"]
            if self.running and not self.stopping and self.cfg.no_stop_mode:
                self.guardian.scan(now_wall)                # t42u: loss guardian, every loop
            if self.running and not self.stopping and self.validation.get("ok") and (not self.risk.halted
                                                                                    or self.cfg.no_stop_mode):
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
        return {"engine": "M1 Rapid Basket Scalper (single engine, M1 only: independent BUY/SELL scores from the M1 main trend "
                          "(EMA + HH/HL-LH/LL swings) + M1 intrabar trigger + RSI/MACD/volume/candle/structure/velocity, "
                          "2-8 order baskets, pyramid adds after BE, spike exits, momentum hold, basket BE + trailing, "
                          "persistent learning)",
                "min_agree": c.mb_min_agree, "candle_poll_s": {"M1": c.candle_poll_m1_s, "M5": c.candle_poll_m5_s},
                "reverse_enabled": c.reverse_enabled, "reverse_edge": c.reverse_edge, "reversals": self.reversals,
                "exit_confirm_s": c.exit_confirm_s, "exit_confirm_ticks": c.exit_confirm_ticks, "exits_filtered": self.exits_filtered,
                "safe_dd_limit": round(self.risk.dd_limit(), 2) if self.risk.peak_equity else None,
                "loss_streak_add": round(max((self.pacer.threshold_add(x) for x in self.selected), default=0.0), 2),
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
                "max_hold_s": c.mb_max_hold_seconds, "exit_min_signals": c.exit_min_signals,
                "be_trigger": f"max({c.be_trigger_atr:g}x ATR, {c.be_trigger_spread:g}x spread)",
                "trail_atr": {"strong": c.trail_atr_strong, "normal": c.trail_atr_frac, "fading": c.trail_atr_fade},
                "protect_fail_max": c.protect_fail_max,
                "sl_structure": c.sl_structure, "sl_atr_range": [c.sl_min_atr, c.sl_max_atr],
                "ind_enabled": c.ind_enabled, "ind_min_score": c.ind_min_score, "ind_sr_near_atr": c.ind_sr_near_atr,
                "ind_chop_veto": c.ind_chop_veto, "require_m1_trigger": c.mb_require_m1_trigger,
                "safe_flatten_pct": c.safe_flatten_pct,
                "mb_cut_frac": c.mb_cut_frac,
                "small_account_batch_risk_pct": c.small_account_batch_risk_pct, "mb_edge": c.mb_edge,
                "runtime_tuning": dict(self.tune), "tunable": sorted(TUNABLE),
                "min_orders_per_batch": c.min_orders_per_batch, "add_enabled": c.add_enabled,
                "broker_sl": ("OFF - per-order SL disabled by owner request (no-stop mode; emergency guards: margin level "
                              f"< {c.emergency_margin_level_pct:g}% or data-feed loss)") if c.no_stop_mode
                             else ("ON" if c.broker_sl else "OFF"), "exec_halt": self.exec_halt,
                "no_stop_mode": c.no_stop_mode, "nostop_margin_use_pct": c.nostop_margin_use_pct,
                "precision_enabled": c.precision_enabled, "m1_only": c.m1_only, "hold_winners": c.hold_winners, "scalp_enabled": c.scalp_enabled, "sniper_enabled": c.sniper_enabled, "compound_enabled": c.compound_enabled, "ip_enabled": c.ip_enabled, "avoid_foreign_conflict": c.avoid_foreign_conflict, "ip_single_owner": c.ip_single_owner, "alloc_enabled": c.alloc_enabled, "alloc_margin_pct": c.alloc_margin_pct, "pe_min": c.pe_min, "pe_rr_min": c.pe_rr_min, "pe_min_confluence": c.pe_min_confluence,
                "batch_margin_pct": c.batch_margin_pct, "avg_max_layers": c.avg_max_layers, "avg_lot_mult": c.avg_lot_mult,
                "avg_spacing_atr": c.avg_spacing_atr, "hedge_enabled": c.hedge_enabled, "recovery_after_s": c.recovery_after_s,
                "recovery_loss_pct": c.recovery_loss_pct, "cycle_tp_pct": c.cycle_tp_pct, "recoveries": dict(self.recoveries),
                "guardian": dict(self.guardian.view),
                "emergency_margin_level_pct": c.emergency_margin_level_pct, "learning_enabled": c.learning_enabled,
                "profit_lock_min_money": c.profit_lock_min_money, "profit_lock_giveback": c.profit_lock_giveback,
                "tp_target_atr": c.tp_target_atr, "tp_target_money": c.tp_target_money, "tp_grab_x": c.tp_grab_x,
                "loss_closes": "ONLY: user STOP, margin level < emergency level, data-feed loss",
                "sl_rule": f"hard stop max({c.sl_atr:g}x M1 ATR, {c.sl_spread_mult:g}x spread) at the broker -> basket BE "
                           f"(avg + costs + {c.be_lock_spread_mult:g}x spread) once net >= max({c.be_trigger_atr:g}x ATR, "
                           f"{c.be_trigger_spread:g}x spread) -> dynamic trail {c.trail_atr_strong:g}/{c.trail_atr_frac:g}/"
                           f"{c.trail_atr_fade:g}x ATR (strong/normal/fading momentum, >= {c.trail_spread_mult:g}x spread); "
                           f"every modify verified on MT5; engine mirror {c.soft_stop_mult:g}x"}

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
                      "planned": g.get("planned"), "protect": g.get("prot") or "hard stop only",
                      "protect_level": g.get("prot_level"), "be_trigger_px": g.get("prot_trigger"),
                      "trail_px": g.get("trail_dist"), "momentum": g.get("momentum"),
                      "trail_moves": int(g.get("trail_moves", 0)),
                      "sl_verified": all(ps.sl_verified for ps in legs),
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

    @staticmethod
    def _precision_report(rows: list, pls: list) -> dict:
        """t45u: the entry-quality metrics of the spec for the REAL closed precision baskets (broker P/L after costs).
        MAE / MFE are sampled by the engine loop (price units and R of the planned stop), not tick-perfect."""
        sel = [(r, pl) for r, pl in zip(rows, pls) if r.get("precision")]
        n = len(sel)
        if not n:
            return {"baskets": 0}
        wins = [pl for _, pl in sel if pl > 0]
        losses = [pl for _, pl in sel if pl <= 0]
        gl = -sum(losses)

        def avg(xs):
            xs = [x for x in xs if x is not None]
            return round(sum(xs) / len(xs), 3) if xs else None
        mae_r, mfe_r = [], []
        for r, _ in sel:
            p = r["precision"]
            rk = float(p.get("risk") or 0.0)
            if rk > 0:
                mae_r.append(float(p.get("mae", 0.0)) / rk)
                mfe_r.append(float(p.get("mfe", 0.0)) / rk)
        tags: dict = {}
        for r, pl in sel:
            for tg in r["precision"].get("tags", []):
                g = tags.setdefault(tg, {"n": 0, "wins": 0, "net": 0.0})
                g["n"] += 1
                g["wins"] += pl > 0
                g["net"] = round(g["net"] + pl, 2)
        ex = [str(r.get("exit", "")) for r, _ in sel]
        return {"baskets": n, "win_rate": round(len(wins) / n * 100, 1), "net_after_costs": round(sum(pls_ for _, pls_ in sel), 2),
                "profit_factor": round(sum(wins) / gl, 2) if gl > 0 else None,
                "avg_win": avg(wins), "avg_loss": avg(losses),
                "target_liq_reached_pct": round(100.0 * sum(e.startswith("TARGET LIQ") for e in ex) / n, 1),
                "invalidation_exit_pct": round(100.0 * sum(e.startswith("STRUCTURE INVALIDATED") for e in ex) / n, 1),
                "broker_stop_pct": round(100.0 * sum(("stop" in e or "SL" in e) and not e.startswith("STRUCTURE") for e in ex) / n, 1),
                "avg_mae_R": avg(mae_r), "avg_mfe_R": avg(mfe_r),
                "avg_initial_drawdown_price": avg([float(r["precision"].get("mae", 0.0)) for r, _ in sel]),
                "avg_planned_rr": avg([r["precision"].get("rr") for r, _ in sel]),
                "avg_entry_depth": avg([r["precision"].get("depth") for r, _ in sel]),
                "avg_entry_score": avg([r["precision"].get("pe") for r, _ in sel]),
                "avg_realised_R": avg([pl / float(r["risk"]) for r, pl in sel if float(r.get("risk") or 0) > 0]),
                "by_confluence": tags}

    def _perf_view(self) -> dict:
        """Results of REAL closed baskets in this process (broker deals: profit + commission + swap + fee).
        Nothing simulated. Attribution = outcome grouped by the state of each family at entry."""
        rows = list(self.perf)
        pls = [float(r["pl"]) for r in rows]
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
        start = float(getattr(self, "start_wall", 0) or 0)
        hours = max(1e-9, ((getattr(self, "now_wall", 0) or time.time()) - start) / 3600.0) if start else None
        slips = [x for r in rows for x in (r.get("slip_pts") or [])]
        spreads = [r["spread_pts"] for r in rows if r.get("spread_pts") is not None]
        re = [r["reentry_s"] for r in rows if r.get("reentry_s") is not None]

        def group(key):
            out = {}
            for r, pl in zip(rows, pls):
                k = (r.get("attr") or {}).get(key, "n/a")
                g = out.setdefault(k, {"n": 0, "wins": 0, "net": 0.0})
                g["n"] += 1
                g["wins"] += pl > 0
                g["net"] = round(g["net"] + pl, 2)
            for g in out.values():
                g["win_rate"] = round(g["wins"] / g["n"] * 100, 1)
            return out
        return {"batches": n, "source": "real broker deals", "wins": len(wins),
                "win_rate": round(len(wins) / n * 100, 1), "net": round(sum(pls), 2),
                "expectancy": round(sum(pls) / n, 2),
                "profit_factor": round(sum(wins) / gl, 2) if gl > 0 else None,
                "max_drawdown": round(dd, 2),
                "avg_win": round(sum(wins) / len(wins), 2) if wins else None,
                "avg_loss": round(sum(losses) / len(losses), 2) if losses else None,
                "orders": sum(int(r.get("legs") or 0) for r in rows),
                "batches_per_hour": round(n / hours, 1) if hours else None,
                "reentry_s": {"n": len(re), "p50": self._pct(re, 0.5), "p90": self._pct(re, 0.9)},
                "avg_spread_pts": round(sum(spreads) / len(spreads), 1) if spreads else None,
                "avg_slip_pts": round(sum(slips) / len(slips), 1) if slips else None,
                "attribution": {k: group(k) for k in ("trend", "macd", "volume", "tick")},
                "orders_per_basket": round(sum(int(r.get("legs") or 0) for r in rows) / n, 2),
                "precision": self._precision_report(rows, pls),
                "entries_per_m1_bar": self._per_bar(rows),
                "protection": dict(self.prot_stats, baskets_with_be=sum(1 for r in rows if r.get("protect") in ("BE", "trail")),
                                   baskets_trailed=sum(1 for r in rows if r.get("protect") == "trail"),
                                   net_be_or_trail=round(sum(float(r["pl"]) for r in rows if r.get("protect") in ("BE", "trail")), 2)),
                "exits": self._exit_mix(rows),
                "last": rows[-5:]}

    @staticmethod
    def _per_bar(rows: list) -> dict:
        bars: dict = {}
        for r in rows:
            if r.get("m1_bar") is not None:
                k = (r.get("symbol"), r["m1_bar"])
                bars[k] = bars.get(k, 0) + 1
        if not bars:
            return {"bars": 0}
        v = list(bars.values())
        return {"bars": len(v), "avg": round(sum(v) / len(v), 2), "max": max(v), "bars_with_2plus": sum(1 for x in v if x >= 2)}

    @staticmethod
    def _exit_mix(rows: list) -> dict:
        out: dict = {}
        for r in rows:
            e = str(r.get("exit") or "")
            k = e.split(":")[0].split(" (")[0][:40] or "other"
            out[k] = out.get(k, 0) + 1
        return out

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
