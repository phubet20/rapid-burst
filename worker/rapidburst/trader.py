"""Autonomous trading loop: connection management, validation, entries, exits,
risk enforcement, STOP handling and restart recovery.

Runs in a single dedicated thread; every MetaTrader call happens on that thread.
Commands from the backend are queued and executed between iterations.
"""
from __future__ import annotations

import json
import logging
import os
import queue
import socket
import threading
import time
import uuid
from collections import deque
from concurrent.futures import Future
from typing import Any, Optional

from . import __version__
from .config import EngineConfig
from .engine import MomentumEngine, PositionState
from .risk import AccountView, EntryPacer, OpenPos, RiskGuard, size_position

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

        avail, why = broker.available()
        if not avail:
            self.conn_state, self.conn_msg = "UNAVAILABLE", why
        st = self._load_state()
        self.selected: list[str] = st.get("selected", ["XAUUSD"])
        # Never resume trading from persisted state: trading starts only on an explicit START.
        self.running: bool = False
        self.was_running_before_restart = bool(st.get("running", False))
        self.batch: dict = st.get("batch", {})
        self.server_offset: float = float(st.get("server_offset", 0.0))
        self.saved_pos_meta: dict = st.get("positions", {})
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
                                   "reason": v.reason} for k, v in self.pos_state.items()},
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
        self._validate()
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
        self.add_log("INFO", f"START - batch {self.batch['id']} on {', '.join(self.selected)}: account re-read "
                             f"(equity {self.account.get('equity')}), {len(self.own_positions)} bot position(s) adopted, "
                             f"{len(own_orders)} bot pending order(s); warming up on fresh live ticks "
                             f"({self.cfg.warmup_seconds:.0f}s) before the first entry")
        return {"ok": True, "msg": "running", "batch": self.batch["id"]}

    def _reset_engines(self) -> None:
        """Discard any previous tick history: the engine only uses ticks that arrive after START."""
        self.sym_status = {}
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

    def cmd_search(self, q: str = "") -> dict:
        if self.conn_state != "CONNECTED":
            return {"ok": False, "msg": "MT5 not connected", "symbols": []}
        res = self.broker.search_symbols(q)
        return {"ok": True, "symbols": res}

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
        self._validate()
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
        self._validate()
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
        self._validate()
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

    def _validate(self) -> None:
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
            good = []
            for s in self.selected:
                info = self.sym_info.get(s) or {}
                if info and int(info.get("trade_mode", 0)) == SYMBOL_TRADE_MODE_FULL:
                    good.append(s)
            detail = ", ".join(good)
            if not good:
                detail = f"none of {self.selected} is tradable on this account"
                try:
                    similar = []
                    for s in self.selected:
                        base = "".join(ch for ch in s if ch.isalnum())[:6]
                        similar += [x["name"] for x in self.broker.search_symbols(base, 8)]
                    if similar:
                        detail += f" - this broker offers: {', '.join(dict.fromkeys(similar))[:120]}; " \
                                  "open Symbols, pick one and press APPLY"
                    else:
                        detail += " - open Symbols, search and APPLY a symbol this broker offers"
                except Exception:  # noqa: BLE001
                    pass
            add("Tradable symbol selected", bool(good), detail)
            hedging = int(acct.get("margin_mode", 0)) == MARGIN_HEDGING
            add("Account margin mode", True, "hedging" if hedging else "netting (1 position per symbol)")
        self.validation = {"ok": all(c["ok"] for c in checks) and connected, "checks": checks,
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
                    peak_units=float(meta.get("peak_units", 0.0)), reason=meta.get("reason", "adopted"))
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
            self.pacer.on_close(ps.symbol, profit if profit is not None else 0.0, time.time())
            self.add_log("INFO", f"Position #{t} {ps.side} {ps.symbol} closed"
                                 + (f", P/L {profit:+.2f}" if profit is not None else ""))
            self.last_deals_check = 0.0
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
            r = self.broker.close_position(int(p["ticket"]), self.cfg.deviation_points, self.cfg.magic)
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
                        cr = self.broker.close_position(t, self.cfg.deviation_points, self.cfg.magic)
                        self.add_log("ERROR", f"#{t} had no SL and SL could not be set - closed: {cr['msg']}")
                        continue
                    self.add_log("WARNING", f"#{t} protective SL attached at {sl}")
            if not self.running and not self.risk.halted:
                continue          # exits are managed only while STARTed (server-side SL/TP protect otherwise)
            eng = self.engines.get(ps.symbol)
            if eng is None:
                continue
            f = eng.features(self._server_now(ps.symbol, now_wall))
            reason = eng.exit_decision(ps, f, now_wall) if self.running else None
            if reason is None and self.risk.halted:
                reason = f"risk halt: {self.risk.halt_reason}"
            if reason:
                r = self.broker.close_position(t, self.cfg.deviation_points, self.cfg.magic)
                ok = r["ok"] and r.get("verified")
                self.add_log("INFO" if ok else "ERROR",
                             f"EXIT #{t} {ps.side} {ps.volume} {ps.symbol}: {reason} -> "
                             f"{'closed (verified)' if ok else r['msg']}")

    # -------------------------------------------------------------- entries
    def _try_entries(self, now_wall: float) -> None:
        cfg = self.cfg
        acct = AccountView(float(self.account.get("balance", 0)), float(self.account.get("equity", 0)),
                           float(self.account.get("margin_free", 0)), float(self.account.get("margin_level", 0)))
        hedging = int(self.account.get("margin_mode", 0)) == MARGIN_HEDGING
        open_pos = [OpenPos(p["symbol"], self._side(p), float(p["volume"]), float(p["profit"]),
                            self._pos_risk(p)) for p in self.own_positions]
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
            if sig.action == "HOLD":
                continue
            if not hedging and (any(p["symbol"] == s for p in self.foreign_positions)
                                or any(p["symbol"] == s for p in self.own_positions)):
                status["reason"] = "netting account: symbol already has a position"
                continue
            ok, why = self.pacer.can_enter(s, now_wall)
            if not ok:
                status["reason"] = f"{sig.action} blocked: {why}"
                continue
            sl_hist = self.slippage.get(s)
            if sl_hist and len(sl_hist) >= 3 and sum(sl_hist) / len(sl_hist) > cfg.max_avg_slippage_points:
                status["reason"] = "paused: poor execution quality (slippage)"
                continue
            self._enter(s, sig, info, acct, open_pos, now_wall, status)

    def _pos_risk(self, p: dict) -> float:
        info = self.sym_info.get(p["symbol"]) or {}
        sl = float(p.get("sl", 0.0))
        ts, tv = float(info.get("trade_tick_size", 0)), float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
        if not sl or not ts or not tv:
            return float(self.account.get("equity", 0)) * self.cfg.risk_per_trade_pct / 100.0
        return max(0.0, abs(float(p["price_open"]) - sl) / ts * tv * float(p["volume"]))

    def _enter(self, s, sig, info, acct, open_pos, now_wall, status) -> None:
        cfg = self.cfg
        f = sig.features
        point = float(info["point"])
        stops = (int(info.get("trade_stops_level", 0)) + 5) * point
        sl_dist = max(cfg.sl_vol_units * f.vol_unit, cfg.min_sl_spread_mult * f.spread, stops)
        tp_dist = max(cfg.tp_vol_units * f.vol_unit, 2 * sl_dist)
        tv = float(info.get("trade_tick_value_loss", 0) or info.get("trade_tick_value", 0))
        size = size_position(acct.equity, cfg, sl_dist, float(info.get("trade_tick_size", 0)), tv,
                             float(info["volume_min"]), float(info["volume_max"]), float(info["volume_step"]))
        if size.volume <= 0:
            status["reason"] = f"{sig.action} skipped: {size.note}"
            return
        price = f.ask if sig.action == "BUY" else f.bid
        margin = self.broker.calc_margin(sig.action, s, size.volume, price)
        if margin is None:
            status["reason"] = "margin calc unavailable"
            return
        add_ok = abs(sig.score) >= cfg.add_threshold
        ok, why = self.risk.check_entry(s, sig.action, open_pos, acct, size.volume, size.risk_money, margin, add_ok)
        if not ok:
            status["reason"] = f"{sig.action} blocked: {why}"
            return
        d = 1 if sig.action == "BUY" else -1
        sl = price - d * sl_dist
        tp = price + d * tp_dist
        comment = f"{cfg.comment_prefix}-{uuid.uuid4().hex[:6]}"
        self.pacer.inflight.add(s)
        self.pacer.on_entry(s, now_wall)
        try:
            r = self.broker.market_order(s, sig.action, size.volume, sl, tp, cfg.deviation_points, cfg.magic, comment)
        finally:
            self.pacer.inflight.discard(s)
        if not r.get("ok"):
            self.add_log("WARNING", f"ENTRY {sig.action} {size.volume} {s} rejected: {r.get('msg')}")
            if "not found" in str(r.get("msg")):
                self.unverified_entry = r["msg"]
                self.running = False
                self._save_state()
            return
        fill = float(r["price"])
        slip = (fill - float(r["req_price"])) * d / point
        self.slippage.setdefault(s, deque(maxlen=10)).append(max(0.0, slip))
        ticket = int(r["ticket"])
        self.pos_state[ticket] = PositionState(ticket=ticket, symbol=s, side=sig.action, volume=float(r["volume"]),
                                               open_price=fill, open_time=now_wall, sl=sl, tp=tp,
                                               entry_vu=f.vol_unit, reason=sig.reason)
        self._save_state()
        self.add_log("INFO", f"ENTRY {sig.action} {r['volume']} {s} @ {fill} #{ticket} score {sig.score:+.2f} "
                             f"[{sig.reason}] SL {sl:.{int(info['digits'])}f} slip {slip:+.0f}pt "
                             f"risk {size.risk_money:.2f} {r.get('latency', 0)*1000:.0f}ms")
        self._refresh_account()
        open_pos.append(OpenPos(s, sig.action, float(r["volume"]), 0.0, size.risk_money))

    # ----------------------------------------------------------------- loop
    def step(self, now_wall: Optional[float] = None) -> None:
        now_wall = time.time() if now_wall is None else now_wall
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
            self._manage_exits(now_wall)
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
                    self.sym_status[s] = {"signal": "HOLD" if not self.running else sig.action, "score": round(sig.score, 3),
                                          "reason": reason, "features": sig.features.brief(),
                                          "tick_age": round(now_wall - self.last_receipt[s], 1) if s in self.last_receipt else None}
        if now_wall - self.last_snapshot >= self.cfg.snapshot_interval:
            self.last_snapshot = now_wall
            self._build_snapshot(now_wall)

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
                              "reason": ps.reason if ps else ""})
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
                       "blocked": self.unverified_entry, "batch": self.batch, "symbols": self.sym_status,
                       "selected": self.selected, "live_allowed": self.allow_live},
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
