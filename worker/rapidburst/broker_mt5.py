"""Real MetaTrader 5 adapter using the official ``MetaTrader5`` Python package.

Windows only. Nothing here fabricates data: if the package or terminal is not
available every call reports that honestly.
"""
from __future__ import annotations

import time
from typing import Optional

try:  # pragma: no cover - only importable on Windows
    import MetaTrader5 as mt5  # type: ignore
    MT5_IMPORT_ERROR = ""
except Exception as exc:  # noqa: BLE001
    mt5 = None
    MT5_IMPORT_ERROR = f"MetaTrader5 package unavailable on this machine ({type(exc).__name__}: {exc})"

RET_DONE = 10009
RET_PLACED = 10008
RET_DONE_PARTIAL = 10010
RET_REQUOTE = 10004
RET_PRICE_CHANGED = 10020
RET_PRICE_OFF = 10021
RET_TIMEOUT = 10012
RET_INVALID_FILL = 10030
RET_CONNECTION = 10031
OK_CODES = {RET_DONE, RET_PLACED, RET_DONE_PARTIAL}
RETRY_PRICE = {RET_REQUOTE, RET_PRICE_CHANGED, RET_PRICE_OFF}

TRADE_MODE_NAMES = {0: "DEMO", 1: "CONTEST", 2: "REAL"}


def _d(obj) -> dict:
    return obj._asdict() if obj is not None else {}


class MT5Broker:
    name = "mt5"

    def __init__(self, terminal_path: Optional[str] = None, portable: bool = True):
        self.terminal_path = terminal_path
        self.portable = portable
        self.initialized = False

    # ---------------------------------------------------------- connection
    @staticmethod
    def available() -> tuple[bool, str]:
        return (mt5 is not None, MT5_IMPORT_ERROR or "ok")

    def last_error(self) -> str:
        if mt5 is None:
            return MT5_IMPORT_ERROR
        code, msg = mt5.last_error()
        return f"{code}: {msg}"

    def connect(self, login: int, password: str, server: str, timeout_ms: int = 60000) -> tuple[bool, str]:
        if mt5 is None:
            return False, MT5_IMPORT_ERROR
        kwargs = dict(login=int(login), password=password, server=server, timeout=timeout_ms)
        if self.terminal_path:
            kwargs["path"] = self.terminal_path
            kwargs["portable"] = self.portable
        if self.initialized:
            ok = mt5.login(int(login), password=password, server=server, timeout=timeout_ms)
        else:
            ok = mt5.initialize(**kwargs)
            if not ok:
                # second attempt: initialise terminal first, then log in
                init_kwargs = {k: v for k, v in kwargs.items() if k in ("path", "portable", "timeout")}
                if mt5.initialize(**init_kwargs):
                    ok = mt5.login(int(login), password=password, server=server, timeout=timeout_ms)
        self.initialized = bool(ok) or self.initialized
        if not ok:
            return False, f"MT5 login failed - {self.last_error()}"
        acct = mt5.account_info()
        if acct is None or int(acct.login) != int(login):
            return False, f"MT5 account not available after login - {self.last_error()}"
        return True, "connected"

    def shutdown(self) -> None:
        if mt5 is not None and self.initialized:
            mt5.shutdown()
        self.initialized = False

    def terminal(self) -> dict:
        if mt5 is None or not self.initialized:
            return {}
        return _d(mt5.terminal_info())

    def account(self) -> dict:
        if mt5 is None or not self.initialized:
            return {}
        return _d(mt5.account_info())

    def is_connected(self) -> bool:
        t = self.terminal()
        return bool(t.get("connected")) and bool(self.account())

    # -------------------------------------------------------------- symbols
    def search_symbols(self, query: str, limit: int = 60) -> list[dict]:
        if mt5 is None or not self.initialized:
            return []
        syms = mt5.symbols_get() or ()
        q = (query or "").strip().lower()
        out = []
        for s in syms:
            if q and q not in s.name.lower() and q not in (s.description or "").lower():
                continue
            out.append({"name": s.name, "desc": s.description, "path": s.path,
                        "visible": bool(s.visible), "trade_mode": int(s.trade_mode)})
        # exact / prefix matches first
        out.sort(key=lambda x: (not x["name"].lower().startswith(q), x["name"]))
        return out[:limit]

    def select_symbol(self, symbol: str) -> bool:
        return bool(mt5 is not None and self.initialized and mt5.symbol_select(symbol, True))

    def symbol_info(self, symbol: str) -> dict:
        if mt5 is None or not self.initialized:
            return {}
        return _d(mt5.symbol_info(symbol))

    def last_tick(self, symbol: str) -> dict:
        if mt5 is None or not self.initialized:
            return {}
        return _d(mt5.symbol_info_tick(symbol))

    def candles(self, symbol: str, timeframe: str = "M1", hours: float = 0.5) -> list[dict]:
        if mt5 is None or not self.initialized:
            return []
        tf = {"M1": (mt5.TIMEFRAME_M1, 1), "M5": (mt5.TIMEFRAME_M5, 5), "M15": (mt5.TIMEFRAME_M15, 15)}.get(timeframe)
        if not tf:
            return []
        n = max(2, int(hours * 60 / tf[1]))
        rates = mt5.copy_rates_from_pos(symbol, tf[0], 0, n)
        if rates is None:
            return []
        return [{"time": float(r["time"]), "open": float(r["open"]), "high": float(r["high"]),
                 "low": float(r["low"]), "close": float(r["close"])} for r in rates]

    def quote_state(self, symbol: str) -> str:
        """MT5 gives server-time ticks only, so freshness cannot be proven here: 'unknown' when a
        last tick exists (the engine's own tick-age checks then decide), 'closed' when there is none."""
        return "unknown" if self.last_tick(symbol) else "closed"

    def ticks_since(self, symbol: str, last_msc: int, max_count: int = 2000) -> list[tuple[float, float, float, int]]:
        """New ticks (time_msc > last_msc) as (t_seconds, bid, ask, time_msc)."""
        if mt5 is None or not self.initialized:
            return []
        if last_msc <= 0:
            lt = mt5.symbol_info_tick(symbol)
            if lt is None:
                return []
            start = int(lt.time) - 180
        else:
            start = int(last_msc // 1000)
        arr = mt5.copy_ticks_from(symbol, start, max_count, mt5.COPY_TICKS_INFO)
        out = []
        if arr is None:
            return out
        for row in arr:
            msc = int(row["time_msc"])
            if msc <= last_msc:
                continue
            bid, ask = float(row["bid"]), float(row["ask"])
            if bid > 0 and ask > 0:
                out.append((msc / 1000.0, bid, ask, msc))
        return out

    # ----------------------------------------------------- positions/orders
    def positions(self, symbol: Optional[str] = None) -> list[dict]:
        if mt5 is None or not self.initialized:
            return []
        res = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
        return [_d(p) for p in (res or ())]

    def position(self, ticket: int) -> Optional[dict]:
        if mt5 is None or not self.initialized:
            return None
        res = mt5.positions_get(ticket=int(ticket))
        return _d(res[0]) if res else None

    def orders(self) -> list[dict]:
        if mt5 is None or not self.initialized:
            return []
        return [_d(o) for o in (mt5.orders_get() or ())]

    def deals(self, t_from: float, t_to: float) -> list[dict]:
        if mt5 is None or not self.initialized:
            return []
        res = mt5.history_deals_get(int(t_from), int(t_to))
        return [_d(d) for d in (res or ())]

    def calc_margin(self, side: str, symbol: str, volume: float, price: float) -> Optional[float]:
        if mt5 is None or not self.initialized:
            return None
        t = mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL
        return mt5.order_calc_margin(t, symbol, volume, price)

    # ------------------------------------------------------------ execution
    @staticmethod
    def _fillings(info: dict) -> list[int]:
        fm = int(info.get("filling_mode", 0))
        modes = []
        if fm & 1:
            modes.append(mt5.ORDER_FILLING_FOK)
        if fm & 2:
            modes.append(mt5.ORDER_FILLING_IOC)
        modes.append(mt5.ORDER_FILLING_RETURN)
        return modes

    def market_order(self, symbol: str, side: str, volume: float, sl: float, tp: float,
                     deviation: int, magic: int, comment: str) -> dict:
        """Validated market order. Returns dict(ok, retcode, msg, ticket, price, req_price)."""
        if mt5 is None or not self.initialized:
            return {"ok": False, "msg": "MT5 not connected"}
        info = self.symbol_info(symbol)
        if not info:
            return {"ok": False, "msg": f"symbol {symbol} unavailable"}
        digits = int(info["digits"])
        fillings = self._fillings(info)
        last = None
        for attempt in range(3):
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                return {"ok": False, "msg": "no tick"}
            price = tick.ask if side == "BUY" else tick.bid
            req = {
                "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": float(volume),
                "type": mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL,
                "price": price, "sl": round(sl, digits), "tp": round(tp, digits) if tp else 0.0,
                "deviation": int(deviation), "magic": int(magic), "comment": comment[:31],
                "type_time": mt5.ORDER_TIME_GTC, "type_filling": fillings[0],
            }
            chk = None
            for fill in fillings:
                req["type_filling"] = fill
                chk = mt5.order_check(req)
                if chk is not None and chk.retcode == 0:
                    break
            if chk is None or chk.retcode != 0:
                return {"ok": False, "retcode": getattr(chk, "retcode", None),
                        "msg": f"order_check rejected: {getattr(chk, 'comment', self.last_error())}"}
            t_send = time.time()
            res = mt5.order_send(req)
            latency = time.time() - t_send
            if res is None or res.retcode == RET_TIMEOUT:
                # ambiguous: verify on the server before any retry (duplicate prevention)
                time.sleep(0.5)
                for p in self.positions(symbol):
                    if p.get("magic") == magic and p.get("comment", "").startswith(comment[:31]):
                        return {"ok": True, "retcode": RET_DONE, "msg": "verified after timeout",
                                "ticket": int(p["ticket"]), "price": p["price_open"], "req_price": price,
                                "volume": p["volume"], "latency": latency}
                return {"ok": False, "retcode": getattr(res, "retcode", None),
                        "msg": f"order_send no result - {self.last_error()}"}
            last = res
            if res.retcode in OK_CODES:
                ticket = int(res.order)
                pos = self.position(ticket)
                if pos is None:
                    # hedging accounts use order==position id; otherwise look up by comment
                    for p in self.positions(symbol):
                        if p.get("magic") == magic and p.get("comment", "").startswith(comment[:31]):
                            pos = p
                            break
                if pos is None:
                    return {"ok": False, "retcode": res.retcode,
                            "msg": "order reported done but position not found - entries paused for verification"}
                return {"ok": True, "retcode": res.retcode, "msg": res.comment, "ticket": int(pos["ticket"]),
                        "price": pos["price_open"], "req_price": price, "volume": pos["volume"],
                        "latency": latency}
            if res.retcode in RETRY_PRICE:
                continue
            return {"ok": False, "retcode": res.retcode, "msg": f"order_send rejected: {res.comment}"}
        return {"ok": False, "retcode": getattr(last, "retcode", None), "msg": "requotes - gave up"}

    def close_position(self, ticket: int, deviation: int, magic: int) -> dict:
        """Close one position and verify it is gone."""
        if mt5 is None or not self.initialized:
            return {"ok": False, "msg": "MT5 not connected"}
        pos = self.position(ticket)
        if pos is None:
            return {"ok": True, "msg": "already closed", "verified": True}
        symbol = pos["symbol"]
        info = self.symbol_info(symbol)
        fillings = self._fillings(info) if info else [mt5.ORDER_FILLING_IOC]
        last_msg = ""
        for attempt in range(4):
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                last_msg = "no tick"
                time.sleep(0.3)
                continue
            is_buy = pos["type"] == mt5.POSITION_TYPE_BUY
            req = {
                "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": float(pos["volume"]),
                "type": mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
                "position": int(ticket), "price": tick.bid if is_buy else tick.ask,
                "deviation": int(deviation), "magic": int(magic), "comment": "RBurst close",
                "type_time": mt5.ORDER_TIME_GTC, "type_filling": fillings[min(attempt, len(fillings) - 1)],
            }
            res = mt5.order_send(req)
            if res is not None and res.retcode in OK_CODES or res is None or res.retcode == RET_TIMEOUT:
                time.sleep(0.2)
                if self.position(ticket) is None:
                    return {"ok": True, "msg": "closed", "verified": True,
                            "price": req["price"]}
            last_msg = getattr(res, "comment", self.last_error()) if res is not None else self.last_error()
            time.sleep(0.3)
            pos = self.position(ticket) or pos
        return {"ok": self.position(ticket) is None, "msg": f"close not verified: {last_msg}",
                "verified": self.position(ticket) is None}

    def cancel_order(self, ticket: int) -> dict:
        if mt5 is None or not self.initialized:
            return {"ok": False, "msg": "MT5 not connected"}
        res = mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": int(ticket)})
        ok = res is not None and res.retcode in OK_CODES
        return {"ok": ok, "msg": getattr(res, "comment", self.last_error())}

    def modify_sl(self, ticket: int, sl: float, tp: float) -> dict:
        if mt5 is None or not self.initialized:
            return {"ok": False, "msg": "MT5 not connected"}
        pos = self.position(ticket)
        if pos is None:
            return {"ok": False, "msg": "no position"}
        res = mt5.order_send({"action": mt5.TRADE_ACTION_SLTP, "position": int(ticket),
                              "symbol": pos["symbol"], "sl": sl, "tp": tp})
        ok = res is not None and res.retcode in OK_CODES
        return {"ok": ok, "msg": getattr(res, "comment", self.last_error())}
