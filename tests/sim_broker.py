"""SimBroker: an in-memory TEST DOUBLE that mimics the MT5Broker interface.

Used only by unit tests / replay harness. It is never used by the deployed
worker and its numbers are never shown as real account data.
"""
from __future__ import annotations

import itertools


class SimBroker:
    name = "sim"

    def __init__(self, ticks_by_symbol: dict, balance: float = 10000.0, trade_mode: int = 0,
                 hedging: bool = True, algo: bool = True, slippage_pts: float = 0.0):
        self.series = ticks_by_symbol
        self.idx = {s: 0 for s in ticks_by_symbol}
        self.now = 0.0
        self.balance = balance
        self.trade_mode = trade_mode
        self.hedging = hedging
        self.algo = algo
        self.slip = slippage_pts
        self.connected_flag = False
        self.drop = False
        self.positions_: dict[int, dict] = {}
        self.orders_: dict[int, dict] = {}
        self.deals_: list[dict] = []
        self.tickets = itertools.count(1000)
        self.sent_orders = 0
        self.reject_next = None
        self.login_calls = 0
        self.spec = {"point": 0.01, "digits": 2, "volume_min": 0.01, "volume_max": 100.0, "volume_step": 0.01,
                     "trade_tick_size": 0.01, "trade_tick_value": 1.0, "trade_tick_value_loss": 1.0,
                     "trade_stops_level": 0, "trade_freeze_level": 0, "filling_mode": 1, "trade_mode": 4,
                     "visible": True}

    # time control
    def advance(self, t: float) -> None:
        self.now = t
        for s, ser in self.series.items():
            i = self.idx[s]
            while i < len(ser) and ser[i][0] <= t:
                i += 1
            self.idx[s] = i
        self._check_stops()

    def _tick(self, s):
        i = self.idx.get(s, 0)
        if i == 0:
            return None
        return self.series[s][i - 1]

    def _pnl(self, p, price):
        d = 1 if p["type"] == 0 else -1
        return (price - p["price_open"]) * d / self.spec["trade_tick_size"] * self.spec["trade_tick_value"] * p["volume"]

    def _check_stops(self):
        for t, p in list(self.positions_.items()):
            tk = self._tick(p["symbol"])
            if not tk:
                continue
            px = tk[1] if p["type"] == 0 else tk[2]
            p["price_current"] = px
            p["profit"] = round(self._pnl(p, px), 2)
            hit = None
            if p["sl"] and ((p["type"] == 0 and px <= p["sl"]) or (p["type"] == 1 and px >= p["sl"])):
                hit = p["sl"]
            elif p["tp"] and ((p["type"] == 0 and px >= p["tp"]) or (p["type"] == 1 and px <= p["tp"])):
                hit = p["tp"]
            if hit is not None:
                self._close(t, hit, magic=p["magic"])

    def _close(self, ticket, price, magic):
        p = self.positions_.pop(ticket)
        profit = round(self._pnl(p, price), 2)
        self.balance += profit
        self.deals_.append({"ticket": next(self.tickets), "position_id": ticket, "time": self.now, "magic": magic,
                            "entry": 1, "profit": profit, "commission": 0.0, "swap": 0.0, "fee": 0.0,
                            "symbol": p["symbol"]})

    # interface
    def available(self):
        return True, "ok"

    def connect(self, login, password, server, timeout_ms=60000):
        self.login_calls += 1
        if self.drop:
            return False, "MT5 login failed - (-10004, 'No IPC connection')"
        self.connected_flag = True
        self.login = login
        self.server = server
        return True, "connected"

    def shutdown(self):
        self.connected_flag = False
        self.shutdown_calls = getattr(self, "shutdown_calls", 0) + 1

    def is_connected(self):
        return self.connected_flag and not self.drop

    def terminal(self):
        return {"connected": self.is_connected(), "trade_allowed": self.algo, "tradeapi_disabled": False,
                "ping_last": 12000} if self.connected_flag else {}

    def account(self):
        if not self.connected_flag:
            return {}
        floating = sum(p["profit"] for p in self.positions_.values())
        return {"login": getattr(self, "login", 1), "server": "Sim-Demo", "name": "test", "company": "SimBroker",
                "currency": "USD", "leverage": 100, "trade_mode": self.trade_mode, "balance": round(self.balance, 2),
                "equity": round(self.balance + floating, 2), "margin_free": round(self.balance + floating - 100, 2),
                "margin_level": 0.0, "profit": floating, "trade_allowed": True, "trade_expert": True,
                "margin_mode": 2 if self.hedging else 0}

    def search_symbols(self, q, limit=60):
        return [{"name": s, "desc": "sim", "path": "sim", "visible": True, "trade_mode": 4}
                for s in self.series if q.lower() in s.lower()]

    def select_symbol(self, s):
        return s in self.series

    def symbol_info(self, s):
        return dict(self.spec, name=s) if s in self.series else {}

    def last_tick(self, s):
        tk = self._tick(s)
        if not tk or not self.is_connected():
            return {}
        return {"time": int(tk[0]), "time_msc": int(round(tk[0] * 1000)), "bid": tk[1], "ask": tk[2]}

    def ticks_since(self, s, last_msc, max_count=2000):
        if not self.is_connected():
            return []
        out = []
        for t, b, a in self.series[s][max(0, self.idx[s] - 3000):self.idx[s]]:
            msc = int(round(t * 1000))
            if msc > last_msc:
                out.append((msc / 1000.0, b, a, msc))
        return out

    def positions(self, symbol=None):
        return [dict(p) for p in self.positions_.values() if symbol is None or p["symbol"] == symbol]

    def position(self, ticket):
        p = self.positions_.get(int(ticket))
        return dict(p) if p else None

    def orders(self):
        return [dict(o) for o in self.orders_.values()]

    def deals(self, t_from, t_to):
        return [dict(d) for d in self.deals_ if t_from <= d["time"] <= t_to]

    def calc_margin(self, side, s, vol, price):
        return price * vol * 100 / 100.0

    def market_order(self, s, side, volume, sl, tp, deviation, magic, comment):
        self.sent_orders += 1
        if self.reject_next:
            msg, self.reject_next = self.reject_next, None
            return {"ok": False, "retcode": 10006, "msg": msg}
        tk = self._tick(s)
        d = 1 if side == "BUY" else -1
        req = tk[2] if side == "BUY" else tk[1]
        fill = round(req + d * self.slip * self.spec["point"], 2)
        t = next(self.tickets)
        self.positions_[t] = {"ticket": t, "symbol": s, "type": 0 if side == "BUY" else 1, "volume": volume,
                              "price_open": fill, "price_current": fill, "sl": round(sl, 2), "tp": round(tp, 2),
                              "profit": 0.0, "swap": 0.0, "magic": magic, "comment": comment, "time": self.now}
        self.deals_.append({"ticket": next(self.tickets), "position_id": t, "time": self.now, "magic": magic,
                            "entry": 0, "profit": 0.0, "commission": 0.0, "swap": 0.0, "fee": 0.0, "symbol": s})
        return {"ok": True, "retcode": 10009, "msg": "done", "ticket": t, "price": fill, "req_price": req,
                "volume": volume, "latency": 0.01}

    def close_position(self, ticket, deviation, magic):
        p = self.positions_.get(int(ticket))
        if p is None:
            return {"ok": True, "msg": "already closed", "verified": True}
        if not self.is_connected():
            return {"ok": False, "msg": "no connection", "verified": False}
        tk = self._tick(p["symbol"])
        px = tk[1] if p["type"] == 0 else tk[2]
        self._close(int(ticket), px, magic)
        return {"ok": True, "msg": "closed", "verified": int(ticket) not in self.positions_, "price": px}

    def cancel_order(self, ticket):
        self.orders_.pop(int(ticket), None)
        return {"ok": True, "msg": "removed"}

    def modify_sl(self, ticket, sl, tp):
        p = self.positions_.get(int(ticket))
        if not p:
            return {"ok": False, "msg": "no position"}
        p["sl"], p["tp"] = sl, tp
        return {"ok": True, "msg": "ok"}

    # helpers for tests
    def add_foreign_position(self, symbol, side="BUY", volume=0.10, magic=0):
        tk = self._tick(symbol)
        t = next(self.tickets)
        self.positions_[t] = {"ticket": t, "symbol": symbol, "type": 0 if side == "BUY" else 1, "volume": volume,
                              "price_open": tk[2], "price_current": tk[1], "sl": 0.0, "tp": 0.0, "profit": 0.0,
                              "swap": 0.0, "magic": magic, "comment": "manual", "time": self.now}
        return t

    def add_pending(self, symbol, magic):
        t = next(self.tickets)
        self.orders_[t] = {"ticket": t, "symbol": symbol, "magic": magic, "type": 2}
        return t
