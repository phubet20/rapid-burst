"""FakeTickerAll: an in-process TEST DOUBLE of the TickerAll REST + WebSocket API.

Implements only the endpoints the adapter uses, with the request/response shapes
from TickerAll's published OpenAPI spec (https://tickerall.com/openapi.json).
All prices/accounts here are SYNTHETIC test data - never real market data.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import time
import threading
from datetime import datetime, timezone

from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestServer

API_KEY = "cf_api_TEST_ONLY_not_a_real_key"
DEMO = {"login": 5550001, "password": "demo-pw", "server": "Fake-Demo"}
LIVE = {"login": 9990001, "password": "live-pw", "server": "Fake-Live"}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class FakeTickerAll:
    def __init__(self):
        self.lock = threading.RLock()
        self.calls: list[tuple] = []
        self.ids = itertools.count(7000)
        self.session = False
        self.hot = True
        self.sessions_opened = 0
        self.sessions_deleted = 0
        self.positions: dict[int, dict] = {}
        self.orders: dict[int, dict] = {}
        self.deals: list[dict] = []
        self.idem: dict[str, tuple] = {}
        self.fail_next_post_after_place = 0
        self.quotes = {"XAUUSD": (2400.00, 2400.20), "EURUSD": (1.10000, 1.10010), "BTCUSDm": (98000.00, 98018.00)}
        self.auto_ticks = False           # when True, subscribed symbols tick twice a second (synthetic)
        self.tick_tasks: list = []
        self.balance = 10000.0
        self.ws_clients: list = []
        self.subscriptions: list = []
        self.loop = None
        self.server = None
        self.base = ""

    # ---------------------------------------------------------- lifecycle
    def start(self) -> str:
        ready = threading.Event()

        def runner():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            app = web.Application()
            r = app.router
            r.add_post("/v1/sessions", self.h_session)
            r.add_delete("/v1/sessions/{aid}", self.h_session_del)
            r.add_get("/v1/accounts", self.h_accounts)
            r.add_get("/v1/accounts/{aid}", self.h_account)
            r.add_get("/v1/accounts/{aid}/symbols", self.h_symbols)
            r.add_get("/v1/accounts/{aid}/symbols/{sym}/spec", self.h_spec)
            r.add_get("/v1/accounts/{aid}/symbol-specs", self.h_specs)
            r.add_get("/v1/accounts/{aid}/symbols/{sym}/quote", self.h_quote)
            r.add_get("/v1/accounts/{aid}/orders", self.h_orders)
            r.add_post("/v1/accounts/{aid}/orders", self.h_place)
            r.add_delete("/v1/accounts/{aid}/orders/{t}", self.h_cancel)
            r.add_delete("/v1/accounts/{aid}/positions/{t}", self.h_close)
            r.add_patch("/v1/accounts/{aid}/positions/{t}", self.h_modify)
            r.add_get("/v1/accounts/{aid}/deals-history", self.h_deals)
            r.add_get("/v1/accounts/{aid}/candles", self.h_candles)
            r.add_get("/v1/stream", self.h_ws)
            self.server = TestServer(app)
            self.loop.run_until_complete(self.server.start_server())
            self.base = str(self.server.make_url("")).rstrip("/")
            ready.set()
            self.loop.run_forever()

        threading.Thread(target=runner, daemon=True).start()
        ready.wait(10)
        return self.base

    def stop(self):
        if self.loop:
            fut = asyncio.run_coroutine_threadsafe(self.server.close(), self.loop)
            try:
                fut.result(5)
            except Exception:  # noqa: BLE001
                pass
            self.loop.call_soon_threadsafe(self.loop.stop)

    # ------------------------------------------------------------ helpers
    @staticmethod
    def err(status, code, message="", details=None):
        return web.json_response({"error": code, "message": message, **({"details": details} if details else {})},
                                 status=status)

    def auth(self, request):
        self.calls.append((request.method, request.path, request.headers.get("Idempotency-Key")))
        return request.headers.get("Authorization") == f"Bearer {API_KEY}"

    def guard(self, request):
        if not self.auth(request):
            return self.err(401, "UNAUTHORIZED", "Missing or invalid Authorization header")
        if request.match_info.get("aid") not in (None, "acc_FAKE1"):
            return self.err(404, "NOT_FOUND")
        if "aid" in request.match_info and (not self.session or not self.hot):
            return self.err(409, "BROKER_ACCOUNT_NOT_HOT", "connection cooled")
        return None

    def add_position(self, symbol="XAUUSD", side="BUY", volume=0.1, comment="manual", sl=0.0, tp=0.0):
        t = next(self.ids)
        bid, ask = self.quotes[symbol]
        with self.lock:
            self.positions[t] = {"ticket": t, "symbol": symbol, "side": side, "volume": volume,
                                 "entryPrice": ask if side == "BUY" else bid, "stopLoss": sl, "takeProfit": tp,
                                 "currentPrice": bid if side == "BUY" else ask, "profit": 0.0, "swap": 0,
                                 "commission": 0, "comment": comment, "magic": 0, "openTime": now_iso()}
            self.deals.append({"ticket": str(next(self.ids)), "positionId": str(t), "symbol": symbol, "side": side,
                               "entry": "IN", "volume": volume, "price": self.positions[t]["entryPrice"],
                               "profit": 0, "swap": 0, "commission": 0, "time": now_iso()})
        return t

    def add_order(self, symbol="XAUUSD", comment="manual"):
        t = next(self.ids)
        self.orders[t] = {"ticket": str(t), "symbol": symbol, "type": "BUY_LIMIT", "side": "BUY", "orderType": "LIMIT",
                          "volume": 0.1, "price": 2000.0, "stopLoss": 1990.0, "takeProfit": 0, "setTime": now_iso(),
                          "comment": comment, "magic": 0}
        return t

    def push_tick(self, symbol, bid, ask, stale=False, ts=None):
        self.quotes[symbol] = (bid, ask)
        msg = {"type": "tick", "accountId": "acc_FAKE1", "symbol": symbol, "bid": bid, "ask": ask,
               "timestamp": ts or now_iso(), "brokerTime": None, "stale": stale}

        async def send():
            for ws in list(self.ws_clients):
                if any(c.get("kind") == "ticks" and symbol in (c.get("symbols") or []) for c in self.subscriptions):
                    await ws.send_json(msg)
        asyncio.run_coroutine_threadsafe(send(), self.loop).result(5)

    # ----------------------------------------------------------- handlers
    async def h_session(self, request):
        if not self.auth(request):
            return self.err(401, "UNAUTHORIZED")
        b = await request.json()
        if b.get("account") == LIVE["login"]:
            return self.err(403, "FREE_TIER_LIVE_REJECTED", "Free plan: live accounts via the API are not available")
        if (b.get("account"), b.get("password"), b.get("server")) != (DEMO["login"], DEMO["password"], DEMO["server"]):
            return self.err(401, "BROKER_AUTH_FAILED", "Invalid account")
        self.session, self.hot = True, True
        self.sessions_opened += 1
        return web.json_response({"accountId": "acc_FAKE1", "isDemo": True, "status": "connected",
                                  "expiresAt": now_iso()})

    async def h_session_del(self, request):
        if not self.auth(request):
            return self.err(401, "UNAUTHORIZED")
        self.session = False
        self.sessions_deleted += 1
        return web.Response(status=204)

    async def h_accounts(self, request):
        if not self.auth(request):
            return self.err(401, "UNAUTHORIZED")
        return web.json_response([{"id": "acc_FAKE1", "broker": "mt5", "server": DEMO["server"],
                                   "accountNumber": "****0001", "isDemo": True, "group": "demo\\Standard",
                                   "status": "CONNECTED", "hot": self.session and self.hot, "marginMode": "hedging",
                                   "readOnly": False, "credentialRejectedAt": None}])

    async def h_account(self, request):
        g = self.guard(request)
        if g:
            return g
        with self.lock:
            fl = sum(p["profit"] for p in self.positions.values())
            return web.json_response({
                "id": "acc_FAKE1", "broker": "mt5", "server": DEMO["server"], "accountNumber": "****0001",
                "isDemo": True, "credentialRejectedAt": None, "status": "online",
                "account": {"name": "Fake Demo", "accountType": "demo", "group": "demo\\Standard", "leverage": 100,
                            "balance": self.balance, "currency": "USD", "equity": self.balance + fl, "margin": 0.0,
                            "freeMargin": self.balance + fl, "marginLevel": 0.0, "credit": None,
                            "serverTimeOffsetSeconds": 7200},
                "positions": list(self.positions.values())})

    async def h_candles(self, request):
        """Synthetic rising bars (TEST DATA) in TickerAll's documented shape."""
        g = self.guard(request)
        if g:
            return g
        q = request.query
        if q.get("symbol") not in self.quotes:
            return web.json_response({"error": {"code": "SYMBOL_NOT_FOUND", "message": "unknown symbol"}}, status=404)
        step = {"M1": 60, "M5": 300}.get(q.get("timeframe"), 60)
        n = max(1, int(float(q.get("hours", 1)) * 3600 / step))
        self.candle_calls = getattr(self, "candle_calls", 0) + 1
        now = int(time.time()) // step * step
        if "." in str(q.get("hours", "1")):
            return web.json_response({"error": {"code": "VALIDATION", "message": "hours must be an integer"}},
                                     status=400)   # TEST DOUBLE: stricter than needed, catches float look-backs
        rows = [{"timestamp": now - (n - 1 - i) * step, "open": 100 + i, "high": 101 + i, "low": 99.5 + i,
                 "close": 100.8 + i, "bid": 100.8 + i, "tickVolume": 50 + i, "spread": 0.2} for i in range(n)]
        return web.json_response({"candles": rows[::-1], "count": n, "coverage": {}, "truncated": False})

    async def h_symbols(self, request):
        g = self.guard(request)
        return g or web.json_response({"symbols": list(self.quotes) + ["BTCUSD", "ETHUSDm", "US30m"], "watched": []})

    @staticmethod
    def spec_for(s):
        if s == "BTCUSDm":     # crypto: large price, tickValueAccount NOT supplied -> adapter must derive it
            return {"name": s, "volumeMin": 0.01, "volumeMax": 20, "volumeStep": 0.01, "digits": 2, "point": 0.01,
                    "tickSize": 0.01, "contractSize": 1, "tickValue": 0.01, "stopLevel": 0, "freezeLevel": 0,
                    "baseCurrency": "BTC", "profitCurrency": "USD", "marginCurrency": "USD"}
        if s == "ETHUSDm":     # crypto where the tick value cannot be derived (profit ccy not account ccy)
            return {"name": s, "volumeMin": 0.1, "volumeMax": 100, "volumeStep": 0.1, "digits": 2, "point": 0.01,
                    "tickSize": 0.01, "contractSize": 1, "baseCurrency": "ETH", "profitCurrency": "EUR"}
        if s == "US30m":
            return {"name": s, "volumeMin": 0.1, "volumeMax": 100, "volumeStep": 0.1, "digits": 2, "point": 0.01,
                    "tickSize": 0.01, "contractSize": 1, "tickValue": 0.01, "tickValueAccount": 0.01,
                    "profitCurrency": "USD", "marginCurrency": "USD"}
        if s == "XAUUSD":
            return {"name": s, "volumeMin": 0.01, "volumeMax": 100, "volumeStep": 0.01, "specSource": "broker",
                    "digits": 2, "point": 0.01, "tickSize": 0.01, "contractSize": 100, "tickValue": 1,
                    "tickValueAccount": 1, "stopLevel": 0, "freezeLevel": 0, "baseCurrency": "XAU",
                    "profitCurrency": "USD", "marginCurrency": "XAU"}
        if s == "EURUSD":
            return {"name": s, "volumeMin": 0.01, "volumeMax": 100, "volumeStep": 0.01, "digits": 5, "point": 1e-05,
                    "tickSize": 1e-05, "contractSize": 100000, "tickValue": 1, "tickValueAccount": 1, "stopLevel": 0,
                    "freezeLevel": 0, "baseCurrency": "EUR", "profitCurrency": "USD", "marginCurrency": "EUR"}
        # unknown geometry (fields absent = unknown, per TickerAll docs)
        return {"name": s, "volumeMin": 0.01, "volumeMax": 1, "volumeStep": 0.01}

    async def h_specs(self, request):
        g = self.guard(request)
        if g:
            return g
        names = list(self.quotes) + ["BTCUSD", "ETHUSDm", "US30m"]
        return web.json_response({"specs": [self.spec_for(n) for n in names]})

    async def h_spec(self, request):
        g = self.guard(request)
        if g:
            return g
        return web.json_response({"spec": self.spec_for(request.match_info["sym"])})

    async def h_quote(self, request):
        g = self.guard(request)
        if g:
            return g
        bid, ask = self.quotes.get(request.match_info["sym"], (None, None))
        return web.json_response({"symbol": request.match_info["sym"], "bid": bid, "ask": ask})

    async def h_orders(self, request):
        g = self.guard(request)
        return g or web.json_response({"orders": list(self.orders.values())})

    async def h_place(self, request):
        g = self.guard(request)
        if g:
            return g
        key = request.headers.get("Idempotency-Key")
        if not key:
            return self.err(400, "IDEMPOTENCY_KEY_REQUIRED")
        if key in self.idem:
            st, body = self.idem[key]
            return web.json_response(body, status=st)
        b = await request.json()
        if b.get("symbol") not in self.quotes:
            return self.err(422, "BROKER_REJECTED", "unknown symbol")
        t = self.add_position(b["symbol"], b["side"], float(b["volume"]), b.get("comment") or "",
                              float(b.get("stopLoss") or 0), float(b.get("takeProfit") or 0))
        body = {"ticket": t, "symbol": b["symbol"], "side": b["side"], "type": "market", "volume": b["volume"],
                "price": None, "stopLoss": b.get("stopLoss"), "takeProfit": b.get("takeProfit"),
                "comment": b.get("comment"), "status": "open", "timestamp": now_iso()}
        self.idem[key] = (201, body)
        if self.fail_next_post_after_place:
            self.fail_next_post_after_place -= 1
            return self.err(503, "UPSTREAM_TIMEOUT", "simulated lost response")
        return web.json_response(body, status=201)

    async def h_close(self, request):
        g = self.guard(request)
        if g:
            return g
        t = int(request.match_info["t"])
        with self.lock:
            p = self.positions.pop(t, None)
            if p is None:
                return self.err(404, "TICKET_NOT_FOUND")
            bid, ask = self.quotes[p["symbol"]]
            px = bid if p["side"] == "BUY" else ask
            prof = round((px - p["entryPrice"]) * (1 if p["side"] == "BUY" else -1) * 100 * p["volume"], 2)
            self.balance += prof
            self.deals.append({"ticket": str(next(self.ids)), "positionId": str(t), "symbol": p["symbol"],
                               "side": "SELL" if p["side"] == "BUY" else "BUY", "entry": "OUT", "volume": p["volume"],
                               "price": px, "profit": prof, "swap": 0, "commission": -0.5, "time": now_iso()})
        return web.json_response({"closed": True, "closePrice": px, "profit": prof})

    async def h_modify(self, request):
        g = self.guard(request)
        if g:
            return g
        t = int(request.match_info["t"])
        p = self.positions.get(t)
        if not p:
            return self.err(404, "TICKET_NOT_FOUND")
        b = await request.json()
        self.modify_calls = getattr(self, "modify_calls", []) + [(t, dict(b))]
        if getattr(self, "modify_error", None):
            return self.err(422, "BROKER_REJECTED", self.modify_error)
        if getattr(self, "modify_ignore", False):          # accepted but not applied (verification test)
            return web.json_response({"ticket": t, "stopLoss": b.get("stopLoss"), "takeProfit": p["takeProfit"]})
        if "stopLoss" in b:
            p["stopLoss"] = b["stopLoss"]
        if "takeProfit" in b:
            p["takeProfit"] = b["takeProfit"]
        return web.json_response({"ticket": t, "stopLoss": p["stopLoss"], "takeProfit": p["takeProfit"]})

    async def h_cancel(self, request):
        g = self.guard(request)
        if g:
            return g
        if self.orders.pop(int(request.match_info["t"]), None) is None:
            return self.err(404, "TICKET_NOT_FOUND")
        return web.json_response({"cancelled": True})

    async def h_deals(self, request):
        g = self.guard(request)
        return g or web.json_response({"deals": list(self.deals), "count": len(self.deals), "hasMore": False,
                                       "nextCursor": None, "serverTimeOffsetSeconds": 7200})

    async def _auto(self, ws, symbols):
        import random
        rnd = random.Random(5)
        while not ws.closed:
            for sym in symbols:
                if sym in self.quotes:
                    bid, ask = self.quotes[sym]
                    sp = ask - bid
                    bid = round(bid + rnd.gauss(0, sp * 0.05), 5)
                    self.quotes[sym] = (bid, round(bid + sp, 5))
                    await ws.send_json({"type": "tick", "accountId": "acc_FAKE1", "symbol": sym, "bid": bid,
                                        "ask": round(bid + sp, 5), "timestamp": now_iso(), "stale": False})
            await asyncio.sleep(0.5)

    async def h_ws(self, request):
        if request.headers.get("Authorization") != f"Bearer {API_KEY}":
            return self.err(401, "UNAUTHORIZED")
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        self.ws_clients.append(ws)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                data = json.loads(msg.data)
                if data.get("type") == "subscribe":
                    acc, rej = [], []
                    for c in data.get("channels") or []:
                        if not (self.session and self.hot):
                            rej.append({"channel": c, "code": "BROKER_ACCOUNT_NOT_HOT"})
                        else:
                            acc.append(c)
                            self.subscriptions.append(c)
                            if c.get("kind") == "ticks" and self.auto_ticks:
                                self.tick_tasks.append(asyncio.ensure_future(self._auto(ws, c.get("symbols") or [])))
                    await ws.send_json({"type": "subscribed", "channels": acc, "rejected": rej,
                                        "correlationId": data.get("correlationId")})
                elif data.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
        finally:
            self.ws_clients.remove(ws)
        return ws
