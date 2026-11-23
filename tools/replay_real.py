"""REPLAY on REAL market data (not the broker's own feed, not live results).

Data: Binance public API (fetched by rb-notes/replay_data/fetch.py): aggTrades downsampled to <= 5 ticks/s and
1-minute klines. BTCUSDT stands in for BTCUSDm; PAXGUSDT (gold-backed token, tracks spot gold) stands in for
XAUUSDm. Broker conditions are MODELLED: fixed spread (BTC $6.0, XAU $0.24 as seen live on the demo), fills
at the quote 0.9 s after the send (live p50 send->fill latency), Exness-style contract specs, 1:2000 leverage.

The real Trader / engine / risk / exit code runs unchanged against this broker double (tests/sim_broker.py).
Usage: python tools/replay_real.py --root <repo> --data BTCUSDT_4h.json PAXGUSDT_8h.json --equity 56.6
"""
from __future__ import annotations

import argparse
import bisect
import json
import os
import sys
import tempfile
import time
from collections import Counter

SPECS = {
    "BTCUSDm": {"point": 0.01, "digits": 2, "trade_tick_size": 0.01, "trade_tick_value": 0.01,
                "trade_tick_value_loss": 0.01, "contract": 1.0, "spread": 6.0},
    "XAUUSDm": {"point": 0.001, "digits": 3, "trade_tick_size": 0.001, "trade_tick_value": 0.1,
                "trade_tick_value_loss": 0.1, "contract": 100.0, "spread": 0.24},
}
MAP = {"BTCUSDT": "BTCUSDm", "PAXGUSDT": "XAUUSDm"}


def build(root: str):
    sys.path.insert(0, os.path.join(root, "worker"))
    sys.path.insert(0, root)
    from tests.sim_broker import SimBroker  # noqa: E402

    class RealDataBroker(SimBroker):
        def __init__(self, series, klines, balance, latency=0.9):
            super().__init__(series, balance=balance)
            self.kl = klines
            self.kt = {s: [k[0] for k in v] for s, v in klines.items()}
            self.times = {s: [x[0] for x in v] for s, v in series.items()}
            self.lat = latency
            self.candle_error = ""
            self.pos_comment = {}

        def _spec(self, s):
            sp = SPECS[s]
            return dict(self.spec, **{k: v for k, v in sp.items() if k not in ("contract", "spread")})

        def symbol_info(self, s):
            return dict(self._spec(s), name=s) if s in self.series else {}

        def _pnl(self, p, price):
            sp = SPECS[p["symbol"]]
            d = 1 if p["type"] == 0 else -1
            return (price - p["price_open"]) * d / sp["trade_tick_size"] * sp["trade_tick_value"] * p["volume"]

        def quote(self, s):
            # the broker REST quote the live engine polls when the stream is quiet (TickerAll has one)
            tk = self._tick(s)
            return {"bid": tk[1], "ask": tk[2], "time": self.now} if tk else None

        def last_tick(self, s):
            # thin market (PAXG): no trade for a few seconds does not mean no quote - the last price is still the
            # quote (MODELLED), as a broker REST quote would be
            q = super().last_tick(s)
            if q:
                q["time"], q["time_msc"] = int(self.now), int(self.now * 1000)
            return q

        def account(self):
            a = super().account()
            if a:
                used = sum(self.calc_margin("", p["symbol"], p["volume"], p["price_open"]) for p in self.positions_.values())
                a["margin"] = round(used, 2)
                a["margin_free"] = round(a["equity"] - used, 2)
                a["margin_level"] = round(100 * a["equity"] / used, 1) if used else 0.0
            return a

        def calc_margin(self, side, s, vol, price):
            return price * vol * SPECS[s]["contract"] / 2000.0

        def candles(self, symbol, timeframe="M1", hours=0.5):
            if timeframe not in ("M1", "M5") or symbol not in self.kl:
                return []
            step = 60 if timeframe == "M1" else 300
            i = bisect.bisect_right(self.kt[symbol], self.now - 60.0)       # closed 1m bars only
            rows = self.kl[symbol][max(0, i - int(hours * 60)):i]
            out = {}
            for k in rows:                                                  # M5 = aggregated real 1m klines
                b0 = k[0] - k[0] % step
                if b0 + step > self.now:
                    continue                                                # only CLOSED bars of that frame
                b = out.get(b0)
                if b is None:
                    out[b0] = {"time": b0, "open": k[1], "high": k[2], "low": k[3], "close": k[4], "tick_volume": k[5]}
                else:
                    b["high"], b["low"] = max(b["high"], k[2]), min(b["low"], k[3])
                    b["close"], b["tick_volume"] = k[4], b["tick_volume"] + k[5]
            return [out[x] for x in sorted(out)]

        def market_order(self, s, side, volume, sl, tp, deviation, magic, comment):
            self.sent_orders += 1
            ser = self.series[s]
            j = bisect.bisect_right(self.times[s], self.now + self.lat) - 1     # quote ~0.9 s after send
            tk = ser[max(j, self.idx[s] - 1)]
            d = 1 if side == "BUY" else -1
            fill = tk[2] if d > 0 else tk[1]
            t = next(self.tickets)
            self.pos_comment[t] = comment
            self.positions_[t] = {"ticket": t, "symbol": s, "type": 0 if d > 0 else 1, "volume": volume,
                                  "price_open": fill, "price_current": fill, "sl": sl, "tp": tp, "profit": 0.0,
                                  "swap": 0.0, "magic": magic, "comment": comment, "time": self.now}
            self.deals_.append({"ticket": next(self.tickets), "position_id": t, "time": self.now, "magic": magic,
                                "entry": 0, "profit": 0.0, "commission": 0.0, "swap": 0.0, "fee": 0.0, "symbol": s})
            return {"ok": True, "retcode": 10009, "msg": "done", "ticket": t, "price": fill, "req_price": fill,
                    "volume": volume, "latency": self.lat}

        def modify_sl(self, ticket, sl, tp):
            p = self.positions_.get(int(ticket))
            if not p:
                return {"ok": False, "msg": "no position"}
            p["sl"], p["tp"] = sl, tp
            return {"ok": True, "msg": "ok"}

    return RealDataBroker


def run(root, files, equity, dt=0.25, label="", dump="", over=None):
    Broker = build(root)
    from rapidburst.config import EngineConfig
    from rapidburst.secure_store import SecureStore
    from rapidburst.trader import Trader
    series, klines, t0, t1 = {}, {}, [], []
    for f in files:
        d = json.load(open(f))
        s = MAP[d["symbol"]]
        sp = SPECS[s]["spread"]
        series[s] = [(t, round(p - sp / 2, 3), round(p + sp / 2, 3)) for t, p in d["ticks"]]
        klines[s] = d["klines"]
        t0.append(d["start"])
        t1.append(d["end"])
    T0, T1 = max(t0), min(t1)
    cfg = EngineConfig(**(over or {}))
    with tempfile.TemporaryDirectory() as tmp:
        br = Broker(series, klines, equity)
        store = SecureStore(tmp)
        store.save_credentials({"login": 12345678, "password": "replay-only", "server": "Replay-Demo"})
        tr = Trader(br, store, cfg, tmp)
        tr.selected = list(series)
        all_logs = []
        _add = tr.add_log

        def add_log(level, msg, *a, **k):
            all_logs.append(msg)
            return _add(level, msg, *a, **k)
        tr.add_log = add_log
        t = T0
        r = {"ok": False}
        while t < T0 + 60 and not r.get("ok"):
            br.advance(t)
            tr.step(t)
            t += 0.25
            if t >= T0 + 2:
                r = tr.cmd_start()
        if not r.get("ok"):
            raise SystemExit(f"START failed: {r}")
        T0 = t
        eq = [equity]
        wall = time.time()
        while t <= T1:
            br.advance(t)
            tr.step(t)
            eq.append(br.account()["equity"])
            t += dt
        tr.cmd_stop()
        br.advance(t)
        tr.step(t)
        closes = [d for d in br.deals_ if d["entry"] == 1 and d["magic"] == cfg.magic]
        baskets = Counter()           # basket P/L from the broker double's DEALS (authoritative), by order comment
        bsym = {}
        for d in closes:
            c = br.pos_comment.get(d["position_id"], "")
            gid = c.split("-")[1] if c.count("-") >= 2 else str(d["position_id"])
            baskets[gid] += d["profit"]
            bsym[gid] = d["symbol"]
        if dump:
            with open(dump, "w") as fh:
                fh.write("\n".join(all_logs))
        pls = list(baskets.values())
        wins = [x for x in pls if x > 0]
        losses = [x for x in pls if x <= 0]
        peak, mdd, mddp = eq[0], 0.0, 0.0
        for e in eq:
            peak = max(peak, e)
            mdd = max(mdd, peak - e)
            mddp = max(mddp, (peak - e) / peak if peak > 0 else 0.0)
        reasons = Counter()
        for msg in all_logs:
            if msg.startswith("GROUP ") and "fully closed" in msg and "[" in msg:
                reasons[msg.split("[", 1)[1].split(":")[0].split("(")[0].strip()[:40]] += 1
        order_pl = [d["profit"] for d in closes]
        per_sym = {}
        for gid, pl in baskets.items():
            x = per_sym.setdefault(bsym[gid], {"baskets": 0, "wins": 0, "net": 0.0})
            x["baskets"] += 1
            x["wins"] += pl > 0
            x["net"] = round(x["net"] + pl, 2)
        for x in per_sym.values():
            x["net"] = round(x["net"], 2)
        return {"label": label, "window_bkk": [time.strftime("%Y-%m-%d %H:%M", time.localtime(T0)),
                                                time.strftime("%H:%M", time.localtime(T1))],
                "start_equity": equity, "end_equity": round(br.account()["equity"], 2),
                "baskets": len(pls), "orders": len(order_pl), "basket_wins": len(wins),
                "win_rate": round(len(wins) / len(pls), 3) if pls else None,
                "profit_factor": round(sum(wins) / -sum(losses), 2) if losses and sum(losses) < 0 else None,
                "net": round(sum(order_pl), 2), "max_dd": round(mdd, 2),
                "max_dd_pct_of_peak": round(100 * mddp, 1),
                "safe_mode_trips": sum(1 for m in all_logs if m.startswith(("SAFE MODE:", "CATASTROPHIC"))),
                "per_symbol": per_sym, "exit_reasons": dict(reasons.most_common(8)),
                "runtime_s": round(time.time() - wall, 1)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--data", nargs="+", required=True)
    ap.add_argument("--equity", type=float, default=56.6)
    ap.add_argument("--label", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--dump", default="")
    ap.add_argument("--cfg", default="{}", help="JSON EngineConfig overrides (tuning experiments)")
    a = ap.parse_args()
    r = run(a.root, a.data, a.equity, label=a.label, dump=a.dump, over=json.loads(a.cfg))
    print(json.dumps(r))
    if a.out:
        json.dump(r, open(a.out, "w"), indent=1)
