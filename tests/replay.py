"""Replay / backtest harness on SYNTHETIC ticks (test-only, not market evidence).

Usage: python -m tests.replay [--seeds 1 2 3] [--hours 2]
Runs the real Trader + engine + risk code against the SimBroker test double and
prints trade statistics. Synthetic results say nothing about real profitability.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "worker"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rapidburst.config import EngineConfig  # noqa: E402
from tests.harness import Invariants, instrument, make, now_base, run  # noqa: E402
from tests.synth import gen_ticks  # noqa: E402


def replay(seed: int, hours: float, cfg: EngineConfig | None = None, noise_only: bool = False,
           slippage_pts: float = 3.0) -> dict:
    cfg = cfg or EngineConfig()
    T0 = now_base() - hours * 3600
    with tempfile.TemporaryDirectory() as d:
        ticks = gen_ticks(seed, T0, hours * 3600, burst_every=1e9 if noise_only else 90.0)
        tr, br = make(d, {"XAUUSD": ticks}, cfg, slippage_pts=slippage_pts)
        inv = Invariants()
        instrument(br, cfg, inv)
        br.advance(T0 + 1)
        tr.step(T0 + 1)
        tr.cmd_start()
        eq = [br.balance]
        run(tr, br, T0 + 1, T0 + hours * 3600, dt=0.3,
            on_step=lambda t: eq.append(br.account()["equity"]))
        tr.cmd_stop()
        closes = [d for d in br.deals_ if d["entry"] == 1 and d["magic"] == cfg.magic]
        pnl = [d["profit"] for d in closes]
        peak, mdd = eq[0], 0.0
        for e in eq:
            peak = max(peak, e)
            mdd = max(mdd, peak - e)
        reasons = Counter()
        for l in tr.logs:
            m = re.match(r"EXIT GROUP \S+ \S+ \S+ \S+: ([a-z\- ]+)", l["msg"])
            if m:
                reasons[m.group(1).strip()] += 1
        return {
            "scenario": "noise-only" if noise_only else "bursts", "seed": seed, "hours": hours, "ticks": len(ticks), "entries": inv.entries,
            "closed": len(pnl), "wins": sum(1 for p in pnl if p > 0),
            "win_rate": round(sum(1 for p in pnl if p > 0) / len(pnl), 3) if pnl else None,
            "net_pnl": round(sum(pnl), 2), "max_drawdown": round(mdd, 2),
            "max_open_positions": inv.max_pos, "orders_without_sl": inv.orders_without_sl,
            "adds_into_loser": inv.adds_into_loser, "halted": tr.risk.halt_reason or None,
            "exit_reasons": dict(reasons),
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, nargs="*", default=[1, 2, 3, 4, 5])
    ap.add_argument("--hours", type=float, default=2.0)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    res = [replay(s, a.hours) for s in a.seeds] + [replay(s, a.hours, noise_only=True) for s in a.seeds]
    for r in res:
        print(json.dumps(r))
    tot = {"label": "SYNTHETIC replay - not market data", "sessions": len(res),
           "net_pnl_bursts": round(sum(r["net_pnl"] for r in res if r["scenario"] == "bursts"), 2),
           "net_pnl_noise_only": round(sum(r["net_pnl"] for r in res if r["scenario"] == "noise-only"), 2),
           "entries": sum(r["entries"] for r in res), "net_pnl": round(sum(r["net_pnl"] for r in res), 2),
           "orders_without_sl": sum(r["orders_without_sl"] for r in res),
           "adds_into_loser": sum(r["adds_into_loser"] for r in res)}
    print(json.dumps(tot))
    if a.out:
        with open(a.out, "w") as fh:
            json.dump({"sessions": res, "total": tot}, fh, indent=2)


if __name__ == "__main__":
    main()
