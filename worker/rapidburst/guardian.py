"""t42u LOSS GUARDIAN (owner-requested, demo): stands guard over every open bot basket, every loop, signal or not.

For each symbol with bot positions it measures each side's net (broker P/L + swap), age and whether it ever went
green, and acts through the trader's own verified execution paths:
  * red and not green after `recovery_after_s`, or red beyond `recovery_loss_pct` of equity, with the OPPOSITE side
    signalling -> opposite (hedge) basket;
  * still red and price `avg_spacing_atr` ATR better than the worst entry -> owner-requested averaging layer
    (<= `avg_max_layers`, lot x `avg_lot_mult`^layer, margin caps always apply);
  * the combined hedge + recovery net is closed in profit by the exit side (`CYCLE TAKE PROFIT`).
No loss close here - the only loss closes left in the engine are the margin-level and data-feed emergencies.
"""
from __future__ import annotations

import time


class LossGuardian:
    def __init__(self, trader):
        self.tr = trader
        self.view: dict = {}          # symbol -> what the guardian sees / did
        self._last_log: dict = {}

    def scan(self, now_wall: float) -> None:
        tr = self.tr
        cfg = tr.cfg
        if not cfg.no_stop_mode:
            self.view = {}
            return
        live = {int(p["ticket"]): p for p in tr.own_positions}
        hedging = int(tr.account.get("margin_mode", 0)) == 2
        view = {}
        for s in list(tr.selected):
            mine = [ps for ps in tr.pos_state.values() if ps.symbol == s and ps.ticket in live
                    and ps.ticket not in tr._closed_tickets]
            if not mine:
                continue
            sides = {}
            for ps in mine:
                sides.setdefault(ps.side, []).append(ps)
            row = {side: {"legs": len(legs), "net": round(tr._net(legs, live), 2)} for side, legs in sides.items()}
            total = round(tr._net(mine, live), 2)
            row["total"] = total
            action = "watching"
            if total < 0:
                info = tr.sym_info.get(s)
                if info and tr.pacer.can_enter(s, now_wall)[0]:
                    m, st = tr._micro(s, now_wall)
                    status = {}
                    if tr._recovery(s, mine, m, st, info, status, now_wall, time.time(), hedging):
                        action = "recovery action taken"
                    else:
                        action = status.get("reason") or "red - waiting for an opposite signal / layer spacing"
                if now_wall - self._last_log.get(s, 0.0) > 15.0:
                    self._last_log[s] = now_wall
                    tr.add_log("INFO", f"GUARD {s}: net {total:+.2f} " + ", ".join(
                        f"{k} {v['legs']} legs {v['net']:+.2f}" for k, v in row.items() if isinstance(v, dict))
                        + f" - {action} (no loss close; closes when the combined net is in profit)")
            row["action"] = action
            view[s] = row
        self.view = view
