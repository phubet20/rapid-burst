"""t44u (owner): hedge + averaging recovery CANCELLED, mixed sets close together in profit, longer profit targets,
fewer early closes, stricter entry against an intact M5 structure. SYNTHETIC data (not live results)."""
import json
from types import SimpleNamespace as NS

from rapidburst import microburst as mb
from rapidburst import regime
from rapidburst.config import TUNABLE, EngineConfig, validate_tune
from tests.harness import run
from tests.paths import T0
from tests.test_t40u import _session


# ---- config: the recovery is off and cannot be switched back on -------------------------------------------------
def test_hedge_and_averaging_are_off_by_default_and_locked():
    c = EngineConfig()
    assert c.hedge_enabled is False and c.avg_enabled is False
    for k in ("hedge_enabled", "avg_enabled", "avg_max_layers", "avg_spacing_atr", "avg_lot_mult",
              "recovery_after_s", "recovery_loss_pct"):
        assert k not in TUNABLE
    ok, bad = validate_tune({"hedge_enabled": 1, "avg_enabled": 1})
    assert not ok and len(bad) == 2                                   # the runtime tune endpoint refuses both


def test_load_forces_recovery_off_even_if_a_config_says_on(monkeypatch):
    monkeypatch.setenv("RB_CONFIG_JSON", json.dumps({"hedge_enabled": True, "avg_enabled": True, "mb_edge": 0.07}))
    c = EngineConfig.load()
    assert c.hedge_enabled is False and c.avg_enabled is False
    assert c.mb_edge == 0.07                                          # other overrides still apply


def test_profit_targets_are_longer_than_t43u():
    c = EngineConfig()
    assert c.tp_target_atr == 0.6 and c.tp_grab_x == 2.0 and c.tp_target_giveback == 0.25
    assert c.tp_target_atr * c.tp_grab_x == 1.2                       # immediate grab at 1.2 ATR (was 0.6)


# ---- no hedge / averaging is ever opened by default ------------------------------------------------------------
def test_red_basket_is_held_without_hedge_or_layers(tmp_path):
    tr, br, ctl = _session(tmp_path)                                  # default config: hedge + averaging off
    assert not tr.cfg.hedge_enabled and not tr.cfg.avg_enabled
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    n0, sides0 = len(br.positions_), {p["type"] for p in br.positions_.values()}
    assert n0 and sides0 == {0}
    ctl["side"] = "SELL"                                              # the opposite side signals while BUY is red ...
    tr._micro_exit = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    tr._net = lambda legs, live: -5.0
    run(tr, br, T0 + 3.3, T0 + 12)
    assert {p["type"] for p in br.positions_.values()} == {0} and len(br.positions_) == n0   # ... nothing is added
    assert tr.recoveries == {"hedges": 0, "layers": 0, "cycle_tp": 0}
    assert not any("RECOVERY" in l["msg"] for l in tr.logs)


# ---- an already-open mixed set closes TOGETHER, in profit ------------------------------------------------------
def _mixed_set(tmp_path):
    tr, br, ctl = _session(tmp_path, recovery_after_s=1.0, hedge_enabled=True, avg_enabled=False)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    ctl["side"] = "SELL"
    tr._micro_exit = lambda *a, **k: None
    tr._net = lambda legs, live: -1.0 if legs and all(ps.side == "BUY" for ps in legs) else 0.0
    run(tr, br, T0 + 3.3, T0 + 6)
    assert {p["type"] for p in br.positions_.values()} == {0, 1}
    tr.cfg.hedge_enabled = False                                      # as after a redeploy: no new hedges
    ctl["side"] = "NO TRADE"
    tr._reversal_check = lambda *a, **k: None
    tr._zone_target = lambda *a, **k: None
    return tr, br, ctl


def test_green_side_is_not_taken_alone_while_the_other_side_is_red(tmp_path):
    tr, br, ctl = _mixed_set(tmp_path)
    before = set(br.positions_)

    def net(legs, live):
        if len({ps.side for ps in legs}) > 1:
            return -40.0                                              # the whole set is still red
        return 500.0 if legs[0].side == "BUY" else -540.0             # BUY is far in profit, SELL is deep red
    tr._net = net
    run(tr, br, T0 + 6.3, T0 + 10)
    assert set(br.positions_) == before                              # BUY is NOT harvested alone
    assert not any(l["msg"].startswith("EXIT GROUP") for l in tr.logs)
    tr._net = lambda legs, live: 50.0 if len({ps.side for ps in legs}) > 1 else 10.0
    run(tr, br, T0 + 10.3, T0 + 12)                                  # the set's combined net reached the target
    assert not br.positions_ and tr.recoveries["cycle_tp"] >= 1


# ---- fewer early closes ----------------------------------------------------------------------------------------
def _green_basket(tmp_path):
    tr, br, ctl = _session(tmp_path)
    ctl["side"] = "BUY"
    run(tr, br, T0 + 1.3, T0 + 3)
    ctl["side"] = "NO TRADE"
    tr._micro_exit = lambda *a, **k: None
    tr._reversal_check = lambda *a, **k: None
    tr._zone_target = lambda *a, **k: None
    g = tr.bursts[next(iter(tr.pos_state.values())).batch_id]
    g["lock_peak"] = 0.0
    return tr, br, ctl, set(br.positions_)


def test_a_velocity_flip_alone_no_longer_closes_a_thin_profit(tmp_path):
    tr, br, ctl, held = _green_basket(tmp_path)
    real = tr._micro

    def flipped(s, now_wall):
        m, st = real(s, now_wall)
        m.vel = -abs(m.vel or 1.0) - 1.0                              # price momentarily moving against the BUY basket
        return m, st
    tr._micro = flipped
    tr._net = lambda legs, live: 0.01                                 # thin green, below half of every lock level
    run(tr, br, T0 + 3.3, T0 + 8)
    assert held <= set(br.positions_)
    assert not any("EARLY CLOSE" in l["msg"] for l in tr.logs)


def test_a_real_opposite_signal_still_closes_early_but_only_after_the_confirmation(tmp_path):
    tr, br, ctl, held = _green_basket(tmp_path)
    tr._net = lambda legs, live: 0.01
    ctl["side"] = "SELL"
    run(tr, br, T0 + 3.3, T0 + 3.5)
    assert held <= set(br.positions_)                                 # < exit_confirm_s: not yet (anti-jitter)
    run(tr, br, T0 + 3.6, T0 + 6)
    assert any("EARLY CLOSE" in l["msg"] for l in tr.logs if l["msg"].startswith("EXIT GROUP"))


# ---- entries: not against an intact M5 structure without a wick / zone -----------------------------------------
def _fake_micro(trend, zones=None, vel=-1.0, mid=100.0):
    return NS(regime="normal", trend=trend, zones=zones, mid=mid, ind=None, mtf=None, early=0, chase_up=0.0,
              chase_dn=0.0, body=0.0, exhaustion=0, rejection=0, abnormal=False, accel=0.0, absorption=False,
              weak_return=False, pb_dir=0, spread_target=0.0, vel=vel)


def _patched_components(monkeypatch):
    comp = dict(velocity=0.8, accel=0.5, efficiency=0.5, sequence=0.5, burst=0.5, activity=0.5, flow=0.5,
                candle=0.5, structure=0.5)
    monkeypatch.setattr(mb, "_components", lambda m, d, cfg: dict(comp))


def _zones(wick=0, holds=False):
    return NS(ready=True, wick=wick, holds=lambda d: holds, score=lambda d: 0.5, confirm=0)


def test_sell_against_an_intact_uptrend_is_vetoed_without_a_wick_or_zone(monkeypatch):
    _patched_components(monkeypatch)
    up = regime.RegimeView(ready=True, state="UP", phase=2, leg="pullback", origin=95.0, atr=1.0)
    cfg = EngineConfig()
    s, c, fam, vetoes = mb.side_strength(_fake_micro(up, _zones()), -1, cfg)
    assert any("against the M5 structure" in v for v in vetoes)
    s, c, fam, vetoes = mb.side_strength(_fake_micro(up, _zones(wick=-1)), -1, cfg)      # a wick tip backs the SELL
    assert not any("against the M5 structure" in v for v in vetoes)
    s, c, fam, vetoes = mb.side_strength(_fake_micro(up, _zones(holds=True)), -1, cfg)   # a supply zone backs it
    assert not any("against the M5 structure" in v for v in vetoes)
    off = EngineConfig(regime_counter_veto=False)
    s, c, fam, vetoes = mb.side_strength(_fake_micro(up, _zones()), -1, off)
    assert not any("against the M5 structure" in v for v in vetoes)


def test_a_flipped_state_is_not_blocked_and_with_trend_entries_are_not_vetoed(monkeypatch):
    _patched_components(monkeypatch)
    side = regime.RegimeView(ready=True, state="SIDEWAYS", range_hi=110.0, range_lo=90.0, atr=1.0)
    cfg = EngineConfig()
    _, _, _, vetoes = mb.side_strength(_fake_micro(side, _zones()), -1, cfg)             # structure already broken
    assert not any("against the M5 structure" in v for v in vetoes)
    dn = regime.RegimeView(ready=True, state="DOWN", phase=2, leg="pullback", origin=105.0, atr=1.0)
    _, _, _, vetoes = mb.side_strength(_fake_micro(dn, _zones()), -1, cfg)               # SELL with a DOWN trend
    assert not any("against the M5 structure" in v for v in vetoes)


def test_the_middle_of_a_sideways_range_costs_the_configured_penalty(monkeypatch):
    _patched_components(monkeypatch)
    side = regime.RegimeView(ready=True, state="SIDEWAYS", range_hi=110.0, range_lo=90.0, atr=1.0)
    m = _fake_micro(side, _zones(), mid=100.0)                                             # pos 0.5 = the middle
    s_none = mb.side_strength(m, -1, EngineConfig(regime_mid_penalty=0.0))[0]
    s_new = mb.side_strength(m, -1, EngineConfig())[0]
    assert abs((s_none - s_new) - 0.10) < 1e-9
