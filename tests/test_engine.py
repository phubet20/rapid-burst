"""Tick buffer / feature layer (SYNTHETIC ticks). Decisions live in microburst.py (see test_microburst)."""
from rapidburst.config import EngineConfig
from rapidburst.engine import MomentumEngine
from tests.synth import burst_series, gen_ticks

T0 = 1_760_000_000.0


def feed(eng, ticks):
    for t, b, a in ticks:
        eng.on_tick(t, b, a)


def test_warmup_not_ready():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    feed(eng, burst_series(T0, 1, pre_seconds=10, burst_seconds=0)[:3])
    f = eng.features()
    assert not f.ready and "warming" in f.reason


def test_warmup_is_seconds_and_candle_seed_is_used():
    cfg = EngineConfig()
    assert cfg.warmup_seconds <= 3.0 and cfg.min_ticks_warmup <= 5
    eng = MomentumEngine("XAUUSD", 0.01, cfg)
    ticks = burst_series(T0, 1, pre_seconds=10, burst_seconds=0)
    eng.seed_vu = 0.5
    feed(eng, [t for t in ticks if t[0] <= ticks[0][0] + 3.0])
    f = eng.features()
    assert f.ready and f.vol_unit >= 0.2
    assert f.tick_rate > 0.3


def test_wide_spread_is_refused_by_the_spread_gate():
    eng = MomentumEngine("XAUUSD", 0.01, EngineConfig(max_spread_points=10))
    feed(eng, burst_series(T0, 1))                    # spread 18 pts > 10
    ok, why = eng.spread_ok(eng.features())
    assert not ok and "spread" in why
    eng2 = MomentumEngine("XAUUSD", 0.01, EngineConfig())
    feed(eng2, burst_series(T0, 1))
    assert eng2.spread_ok(eng2.features())[0]


def test_features_bounded_on_random_sessions():
    for seed in (11, 12):
        eng = MomentumEngine("XAUUSD", 0.01, EngineConfig())
        feed(eng, gen_ticks(seed, T0, 600))
        f = eng.features()
        assert f.ready and f.vol_unit > 0 and f.tick_rate > 0
