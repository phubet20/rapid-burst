"""Adaptive learning from closed bursts: bucket stats, cold start, disabling, decay, sizing,
persistence, and the trader integration (SimBroker + SYNTHETIC ticks only)."""
import collections
import json
import random

from rapidburst.config import EngineConfig
from rapidburst.learner import Learner, regime
from tests.harness import make, run
from tests.paths import _small_account, _start
from tests.paths import T0, trend_then_pullback

NOW = 1_800_000_000.0


def _feed(lr, key, rs, now=NOW):
    for r in rs:
        lr.record({"bucket": key, "r": r, "symbol": "X"}, now=now)


def test_cold_start_is_min_lot_and_fast_cut():
    lr = Learner(rng=random.Random(1))
    d = lr.decide("impulse|aligned|tight", now=NOW)
    assert d.allow and d.min_lot and d.fast_cut and d.state == "cold"


def test_few_losses_do_not_disable_but_many_do_and_decay_reexplores():
    lr = Learner(min_samples=8, rng=random.Random(2))
    k = "momentum|counter|wide"
    _feed(lr, k, [-1.0] * 5)
    assert lr.decide(k, now=NOW).allow                      # under the minimum sample: never disabled
    _feed(lr, k, [-1.0, -0.8, -1.0, -0.9, -1.0, -1.0, -0.7])
    allows = sum(lr.decide(k, now=NOW).allow for _ in range(200))
    assert allows < 10                                      # clearly losing -> skipped
    later = NOW + 10 * 36 * 3600                            # 10 half-lives later
    d = lr.decide(k, now=later)
    assert d.allow and d.state == "cold" and d.min_lot      # decayed back to exploration at min lot


def test_only_proven_positive_expectancy_scales_size_and_is_capped():
    lr = Learner(min_samples=8, max_mult=2.0, rng=random.Random(3))
    k = "impulse|aligned|tight"
    _feed(lr, k, [0.9, 0.7, -0.4, 0.8, 1.1, 0.6, -0.3, 0.9, 0.8, 1.0, 0.7, -0.2, 0.9, 0.8])
    d = lr.decide(k, now=NOW)
    assert d.state == "proven" and not d.min_lot and 1.0 < d.mult <= 2.0
    lr2 = Learner(min_samples=8, rng=random.Random(3))
    _feed(lr2, k, [0.5, -0.5] * 6)                         # ~zero expectancy -> stays min lot
    d2 = lr2.decide(k, now=NOW)
    assert d2.allow and d2.min_lot and d2.mult == 1.0


def test_outliers_are_clipped_and_journal_persists(tmp_path):
    p = str(tmp_path / "learning.json")
    lr = Learner(path=p)
    _feed(lr, "impulse|neutral|mid", [50.0, -0.2])
    assert lr.journal[0]["r"] == 3.0 and lr.total == 2
    lr2 = Learner(path=p)
    assert lr2.total == 2 and "impulse|neutral|mid" in lr2.buckets and len(lr2.journal) == 2
    s = lr2.summary(now=NOW)
    assert s["trades"] == 2 and s["top"][0]["bucket"] == "impulse|neutral|mid"
    open(p, "w").write("{corrupt")
    assert Learner(path=p).total == 0                       # corrupt file -> cold start, no crash


def test_regime_buckets():
    assert regime(0.1, 1.0) == "tight" and regime(0.2, 1.0) == "mid" and regime(0.5, 1.0) == "wide"
    assert regime(0.1, 0.0) == "unknown"


def test_trader_cold_start_min_lot_journals_closed_bursts(tmp_path):
    cfg = EngineConfig()
    cfg.learn_min_samples = 100                          # stay in cold start for this run
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=100_000.0, margin_per_lot=1.0)     # risk sizing alone would be ~1 lot
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200)
    entries = [l["msg"] for l in tr.logs if l["msg"].startswith("ENTRY")]
    assert entries and all(float(m.split()[2]) == 0.01 for m in entries), entries[:3]
    assert tr.learner.total >= 1
    j = tr.learner.journal[0]
    for k in ("symbol", "side", "bucket", "type", "rel", "regime", "hour", "comps", "hold_s", "exit", "pl", "r"):
        assert k in j, k
    for k in ("velocity", "accel", "efficiency", "sequence", "candle", "structure", "flow"):
        assert k in j["comps"], k
    snap = json.loads(json.dumps(tr.get_snapshot()))
    learn = snap["engine"]["learning"]
    assert learn["trades"] == tr.learner.total and learn["top"]
    saved = json.load(open(tmp_path / "learning.json"))
    assert saved["total"] == tr.learner.total                       # survives an engine restart


def test_trader_skips_a_disabled_bucket_with_visible_reason(tmp_path):
    cfg = EngineConfig()
    tr, br = make(tmp_path, {"BTCUSDm": trend_then_pullback()}, cfg)
    _small_account(br, balance=100_000.0, margin_per_lot=1.0)
    for rel in ("fast", "normal", "quiet", "chop"):
        for reg in ("tight", "mid", "wide", "unknown"):
            _feed(tr.learner, f"micro|{rel}|{reg}", [-1.0] * 15, now=T0)
    tr.decisions = collections.deque(maxlen=10_000)
    _start(tr, br, T0 + 1)
    run(tr, br, T0 + 1, T0 + 200)
    assert not [l for l in tr.logs if l["msg"].startswith("ENTRY")]
    assert any("losing bucket" in d["outcome"] for d in tr.decisions)
