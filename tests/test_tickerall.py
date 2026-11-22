"""TickerAll adapter tests against FakeTickerAll (a MOCKED TickerAll API - TEST DOUBLE,
SYNTHETIC data). They verify the adapter's mapping and safety rules, not TickerAll itself."""
import logging
import time

import pytest

from rapidburst.broker_tickerall import TickerAllBroker
from rapidburst.config import EngineConfig
from rapidburst.trader import Trader
from tests.fake_tickerall import API_KEY, DEMO, LIVE, FakeTickerAll

MAGIC = EngineConfig().magic


class MemStore:
    def __init__(self, creds=None):
        self.c = creds

    def save_credentials(self, c):
        self.c = dict(c)
        return "memory"

    def load_credentials(self):
        return dict(self.c) if self.c else None

    def forget_credentials(self):
        self.c = None


@pytest.fixture
def fake():
    f = FakeTickerAll()
    f.start()
    yield f
    f.stop()


def mk(fake, tmp_path, key=API_KEY):
    return TickerAllBroker(api_key=key, base_url=fake.base, magic=MAGIC, comment_prefix="RBurst",
                           data_dir=str(tmp_path), cache_ttl=0.0)


def wait(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def test_missing_key_and_bad_key(fake, tmp_path):
    b = mk(fake, tmp_path, key="")
    assert b.available()[0] is False
    ok, msg = b.connect(DEMO["login"], DEMO["password"], DEMO["server"])
    assert not ok and "TICKERALL_API_KEY" in msg
    b = mk(fake, tmp_path, key="cf_api_wrong")
    ok, msg = b.connect(DEMO["login"], DEMO["password"], DEMO["server"])
    assert not ok and "UNAUTHORIZED" in msg


def test_connect_demo_maps_account_and_never_logs_secrets(fake, tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    b = mk(fake, tmp_path)
    ok, msg = b.connect(DEMO["login"], DEMO["password"], DEMO["server"], 20000)
    assert ok, msg
    a = b.account()
    assert a["trade_mode"] == 0 and a["is_demo"] and a["login"] == DEMO["login"]
    assert a["balance"] == 10000.0 and a["currency"] == "USD" and a["margin_mode"] == 2
    assert b.terminal()["trade_allowed"] and b.is_connected()
    assert wait(lambda: b._ws_ok)                      # stream subscribed (keeps the account hot)
    b.shutdown()
    assert fake.sessions_deleted == 1 and not b.is_connected()
    text = caplog.text
    assert API_KEY not in text and DEMO["password"] not in text


def test_live_account_rejected_on_free_plan(fake, tmp_path):
    b = mk(fake, tmp_path)
    ok, msg = b.connect(LIVE["login"], LIVE["password"], LIVE["server"])
    assert not ok and "FREE_TIER_LIVE_REJECTED" in msg and "DEMO" in msg
    ok, msg = b.connect(DEMO["login"], "wrong", DEMO["server"])
    assert not ok and "BROKER_AUTH_FAILED" in msg


def test_ownership_by_comment_and_registry(fake, tmp_path):
    manual = fake.add_position(comment="manual")
    bot = fake.add_position(comment="RBurst-abc123")
    own_pend = fake.add_order(comment="RBurst-p1")
    other_pend = fake.add_order(comment="other-ea")
    b = mk(fake, tmp_path)
    assert b.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    pos = {p["ticket"]: p for p in b.positions()}
    assert pos[bot]["magic"] == MAGIC and pos[manual]["magic"] == 0
    assert pos[bot]["type"] == 0 and pos[bot]["price_open"] > 0
    orders = {o["ticket"]: o for o in b.orders()}
    assert orders[own_pend]["magic"] == MAGIC and orders[other_pend]["magic"] == 0
    # registry survives a new adapter instance (same data dir) even if the comment were lost
    fake.positions[bot]["comment"] = ""
    b2 = mk(fake, tmp_path)
    assert b2.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    assert {p["ticket"]: p for p in b2.positions()}[bot]["magic"] == MAGIC
    deals = b2.deals(time.time() - 86400, time.time() + 86400 * 2)
    assert {d["position_id"] for d in deals if d["magic"] == MAGIC} == {bot}
    b.shutdown(); b2.shutdown()


def test_market_order_idempotent_retry_close_modify_cancel(fake, tmp_path):
    b = mk(fake, tmp_path)
    assert b.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    assert b.select_symbol("XAUUSD") and not b.select_symbol("NOPE")
    fake.fail_next_post_after_place = 1          # order placed but the first response is lost
    r = b.market_order("XAUUSD", "BUY", 0.05, 2390.123, 2410.0, 20, MAGIC, "RBurst-ord1")
    assert r["ok"], r
    assert len([p for p in fake.positions.values() if p["comment"] == "RBurst-ord1"]) == 1   # no duplicate
    posts = [c for c in fake.calls if c[0] == "POST" and c[1].endswith("/orders")]
    assert len(posts) == 2 and posts[0][2] and posts[0][2] == posts[1][2]                   # same Idempotency-Key
    t = r["ticket"]
    assert fake.positions[t]["stopLoss"] == 2390.12                                         # rounded to digits
    assert b.modify_sl(t, 2395.0, 0.0)["ok"] and fake.positions[t]["stopLoss"] == 2395.0
    c = b.close_position(t, 20, MAGIC)
    assert c["ok"] and c["verified"] and t not in fake.positions
    assert b.close_position(t, 20, MAGIC)["verified"]                                       # already closed
    o = fake.add_order(comment="RBurst-x")
    assert b.cancel_order(o)["ok"] and o not in fake.orders
    rj = b.market_order("BTCUSD", "BUY", 0.01, 1.0, 0.0, 20, MAGIC, "RBurst-bad")
    assert not rj["ok"]                                                                     # unknown geometry -> refused
    b.shutdown()


def test_stream_ticks_increasing_and_stale_ignored(fake, tmp_path):
    b = mk(fake, tmp_path)
    assert b.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    assert b.select_symbol("XAUUSD")
    assert wait(lambda: any(c.get("kind") == "ticks" for c in fake.subscriptions))
    fake.push_tick("XAUUSD", 2401.0, 2401.2, stale=True)
    ts = "2026-11-22T10:00:00.000Z"
    fake.push_tick("XAUUSD", 2401.1, 2401.3, ts=ts)
    fake.push_tick("XAUUSD", 2401.2, 2401.4, ts=ts)            # same timestamp -> still strictly increasing
    assert wait(lambda: len(b.ticks_since("XAUUSD", 0)) == 2)
    ticks = b.ticks_since("XAUUSD", 0)
    assert ticks[0][1] == 2401.1 and ticks[1][3] > ticks[0][3]
    assert b.ticks_since("XAUUSD", ticks[0][3]) == ticks[1:]
    lt = b.last_tick("XAUUSD")
    assert lt["bid"] == 2401.2 and lt["time_msc"] == ticks[1][3]
    # server time = UTC + serverTimeOffsetSeconds (7200 in the fake), like MT5 tick times
    assert abs(ticks[0][0] - (1795341600 + 7200)) < 1
    b.shutdown()


def test_cooled_account_is_rearmed(fake, tmp_path):
    b = mk(fake, tmp_path)
    assert b.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    assert wait(lambda: b._ws_ok)
    opened = fake.sessions_opened
    fake.hot = False                     # TickerAll cooled / restarted the session
    assert b.account() != {}             # adapter re-armed with the in-memory credentials
    assert fake.sessions_opened == opened + 1
    b.shutdown()


def test_margin_estimate_only_when_currency_known(fake, tmp_path):
    b = mk(fake, tmp_path)
    assert b.connect(DEMO["login"], DEMO["password"], DEMO["server"])[0]
    m = b.calc_margin("BUY", "XAUUSD", 0.1, 2400.0)    # profit currency == USD account
    assert m == pytest.approx(100 * 0.1 * 2400.0 / 100)
    assert b.calc_margin("BUY", "BTCUSD", 0.1, 50000.0) is None
    info = b.symbol_info("XAUUSD")
    assert info["trade_tick_value"] == 1 and "assumed" in info["trade_mode_source"]
    b.shutdown()


def test_trader_stop_flattens_only_bot_trades_over_tickerall(fake, tmp_path):
    """Real Trader loop + TickerAll adapter + mocked API: STOP closes/cancels only bot trades, verified."""
    bot = fake.add_position(comment="RBurst-seed1", sl=2380.0)
    manual = fake.add_position(side="SELL", comment="manual")
    own_pend = fake.add_order(comment="RBurst-p")
    other_pend = fake.add_order(comment="manual")
    b = mk(fake, tmp_path)
    tr = Trader(b, MemStore(dict(DEMO)), EngineConfig(), str(tmp_path))
    tr.selected = ["XAUUSD"]
    tr.step(time.time())
    assert tr.conn_state == "CONNECTED", tr.conn_msg
    assert tr.validation["ok"], tr.validation
    r = tr.cmd_start()
    assert r["ok"], r
    assert [p["ticket"] for p in tr.own_positions] == [bot] and len(tr.foreign_positions) == 1
    for _ in range(5):
        tr.step(time.time())
    r = tr.cmd_stop()
    assert r["ok"], r
    assert bot not in fake.positions and manual in fake.positions
    assert own_pend not in fake.orders and other_pend in fake.orders
    assert not [c for c in fake.calls if c[0] == "POST" and c[1].endswith("/orders")]   # no entries sent
    assert tr.cmd_shutdown()["ok"] and fake.sessions_deleted == 1


def test_trader_enters_with_sl_on_streamed_burst(fake, tmp_path):
    """SYNTHETIC burst streamed over the mocked WebSocket: the unchanged engine opens a bot
    position through the adapter (comment-tagged, with SL) and STOP closes it (verified)."""
    from datetime import datetime, timezone
    from tests.synth import burst_series
    fake.quotes["XAUUSD"] = (2650.0, 2650.18)
    b = mk(fake, tmp_path)
    tr = Trader(b, MemStore(dict(DEMO)), EngineConfig(), str(tmp_path))
    tr.selected = ["XAUUSD"]
    tr.step(time.time())
    assert tr.cmd_start()["ok"]
    assert wait(lambda: any(c.get("kind") == "ticks" for c in fake.subscriptions))
    t0 = time.time()
    entered = False
    for i, (t, bid, ask) in enumerate(burst_series(t0, +1, pre_seconds=120, burst_seconds=8)):
        ts = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        fake.push_tick("XAUUSD", bid, ask, ts=ts)
        if i % 10 == 0:
            time.sleep(0.02)
            tr.step(time.time())
            if tr.own_positions:
                entered = True
                break
    for _ in range(20):
        if entered:
            break
        time.sleep(0.05)
        tr.step(time.time())
        entered = bool(tr.own_positions)
    assert entered, [l["msg"] for l in tr.logs][-8:]
    p = tr.own_positions[0]
    fp = fake.positions[p["ticket"]]
    assert fp["comment"].startswith("RBurst-") and fp["stopLoss"] > 0 and fp["side"] == "BUY"
    assert tr.cmd_stop()["ok"] and not [x for x in fake.positions.values() if x["comment"].startswith("RBurst")]
    tr.cmd_shutdown()
