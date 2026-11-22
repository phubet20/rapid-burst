"""Cloud engine mode end-to-end: backend + the real launcher and engine *processes*
running beside it (as on Render), with MT5 reached through FakeTickerAll - a MOCKED
TickerAll API (TEST DOUBLE, SYNTHETIC data). No Windows, no MT5 terminal.

Verifies: the backend spawns the launcher; CONNECT (login/password/server,
RSA-encrypted in the 'phone') opens a TickerAll DEMO session and validates;
START re-reads state and adopts the bot's position; keep-awake pings run only
while STARTED; STOP closes/cancels only bot trades (verified), shuts the engine
down and releases the TickerAll session; remote launchers are refused in cloud
mode; no secrets in logs.
"""
import asyncio
import logging
import os
import socket

import aiohttp
from aiohttp.test_utils import TestServer

from backend.cloud import CloudEngine, cloud_mode_from_env
from backend.server import HUB, make_app
from rapidburst.launcher import find_processes
from tests.e2e_util import ROOT, SECRET, UI_PW, cmd, encrypt_creds, login, state, wait_for
from tests.fake_tickerall import API_KEY, DEMO, LIVE, FakeTickerAll


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_cloud_mode_env_switch():
    assert cloud_mode_from_env({"TICKERALL_API_KEY": "x"})
    assert not cloud_mode_from_env({"TICKERALL_API_KEY": "x", "ENGINE_MODE": "launcher"})
    assert cloud_mode_from_env({"ENGINE_MODE": "cloud"})
    assert not cloud_mode_from_env({})


def test_cloud_engine_connect_start_stop(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    fake = FakeTickerAll()
    fake.start()
    fake.auto_ticks = True
    bot = fake.add_position(comment="RBurst-seed1", sl=2380.0)
    manual = fake.add_position(side="SELL", comment="manual")
    own_pend = fake.add_order(comment="RBurst-p")
    other_pend = fake.add_order(comment="manual")

    async def go():
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        cloud = CloudEngine(port, SECRET, public_url=base, ping_interval=1.0, data_dir=str(tmp_path / "data"),
                            extra_env={"TICKERALL_API_KEY": API_KEY, "TICKERALL_BASE_URL": fake.base,
                                       "PYTHONPATH": ROOT + os.pathsep + os.path.join(ROOT, "worker")})
        server = TestServer(make_app(UI_PW, SECRET, "https://ntfy.invalid", cloud=cloud), port=port)
        await server.start_server()
        hub = server.app[HUB]
        try:
            assert hub.wake_url == ""                                   # no wake channel needed in cloud mode
            assert await wait_for(lambda: hub.worker_online() and hub.snapshot.get("pubkey"), 60)
            async with aiohttp.ClientSession() as s:
                H = await login(s, base)
                st = await state(s, base, H)
                assert st["mode"] == "cloud" and st["cloud"]["api_key_set"]
                assert st["snapshot"]["launcher"]["phase"] == "IDLE"
                assert not st["cloud"]["keepalive"]["active"]

                # a remote (proxied) launcher may not take over the worker slot
                r = await s.get(base + "/worker/ws", headers={"Authorization": f"Bearer {SECRET}",
                                                             "X-Forwarded-For": "203.0.113.9"})
                assert r.status == 409

                pk = st["snapshot"]["pubkey"]
                # LIVE account refused by the free plan -> connect fails honestly
                r = await cmd(s, base, H, "connect", {"enc": encrypt_creds(pk, str(LIVE["login"]), LIVE["password"], LIVE["server"])})
                assert not r.get("ok") and "FREE_TIER_LIVE_REJECTED" in str(r), r

                r = await cmd(s, base, H, "connect", {"enc": encrypt_creds(pk, str(DEMO["login"]), DEMO["password"], DEMO["server"])})
                assert r.get("ok"), r
                assert [c["name"] for c in r["validation"]["checks"] if not c["ok"] and not c.get("soft")] == ["Tradable symbol selected"]
                assert fake.session

                r = await cmd(s, base, H, "start")
                assert not r["ok"] and "no symbol selected" in r["msg"], r      # nothing selected yet: START refused
                r = await cmd(s, base, H, "search", {"q": "gold"})
                assert r["ok"] and [x["name"] for x in r["symbols"]] == ["XAUUSD"], r
                r = await cmd(s, base, H, "select", {"symbols": ["XAUUSD"]})
                assert r["ok"] and r["validation"]["ok"], r
                r = await cmd(s, base, H, "start")
                assert r["ok"] and r["msg"] == "running", r
                snap = await wait_for(lambda: hub.snapshot if hub.snapshot.get("positions") else None, 20)
                assert [p["ticket"] for p in snap["positions"]] == [bot]
                assert snap["account"]["mode"] == "DEMO" and snap["account"]["company"].startswith("via TickerAll")
                assert await wait_for(lambda: cloud.keepalive["pings_ok"] >= 1, 20)   # keep-awake while STARTED

                r = await cmd(s, base, H, "stop")
                assert r["ok"], r
                assert bot not in fake.positions and manual in fake.positions
                assert own_pend not in fake.orders and other_pend in fake.orders
                assert not [c for c in fake.calls if c[0] == "POST" and c[1].endswith("/orders")]
                assert await wait_for(lambda: not find_processes(cmd_marker="engine_main.py"), 20)
                assert await wait_for(lambda: fake.sessions_deleted >= 1 and not fake.session, 10)
                assert await wait_for(lambda: not cloud.keepalive["active"], 15)
                pings = cloud.keepalive["pings_ok"]
                await asyncio.sleep(3)
                assert cloud.keepalive["pings_ok"] == pings                         # Render may sleep again
        finally:
            await server.close()
        logs = caplog.text + "".join(str(l) for l in hub.logs)
        for secret in (API_KEY, DEMO["password"], LIVE["password"], SECRET, UI_PW):
            assert secret not in logs
    try:
        asyncio.run(go())
    finally:
        fake.stop()
