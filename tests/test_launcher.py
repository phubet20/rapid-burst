"""On-demand lifecycle tests: launcher + real engine *process* + backend, with the
real-time SimBroker TEST DOUBLE inside the engine process (SYNTHETIC data).

Verifies: idle = no engine/terminal process; CONNECT validates then shuts down when
no START follows; START launches the engine (and 'terminal'), re-reads state and
re-adopts bot positions; START is idempotent; a crashed engine is restarted with a
fresh START; STOP flattens only bot positions/pendings and then terminates the
engine and the bot's terminal; START after STOP does not duplicate orders; stale
or replayed commands are ignored; the launcher releases the backend when the app
is closed and comes back on a wake signal.
"""
import asyncio
import json
import os
import time

import aiohttp

from backend.server import HUB
import psutil

from rapidburst.config import EngineConfig
from rapidburst.launcher import Launcher, find_processes
from tests.e2e_util import (ROOT, SECRET, FakeNtfy, cmd, encrypt_creds, fake_terminal, login, start_backend, state,
                            wait_for)
from tests.sim_factory import seed_state

MAGIC = EngineConfig().magic


def run(coro):
    return asyncio.run(coro)


def make_launcher(tmp_path, base, wake_url="", **kw):
    term = fake_terminal(tmp_path)
    sim_state = os.path.join(str(tmp_path), "sim_broker_state.json")
    env = {"RB_TEST_BROKER_FACTORY": "tests.sim_factory:make", "RB_SIM_STATE": sim_state,
           "RB_SIM_TERMINAL": term, "PYTHONPATH": ROOT + os.pathsep + os.path.join(ROOT, "worker")}
    data = os.path.join(str(tmp_path), "data")
    os.makedirs(data, exist_ok=True)
    with open(os.path.join(data, "state.json"), "w") as fh:     # no built-in default symbol any more: user chose one
        json.dump({"selected": ["XAUUSD"]}, fh)
    L = Launcher(base, SECRET, data, mt5_path=term, wake_url=wake_url, engine_env=env, **kw)
    return L, sim_state, term


def engines():
    return find_processes(cmd_marker="engine_main.py")


def sim(path):
    with open(path) as fh:
        return json.load(fh)


def test_start_stop_lifecycle(tmp_path):
    async def go():
        server, base = await start_backend()
        hub = server.app[HUB]
        L, sim_state, term = make_launcher(tmp_path, base, standby_timeout=600)
        seed_state(sim_state, MAGIC)
        task = asyncio.create_task(L.run())
        assert await wait_for(lambda: hub.worker_online() and hub.snapshot)
        async with aiohttp.ClientSession() as s:
            H = await login(s, base)
            # 1. idle: nothing running
            snap = (await state(s, base, H))["snapshot"]
            assert snap["conn"]["state"] == "IDLE" and snap["launcher"]["phase"] == "IDLE"
            assert not engines() and not find_processes(exe_path=term)

            # 2. CONNECT: validates account (DEMO), engine in standby, terminal started
            r = await cmd(s, base, H, "connect", {"enc": encrypt_creds(snap["pubkey"], "5550001", "pw", "Sim-Demo")})
            assert r["ok"] and r["validation"]["ok"], r
            assert len(engines()) == 1 and find_processes(exe_path=term)

            # 3. START: re-reads account/positions/orders, adopts the bot position, warm-up on fresh ticks
            r = await cmd(s, base, H, "start")
            assert r["ok"] and r["msg"] == "running", r
            snap = await wait_for(lambda: (hub.snapshot if hub.snapshot.get("positions") else None), 10)
            assert [p["ticket"] for p in snap["positions"]] == [7001]
            assert snap["foreign_positions"] == 1
            pid1 = snap["launcher"]["engine_pid"]
            assert any("1 bot pending order" in l["msg"] and "adopted" in l["msg"] for l in L.logs)

            # 4. START again is idempotent (same engine, no new batch)
            r = await cmd(s, base, H, "start")
            assert r["ok"] and r["msg"] == "already running"
            assert L.engine.pid == pid1 and len(engines()) == 1

            # 5. engine crash while trading -> launcher restarts it with a fresh START
            psutil.Process(pid1).kill()
            ok = await wait_for(lambda: L.engine and L.engine.alive() and L.engine.pid != pid1 and
                                (L.engine.snapshot.get("engine") or {}).get("running"), 40)
            assert ok, [l["msg"] for l in L.logs][-10:]
            assert len(engines()) == 1

            # 6. STOP: blocks entries, cancels bot pending, closes bot position (verified),
            #    then terminates engine + the bot's terminal; manual/other-EA untouched
            r = await cmd(s, base, H, "stop")
            assert r["ok"] and "shut down" in r["msg"], r
            st = sim(sim_state)
            assert not [p for p in st["positions"].values() if p["magic"] == MAGIC]
            assert "7002" in st["positions"]                       # manual trade untouched
            assert "7003" not in st["orders"] and "7004" in st["orders"]
            assert st["sent_orders"] == 0                          # no new entries were sent
            assert await wait_for(lambda: not engines() and not find_processes(exe_path=term), 15)
            assert L.phase == "IDLE" and L.desired == "STOPPED"
            snap = await wait_for(lambda: hub.snapshot if (hub.snapshot.get("conn") or {}).get("state") == "IDLE" else None, 10)
            assert snap["launcher"]["engine_pid"] is None and snap["launcher"]["terminal_running"] is False

            # 7. START after STOP: fresh engine, re-reads state, no duplicate/stale orders
            r = await cmd(s, base, H, "start")
            assert r["ok"], r
            await asyncio.sleep(8)
            assert sim(sim_state)["sent_orders"] == 0, "orders sent from stale state"
            assert len(engines()) == 1
            r = await cmd(s, base, H, "stop")
            assert r["ok"], r
            assert await wait_for(lambda: not engines() and not find_processes(exe_path=term), 15)
        task.cancel()
        await server.close()
    run(go())


def test_connect_without_start_shuts_down_after_standby(tmp_path):
    async def go():
        server, base = await start_backend()
        hub = server.app[HUB]
        L, sim_state, term = make_launcher(tmp_path, base, standby_timeout=3)
        task = asyncio.create_task(L.run())
        assert await wait_for(lambda: hub.worker_online() and hub.snapshot)
        async with aiohttp.ClientSession() as s:
            H = await login(s, base)
            snap = (await state(s, base, H))["snapshot"]
            r = await cmd(s, base, H, "connect", {"enc": encrypt_creds(snap["pubkey"], "5550001", "pw", "Sim-Demo")})
            assert r["ok"]
            r = await cmd(s, base, H, "search", {"q": "XAU"})
            assert r["ok"] and r["symbols"][0]["name"] == "XAUUSD"
            assert engines()
            assert await wait_for(lambda: not engines() and not find_processes(exe_path=term), 20)
            assert L.phase == "IDLE"
            assert L.last_connect["validation_ok"] is True
            # STOP while idle with no bot positions: quick check, then nothing left running
            r = await cmd(s, base, H, "stop")
            assert r["ok"], r
            assert await wait_for(lambda: not engines() and not find_processes(exe_path=term), 15)
        task.cancel()
        await server.close()
    run(go())


def test_stale_and_replayed_commands_are_ignored(tmp_path):
    async def go():
        L, _, _ = make_launcher(tmp_path, "http://127.0.0.1:9")

        class WS:
            def __init__(self):
                self.sent = []

            async def send_json(self, m):
                self.sent.append(m)
        ws = WS()
        await L._run_cmd(ws, {"id": "a1", "cmd": "start", "ts": time.time() - 120})
        assert ws.sent[-1]["result"]["ok"] is False and "stale" in ws.sent[-1]["result"]["msg"]
        assert L.engine is None and L.desired == "STOPPED"
        await L._run_cmd(ws, {"id": "a1", "cmd": "start", "ts": time.time()})   # replayed id
        assert len(ws.sent) == 1
        await L._run_cmd(ws, {"id": "a2", "cmd": "rm -rf", "ts": time.time()})
        assert ws.sent[-1]["result"]["msg"] == "unknown command"
        assert not engines()
    run(go())


def test_launcher_releases_backend_when_app_closed_and_wakes_on_demand(tmp_path):
    async def go():
        ntfy = FakeNtfy()
        wake_url = await ntfy.start()
        server, base = await start_backend(wake_url)
        hub = server.app[HUB]
        L, _, _ = make_launcher(tmp_path, base, wake_url=wake_url, linger=2)
        task = asyncio.create_task(L.run())
        assert await wait_for(lambda: hub.worker_online(), 15)
        # nobody has the app open -> launcher disconnects so the backend can sleep
        assert await wait_for(lambda: not hub.worker_online(), 20)
        assert not engines()
        async with aiohttp.ClientSession() as s:
            H = await login(s, base)
            # a command from the app wakes the launcher through the wake channel
            r = await cmd(s, base, H, "stop")
            assert r["ok"] and "already stopped" in r["msg"], r
            assert ntfy.published >= 1 and L.counters["wakes"] >= 1 and L.counters["backend_sessions"] >= 2
            # an open app keeps it connected
            async with s.ws_connect(base.replace("http", "ws") + "/api/ws", headers={"Cookie": H["Cookie"]}):
                await asyncio.sleep(5)
                assert hub.worker_online()
            assert await wait_for(lambda: not hub.worker_online(), 20)
        task.cancel()
        await server.close()
    run(go())
