"""Backend auth/static tests + backend<->launcher end-to-end with the REAL MT5 adapter.

On this Linux box the official MetaTrader5 package cannot load, so the system must
honestly report that and expose no account data.
"""
import asyncio
import io
import json
import zipfile

import aiohttp

from backend.server import HUB

from rapidburst.launcher import Launcher, find_processes
from tests.e2e_util import (SECRET, UI_PW, cmd, encrypt_creds, login, start_backend, state, wait_for)


def run(coro):
    return asyncio.run(coro)


async def _login(s, base, pw=UI_PW):
    r = await s.post(base + "/api/login", json={"password": pw}, headers={"X-RB": "1"})
    cookie = r.cookies.get("rb_session")
    return r.status, (cookie.value if cookie else None)


def test_auth_and_static():
    async def go():
        server, base = await start_backend()
        async with aiohttp.ClientSession() as s:
            assert (await s.get(base + "/healthz")).status == 200
            html = await (await s.get(base + "/")).text()
            assert "CONNECT MT5" in html and "START" in html and "STOP" in html
            js = await (await s.get(base + "/static/app.js")).text()
            assert SECRET not in js and UI_PW not in js and SECRET not in html
            assert (await s.get(base + "/api/state")).status == 401
            assert (await s.post(base + "/api/cmd", json={"cmd": "start"}, headers={"X-RB": "1"})).status == 401
            st, _ = await _login(s, base, "wrong")
            assert st == 401
            st, cookie = await _login(s, base)
            assert st == 200 and cookie
            r = await s.get(base + "/api/state", headers={"Cookie": f"rb_session={cookie}"})
            assert r.status == 200 and (await r.json())["worker_online"] is False
            r = await s.post(base + "/api/cmd", json={"cmd": "start"},
                             headers={"Cookie": f"rb_session={cookie}", "X-RB": "1"})
            assert (await r.json())["ok"] is False
            r = await s.post(base + "/api/cmd", json={"cmd": "stop"}, headers={"Cookie": f"rb_session={cookie}"})
            assert r.status == 403
            r = await s.get(base + "/api/state", headers={"Cookie": "rb_session=9999999999.abc.def"})
            assert r.status == 401
            assert (await s.get(base + "/bootstrap/worker.zip")).status == 401
            r = await s.get(base + "/bootstrap/worker.zip", headers={"X-Worker-Secret": SECRET})
            z = zipfile.ZipFile(io.BytesIO(await r.read()))
            names = z.namelist()
            assert "main.py" in names and "engine_main.py" in names and "rapidburst/launcher.py" in names
            ps1 = await (await s.get(base + "/bootstrap/install.ps1")).text()
            assert base in ps1 and SECRET not in ps1
            try:
                await s.ws_connect(base.replace("http", "ws") + "/worker/ws", headers={"Authorization": "Bearer nope"})
                assert False, "bad worker secret accepted"
            except aiohttp.WSServerHandshakeError as e:
                assert e.status == 401
            for _ in range(8):
                await _login(s, base, "bad")
            st, _ = await _login(s, base)
            assert st == 429
        await server.close()
    run(go())


def test_launcher_with_real_mt5_adapter_reports_unavailable_honestly(tmp_path):
    async def go():
        server, base = await start_backend()
        hub = server.app[HUB]
        L = Launcher(base, SECRET, str(tmp_path), wake_url="", standby_timeout=5)
        task = asyncio.create_task(L.run())
        assert await wait_for(lambda: hub.worker_online() and hub.snapshot)
        async with aiohttp.ClientSession() as s:
            H = await login(s, base)
            st = await state(s, base, H)
            snap = st["snapshot"]
            assert snap["conn"]["state"] == "IDLE" and snap["account"] is None
            assert snap["launcher"]["engine_pid"] is None
            assert not find_processes(cmd_marker="engine_main.py")
            # START without credentials is refused, nothing spawned
            r = await cmd(s, base, H, "start")
            assert not r["ok"]
            enc = encrypt_creds(snap["pubkey"], "1234567", "never-log-me", "Demo-Server")
            r = await cmd(s, base, H, "connect", {"enc": enc})
            assert r["ok"] is False and "MetaTrader5 package unavailable" in r["msg"]
            snap = (await state(s, base, H))["snapshot"]
            assert snap["launcher"]["last_connect"]["validation_ok"] is False
            r = await cmd(s, base, H, "start")
            assert r["ok"] is False and "START refused" in r["msg"]
            # standby engine is shut down by itself (no START) -> back to zero processes
            assert await wait_for(lambda: not find_processes(cmd_marker="engine_main.py"), 20)
            assert await wait_for(lambda: L.phase == "IDLE", 10)
            blob = json.dumps(await state(s, base, H)) + json.dumps(list(L.logs))
            assert "never-log-me" not in blob
        task.cancel()
        await server.close()
    run(go())
