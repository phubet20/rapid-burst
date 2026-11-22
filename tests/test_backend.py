"""Backend + worker protocol end-to-end test (local).

Runs the real backend and the real worker (Trader + MT5Broker + BackendLink) in
one process. On this Linux box the official MetaTrader5 package cannot load, so
the worker must honestly report that MT5 is unavailable and expose no account data.
"""
import asyncio
import base64
import io
import json
import threading
import time
import zipfile

import aiohttp
from aiohttp.test_utils import TestServer
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from backend.server import make_app
from rapidburst.broker_mt5 import MT5Broker
from rapidburst.config import EngineConfig
from rapidburst.link import BackendLink
from rapidburst.secure_store import SecureStore
from rapidburst.trader import Trader

UI_PW = "test-ui-password-123"
SECRET = "test-worker-secret-0123456789abcdef"


def run(coro):
    return asyncio.run(coro)


async def _login(s, base, pw=UI_PW):
    r = await s.post(base + "/api/login", json={"password": pw}, headers={"X-RB": "1"})
    cookie = r.cookies.get("rb_session")
    return r.status, (cookie.value if cookie else None)


def test_auth_and_static():
    async def go():
        server = TestServer(make_app(UI_PW, SECRET))
        await server.start_server()
        base = str(server.make_url(""))
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
            # csrf header required
            r = await s.post(base + "/api/cmd", json={"cmd": "stop"}, headers={"Cookie": f"rb_session={cookie}"})
            assert r.status == 403
            # forged session
            r = await s.get(base + "/api/state", headers={"Cookie": "rb_session=9999999999.abc.def"})
            assert r.status == 401
            # worker endpoints need the secret
            assert (await s.get(base + "/bootstrap/worker.zip")).status == 401
            r = await s.get(base + "/bootstrap/worker.zip", headers={"X-Worker-Secret": SECRET})
            z = zipfile.ZipFile(io.BytesIO(await r.read()))
            assert "main.py" in z.namelist() and "rapidburst/trader.py" in z.namelist()
            ps1 = await (await s.get(base + "/bootstrap/install.ps1")).text()
            assert base.rstrip("/") in ps1 and SECRET not in ps1
            try:
                await s.ws_connect(base.replace("http", "ws") + "/worker/ws", headers={"Authorization": "Bearer nope"})
                assert False, "bad worker secret accepted"
            except aiohttp.WSServerHandshakeError as e:
                assert e.status == 401
            # brute-force limit
            for _ in range(8):
                await _login(s, base, "bad")
            st, _ = await _login(s, base)
            assert st == 429
        await server.close()
    run(go())


def test_worker_link_end_to_end_reports_mt5_unavailable_honestly(tmp_path):
    async def go():
        server = TestServer(make_app(UI_PW, SECRET))
        await server.start_server()
        base = str(server.make_url("")).rstrip("/")
        trader = Trader(MT5Broker(), SecureStore(str(tmp_path)), EngineConfig(), str(tmp_path))
        th = threading.Thread(target=trader.run_forever, daemon=True)
        th.start()
        link = BackendLink(trader, base, SECRET, str(tmp_path / "hb.txt"))
        task = asyncio.create_task(link.run())
        hub = server.app["hub"]
        for _ in range(100):
            if hub.worker_online() and hub.snapshot:
                break
            await asyncio.sleep(0.1)
        assert hub.worker_online()
        async with aiohttp.ClientSession() as s:
            _, cookie = await _login(s, base)
            H = {"Cookie": f"rb_session={cookie}", "X-RB": "1"}
            st = await (await s.get(base + "/api/state", headers=H)).json()
            snap = st["snapshot"]
            assert st["worker_online"] is True
            assert snap["conn"]["state"] == "UNAVAILABLE" and "MetaTrader5 package unavailable" in snap["conn"]["msg"]
            assert snap["account"] is None and snap["pnl"] is None and snap["positions"] == []
            assert snap["worker"]["mt5_package"] is False
            # UI encrypts credentials with the worker public key (same as WebCrypto RSA-OAEP/SHA-256)
            pub = serialization.load_der_public_key(base64.b64decode(snap["pubkey"]))
            ct = pub.encrypt(json.dumps({"login": "1234567", "password": "never-log-me", "server": "Demo-Server"}).encode(),
                             padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
            r = await (await s.post(base + "/api/cmd", headers=H,
                                    json={"cmd": "connect", "args": {"enc": base64.b64encode(ct).decode()}})).json()
            assert r["ok"] is False and "MetaTrader5 package unavailable" in r["msg"]
            r = await (await s.post(base + "/api/cmd", headers=H, json={"cmd": "start"})).json()
            assert r["ok"] is False and "START refused" in r["msg"]
            r = await (await s.post(base + "/api/cmd", headers=H, json={"cmd": "search", "args": {"q": "XAU"}})).json()
            assert r["ok"] is False and r["symbols"] == []
            r = await (await s.post(base + "/api/cmd", headers=H, json={"cmd": "stop"})).json()
            assert "not connected" in r["msg"]
            await asyncio.sleep(1.5)
            st = await (await s.get(base + "/api/state", headers=H)).json()
            assert st["snapshot"]["conn"]["state"] == "UNAVAILABLE"
            assert st["snapshot"]["account"] is None
            blob = json.dumps(st)
            assert "never-log-me" not in blob
            assert all("never-log-me" not in l["msg"] for l in trader.logs)
            # UI websocket receives live state
            async with s.ws_connect(base.replace("http", "ws") + "/api/ws", headers={"Cookie": H["Cookie"]}) as ws:
                m = await ws.receive_json(timeout=5)
                assert m["type"] == "state" and m["worker_online"] is True
        task.cancel()
        trader.stop_event.set()
        await server.close()
    run(go())
