"""Helpers for local end-to-end tests: backend TestServer, fake ntfy wake server, login, encryption."""
from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import time

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from backend.server import make_app

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UI_PW = "test-ui-password-123"
SECRET = "test-worker-secret-0123456789abcdef"


class FakeNtfy:
    """Minimal ntfy-compatible pub/sub (POST /topic, GET /topic/json stream) for tests."""

    def __init__(self):
        self.subs: dict[str, list[asyncio.Queue]] = {}
        self.published = 0
        app = web.Application()
        app.router.add_post("/{topic}", self.pub)
        app.router.add_get("/{topic}/json", self.sub)
        self.server = TestServer(app)

    async def pub(self, request):
        self.published += 1
        for q in self.subs.get(request.match_info["topic"], []):
            q.put_nowait({"event": "message", "message": (await request.read()).decode()})
        return web.json_response({"ok": True})

    async def sub(self, request):
        topic = request.match_info["topic"]
        q: asyncio.Queue = asyncio.Queue()
        self.subs.setdefault(topic, []).append(q)
        resp = web.StreamResponse()
        await resp.prepare(request)
        await resp.write(json.dumps({"event": "open"}).encode() + b"\n")
        try:
            while True:
                try:
                    ev = await asyncio.wait_for(q.get(), 5)
                except asyncio.TimeoutError:
                    ev = {"event": "keepalive"}
                await resp.write(json.dumps(ev).encode() + b"\n")
        finally:
            self.subs[topic].remove(q)

    async def start(self):
        await self.server.start_server()
        return str(self.server.make_url("")).rstrip("/")


async def start_backend(wake_url: str = ""):
    server = TestServer(make_app(UI_PW, SECRET, wake_url))
    await server.start_server()
    return server, str(server.make_url("")).rstrip("/")


async def login(s, base):
    r = await s.post(base + "/api/login", json={"password": UI_PW}, headers={"X-RB": "1"})
    c = r.cookies.get("rb_session")
    return {"Cookie": f"rb_session={c.value}", "X-RB": "1"}


async def cmd(s, base, H, c, args=None):
    r = await s.post(base + "/api/cmd", headers=H, json={"cmd": c, "args": args or {}})
    return await r.json()


async def state(s, base, H):
    return await (await s.get(base + "/api/state", headers=H)).json()


def encrypt_creds(pubkey_b64: str, login_: str, password: str, server: str) -> str:
    pub = serialization.load_der_public_key(base64.b64decode(pubkey_b64))
    ct = pub.encrypt(json.dumps({"login": login_, "password": password, "server": server}).encode(),
                     padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
    return base64.b64encode(ct).decode()


async def wait_for(pred, timeout=30.0, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if asyncio.iscoroutine(v):
            v = await v
        if v:
            return v
        await asyncio.sleep(step)
    return None


def fake_terminal(tmp_path) -> str:
    """A stand-in 'terminal64' executable (a copy of /bin/sleep) so the launcher's
    kill-the-bot's-terminal logic can be exercised on Linux."""
    p = os.path.join(str(tmp_path), "terminal64")
    shutil.copy("/bin/sleep", p)
    os.chmod(p, 0o755)
    return p
