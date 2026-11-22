"""Rapid Burst backend: authenticated relay between the iPhone UI and the
Windows MT5 worker.  Holds no trading logic and never sees MT5 passwords
(credentials are RSA-encrypted in the browser with the worker's public key).
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import io
import json
import logging
import os
import secrets
import time
import uuid
import zipfile
from collections import deque
from pathlib import Path

import aiohttp
from aiohttp import WSMsgType, web

from .cloud import CloudEngine, cloud_mode_from_env

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent
STATIC = ROOT / "static"
log = logging.getLogger("rb.backend")

UI_CMDS = {"connect", "start", "stop", "search", "select", "refresh", "forget"}
SESSION_TTL = 30 * 86400


def _env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"{name} must be set")
    return v


def wake_topic(secret: str) -> str:
    return "rb-" + hmac.new(secret.encode(), b"rapid-burst-wake", hashlib.sha256).hexdigest()[:40]


class Hub:
    def __init__(self, ui_password: str, worker_secret: str, wake_url: str = ""):
        self.ui_password = ui_password
        self.worker_secret = worker_secret
        self.session_key = hashlib.sha256(("rb-session|" + ui_password + "|" + worker_secret).encode()).digest()
        self.worker_ws: web.WebSocketResponse | None = None
        self.worker_seen = 0.0
        self.worker_connected_at = 0.0
        self.snapshot: dict = {}
        self.logs: deque = deque(maxlen=300)
        self.pending: dict[str, asyncio.Future] = {}
        self.ui_clients: set[web.WebSocketResponse] = set()
        self.login_attempts: dict[str, deque] = {}
        self.worker_zip = self._build_worker_zip()
        self.wake_url = wake_url.rstrip("/")
        self.last_wake = 0.0
        self.wakes_sent = 0
        self.cloud: CloudEngine | None = None

    async def wake(self) -> None:
        """Ask the sleeping launcher to connect (ntfy publish of a content-free 'wake')."""
        if self.worker_online() or not self.wake_url or time.time() - self.last_wake < 10:
            return
        self.last_wake = time.time()
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
                await s.post(f"{self.wake_url}/{wake_topic(self.worker_secret)}", data=b"wake")
            self.wakes_sent += 1
            log.info("wake signal sent to launcher")
        except Exception as exc:  # noqa: BLE001
            log.warning("wake failed: %s", exc)

    async def wait_worker(self, timeout: float) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            if self.worker_online():
                return True
            await asyncio.sleep(0.25)
        return self.worker_online()

    async def notify_ui_count(self) -> None:
        if self.worker_online():
            try:
                await self.worker_ws.send_json({"type": "ui", "clients": len(self.ui_clients)})
            except Exception:  # noqa: BLE001
                pass

    # ---------------------------------------------------------- sessions
    def make_session(self) -> str:
        exp = str(int(time.time()) + SESSION_TTL)
        nonce = secrets.token_hex(8)
        body = f"{exp}.{nonce}"
        sig = hmac.new(self.session_key, body.encode(), hashlib.sha256).hexdigest()
        return f"{body}.{sig}"

    def check_session(self, token: str | None) -> bool:
        if not token or token.count(".") != 2:
            return False
        exp, nonce, sig = token.split(".")
        good = hmac.new(self.session_key, f"{exp}.{nonce}".encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig, good) and exp.isdigit() and int(exp) > time.time()

    def worker_online(self) -> bool:
        return self.worker_ws is not None and not self.worker_ws.closed

    def state(self) -> dict:
        return {"worker_online": self.worker_online(), "worker_seen": self.worker_seen,
                "server_time": time.time(), "snapshot": self.snapshot,
                "mode": "cloud" if self.cloud else "launcher",
                "cloud": self.cloud.status() if self.cloud else None}

    async def broadcast(self, msg: dict) -> None:
        dead = []
        for ws in self.ui_clients:
            try:
                await ws.send_json(msg)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.ui_clients.discard(ws)

    async def send_cmd(self, cmd: str, args: dict, timeout: float = 95.0) -> dict:
        if not self.worker_online():
            return {"ok": False, "msg": "Worker offline - the Windows MT5 worker is not connected to the backend"}
        cid = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self.pending[cid] = fut
        try:
            await self.worker_ws.send_json({"type": "cmd", "id": cid, "cmd": cmd, "args": args, "ts": time.time()})
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return {"ok": False, "msg": "worker did not answer in time"}
        finally:
            self.pending.pop(cid, None)

    @staticmethod
    def _build_worker_zip() -> bytes:
        buf = io.BytesIO()
        wdir = REPO / "worker"
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for p in sorted(wdir.rglob("*")):
                if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc" and "data" not in p.relative_to(wdir).parts:
                    z.write(p, p.relative_to(wdir).as_posix())
        return buf.getvalue()


HUB = web.AppKey("hub", Hub)


def client_ip(request: web.Request) -> str:
    fwd = request.headers.get("X-Forwarded-For", "")
    return fwd.split(",")[0].strip() if fwd else (request.remote or "?")


def require_ui(request: web.Request) -> None:
    hub: Hub = request.app[HUB]
    if not hub.check_session(request.cookies.get("rb_session")):
        raise web.HTTPUnauthorized(text=json.dumps({"ok": False, "msg": "login required"}),
                                   content_type="application/json")


def require_csrf(request: web.Request) -> None:
    if request.headers.get("X-RB") != "1":
        raise web.HTTPForbidden(text="missing header")


@web.middleware
async def security_headers(request: web.Request, handler):
    resp = await handler(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000")
    resp.headers.setdefault("Content-Security-Policy",
                            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
                            "connect-src 'self' wss: ws:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
    return resp


# ----------------------------------------------------------------- routes
async def index(request: web.Request) -> web.StreamResponse:
    resp = web.FileResponse(STATIC / "index.html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


async def healthz(request: web.Request) -> web.Response:
    hub: Hub = request.app[HUB]
    return web.json_response({"ok": True, "worker_online": hub.worker_online(),
                              "mode": "cloud" if hub.cloud else "launcher"})


async def check_ui_password(request: web.Request, password: str) -> web.Response | None:
    """Rate-limited UI-password check used by login and worker enroll.

    Returns an error response, or None if the password is correct. Failures (not
    successes) count toward the per-IP cap of 8 attempts / 15 minutes. Never logs
    the password.
    """
    hub: Hub = request.app[HUB]
    ip = client_ip(request)
    q = hub.login_attempts.setdefault(ip, deque())
    now = time.time()
    while q and now - q[0] > 900:
        q.popleft()
    if len(q) >= 8:
        return web.json_response({"ok": False, "msg": "too many attempts, wait 15 minutes"}, status=429)
    if not hmac.compare_digest(password.encode(), hub.ui_password.encode()):
        q.append(now)
        await asyncio.sleep(1.0)
        return web.json_response({"ok": False, "msg": "wrong password"}, status=401)
    return None


async def login(request: web.Request) -> web.Response:
    require_csrf(request)
    hub: Hub = request.app[HUB]
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    err = await check_ui_password(request, str(body.get("password", "")))
    if err is not None:
        return err
    resp = web.json_response({"ok": True})
    resp.set_cookie("rb_session", hub.make_session(), max_age=SESSION_TTL, httponly=True, secure=True,
                    samesite="Strict", path="/")
    return resp


async def worker_enroll(request: web.Request) -> web.Response:
    """Give a Windows installer the worker secret after proving the app password.

    Used by bootstrap/install.ps1 so the user never has to paste WORKER_SECRET.
    The body is {"password": "<UI password>"}; the secret is returned once and is
    never written to logs.
    """
    require_csrf(request)
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    err = await check_ui_password(request, str(body.get("password", "")))
    if err is not None:
        return err
    hub: Hub = request.app[HUB]
    log.info("worker enroll ok from %s", client_ip(request))
    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    base = f"{proto}://{request.host}"
    return web.json_response({"ok": True, "worker_secret": hub.worker_secret, "backend_url": base})


async def logout(request: web.Request) -> web.Response:
    resp = web.json_response({"ok": True})
    resp.del_cookie("rb_session", path="/")
    return resp


async def api_state(request: web.Request) -> web.Response:
    require_ui(request)
    hub: Hub = request.app[HUB]
    if not hub.worker_online():
        await hub.wake()
    return web.json_response({**hub.state(), "logs": list(hub.logs)[-150:]})


def _validate_cmd(cmd: str, args: dict) -> dict:
    if cmd not in UI_CMDS:
        raise web.HTTPBadRequest(text="unknown command")
    if cmd == "connect":
        enc = args.get("enc")
        if not isinstance(enc, str) or not (100 < len(enc) < 4096):
            raise web.HTTPBadRequest(text="encrypted credentials required")
        return {"enc": enc}
    if cmd == "search":
        return {"q": str(args.get("q", ""))[:40]}
    if cmd == "select":
        syms = args.get("symbols")
        if not isinstance(syms, list):
            raise web.HTTPBadRequest(text="symbols list required")
        return {"symbols": [str(s)[:40] for s in syms][:6]}
    return {}


async def api_cmd(request: web.Request) -> web.Response:
    require_ui(request)
    require_csrf(request)
    hub: Hub = request.app[HUB]
    body = await request.json()
    cmd = str(body.get("cmd", ""))
    args = _validate_cmd(cmd, body.get("args") or {})
    if not hub.worker_online():
        await hub.wake()
        if not await hub.wait_worker(30):
            if hub.cloud:
                return web.json_response({"ok": False, "msg": "Cloud engine is starting - try again in a few seconds"})
            return web.json_response({"ok": False, "msg": "Launcher offline - wake signal sent but the Windows launcher "
                                                          "did not connect (is the PC/VPS on?)"})
    if cmd == "start":
        snap = hub.snapshot or {}
        lc = ((snap.get("launcher") or {}).get("last_connect") or {})
        live_ok = (snap.get("conn") or {}).get("state") == "CONNECTED" and (snap.get("validation") or {}).get("ok")
        stored_ok = (snap.get("launcher") or {}).get("has_credentials") and lc.get("validation_ok")
        if not (live_ok or stored_ok):
            det = "; ".join(lc.get("failed_details") or lc.get("failed_checks") or [])
            return web.json_response({"ok": False, "msg": "START refused: MT5 connection/validation not confirmed"
                                                          + (f" ({det})" if det else " - use CONNECT MT5 first")})
    log.info("ui command %s from %s", cmd, client_ip(request))  # never log args (may hold ciphertext)
    result = await hub.send_cmd(cmd, args)
    return web.json_response(result)


async def ui_ws(request: web.Request) -> web.WebSocketResponse:
    require_ui(request)
    hub: Hub = request.app[HUB]
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    hub.ui_clients.add(ws)
    await hub.notify_ui_count()
    if not hub.worker_online():
        await hub.wake()
    await ws.send_json({"type": "state", **hub.state(), "logs": list(hub.logs)[-150:]})
    try:
        async for _ in ws:
            pass
    finally:
        hub.ui_clients.discard(ws)
        await hub.notify_ui_count()
    return ws


async def worker_ws(request: web.Request) -> web.WebSocketResponse:
    hub: Hub = request.app[HUB]
    auth = request.headers.get("Authorization", "")
    if not (auth.startswith("Bearer ") and hmac.compare_digest(auth[7:].encode(), hub.worker_secret.encode())):
        await asyncio.sleep(1.0)
        raise web.HTTPUnauthorized(text="bad worker secret")
    if hub.cloud and (request.headers.get("X-Forwarded-For") or request.remote not in ("127.0.0.1", "::1")):
        # cloud mode: only the in-process launcher may hold the worker slot (avoids two engines fighting)
        raise web.HTTPConflict(text="backend runs the cloud engine (ENGINE_MODE=cloud); remote launchers are disabled")
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=4 * 1024 * 1024)
    await ws.prepare(request)
    old = hub.worker_ws
    hub.worker_ws = ws
    hub.worker_connected_at = hub.worker_seen = time.time()
    if old is not None and not old.closed:
        await old.close(message=b"replaced by new worker connection")
    log.info("worker connected from %s", client_ip(request))
    await ws.send_json({"type": "welcome", "server_time": time.time()})
    await hub.notify_ui_count()
    await hub.broadcast({"type": "worker", "worker_online": True})
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            hub.worker_seen = time.time()
            data = json.loads(msg.data)
            t = data.get("type")
            if t in ("hello", "snapshot"):
                if data.get("snapshot"):
                    hub.snapshot = data["snapshot"]
                if t == "hello":
                    hub.logs.clear()
                    hub.logs.extend(data.get("logs") or [])
                await hub.broadcast({"type": "state", **hub.state(),
                                     **({"logs": list(hub.logs)[-150:]} if t == "hello" else {})})
            elif t == "logs":
                items = data.get("logs") or []
                hub.logs.extend(items)
                await hub.broadcast({"type": "logs", "logs": items})
            elif t == "reply":
                fut = hub.pending.get(data.get("id"))
                if fut and not fut.done():
                    fut.set_result(data.get("result") or {})
    finally:
        if hub.worker_ws is ws:
            hub.worker_ws = None
            log.info("worker disconnected")
            await hub.broadcast({"type": "worker", "worker_online": False})
    return ws


async def bootstrap_script(request: web.Request) -> web.Response:
    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    base = f"{proto}://{request.host}"
    text = (REPO / "bootstrap" / "install.ps1").read_text(encoding="utf-8").replace("__BACKEND_URL__", base)
    return web.Response(text=text, content_type="text/plain", charset="utf-8")


async def bootstrap_zip(request: web.Request) -> web.Response:
    hub: Hub = request.app[HUB]
    key = request.headers.get("X-Worker-Secret", "")
    if not hmac.compare_digest(key.encode(), hub.worker_secret.encode()):
        await asyncio.sleep(1.0)
        raise web.HTTPUnauthorized(text="bad worker secret")
    return web.Response(body=hub.worker_zip, content_type="application/zip")


def make_app(ui_password: str, worker_secret: str, wake_url: str = "",
             cloud: CloudEngine | None = None) -> web.Application:
    app = web.Application(middlewares=[security_headers], client_max_size=64 * 1024)
    hub = Hub(ui_password, worker_secret, "" if cloud else wake_url)
    app[HUB] = hub
    if cloud:
        hub.cloud = cloud
        cloud.snapshot_fn = lambda: hub.snapshot

        async def _cloud_start(_app):
            await cloud.start()

        async def _cloud_stop(_app):
            await cloud.stop()
        app.on_startup.append(_cloud_start)
        app.on_cleanup.append(_cloud_stop)
    app.router.add_get("/", index)
    app.router.add_get("/healthz", healthz)
    app.router.add_post("/api/login", login)
    app.router.add_post("/api/worker/enroll", worker_enroll)
    app.router.add_post("/api/logout", logout)
    app.router.add_get("/api/state", api_state)
    app.router.add_post("/api/cmd", api_cmd)
    app.router.add_get("/api/ws", ui_ws)
    app.router.add_get("/worker/ws", worker_ws)
    app.router.add_get("/bootstrap/install.ps1", bootstrap_script)
    app.router.add_get("/bootstrap/worker.zip", bootstrap_zip)
    app.router.add_static("/static/", STATIC, show_index=False)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ui_pw, secret = _env("UI_PASSWORD"), _env("WORKER_SECRET")
    if len(ui_pw) < 10 or len(secret) < 24:
        raise SystemExit("UI_PASSWORD (>=10 chars) and WORKER_SECRET (>=24 chars) are too short")
    wake = os.environ.get("WAKE_URL", "https://ntfy.sh").strip()
    port = int(os.environ.get("PORT", "10000"))
    cloud = None
    if cloud_mode_from_env():
        cloud = CloudEngine(port, secret, public_url=os.environ.get("RENDER_EXTERNAL_URL", "").strip(),
                            ping_interval=float(os.environ.get("RB_KEEPALIVE_SECONDS", "240")))
        log.info("cloud engine mode (TickerAll); TICKERALL_API_KEY %s",
                 "set" if os.environ.get("TICKERALL_API_KEY", "").strip() else "NOT set")
    web.run_app(make_app(ui_pw, secret, "" if wake.lower() == "off" else wake, cloud=cloud), host="0.0.0.0",
                port=port)


if __name__ == "__main__":
    main()
