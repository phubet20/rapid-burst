"""Cloud engine mode: run the trading launcher + engine next to the backend on
Linux (e.g. Render free), talking to MT5 through TickerAll instead of a local
Windows terminal.

* The unchanged launcher (worker/main.py) is started as a child process and
  joins this backend over the loopback WebSocket with WORKER_SECRET, so every
  START/STOP/CONNECT rule, DEMO gate and ownership rule is exactly the same as
  on Windows.
* Keep-awake: Render free sleeps after 15 minutes without inbound traffic. While
  the engine is STARTING/RUNNING/STOPPING (or holds bot positions) this module
  requests the service's own public /healthz every few minutes; after STOP it
  stops, so the service can sleep again. Every ping is counted and shown.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import aiohttp

log = logging.getLogger("rb.cloud")
REPO = Path(__file__).resolve().parent.parent

ACTIVE_PHASES = {"STARTING", "RUNNING", "STOPPING"}


def cloud_mode_from_env(env: Optional[dict] = None) -> bool:
    env = os.environ if env is None else env
    mode = env.get("ENGINE_MODE", "").strip().lower()
    if mode == "cloud":
        return True
    if mode in ("launcher", "windows"):
        return False
    return bool(env.get("TICKERALL_API_KEY", "").strip())


def engine_active(snapshot: dict) -> bool:
    snap = snapshot or {}
    L = snap.get("launcher") or {}
    eng = snap.get("engine") or {}
    return (L.get("phase") in ACTIVE_PHASES or L.get("desired") == "RUNNING" or bool(eng.get("running"))
            or bool(snap.get("positions")))


class CloudEngine:
    def __init__(self, port: int, worker_secret: str, public_url: str = "",
                 ping_interval: float = 240.0, data_dir: str = "",
                 snapshot_fn: Optional[Callable[[], dict]] = None,
                 launcher_cmd: Optional[list] = None, extra_env: Optional[dict] = None):
        self.port = port
        self.secret = worker_secret
        self.public_url = public_url.rstrip("/")
        self.ping_interval = ping_interval
        self.data_dir = data_dir or os.environ.get("RB_CLOUD_DATA_DIR", "/tmp/rapid-burst-data")
        self.snapshot_fn = snapshot_fn or (lambda: {})
        self.launcher_cmd = launcher_cmd or [sys.executable, "-u", str(REPO / "worker" / "main.py")]
        self.extra_env = extra_env or {}
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.tasks: list[asyncio.Task] = []
        self.restarts = 0
        self.keepalive = {"active": False, "pings_ok": 0, "pings_failed": 0, "last_ping": None,
                          "interval_s": ping_interval, "target": (self.public_url + "/healthz") if self.public_url else None}

    def status(self) -> dict:
        return {"mode": "cloud", "provider": "TickerAll",
                "launcher_pid": self.proc.pid if self.proc and self.proc.returncode is None else None,
                "launcher_restarts": self.restarts, "keepalive": dict(self.keepalive),
                "api_key_set": bool((self.extra_env.get("TICKERALL_API_KEY") or os.environ.get("TICKERALL_API_KEY", "")).strip())}

    def child_env(self) -> dict:
        env = dict(os.environ)
        env.update({
            "BACKEND_URL": f"http://127.0.0.1:{self.port}", "WORKER_SECRET": self.secret,
            "RB_WAKE_URL": "off", "RB_BROKER": "tickerall", "RB_DATA_DIR": self.data_dir,
            "RB_ENV_FILE": os.path.join(self.data_dir, "no-worker.env"), "MT5_PATH": "",
        })
        for k in ("UI_PASSWORD", "RB_UI_PASSWORD"):
            env.pop(k, None)               # the launcher/engine never need the app password
        env.update(self.extra_env)
        return env

    async def start(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.tasks = [asyncio.create_task(self._supervise()), asyncio.create_task(self._keepalive())]

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), 20)
            except asyncio.TimeoutError:
                self.proc.kill()

    async def _supervise(self) -> None:
        backoff = 2.0
        while True:
            started = time.time()
            try:
                self.proc = await asyncio.create_subprocess_exec(*self.launcher_cmd, env=self.child_env(),
                                                                 cwd=str(REPO / "worker"))
                log.info("cloud launcher started (pid %s)", self.proc.pid)
                rc = await self.proc.wait()
                log.warning("cloud launcher exited rc=%s", rc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("cloud launcher spawn failed: %s", exc)
            self.restarts += 1
            if time.time() - started > 120:
                backoff = 2.0
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _keepalive(self) -> None:
        last = 0.0
        while True:
            await asyncio.sleep(5.0)
            active = engine_active(self.snapshot_fn())
            self.keepalive["active"] = active
            if not active or not self.public_url or time.time() - last < self.ping_interval:
                continue
            last = time.time()
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as s:
                    async with s.get(self.public_url + "/healthz", params={"keepalive": "1"}) as r:
                        ok = r.status == 200
                self.keepalive["pings_ok" if ok else "pings_failed"] += 1
            except Exception as exc:  # noqa: BLE001
                self.keepalive["pings_failed"] += 1
                log.warning("keepalive ping failed: %s", exc)
            self.keepalive["last_ping"] = time.time()
