"""Outbound websocket link from the worker to the backend (no inbound ports)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import aiohttp

from .trader import Trader

log = logging.getLogger("rapidburst.link")

ALLOWED_CMDS = {"connect", "start", "stop", "search", "select", "refresh", "forget"}


class BackendLink:
    def __init__(self, trader: Trader, backend_url: str, secret: str, heartbeat_file: str):
        self.trader = trader
        self.url = backend_url.rstrip("/") + "/worker/ws"
        self.http_url = backend_url.rstrip("/") + "/healthz"
        self.secret = secret
        self.heartbeat_file = heartbeat_file
        self.connected = False

    async def run(self) -> None:
        backoff = 2.0
        while True:
            try:
                await self._session()
                backoff = 2.0
            except Exception as exc:  # noqa: BLE001
                log.warning("backend link error: %s: %s", type(exc).__name__, exc)
            self.connected = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 30.0)

    async def _session(self) -> None:
        headers = {"Authorization": f"Bearer {self.secret}"}
        timeout = aiohttp.ClientTimeout(total=None, connect=30)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.ws_connect(self.url, headers=headers, heartbeat=20, max_msg_size=4 * 1024 * 1024) as ws:
                self.connected = True
                self.trader.add_log("INFO", "Linked to backend")
                await ws.send_json({"type": "hello", "snapshot": self.trader.get_snapshot(),
                                    "logs": self.trader.logs_after(0)[-200:]})
                sender = asyncio.create_task(self._sender(ws))
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._on_message(ws, json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                            break
                finally:
                    sender.cancel()

    async def _sender(self, ws) -> None:
        last_seq = self.trader.log_seq
        last_ts = 0.0
        while not ws.closed:
            snap = self.trader.get_snapshot()
            if snap and snap.get("ts", 0) != last_ts:
                last_ts = snap["ts"]
                await ws.send_json({"type": "snapshot", "snapshot": snap})
            new = self.trader.logs_after(last_seq)
            if new:
                last_seq = new[-1]["seq"]
                await ws.send_json({"type": "logs", "logs": new})
            await asyncio.sleep(0.5)

    async def _on_message(self, ws, msg: dict) -> None:
        if msg.get("type") != "cmd":
            return
        cmd, args, cid = msg.get("cmd"), msg.get("args") or {}, msg.get("id")
        if cmd not in ALLOWED_CMDS:
            await ws.send_json({"type": "reply", "id": cid, "result": {"ok": False, "msg": "unknown command"}})
            return
        fut = self.trader.submit(cmd, args)
        try:
            result = await asyncio.wait_for(asyncio.wrap_future(fut), timeout=90)
        except asyncio.TimeoutError:
            result = {"ok": False, "msg": "worker busy - command timed out"}
        await ws.send_json({"type": "reply", "id": cid, "result": result})


async def watchdog(trader: Trader, heartbeat_file: str, stale_after: float = 45.0) -> None:
    """Exit the process if the trading thread stops producing snapshots, so the
    supervisor (Task Scheduler / run_worker.ps1) restarts it."""
    start = time.time()
    while True:
        await asyncio.sleep(5)
        snap_ts = trader.get_snapshot().get("ts", 0) or start
        try:
            with open(heartbeat_file, "w", encoding="utf-8") as fh:
                fh.write(str(snap_ts))
        except OSError:
            pass
        if time.time() - snap_ts > stale_after and time.time() - start > 60:
            log.error("trading thread stalled for %.0fs - exiting for restart", time.time() - snap_ts)
            os._exit(3)
