"""Always-on, lightweight launcher (runs on the user's Windows PC or a VPS).

Idle footprint: one sleeping Python process, no MetaTrader import, no MT5
terminal, no trading engine. It:
  * keeps the MT5 credentials (DPAPI) and the RSA key used by the app to encrypt them
  * waits for a wake signal (ntfy topic derived from WORKER_SECRET) and then opens an
    outbound websocket to the backend; it disconnects again when nobody is using the
    app, so the free Render backend can sleep
  * spawns the trading engine process (engine_main.py, which starts the MT5 terminal)
    on CONNECT/START and shuts the engine and the bot's own MT5 terminal down after STOP
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib.util
import json
import logging
import os
import socket
import sys
import time
import uuid
from collections import deque
from typing import Optional

import aiohttp

from . import __version__
from .secure_store import SecureStore

log = logging.getLogger("rapidburst.launcher")

UI_CMDS = {"connect", "start", "stop", "search", "select", "refresh", "forget"}
CMD_TTL = 25.0  # seconds; older commands are ignored (no replay of stale commands)


def wake_topic(secret: str) -> str:
    return "rb-" + hmac.new(secret.encode(), b"rapid-burst-wake", hashlib.sha256).hexdigest()[:40]


def _norm(p: str) -> str:
    return os.path.normcase(os.path.abspath(p)) if p else ""


def find_processes(exe_path: str = "", cmd_marker: str = "") -> list:
    """Processes whose executable is exe_path, or whose command line contains cmd_marker."""
    import psutil
    out = []
    target = _norm(exe_path)
    for p in psutil.process_iter(["pid", "exe", "cmdline"]):
        try:
            if p.pid == os.getpid():
                continue
            if target and p.info.get("exe") and _norm(p.info["exe"]) == target:
                out.append(p)
            elif cmd_marker and any(cmd_marker in (c or "") for c in (p.info.get("cmdline") or [])):
                out.append(p)
        except Exception:  # noqa: BLE001
            continue
    return out


def kill_processes(procs: list, timeout: float = 8.0) -> int:
    import psutil
    for p in procs:
        try:
            p.terminate()
        except Exception:  # noqa: BLE001
            pass
    _, alive = psutil.wait_procs(procs, timeout=timeout)
    for p in alive:
        try:
            p.kill()
        except Exception:  # noqa: BLE001
            pass
    return len(procs)


class EngineProc:
    """The trading engine child process and its JSON-lines pipe."""

    def __init__(self, launcher: "Launcher", proc: asyncio.subprocess.Process):
        self.l = launcher
        self.proc = proc
        self.pid = proc.pid
        self.pending: dict[str, asyncio.Future] = {}
        self.snapshot: dict = {}
        self.ready = asyncio.get_running_loop().create_future()
        self.exited = False
        self.expected_exit = False
        self.reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        try:
            while True:
                line = await self.proc.stdout.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                t = msg.get("type")
                if t == "ready" and not self.ready.done():
                    self.ready.set_result(True)
                elif t == "fatal" and not self.ready.done():
                    self.ready.set_exception(RuntimeError(msg.get("msg")))
                elif t == "snapshot":
                    self.snapshot = msg.get("snapshot") or {}
                    self.l.on_engine_snapshot()
                elif t == "logs":
                    for item in msg.get("logs") or []:
                        self.l.add_log(item.get("level", "INFO"), item.get("msg", ""), ts=item.get("ts"))
                elif t == "reply":
                    fut = self.pending.pop(msg.get("id"), None)
                    if fut and not fut.done():
                        fut.set_result(msg.get("result") or {})
        finally:
            self.exited = True
            if not self.ready.done():
                self.ready.set_exception(RuntimeError("engine exited during startup"))
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_result({"ok": False, "msg": "engine process exited"})

    async def request(self, cmd: str, args: Optional[dict] = None, timeout: float = 120.0) -> dict:
        if self.exited or self.proc.returncode is not None:
            return {"ok": False, "msg": "engine not running"}
        cid = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self.pending[cid] = fut
        try:
            self.proc.stdin.write((json.dumps({"type": "cmd", "id": cid, "cmd": cmd, "args": args or {}}) + "\n").encode())
            await self.proc.stdin.drain()
            return await asyncio.wait_for(fut, timeout)
        except (asyncio.TimeoutError, ConnectionError, BrokenPipeError) as exc:
            return {"ok": False, "msg": f"engine did not answer ({type(exc).__name__})"}
        finally:
            self.pending.pop(cid, None)

    def alive(self) -> bool:
        return not self.exited and self.proc.returncode is None


class Launcher:
    def __init__(self, backend_url: str, secret: str, data_dir: str, mt5_path: str = "",
                 wake_url: str = "https://ntfy.sh", linger: float = 300.0, standby_timeout: float = 600.0,
                 fallback_poll_minutes: float = 0.0, engine_cmd: Optional[list] = None,
                 engine_env: Optional[dict] = None, stop_retry_seconds: float = 300.0):
        self.backend_url = backend_url.rstrip("/")
        self.secret = secret
        self.data_dir = data_dir
        self.mt5_path = mt5_path
        self.wake_url = (wake_url or "").rstrip("/")
        self.linger = linger
        self.standby_timeout = standby_timeout
        self.fallback_poll = fallback_poll_minutes * 60.0
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.engine_cmd = engine_cmd or [sys.executable, "-u", os.path.join(here, "engine_main.py")]
        self.engine_env = engine_env or {}
        self.stop_retry = stop_retry_seconds
        self.stopping_since = 0.0
        os.makedirs(data_dir, exist_ok=True)
        self.store = SecureStore(data_dir)
        self.state_file = os.path.join(data_dir, "launcher.json")
        st = self._load()
        self.desired: str = st.get("desired", "STOPPED")
        self.last_connect: Optional[dict] = st.get("last_connect")
        self.engine: Optional[EngineProc] = None
        self.phase = "IDLE"            # IDLE / STARTING / STANDBY / RUNNING / STOPPING
        self.logs: deque = deque(maxlen=300)
        self.log_seq = 0
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.ui_clients = 0
        self.want_until = time.time() + linger
        self.wake_event = asyncio.Event()
        self.snapshot_dirty = asyncio.Event()
        self.server_offset = 0.0
        self.seen_cmds: deque = deque(maxlen=500)
        self.inflight = 0
        self.last_cmd_wall = 0.0
        self.respawn_backoff = 5.0
        self.next_respawn = 0.0
        self.lock = asyncio.Lock()
        self.counters = {"wakes": 0, "backend_sessions": 0, "engine_spawns": 0, "terminals_killed": 0}

    # ------------------------------------------------------------- utils
    def _load(self) -> dict:
        try:
            with open(self.state_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:  # noqa: BLE001
            return {}

    def _save(self) -> None:
        tmp = self.state_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"desired": self.desired, "last_connect": self.last_connect}, fh)
        os.replace(tmp, self.state_file)

    def add_log(self, level: str, msg: str, ts: Optional[float] = None) -> None:
        self.log_seq += 1
        self.logs.append({"seq": self.log_seq, "ts": ts or time.time(), "level": level, "msg": msg})
        getattr(log, level.lower() if level.lower() in ("info", "warning", "error") else "info")(msg)
        self.snapshot_dirty.set()

    def on_engine_snapshot(self) -> None:
        self.snapshot_dirty.set()

    def has_credentials(self) -> bool:
        return self.store.load_credentials() is not None

    def bot_terminals(self) -> list:
        return find_processes(exe_path=self.mt5_path) if self.mt5_path else []

    def snapshot(self) -> dict:
        import psutil
        me = psutil.Process()
        info = {
            "phase": self.phase, "desired": self.desired,
            "engine_pid": self.engine.pid if self.engine and self.engine.alive() else None,
            "terminal_running": bool(self.bot_terminals()),
            "has_credentials": self.has_credentials(),
            "last_connect": self.last_connect,
            "rss_mb": round(me.memory_info().rss / 1e6, 1),
            "counters": self.counters, "version": __version__,
        }
        if self.engine and self.engine.alive() and self.engine.snapshot:
            snap = dict(self.engine.snapshot)
        else:
            snap = {
                "ts": time.time(),
                "worker": {"version": __version__, "host": socket.gethostname(),
                           "mt5_package": importlib.util.find_spec("MetaTrader5") is not None},
                "conn": {"state": "IDLE", "msg": "Trading engine stopped - no MT5 terminal or engine running",
                         "terminal_connected": False, "algo_trading": False, "ping_ms": None},
                "account": None, "validation": {"ok": False, "checks": [], "idle": True},
                "engine": {"running": False, "halted": False, "symbols": {}, "selected": self._selected()},
                "pnl": None, "positions": [], "foreign_positions": None,
            }
        snap["pubkey"] = self.store.public_key_spki_b64()
        snap["launcher"] = info
        return snap

    def _selected(self) -> list:
        try:
            with open(os.path.join(self.data_dir, "state.json"), "r", encoding="utf-8") as fh:
                return json.load(fh).get("selected", [])
        except Exception:  # noqa: BLE001
            return []

    # ------------------------------------------------------------ engine
    async def ensure_engine(self) -> Optional[str]:
        """Start the engine process if needed and hand it the credentials. Returns error text or None."""
        if self.engine and self.engine.alive():
            return None
        creds = self.store.load_credentials()
        if not creds:
            return "no MT5 credentials stored - use CONNECT MT5 first"
        stale = find_processes(cmd_marker="engine_main.py")
        if stale:
            self.add_log("WARNING", f"Killing {len(stale)} orphan engine process(es) before spawning")
            kill_processes(stale)
        env = dict(os.environ, **self.engine_env)
        env["RB_DATA_DIR"] = self.data_dir
        env.pop("WORKER_SECRET", None)  # the engine never needs the backend secret
        proc = await asyncio.create_subprocess_exec(
            *self.engine_cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, env=env, cwd=os.path.dirname(self.engine_cmd[-1]))
        self.engine = EngineProc(self, proc)
        self.counters["engine_spawns"] += 1
        try:
            await asyncio.wait_for(asyncio.shield(self.engine.ready), 60)
        except Exception as exc:  # noqa: BLE001
            await self._kill_engine()
            return f"engine failed to start: {exc}"
        r = await self.engine.request("set_credentials", {"creds": creds}, timeout=15)
        if not r.get("ok"):
            await self.shutdown_engine("credential hand-off failed")
            return "engine credential hand-off failed"
        self.phase = "STANDBY"
        self.add_log("INFO", f"Engine process started (pid {self.engine.pid})")
        return None

    async def _kill_engine(self) -> None:
        if self.engine:
            if self.engine.proc.returncode is None:
                try:
                    self.engine.proc.kill()
                except ProcessLookupError:
                    pass
            try:
                await asyncio.wait_for(self.engine.proc.wait(), 10)
            except asyncio.TimeoutError:
                pass

    async def shutdown_engine(self, why: str, force: bool = False) -> None:
        if self.engine:
            self.engine.expected_exit = True
            if self.engine.alive():
                await self.engine.request("shutdown", {"force": force}, timeout=20)
                try:
                    await asyncio.wait_for(self.engine.proc.wait(), 15)
                except asyncio.TimeoutError:
                    self.add_log("WARNING", "engine did not exit - killing it")
                    await self._kill_engine()
            pid = self.engine.pid
            self.engine = None
            self.add_log("INFO", f"Engine process {pid} ended ({why})")
        n = await asyncio.to_thread(lambda: kill_processes(self.bot_terminals()))
        if n:
            self.counters["terminals_killed"] += n
            self.add_log("INFO", f"Closed the bot's MT5 terminal ({n} process)")
        self.phase = "IDLE"
        self.snapshot_dirty.set()

    # ----------------------------------------------------------- commands
    async def handle(self, cmd: str, args: dict) -> dict:
        self.last_cmd_wall = time.time()
        self.want_until = max(self.want_until, time.time() + self.linger)
        async with self.lock:
            try:
                return await self._handle(cmd, args)
            finally:
                self.last_cmd_wall = time.time()
                self.snapshot_dirty.set()

    async def _handle(self, cmd: str, args: dict) -> dict:
        if cmd == "connect":
            try:
                creds = self.store.decrypt_credentials(args.get("enc", ""))
            except Exception as exc:  # noqa: BLE001
                return {"ok": False, "msg": f"could not decrypt credentials ({type(exc).__name__})"}
            where = self.store.save_credentials(creds)
            self.add_log("INFO", f"Credentials for {creds['login']}@{creds['server']} {where}")
            if self.engine and self.engine.alive():
                await self.engine.request("set_credentials", {"creds": creds}, timeout=15)
            err = await self.ensure_engine()
            if err:
                self.last_connect = {"ok": False, "ts": time.time(), "msg": err}
                self._save()
                return {"ok": False, "msg": err}
            r = await self.engine.request("connect_stored", timeout=120)
            v = r.get("validation") or {}
            self.last_connect = {"ok": bool(r.get("ok")), "validation_ok": bool(v.get("ok")), "ts": time.time(),
                                 "msg": r.get("msg"), "account": r.get("account"),
                                 "failed_checks": [c["name"] for c in v.get("checks", []) if not c.get("ok")],
                                 "failed_details": [f'{c["name"]}: {c.get("detail", "")}' for c in v.get("checks", [])
                                                    if not c.get("ok")]}
            self._save()
            return {"ok": bool(r.get("ok")), "msg": r.get("msg"), "validation": v}

        if cmd in ("search", "select", "refresh"):
            err = await self.ensure_engine()
            if err:
                return {"ok": False, "msg": err, "symbols": []}
            if cmd == "refresh":
                r = await self.engine.request("connect_stored", timeout=120)
                v = r.get("validation") or {}
                self.last_connect = {"ok": bool(r.get("ok")), "validation_ok": bool(v.get("ok")), "ts": time.time(),
                                     "msg": r.get("msg"), "account": r.get("account"),
                                     "failed_checks": [c["name"] for c in v.get("checks", []) if not c.get("ok")],
                                 "failed_details": [f'{c["name"]}: {c.get("detail", "")}' for c in v.get("checks", [])
                                                    if not c.get("ok")]}
                self._save()
                return {"ok": bool(r.get("ok")), "msg": r.get("msg"), "validation": v}
            r = await self.engine.request(cmd, args, timeout=60)
            if cmd == "select" and r.get("ok") and r.get("validation"):
                v = r["validation"]       # keep the stored 'validated' flag in step with the new symbol choice
                self.last_connect = {"ok": True, "validation_ok": bool(v.get("ok")), "ts": time.time(),
                                     "msg": "symbols changed", "account": r.get("account"),
                                     "failed_checks": [c["name"] for c in v.get("checks", []) if not c.get("ok")],
                                     "failed_details": [f'{c["name"]}: {c.get("detail", "")}' for c in v.get("checks", [])
                                                        if not c.get("ok")]}
                self._save()
            return r

        if cmd == "start":
            if self.desired == "RUNNING" and self.engine and self.engine.alive() and \
                    (self.engine.snapshot.get("engine") or {}).get("running"):
                return {"ok": True, "msg": "already running"}
            self.desired = "RUNNING"
            self._save()
            self.phase = "STARTING"
            recovery = bool(args.get("recovery"))
            err = await self.ensure_engine()
            if err:
                if not recovery:
                    self.desired = "STOPPED"; self._save()
                self.phase = "IDLE"
                return {"ok": False, "msg": err}
            r = await self.engine.request("start", timeout=120)
            if not r.get("ok"):
                if not recovery:          # recovery keeps retrying with backoff
                    self.desired = "STOPPED"
                    self._save()
                self.add_log("WARNING", f"START refused: {r.get('msg')}")
                await self.shutdown_engine("START refused")
                return r
            self.phase = "RUNNING"
            self.respawn_backoff = 5.0
            return r

        if cmd == "stop":
            self.desired = "STOPPED"
            self._save()
            if not (self.engine and self.engine.alive()):
                if not self.has_credentials():
                    return {"ok": True, "msg": "already stopped (no engine running)"}
                # make sure no bot positions/pendings were left behind (e.g. after a crash)
                self.phase = "STOPPING"
                err = await self.ensure_engine()
                if err:
                    self.phase = "IDLE"
                    return {"ok": False, "msg": err}
            self.phase = "STOPPING"
            self.stopping_since = time.time()
            r = await self.engine.request("stop", timeout=180)
            if r.get("ok"):
                await self.shutdown_engine("STOP")
                return {"ok": True, "msg": f"{r.get('msg')}; engine and MT5 terminal shut down"}
            if "not connected" in str(r.get("msg")):
                return {"ok": False, "msg": f"{r.get('msg')}. Engine will be shut down; press STOP again when MT5 "
                                            f"is reachable to verify."}
            self.add_log("ERROR", "STOP: bot positions not all closed - engine kept alive and retrying")
            return {"ok": False, "msg": f"{r.get('msg')} (engine keeps retrying, will shut down when flat)"}

        if cmd == "forget":
            if self.desired == "RUNNING":
                return {"ok": False, "msg": "STOP first"}
            if self.engine and self.engine.alive():
                await self.shutdown_engine("credentials removed")
            self.store.forget_credentials()
            self.last_connect = None
            self._save()
            self.add_log("INFO", "MT5 credentials removed")
            return {"ok": True}
        return {"ok": False, "msg": "unknown command"}

    # ------------------------------------------------------------ monitor
    async def monitor(self) -> None:
        hb = os.path.join(self.data_dir, "heartbeat.txt")
        last_hb = 0.0
        while True:
            await asyncio.sleep(1.0)
            now = time.time()
            if now - last_hb > 10:
                last_hb = now
                with open(hb, "w") as fh:
                    fh.write(str(now))
            if self.lock.locked():
                continue
            eng = self.engine
            if eng and not eng.alive():
                if not eng.expected_exit:
                    self.add_log("ERROR", f"Engine process {eng.pid} exited unexpectedly")
                    self.engine = None
                    await self.shutdown_engine("crash cleanup")
                continue
            if not eng:
                if self.desired == "RUNNING" and now >= self.next_respawn:
                    self.add_log("WARNING", "Desired state RUNNING but no engine - restarting it with a fresh START")
                    self.next_respawn = now + self.respawn_backoff
                    self.respawn_backoff = min(self.respawn_backoff * 2, 300.0)
                    asyncio.create_task(self.handle("start", {"recovery": True}))
                continue
            es = eng.snapshot or {}
            e = es.get("engine") or {}
            positions = es.get("positions") or []
            if self.phase == "RUNNING" and not e.get("running") and e.get("halted"):
                self.add_log("WARNING", "Risk halt - stopping and shutting the engine down for today")
                asyncio.create_task(self.handle("stop", {}))
            elif self.phase == "STOPPING" and (es.get("conn") or {}).get("state") != "CONNECTED" and (
                    (es.get("conn") or {}).get("state") == "UNAVAILABLE" or now - self.stopping_since > self.stop_retry):
                self.add_log("ERROR", "STOP: MT5 unreachable, bot positions cannot be verified - shutting the engine "
                                      "down; any bot positions keep their server-side SL/TP. Press STOP again "
                                      "when MT5 is reachable.")
                async with self.lock:
                    await self.shutdown_engine("STOP gave up: MT5 unreachable", force=True)
            elif self.phase == "STOPPING" and es and not es.get("flatten_pending") and not positions and not e.get("running"):
                async with self.lock:
                    await self.shutdown_engine("STOP completed after retry")
            elif self.phase == "STANDBY" and self.desired != "RUNNING" and now - self.last_cmd_wall > self.standby_timeout:
                if positions:
                    continue
                async with self.lock:
                    await self.shutdown_engine(f"standby timeout {self.standby_timeout:.0f}s without START")

    # ----------------------------------------------------------- backend
    async def wake_listener(self) -> None:
        if not self.wake_url:
            return
        url = f"{self.wake_url}/{wake_topic(self.secret)}/json"
        backoff = 5.0
        while True:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_read=180)) as s:
                    async with s.get(url) as r:
                        r.raise_for_status()
                        backoff = 5.0
                        async for raw in r.content:
                            try:
                                ev = json.loads(raw)
                            except ValueError:
                                continue
                            if ev.get("event") == "message":
                                self.counters["wakes"] += 1
                                self.want_until = time.time() + self.linger
                                self.wake_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("wake listener: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120.0)

    def want_connection(self) -> bool:
        if not self.wake_url:
            return True       # no wake channel: stay connected (backend never sleeps)
        return time.time() < self.want_until or self.inflight > 0 or self.lock.locked()

    async def backend_loop(self) -> None:
        backoff = 2.0
        while True:
            if not self.want_connection():
                self.wake_event.clear()
                timeout = self.fallback_poll or None
                try:
                    await asyncio.wait_for(self.wake_event.wait(), timeout)
                except asyncio.TimeoutError:
                    self.want_until = time.time() + 60
                continue
            try:
                await self._session()
                backoff = 2.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("backend link: %s: %s", type(exc).__name__, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.7, 30.0)

    async def _session(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=60)) as http:
            async with http.ws_connect(self.backend_url + "/worker/ws", heartbeat=20,
                                       headers={"Authorization": f"Bearer {self.secret}"}) as ws:
                self.ws = ws
                self.counters["backend_sessions"] += 1
                self.want_until = max(self.want_until, time.time() + self.linger)
                await ws.send_json({"type": "hello", "snapshot": self.snapshot(), "logs": list(self.logs)[-200:]})
                sender = asyncio.create_task(self._sender(ws))
                try:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            break
                        data = json.loads(msg.data)
                        t = data.get("type")
                        if t == "welcome":
                            self.server_offset = float(data.get("server_time", time.time())) - time.time()
                        elif t == "ui":
                            self.ui_clients = int(data.get("clients", 0))
                            if self.ui_clients:
                                self.want_until = time.time() + self.linger
                        elif t == "cmd":
                            asyncio.create_task(self._run_cmd(ws, data))
                finally:
                    sender.cancel()
                    self.ws = None

    async def _run_cmd(self, ws, data: dict) -> None:
        cid, cmd = data.get("id"), data.get("cmd")
        age = time.time() + self.server_offset - float(data.get("ts", 0))
        if cid in self.seen_cmds:
            return
        self.seen_cmds.append(cid)
        if cmd not in UI_CMDS:
            result = {"ok": False, "msg": "unknown command"}
        elif age > CMD_TTL:
            result = {"ok": False, "msg": f"stale command ignored ({age:.0f}s old)"}
            self.add_log("WARNING", f"Ignored stale {cmd} command ({age:.0f}s old)")
        else:
            self.inflight += 1
            try:
                result = await self.handle(cmd, data.get("args") or {})
            finally:
                self.inflight -= 1
        try:
            await ws.send_json({"type": "reply", "id": cid, "result": result})
        except Exception:  # noqa: BLE001
            pass

    async def _sender(self, ws) -> None:
        last_seq = self.log_seq
        while not ws.closed:
            try:
                await asyncio.wait_for(self.snapshot_dirty.wait(), 1.0)
            except asyncio.TimeoutError:
                pass
            self.snapshot_dirty.clear()
            await ws.send_json({"type": "snapshot", "snapshot": self.snapshot()})
            new = [x for x in self.logs if x["seq"] > last_seq]
            if new:
                last_seq = new[-1]["seq"]
                await ws.send_json({"type": "logs", "logs": new})
            if not self.want_connection() and self.ui_clients == 0:
                self.add_log("INFO", "No app open - releasing backend connection (backend may sleep)")
                await ws.close()
                return

    # -------------------------------------------------------------- main
    async def run(self) -> None:
        self.add_log("INFO", f"Launcher {__version__} started on {socket.gethostname()} (idle: no engine, no MT5)")
        orphans = find_processes(cmd_marker="engine_main.py")
        if orphans:
            self.add_log("WARNING", f"Stopping {len(orphans)} orphan engine process(es) from a previous run")
            kill_processes(orphans)
        n = kill_processes(self.bot_terminals())
        if n:
            self.add_log("INFO", f"Closed {n} leftover bot MT5 terminal process(es)")
        if self.desired == "RUNNING":
            self.add_log("WARNING", "Launcher restarted while trading was ON - restarting engine with a fresh START "
                                    "(re-reads account, re-adopts bot positions)")
            asyncio.create_task(self.handle("start", {"recovery": True}))
        await asyncio.gather(self.monitor(), self.wake_listener(), self.backend_loop())
