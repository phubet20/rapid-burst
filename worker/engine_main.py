"""Trading engine process (spawned on demand by the launcher; never started on its own).

Talks to the launcher over stdin/stdout (JSON lines). Owns the MetaTrader5
connection. Exits on `shutdown`, or safely (entries blocked, MT5 released) if the
launcher disappears (stdin EOF).
"""
from __future__ import annotations

import importlib
import json
import logging
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from rapidburst.broker_mt5 import MT5Broker  # noqa: E402
from rapidburst.config import EngineConfig  # noqa: E402
from rapidburst.proclock import ProcessLock  # noqa: E402
from rapidburst.trader import Trader  # noqa: E402

ENGINE_CMDS = {"set_credentials", "connect_stored", "start", "stop", "search", "select", "refresh", "shutdown", "detach"}


class MemoryCredStore:
    """Engine-side credentials: memory only (the launcher keeps the DPAPI copy)."""

    def __init__(self):
        self._c = None

    def save_credentials(self, creds):
        self._c = dict(creds)
        return "memory"

    def load_credentials(self):
        return dict(self._c) if self._c else None

    def forget_credentials(self):
        self._c = None

    def decrypt_credentials(self, _):
        raise ValueError("engine does not decrypt; launcher does")


def make_broker():
    factory = os.environ.get("RB_TEST_BROKER_FACTORY")  # TEST HOOK ONLY (never set on a real worker)
    if factory:
        mod, fn = factory.split(":")
        return getattr(importlib.import_module(mod), fn)()
    if os.environ.get("RB_BROKER", "mt5").strip().lower() == "tickerall":
        # Linux/cloud path: hosted MT5 connection through TickerAll (no terminal, no Windows)
        from rapidburst.broker_tickerall import TickerAllBroker
        cfg = EngineConfig.load()
        return TickerAllBroker(magic=cfg.magic, comment_prefix=cfg.comment_prefix,
                               data_dir=os.environ.get("RB_DATA_DIR"))
    return MT5Broker(terminal_path=os.environ.get("MT5_PATH") or None,
                     portable=os.environ.get("MT5_PORTABLE", "1") == "1")


def main() -> None:
    data_dir = os.environ.get("RB_DATA_DIR", os.path.join(HERE, "data"))
    os.makedirs(data_dir, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s engine: %(message)s",
                        handlers=[logging.FileHandler(os.path.join(data_dir, "engine.log"), encoding="utf-8")])
    out = sys.stdout
    out_lock = threading.Lock()

    def emit(obj):
        with out_lock:
            out.write(json.dumps(obj, default=str) + "\n")
            out.flush()

    lock = ProcessLock(os.path.join(data_dir, "engine.lock"))
    if not lock.acquire():
        emit({"type": "fatal", "msg": "another engine process is already running"})
        sys.exit(4)
    with open(os.path.join(data_dir, "engine.pid"), "w") as fh:
        fh.write(str(os.getpid()))

    trader = Trader(make_broker(), MemoryCredStore(), EngineConfig.load(), data_dir,
                    allow_live=os.environ.get("RB_ALLOW_LIVE", "") == "I_UNDERSTAND_LIVE_RISK")
    th = threading.Thread(target=trader.run_forever, daemon=True, name="trader")
    th.start()

    def cmd_detach():
        trader.running = False
        trader.add_log("WARNING", "Launcher connection lost - engine detaching (entries blocked, MT5 released)")
        trader.broker.shutdown()
        trader.stop_event.set()
        return {"ok": True}
    trader.cmd_detach = cmd_detach

    def writer():
        last_ts, seq = 0.0, 0
        while th.is_alive():
            snap = trader.get_snapshot()
            if snap and snap.get("ts") != last_ts:
                last_ts = snap["ts"]
                emit({"type": "snapshot", "snapshot": snap})
            new = trader.logs_after(seq)
            if new:
                seq = new[-1]["seq"]
                emit({"type": "logs", "logs": new})
            time.sleep(0.5)
        new = trader.logs_after(seq)
        if new:
            emit({"type": "logs", "logs": new})
        emit({"type": "exit"})
        lock.release()
        try:
            os.remove(os.path.join(data_dir, "engine.pid"))
        except OSError:
            pass
        os._exit(0)

    threading.Thread(target=writer, daemon=True, name="writer").start()
    emit({"type": "ready", "pid": os.getpid()})

    for line in sys.stdin:
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("type") != "cmd" or msg.get("cmd") not in ENGINE_CMDS:
            continue
        cid = msg.get("id")
        fut = trader.submit(msg["cmd"], msg.get("args") or {})
        fut.add_done_callback(lambda f, cid=cid: emit({"type": "reply", "id": cid, "result": f.result()}))
    # stdin closed: launcher is gone
    trader.submit("detach")
    th.join(timeout=30)
    time.sleep(1.0)
    os._exit(0)


if __name__ == "__main__":
    main()
