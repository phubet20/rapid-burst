"""Rapid Burst worker entry point.

Reads settings from environment or worker.env:
  BACKEND_URL     https://<your-app>.onrender.com
  WORKER_SECRET   shared secret (same value as on the backend)
  MT5_PATH        path to terminal64.exe (portable install)
  RB_DATA_DIR     data directory (state, logs, encrypted credentials)
  RB_ALLOW_LIVE   leave unset. Only "I_UNDERSTAND_LIVE_RISK" enables a REAL account.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from rapidburst.broker_mt5 import MT5Broker  # noqa: E402
from rapidburst.config import EngineConfig  # noqa: E402
from rapidburst.link import BackendLink, watchdog  # noqa: E402
from rapidburst.secure_store import SecureStore  # noqa: E402
from rapidburst.trader import Trader  # noqa: E402


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8-sig") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"'))


class _Redact(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        secret = os.environ.get("WORKER_SECRET")
        if secret and secret in str(record.msg):
            record.msg = str(record.msg).replace(secret, "***")
        return True


def main() -> None:
    load_env_file(os.environ.get("RB_ENV_FILE", os.path.join(HERE, "worker.env")))
    data_dir = os.environ.get("RB_DATA_DIR", os.path.join(HERE, "data"))
    os.makedirs(data_dir, exist_ok=True)
    handlers = [RotatingFileHandler(os.path.join(data_dir, "worker.log"), maxBytes=5_000_000, backupCount=5,
                                    encoding="utf-8"), logging.StreamHandler(sys.stdout)]
    for h in handlers:
        h.addFilter(_Redact())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        handlers=handlers)
    backend = os.environ.get("BACKEND_URL", "").strip()
    secret = os.environ.get("WORKER_SECRET", "").strip()
    if not backend or not secret:
        logging.error("BACKEND_URL and WORKER_SECRET are required")
        sys.exit(2)
    allow_live = os.environ.get("RB_ALLOW_LIVE", "") == "I_UNDERSTAND_LIVE_RISK"
    cfg = EngineConfig.load()
    broker = MT5Broker(terminal_path=os.environ.get("MT5_PATH") or None,
                       portable=os.environ.get("MT5_PORTABLE", "1") == "1")
    store = SecureStore(data_dir)
    trader = Trader(broker, store, cfg, data_dir, allow_live=allow_live)
    t = threading.Thread(target=trader.run_forever, name="trader", daemon=True)
    t.start()
    link = BackendLink(trader, backend, secret, os.path.join(data_dir, "heartbeat.txt"))

    async def runner():
        await asyncio.gather(link.run(), watchdog(trader, os.path.join(data_dir, "heartbeat.txt")))

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        trader.stop_event.set()


if __name__ == "__main__":
    main()
