"""Rapid Burst launcher entry point (the always-on, near-zero-resource process).

The trading engine (engine_main.py + MT5 terminal) runs only between START and STOP.

Settings (environment or worker.env):
  BACKEND_URL        https://<your-app>.onrender.com
  WORKER_SECRET      shared secret (same value as on the backend)
  MT5_PATH           path to the bot's own terminal64.exe (portable install)
  RB_DATA_DIR        data directory (state, logs, encrypted credentials)
  RB_WAKE_URL        wake channel (default https://ntfy.sh; "off" = stay connected 24/7)
  RB_LINGER_SECONDS  keep the backend connection this long after the app was last open (300)
  RB_STANDBY_SECONDS shut an engine that was only CONNECTed (not STARTed) after this long (600)
  RB_FALLBACK_POLL_MINUTES  optional periodic check-in if the wake channel is unreachable (0 = off)
  RB_ALLOW_LIVE      leave unset. Only "I_UNDERSTAND_LIVE_RISK" lets the engine trade a REAL account.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from rapidburst.launcher import Launcher  # noqa: E402


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
    handlers = [RotatingFileHandler(os.path.join(data_dir, "launcher.log"), maxBytes=2_000_000, backupCount=3,
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
    wake = os.environ.get("RB_WAKE_URL", "https://ntfy.sh").strip()
    launcher = Launcher(backend, secret, data_dir, mt5_path=os.environ.get("MT5_PATH", "").strip(),
                        wake_url="" if wake.lower() == "off" else wake,
                        linger=float(os.environ.get("RB_LINGER_SECONDS", "300")),
                        standby_timeout=float(os.environ.get("RB_STANDBY_SECONDS", "600")),
                        fallback_poll_minutes=float(os.environ.get("RB_FALLBACK_POLL_MINUTES", "0")))
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(launcher.run())


if __name__ == "__main__":
    main()
