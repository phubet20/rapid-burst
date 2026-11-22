# Rapid Burst Auto Scalping Bot (MetaTrader 5)

Autonomous tick-based momentum scalper for MT5 (primary symbol XAUUSD, any broker symbol selectable),
operated from a single-screen iPhone web app. **DEMO only by default.**

```
 iPhone (Safari) ─HTTPS/WSS─> Backend + UI (Render free, sleeps when unused) <─outbound WSS (only while needed)─ Launcher
                                   │ publishes content-free "wake"                                               (always on,
                                   └──────────> ntfy.sh topic (derived from WORKER_SECRET) ──> launcher listens    ~0% CPU)
                                                                                                               │ START/STOP
                                                                                       engine_main.py + bot's MT5 terminal
                                                                                       (exist only between START and STOP)
```

* **worker/main.py → rapidburst/launcher.py** – the tiny always-on process on your Windows PC (or a VPS).
  Idle = no MetaTrader import, no MT5 terminal, no engine. Holds the RSA key and DPAPI-encrypted MT5 credentials.
* **worker/engine_main.py** – the trading engine process (Trader + official `MetaTrader5` package). Spawned on
  CONNECT/START, shut down (with the bot's own terminal) after STOP. Exclusive lock: only one engine can exist.
* **backend/** – aiohttp server: password-protected UI, websocket relay, wake signal, bootstrap download.
* **bootstrap/install.ps1** – one-shot Windows installer (Python, portable MT5 terminal, packages, launcher,
  Task Scheduler supervisor + watchdog, optional auto-logon). Served at `/bootstrap/install.ps1`.
* **tests/** – unit tests, SimBroker test double, synthetic replay, launcher/engine-process lifecycle e2e tests.

## On-demand operation (START/STOP control everything)
| Button | What happens |
|---|---|
| CONNECT MT5 | Launcher decrypts + stores creds (DPAPI), spawns the engine, which starts the bot's MT5 terminal, logs in and validates (DEMO, algo trading, symbol specs). If START doesn't follow within `RB_STANDBY_SECONDS` (600) the engine and terminal are shut down again. |
| START | Spawns engine + terminal if needed, fresh login, re-reads account/positions/orders, re-adopts bot positions by magic, discards all old tick history and warms up on live ticks only (60 s) before the first entry. Idempotent (second START = "already running"). Desired state is persisted so a crashed engine / rebooted PC resumes with a fresh START. |
| STOP | Blocks entries, cancels bot pendings, closes bot positions and verifies, then shuts the engine down and kills the bot's own `terminal64.exe` (matched by its exact path – your own MT5 is never touched). If closing can't be verified the engine stays alive and keeps retrying; if MT5 is unreachable it gives up after 5 min (positions keep server-side SL/TP). Manual/other-EA trades are never touched. |
| (nothing pressed) | Only the launcher runs (measured on Linux: ~0.03% CPU, ~50 MB RSS, 0 child processes). |

Commands are never queued: the backend forwards a command only to a connected launcher (waking it first, up to
30 s) and the launcher drops commands older than 25 s or with a repeated id.

**Render usage:** free web services sleep after 15 min without inbound HTTP/WebSocket traffic. The launcher keeps
its websocket only while the app is open (plus `RB_LINGER_SECONDS`=300) or a command/START/STOP is in progress,
then disconnects. Opening the app wakes Render (cold start ≈ 1 min on free tier), the backend publishes a
content-free wake to the ntfy topic and the launcher reconnects in < 1 s. Trading continues while the app is
closed – the engine doesn't need the backend. Set `RB_WAKE_URL=off` (launcher) / `WAKE_URL=off` (backend) to
stay connected permanently (Render then never sleeps). `RB_FALLBACK_POLL_MINUTES` adds a periodic check-in if
ntfy.sh is unreachable (each check-in keeps Render awake ~15 min).

**Hosting cost:** the launcher runs on your own Windows 10/11 PC at $0 (PC must be on and awake while trading;
the installer disables sleep on AC). GitHub Actions Windows runners are **not** an option (6 h job limit,
ephemeral, and GitHub's terms restrict hosted runners to building/testing/deploying the repo's software).
Oracle Cloud Always Free and Google Cloud free tiers have no free Windows. AWS/Azure free trials can run a small
Windows VM for a limited period with a card on file (1 GB RAM is tight for MT5) – the same installer works
there; nothing in this repo buys or starts cloud resources.

## Trading engine (worker/rapidburst/engine.py)
Per symbol, on every new tick (pulled with `copy_ticks_from`), normalised by a short-term volatility unit:
displacement (3 s / 15 s), velocity and acceleration, tick-direction + magnitude pressure, micro-breakout of
the 20 s structure, compression→expansion, exhaustion (z-score vs 60 s mean with deceleration),
pullback-and-renewal inside a 30 s trend, spread/volatility/tick-rate quality. These blend into one score
in [-1, 1] (no RSI, no fixed confirmation chain); BUY/SELL when |score| ≥ 0.60 and a real one-sided impulse
(≥ 3 vol units with pressure) is present, HOLD otherwise. Extended/stalling moves are not chased.

Exits (checked every loop, ~150 ms): profit lock with give-back from peak, momentum weakening while in
profit, exhaustion take-profit, reversal score, time stop, setup invalid; plus a mandatory server-side SL
(and a wide backstop TP) on every order.

## Risk & execution (risk.py, trader.py, broker_mt5.py)
* Size from SL distance: `risk_per_trade_pct` (0.25 % equity) via `trade_tick_value_loss/trade_tick_size`,
  floored to `volume_step`, clamped to `volume_min/max` and `max_lot_per_order`.
* `order_check` before every `order_send`; filling mode from `symbol_info.filling_mode` (FOK→IOC→RETURN);
  `deviation` slippage cap; requote retry with fresh price; timeouts verified on the server before any retry.
* Max positions (4 total / 2 per symbol), max total lots, max open SL risk (1 % equity), margin checks.
* Adds only when every existing leg is in profit and momentum is stronger (no martingale/averaging/grid),
  no hedging, cooldowns, entries/minute cap, loss-streak pause, in-flight lock (duplicate prevention).
* Daily loss limit (3 % of day-start balance, bot P/L) and equity drawdown stop (5 % from day peak) → halt +
  flatten bot positions for the rest of the broker day.
* Only touches orders/positions with its magic number (26112201). Manual trades and other EAs are ignored;
  on netting accounts it will not trade a symbol that already has someone else's position.
* STOP: blocks entries, cancels bot pending orders, closes bot positions and verifies each close; retries
  each loop if any close could not be verified.
* Recovery: state in `data/state.json`; after a crash/reboot the worker reconnects, re-adopts bot positions by
  magic, and resumes only if it was running and validation passes. Server-side SL protects positions while
  disconnected. Watchdog restarts a stalled process.
* LIVE accounts are refused unless the worker machine sets `RB_ALLOW_LIVE=I_UNDERSTAND_LIVE_RISK`
  (never from the UI, never automatic).

All parameters: `worker/rapidburst/config.py` (override with a JSON file via `RB_CONFIG_FILE`).
Defaults are conservative and **not yet calibrated on real ticks** – run on DEMO first.

## Security
* UI: access password (`UI_PASSWORD` env on Render) → HMAC-signed HttpOnly/Secure/SameSite=Strict cookie,
  login rate limit, CSRF header, strict CSP.
* Worker ↔ backend: `WORKER_SECRET` bearer token over TLS (env on Render + `worker.env` on the VPS, ACL’d).
* MT5 credentials: encrypted in the browser with the worker's RSA-3072 public key (RSA-OAEP-SHA256); the
  backend only relays ciphertext and never logs command arguments. Stored by the launcher with Windows DPAPI and handed to the engine process over a local pipe (never argv/env).

## Run tests
```
python -m venv .venv && . .venv/bin/activate && pip install aiohttp cryptography pytest
python -m pytest -q tests
python -m tests.replay --seeds 1 2 3 --hours 2     # SYNTHETIC replay, not market evidence
```

## Install the launcher (your Windows PC or a Windows VPS, elevated PowerShell)
```
& ([scriptblock]::Create((irm https://<backend>/bootstrap/install.ps1))) -WorkerSecret '<WORKER_SECRET>' [-AutoLogonPassword '<windows pw>']
```
Then open the backend URL on the iPhone → unlock → CONNECT MT5 (demo login/password/server) → select
symbols → START. STOP closes bot positions and shuts the engine + bot terminal down.
