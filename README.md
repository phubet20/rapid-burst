# Rapid Burst Auto Scalping Bot (MetaTrader 5)

Autonomous tick-based momentum scalper for MT5 (primary symbol XAUUSD, any broker symbol selectable),
operated from a single-screen iPhone web app. **DEMO only by default.**

```
 iPhone (Safari)  ──HTTPS/WSS──>  Backend + UI (Render, aiohttp)  <──outbound WSS──  Windows worker
   single screen                  auth, relay, no trading logic                       Python + MetaTrader5 pkg
                                  never sees MT5 password                             MT5 terminal (portable)
```

* **worker/** – runs on a Windows VPS/PC next to a real MT5 terminal. Official `MetaTrader5` package.
  Engine, risk, execution, STOP, recovery. Connects *outbound* to the backend → no open ports on the VPS.
* **backend/** – aiohttp server: password-protected UI, websocket relay, worker bootstrap download.
* **bootstrap/install.ps1** – one-shot Windows installer (Python, MT5 terminal, packages, worker,
  Task Scheduler supervisor + watchdog, optional auto-logon). Served at `/bootstrap/install.ps1`.
* **tests/** – unit tests, SimBroker test double, synthetic replay harness, backend↔worker e2e test.

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
  backend only relays ciphertext and never logs command arguments. Stored on the worker with Windows DPAPI.

## Run tests
```
python -m venv .venv && . .venv/bin/activate && pip install aiohttp cryptography pytest
python -m pytest -q tests
python -m tests.replay --seeds 1 2 3 --hours 2     # SYNTHETIC replay, not market evidence
```

## Install the worker (Windows VPS, elevated PowerShell)
```
& ([scriptblock]::Create((irm https://<backend>/bootstrap/install.ps1))) -WorkerSecret '<WORKER_SECRET>' [-AutoLogonPassword '<windows pw>']
```
Then open the backend URL on the iPhone → unlock → CONNECT MT5 (demo login/password/server) → select
symbols → START when all validation checks are green.
