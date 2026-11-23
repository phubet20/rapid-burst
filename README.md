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
| START | Spawns engine + terminal if needed, fresh login, re-reads account/positions/orders, re-adopts bot positions by magic, discards all old tick history and warms up on live ticks only (about 2.5 s / 4 ticks, scale seeded from broker M1 candles) before the first entry. Idempotent (second START = "already running"). Desired state is persisted so a crashed engine / rebooted PC resumes with a fresh START. |
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

## Trading engine: ONE Rapid Micro-Burst Scalping Engine (worker/rapidburst/microburst.py)

There is exactly one signal/entry/exit system. The old momentum score, the M1 impulse detectors, the
burst-batch / basket / scalp modes, their fixed TPs and separate exit stacks were removed (no RSI, no
lagging oscillator, no fixed confirmation chain, no waiting for the candle close).

Loop (every ~0.1 s, per selected symbol): real ticks -> analysis -> BUY/SELL strength -> validation & risk
-> entry -> tick-by-tick management -> exit -> position verification -> next scan.

**Analysis from real ticks** (bid/ask/time only):
* A. tick flow: tick rate vs its 60 s baseline, same-direction tick runs/consistency, velocity and
  acceleration (in M1-ATR units), directional efficiency, short burst size vs the typical move;
* B. tick activity = number of quotes the broker sent. It is **not** exchange volume and is never shown or
  used as "real volume"; absorption = activity up while price does not move;
* C. live (unfinished) M1 candle: body/range, wicks, close location, range expansion vs previous broker
  candles, consecutive candles, abnormal bars, wick rejection;
* D. micro structure (20 s): breakout, failed breakout, structure break, compression -> expansion;
* E. pullback depth, continuation, weak return and **anti-chase** (distance from the recent swing, live
  candle body already travelled);
* F. pressure **proxies**: up- vs down-ticks and bid/ask up vs down changes. Market depth is not used:
  neither TickerAll nor the MT5 bridge gives a reliable book, the UI says "n/a";
* G. context: spread vs its median, spread vs the capture target, slippage history, tick latency and
  gaps (feed stability), regime = fast / normal / quiet / chop.

**Strength**: each signal family is scored 0..1 for BUY and for SELL, weighted by regime, minus penalties
(chasing, opposite wick, exhaustion). A side trades only when its strength >= an adaptive threshold
(0.55 base; higher in quiet/chop markets, with a wide spread or bad slippage; lower in fast markets),
beats the other side by 0.12, has >= 3 independent supporting families, live velocity in that direction,
a stable feed and no veto. Big-bar-only, volume-only or candle-only entries are impossible by design.
Trend (M1/M5 from broker bars) is shown as context only.

**Entry** (inside the bar, no candle-close wait): side chosen automatically; anti-chase (> 0.9 ATR from the
swing or a live candle body > 1.0 ATR in that direction = no entry; 1.3x in a fast regime);
duplicate prevention (the same burst, identified by its swing origin, and the same tick are never traded
twice); spread / tick-rate / slippage / execution-failure gates; timing by speed (fast and normal markets
enter at once, slow markets need 0.4 s of persistence). More orders in the same group only when
justified: same side, strength >= threshold + 0.10, group already in net profit, not in quiet/chop, spaced
by market speed, max 3 orders per group, **each add the same lot as the first order** (no martingale, no
averaging down, no grid).

**Exit** (one system, evaluated tick by tick, never slowed by confirmations):
1. hard risk first (always wins): risk halt (daily loss / drawdown), broker-stop / locked-stop mirror,
   hard stop mirror, group loss cap, exposure breach, emergency (no fresh price), execution failure;
2. profit capture net of costs: target = max(0.20 x M1 ATR, 2 x spread) (min 0.3 x spread net);
   take it at 2x target at once, at target on velocity decay / tick flip / opposing pressure / stall,
   or on a 40 % give-back from the peak;
3. micro-reversal: failed breakout, micro structure break, wick rejection + tick flip, opposite strength
   above the threshold;
4. no follow-through within ~4 s (scaled by regime), momentum against beyond half the stop, stale after 120 s
   without net profit. The whole group closes together.

Broker SL on every order: max(0.8 x M1 ATR, 4 x spread, broker stop level); no broker TP (exits are
engine-driven); the SL moves to breakeven+ and trails (max(0.15 x ATR, 1 x spread)) while in profit.

## Risk & execution (risk.py, trader.py, broker_mt5.py / broker_tickerall.py)
Defaults (all in `worker/rapidburst/config.py`, overridable by env / `RB_CONFIG_JSON`):
* risk per order 0.5 % of equity (SL distance x tick value; min-lot floor for tiny accounts) –
  `RB_RISK_PER_TRADE_PCT`;
* total open risk <= 3 % of equity (`RB_MAX_TOTAL_RISK_PCT`); 1.5x breach closes the newest group;
* **margin use cap 50 % of equity** (`RB_MAX_MARGIN_USE_PCT`) and min margin level 200 % – the earlier
  100 % margin setting was replaced by this risk-based cap;
* max 3 orders per group (`RB_MAX_ORDERS_PER_GROUP`), 3 per symbol, 6 in total, 2.0 lots total, max
  1.0 lot per order, exposure <= 500x equity notional;
* group loss cap 2 % of equity (`RB_GROUP_LOSS_PCT`, never inside 3x the spread cost);
* **daily loss limit 5 %** (`RB_DAILY_LOSS_PCT`) and **equity drawdown stop 8 %** from the day peak
  (`RB_MAX_DD_PCT`) -> halt + flatten bot positions for the rest of the day (0 disables – not
  recommended);
* execution protection: 3 rejected opens within 60 s pause the symbol 60 s; 5 failed close attempts halt new entries;
  15 s without a fresh price = emergency exit (REST quote used first when the stream lags);
* pacing: 1 s cooldown (2 s after a loss), 30 entries/min, 4 losses in a row pause the symbol 30 s,
  never size up after a losing group.
* `order_check` before every send where the broker supports it; filling mode from the symbol; deviation
  cap; timeouts verified on the server before any retry; only the bot's magic number (26112201) is ever
  touched – manual trades and other EAs are ignored; netting accounts are not traded on a symbol that
  already has someone else's position.
* STOP blocks entries and closes every bot position, verifying each close (retried each loop).
* Recovery: bot positions are re-adopted by magic after a restart and managed by the same engine; trading
  only resumes on an explicit START. LIVE accounts are refused unless the worker sets
  `RB_ALLOW_LIVE=I_UNDERSTAND_LIVE_RISK` (never from the UI, never automatic).

Defaults are a starting point, **not calibrated on enough real ticks** – run on DEMO first. No profit
guarantee.

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

## No-computer mode: cloud engine on Render + TickerAll (free, DEMO only)

If you have no Windows PC/VPS, the same launcher + engine run **beside the backend on
Render** and reach MetaTrader 5 through [TickerAll](https://tickerall.com) (a hosted MT5
API; free plan = demo accounts only, no card). Nothing in the engine, risk rules,
DEMO gate or START/STOP lifecycle changes - only the broker adapter
(`worker/rapidburst/broker_tickerall.py`) is different.

Setup (phone only):
1. Open a free MT5 **demo** account with your broker (or use an existing demo login).
2. Sign up at tickerall.com (Free plan) and create an API key (`cf_api_...`).
3. In Render -> rapid-burst -> Environment add `TICKERALL_API_KEY` = that key
   (`ENGINE_MODE=cloud` is already set). Save; Render redeploys.
4. Open the app, CONNECT MT5 (login / password / server), then START.

How it behaves:
* CONNECT/START: the engine opens a TickerAll session (`POST /v1/sessions`), re-reads
  account/positions/orders, subscribes to the tick WebSocket (which also keeps the
  TickerAll account from cooling) and warms up on fresh ticks only.
* STOP: entries blocked, bot pending orders cancelled, bot positions closed and verified,
  then the engine exits and the TickerAll session is released (`DELETE /v1/sessions/{id}`).
* Bot trades are identified by the comment prefix `RBurst` (TickerAll cannot set an MT5
  magic number) plus a ticket registry; manual trades and other EAs are never touched.
* Keep-awake: while STARTED (or holding bot positions) the server requests its own
  `/healthz` every 4 minutes so Render's free instance does not sleep; after STOP it
  stops and Render sleeps after ~15 idle minutes.
* Live accounts are refused (TickerAll free: `FREE_TIER_LIVE_REJECTED`; engine: DEMO gate).

Limits to know (honest):
* Render free = 750 instance-hours per month per workspace, shared by all free services
  (e.g. `grokbot-trader`). Running 24h/day uses ~720 h/month on its own; if the total
  runs out, Render suspends free services until the next month.
* Render restarts (deploys, crashes, maintenance) wipe memory: the MT5 password is kept
  in memory only, so press CONNECT MT5 again after a restart (or set `MT5_LOGIN`,
  `MT5_PASSWORD`, `MT5_SERVER` on Render for automatic reconnects). Bot positions keep
  their broker-side SL/TP meanwhile. The daily-P/L history of trades closed before a
  restart is recovered only for positions whose tickets the adapter saw.
* TickerAll holds the MT5 password in its memory only; after its weekly maintenance
  (Sunday 06:00-10:00 UTC) the adapter re-opens the session with the in-memory password.
* Execution goes phone -> Render (Singapore) -> TickerAll -> broker, so latency is higher
  than a local terminal. Margin is an estimate (contract x lots / leverage) and only when
  the symbol's margin/profit currency is the account currency; otherwise no entry.
* Windows launcher mode still works: unset `TICKERALL_API_KEY` and set `ENGINE_MODE=launcher`.

## Trading any asset (BTC, gold, FX, indices ...)

* No symbol is preselected. After CONNECT MT5, **Symbols** lists the broker's full catalogue
  (search by name, class or alias: `btc`, `bitcoin`, `crypto`, `gold`, `fx`; empty search = all;
  "Load more" pages through it). Tick up to 6 and press APPLY.
* Per symbol the app shows: tradable (spec known, tick value known/derivable, volume limits) and
  quoting (a fresh live tick in the last 60 s). A closed/not-quoting symbol is marked
  "no quotes / market closed" and skipped; it resumes by itself when quotes arrive.
  START needs at least one tradable symbol that is quoting now.
* Parameters are symbol-agnostic: volatility unit from the symbol's own ticks, spread cap in basis
  points of price (`max_spread_bps`), stop/take-profit from volatility, spread and stops level,
  size from the account-currency tick value (`tickValueAccount`; derived from contract x tick only
  when the quote or base currency is the account currency, otherwise the symbol is refused with
  "tick value unknown"), volume min/step/max, slippage in volatility units, per-symbol position
  limits and a total notional cap (`max_total_notional_x`).

## Adaptive learning from real closed trades (learner.py)

* Every fully closed group is journaled (`learning.json`): symbol, side, regime, strength components,
  spread/ATR regime, UTC hour, hold time, exit reason, P/L and P/L in R (realised / risk at open).
* Bucket = `micro | market regime | spread-ATR regime`. Stats are time-decayed (half-life 36 h) and shrunk
  toward a slightly negative prior; outliers clipped to +/-3R.
* Cold start (< 8 trades in a bucket): min lot only. Losing buckets are skipped (with the reason shown) and
  re-explored as old results decay; only buckets with proven positive expectancy may size up, capped at
  1.5x and never above the risk caps. `RB_LEARNING_ENABLED=false` turns it off.
* Render free has no persistent disk: the journal survives STOP/START but a redeploy resets it.

## Path to a LIVE account (not enabled)

LIVE stays disabled. Two independent gates must both be opened deliberately:
1. **Price/execution path.** TickerAll Free refuses live accounts (`FREE_TIER_LIVE_REJECTED`).
   Either upgrade to a paid TickerAll tier that allows live accounts, or run the Windows
   launcher with a real MT5 terminal (see "Install the launcher").
2. **Engine gate.** The engine refuses non-DEMO accounts unless the machine running it sets
   `RB_ALLOW_LIVE=I_UNDERSTAND_LIVE_RISK` (environment variable on Render or the Windows PC). It
   can never be enabled from the web UI.
Test on DEMO first, then reduce `small_account_max_loss_pct` / `batch_max` for real money.

## Install the launcher (your Windows PC or a Windows VPS, elevated PowerShell)
```
& ([scriptblock]::Create((irm https://<backend>/bootstrap/install.ps1)))
```
The script prompts for the same password you use to unlock the app, enrolls the PC over HTTPS
(`/api/worker/enroll`, rate-limited like login) and writes WORKER_SECRET to `worker.env` itself.
`-WorkerSecret` is still accepted if you already have it. Then open the backend URL on the iPhone
→ unlock → CONNECT MT5 (demo login/password/server) → select symbols → START. STOP closes bot
positions and shuts the engine + bot terminal down.
