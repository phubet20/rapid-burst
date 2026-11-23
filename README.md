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

## Trading engine: ONE Ultra-Fast Pre-Burst Short Scalping Engine (worker/rapidburst/microburst.py)

There is exactly one signal/entry/exit system (the earlier Rapid Micro-Burst engine refactored in place;
old momentum score, impulse detectors, basket/scalp modes, persistence timers, later adds and fixed TPs are
gone). No RSI or lagging oscillator, no candle-close wait, no fixed steps, no multi-step confirmation.

Loop (every ~0.1 s, per selected symbol): new ticks -> three scanners (updated per tick) -> ONE decision ->
risk & execution checks -> one burst batch -> basket managed tick by tick -> verified exit.

**Three scanners on the same tick stream, incremental O(1) per tick** (rolling sums + monotonic deques;
no rescans of long history per tick):
* **A, tick velocity & acceleration**: tick rate vs baseline, velocity/acceleration in M1-ATR units,
  same-direction tick runs, directional efficiency, short burst size, early acceleration, exhaustion
  (extended move that is decelerating);
* **B, activity & candle pressure**: tick activity (= number of quotes, **not** exchange volume), activity
  expansion counted only when price responds, up/down-tick and bid/ask **proxies**, live M1 body expansion,
  body/range, wick rejection, consecutive pressure, absorption (activity without price response);
* **C, micro-structure pre-burst trigger**: micro high/low, compression -> expansion, micro breakout, failed
  breakout, structure break, pullback recovery, short rebound, anti-chase distance from the swing.
Market depth is not used (no reliable book from TickerAll / the MT5 bridge).

**Decision**: BUY and SELL strength (regime-weighted A/B/C) vs an adaptive threshold (0.55 base; higher in
quiet/chop, wide spread or bad slippage; lower in fast markets). A side trades only when strength >=
threshold, it beats the other side, **scanner A >= 0.35 and scanner B or C >= 0.35** (no speed-only and no
volume-only entries), the feed is stable and there is no veto (anti-chase, exhaustion, candle already
travelled, spread vs target). A move that is just starting (compressed, velocity and acceleration aligned,
little travelled) gets a small bonus. Otherwise NO TRADE. Trend is display-only context.

**Burst batch entry** (one batch per signal): 1 order, +1 per 0.08 of strength above the threshold, max 3
(1 in quiet/chop and during safe-mode probing); every order the same lot; batch risk <= 1 % of equity
(per order min(0.5 %, 1 %/n)), batch lots <= 1.0, all global caps still apply. Spread, slippage and
margin are checked before each order; the remaining orders are cancelled if price has run more than half
the target past the decision price, moved against it, or the spread widened. Each fill is verified.
The basket is never added to later: no martingale, no averaging down, no size-up after a loss.

**Basket exit** (net P/L after spread and commission, tick by tick):
1. hard risk first (always wins): safe mode, broker-stop / locked-stop mirror, hard stop mirror, basket loss
   cap, exposure breach, emergency (no fresh price);
2. short profit capture: target = max(0.20 x M1 ATR, 2 x spread); at 2x target close at once; at target
   close unless momentum is still continuing (velocity on, no decay, no opposing pressure, no exhaustion);
3. give-back protection once the peak reached 0.6x target: 40 % give-back, or 60 % while momentum continues
   (so small pullbacks inside a running move are not closed);
4. exhaustion / tick reversal in profit, failed breakout, structure break, opposite strength, no
   follow-through within ~4 s, momentum against, stale after 120 s.
Each close is verified on the broker; a verified-closed ticket is never closed or adopted again and only the
bot's own live tickets of that basket (same symbol/side/magic) are touched.

**Latency** is measured on real clocks for every order and shown in the UI (p50/p90): tick receipt ->
decision, decision -> send, send -> broker ack, send -> fill verified, tick -> fill, broker -> worker feed
delay (TickerAll timestamp, only when plausible) and close round trip. Limits: TickerAll WebSocket tick
frequency is set by TickerAll/broker; account/positions REST cache 1 s; fill verification polls positions
(up to 6 x 0.4 s); Render free plan latency to the broker API. Nothing is reported as faster unless measured.

Broker SL on every order: max(0.8 x M1 ATR, 4 x spread, broker stop level); no broker TP; the SL moves to
breakeven+ and trails while in profit.

## Risk & execution (risk.py, trader.py, broker_mt5.py / broker_tickerall.py)
Defaults (all in `worker/rapidburst/config.py`, overridable by env / `RB_CONFIG_JSON`):
* risk per order 0.5 % of equity (SL distance x tick value; min-lot floor for tiny accounts) –
  `RB_RISK_PER_TRADE_PCT`;
* total open risk <= 3 % of equity (`RB_MAX_TOTAL_RISK_PCT`); 1.5x breach closes the newest group;
* **margin use cap 50 % of equity** (`RB_MAX_MARGIN_USE_PCT`) and min margin level 200 % – the earlier
  100 % margin setting was replaced by this risk-based cap;
* batch: max 3 orders (`RB_MAX_ORDERS_PER_BATCH`), batch risk 1 % (`RB_RISK_PER_BATCH_PCT`), 1.0 lots per
  batch; 3 per symbol, 6 in total, 2.0 lots total, max 1.0 lot per order, exposure <= 500x equity notional;
* basket loss cap 2 % of equity (`RB_GROUP_LOSS_PCT`, never inside 3x the spread cost);
* **no daily-loss all-day stop** (removed; `RB_DAILY_LOSS_PCT` is ignored). **Equity drawdown 8 %** from the
  session peak (`RB_MAX_DD_PCT`) -> **SAFE MODE**: bot positions flattened, new entries paused 5 min
  (doubling on repeats, max 30 min, `RB_SAFE_PAUSE_S`), then entries resume from a new reference peak with
  3 single min-lot batches. The bot keeps running after START;
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
