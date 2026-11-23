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

## Burst batches, small accounts and the trend filter

**M1 impulse entries.** Every loop (0.15 s) the engine checks whether the current M1 bar is starting
to surge, from real tick order flow: 3 s velocity vs the M1 ATR (from broker M1 bars), up-ticks vs
down-ticks over 5 s, the bar's expansion vs ATR, the move vs the spread, not decelerating and not
already exhausted. On an impulse it enters at once in the surge direction (1 order; 2 on a very
strong impulse when the equity tier allows). The main trend is a bias only: a counter-trend impulse
needs strength >= 0.70 and opens a single order. The older momentum score remains as a fallback.

**Broker SL on every order (default, `RB_BROKER_SL=1`).** Initial SL = max(broker stop level,
2 x spread, 0.5 x M1 ATR). When the trade runs in our favour the SL is moved to breakeven+
(open + 0.3 x spread) and then trailed (max(0.25 x ATR, 1 x spread)) on the broker via TickerAll
`PATCH /positions/{ticket}` (at most once per second per position; never backwards). A software
mirror of every level is kept, so if the broker refuses a modify the engine still closes at that
level. Wrong way: no follow-through within 6 s, or 0.6 x SL against with momentum against -> cut
at once. Right way: the trailing SL governs (early profit exits are suppressed once the SL is
locked); a short take-profit at 0.6 x ATR closes the burst, and the next impulse re-opens.
Pyramiding adds one order at a time only while every open order's SL is locked in profit, never
against the trend, at most N adds per burst (N = equity tier) and within the notional cap.
No martingale: after a losing burst the lot can never be larger until a winning burst.
Software backup stop at 1.3 x the SL distance and the per-burst loss cap stay active.

**Daily-loss / drawdown halts: OFF by default (user choice).** Re-enable with
`RB_DAILY_LOSS_PCT=20` / `RB_MAX_DD_PCT=30` on the server. Margin check, notional cap,
per-order SL and per-burst loss caps always stay on.

**Burst batch.** On a strong momentum score the engine opens 2-5 same-direction market
orders that share a batch id (count scales with |score| from `entry_threshold` to
`batch_full_score`; every leg is re-checked by the risk guard and the batch is cut short at
the first refusal). Every leg has its own server-side SL. The whole batch is closed together on:
batch target (`batch_tp_units` vol units, measured at the executable bid/ask so the spread is
already paid), momentum fade, reversal, or time stop (`batch_max_hold_seconds`). After a batch is
flat there is a `batch_cooldown_seconds` pause. No martingale, no grid, never adds to a losing
position, never opens against an open batch. Lots compound with equity through `risk_per_trade_pct`.

**Any equity, no minimum balance.** When the risk-based lot is below the broker minimum the bot
trades the broker minimum lot instead of skipping on risk % (the engine hard-close, batch loss cap
and the 20% daily-loss / 30% drawdown kill-switch still bound the loss; optional
`small_account_max_loss_pct` > 0 re-adds a per-order cap). The only skip is **insufficient free
margin for the minimum lot** (the broker would reject it) - shown clearly in the app. Orders per
batch scale with equity (`batch_tiers`, default below $5: 1, $5-20: 2, $20-50: 3, $50-200: 4, above:
`batch_max`; pyramiding may add one more into a winning, trend-aligned batch). Lots grow
automatically with equity through `risk_per_trade_pct`.
Example (Exness BTCUSDm, $10): spread ~$6.4, hard-close ~$20-30 -> 0.01 lot risks $0.20-0.30 (2-3%).

**Trend filter.** Every 60 s the engine fetches real M1 (45 min) and M5 (90 min) bars from the
broker (TickerAll `GET /v1/accounts/{id}/candles`, or MT5 `copy_rates_from_pos`) and computes
trend direction + strength from M1 regression slope, M1 EMA(8/21) gap and M5 slope, each
normalised by the average bar range. With-trend (or flat) signals open a full batch; a
counter-trend signal needs |score| >= `countertrend_min_score` (0.85) and opens a single order
at half risk; adding to an open batch (pyramiding) happens only when the trend is aligned and every
leg is in profit. If no bars are available the trend shows "unknown" and is treated as neutral.

**Why no trade?** Each symbol keeps counters of skip reasons (sampled once per second: warming
up, no momentum edge, spread filter, sizing refused, counter-trend filter, cooldown, risk cap ...),
shown in the Engine card together with the recent decisions and the active limits; a
`WHY NO TRADE (last 60s)` summary is written to the log every minute while running.

**Changing limits server-side.** Set `RB_CONFIG_JSON` on the Render service (or a JSON file via
`RB_CONFIG_FILE`), e.g. `{"small_account_max_loss_pct": 3, "batch_max": 3, "daily_loss_limit_pct": 10}`.
The UI cannot change risk limits.

## Scalp mode (DEFAULT): early entry, many small orders, exit on reversal

Replaces the basket/hold mode as the default (`RB_BASKET_ENABLED=1` still turns the basket mode on; `RB_SCALP_ENABLED=0`
returns to the older burst logic). Demo only, bot positions only, no change to any security rule.

- **Early entry** (`impulse.detect_early`): tick velocity over the last 1.5 s must be fast **and accelerating**, one-sided
  order flow (>= 0.4), a real but small move since its start (>= 1x spread), all inside the first seconds of the push.
- **Anti-chase**: refused when price is already more than `RB_SCALP_MAX_BAR_ATR` (0.6) M1-ATR from the M1 bar open, or more
  than `RB_SCALP_MAX_SWING_ATR` (0.7) ATR from the 20 s swing, or the velocity is decelerating.
- **Many small orders**: up to `RB_SCALP_MAX_ORDERS` (20) orders of the SAME lot (multiplier 1.0, no martingale, never
  averaging into a loser), at least `RB_SCALP_ADD_INTERVAL` (1 s) apart, only while a same-direction early impulse continues.
  The lot is sized so 20 orders fill the margin cap and compounds with equity. Total used margin <= `RB_MAX_MARGIN_USE_PCT`
  (default **100**) of equity from the broker's own numbers, and free margin must stay >= `RB_SCALP_MIN_FREE_MARGIN_PCT` (3%)
  of equity after each order, so orders are not rejected near stop-out.
- **Exits**: each order has a quick fixed TP (max(0.25 ATR, 2x spread)); the whole group closes the moment the bar's force
  reverses (velocity flips sign, velocity decays with opposing pressure, or a wick forms against) while in profit; a loser is
  cut when the reversal says the trade is wrong (>= 1x spread against + flip/pressure, 0.3 ATR hard cut, no profit in 5 s,
  60 s max hold). BE+/trailing stays; the broker SL is a far emergency stop (8 ATR); group loss stop 10% of equity.
- UI: *Scalp* panel (orders, floating P/L, margin used vs cap, live reversal flags). The learner keeps recording every group.

High risk: up to 100% of equity can be in margin across 20 orders; a gap through the far SL can lose a large part of the account.

## Basket mode: averaging, pyramiding, compounding (aggressive - user-accepted risk, DEMO)

Default ON (`RB_BASKET_ENABLED=0` returns to the old tight-SL burst/scalp behaviour). All orders of a symbol/direction
form ONE basket:

- **Entry**: the M1 impulse / momentum signal opens the first order. Further orders: **averaging** when price moves
  `basket_avg_atr` (1.0) M1-ATR against the last order, **pyramiding** when it moves `basket_pyr_atr` (0.6) ATR in favour
  and a same-direction impulse confirms (never against the trend). Same lot (`basket_avg_mult` = 1.0, no martingale),
  `basket_add_cooldown` 3 s apart, at most `basket_max_orders` (8).
- **Margin-use cap (70%)**: `RB_MAX_MARGIN_USE_PCT=70` - an order is refused when total used margin would exceed 70% of
  equity (the lot is shrunk to fit first); adds also stop while margin level < 150%. This caps order count and size.
- **Compounding**: the base lot follows the balance (first order loses `basket_risk_pct`=3% of balance at its broker SL,
  limited so a full basket fits the margin cap); it grows as the balance grows.
- **Exits** (whole basket together): closes at the profit target (`basket_target_pct` 2% of equity at basket start,
  never below 3x the spread cost), then the **profit is trailed** (closes when it gives back 30% of its peak, floor 50% of
  target). **Basket loss stop** `RB_BASKET_MAX_LOSS_PCT=30` (% of equity at basket start) - NOT 70% (70% is margin use).
  No wrong-way cut, no tight initial SL, no per-order BE/trailing; each order has only a far broker SL
  (`basket_sl_atr` 10 x M1 ATR) as a disaster stop.
- The learner still records each closed basket (cold-start min-lot sizing no longer applies to baskets; a losing bucket
  still blocks new baskets). UI: *Baskets* panel (floating P/L, target, trail floor, loss stop, margin used vs cap).

This is high risk: averaging into a trend can reach the 30% loss stop or the far broker SL.

## Adaptive learning from real closed trades (learner.py)

* Every fully closed bot burst is written to a journal (`learning.json` in the engine data dir):
  symbol, side, signal type (impulse / momentum), component scores (score, impulse strength, tick
  pressure, velocity, breakout, trend), trend relation, spread/ATR regime (tight / mid / wide), UTC
  hour, hold time, exit reason, P/L and P/L in R (realised P/L / risk at open).
* Bucket = signal type x trend relation x spread/ATR regime. Stats are time-decayed (half-life
  `learn_half_life_hours`, 36 h) and shrunk toward a slightly negative prior, outliers clipped to +/-3R.
* Cold start (fewer than `learn_min_samples`, 8, trades in a bucket): MIN LOT only and a 40% faster
  wrong-way cut. Losing buckets (shrunk expectancy < -0.15R) are skipped via a Thompson-style gate and
  come back to exploration as old results decay. Only buckets whose expectancy is positive beyond one
  standard error get normal risk sizing x a multiplier (1..`learn_max_mult`, 2). The no-martingale
  cap still applies after a losing burst.
* UI: "Learning" panel with trades learned, best/worst buckets (n, win rate, expectancy) and the
  current gate / size multiplier per symbol. `RB_LEARNING_ENABLED=false` turns it off.
* Persistence limit: Render free has no persistent disk and TickerAll history carries no order
  comment / magic, so the journal survives STOP/START and engine restarts but is reset by a redeploy
  or a Render instance restart (it then starts cold again at min lot). No guarantee of profit or of a
  smooth equity curve: the learner only re-weights what real trades have shown so far.

## Speed settings (second-by-second entries)

Warm-up 2.5 s / 4 ticks (vol scale seeded from TickerAll M1 candles, tick rate measured over the real
span), entry cooldown 1.5 s, after a loss 2 s, after a closed batch 2 s, up to 30 entries/minute,
entry threshold 0.42, loop every 0.1 s. Entries are evaluated on every loop. The slippage pause only
counts fills of the last 90 s (it used to stick until the next fill), and a losing streak pauses a
symbol for 20 s (was 10 min). Kept: broker SL at open, BE+/trail, fast wrong-way cut, learner
cold start at min lot, no martingale, idempotency. A PRICE MISMATCH warning is logged when the engine's
tick price differs from the broker's own position price by more than 4x spread.

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
