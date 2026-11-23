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

## t30u: M1 Rapid Basket Scalper (current engine, refactored in place)

Owner request (t30u): faster, M1 only, continuous baskets, no risk-amount limits. Changes on top of the engine
described below:
* **M1 only.** No M5 polling or gating. Family E is the **M1 main trend**: EMA trend + **HH/HL vs LH/LL swing
  structure** (pivot highs/lows of the last 30 M1 bars) + MACD + RSI. Trade with the trend; counter-trend only
  right after a confirmed structure break (price beyond the last M1 swing, or a micro structure break). Broker
  M1 candles (2 h) are loaded at START and refreshed every 8 s; decisions every loop (~0.1 s).
* **Fewer slow filters** (from the real "why no trade" counts: no live force 1045, exhaustion 205, anti-chase
  161, risk cap 72 in 17 min): exhaustion is a score penalty (no veto), anti-chase 1.8 ATR / bar 2.2 ATR,
  minimum live velocity 0.15, loss-streak threshold raise 0, batch run-cancel 1.0 x target.
* **Baskets of 2-8 orders** (never a single order unless the risk budget cannot hold two min lots), count from
  the score edge and the learner. **Pyramid ADD** (same lot, max 8 legs, <= 2 per step) only when the basket's
  stop is already at break-even/trailing, every leg is in profit, price pulled back >= 0.12 ATR from its best and
  is resuming with a valid signal for that side. Never averaging down, no martingale.
* **Re-entry**: unlimited cycles per M1 bar; a basket closed at break-even / by the trail never blocks the same
  signal (only a real losing basket does).
* **Exits**: spike exit - after a peak >= 0.5 ATR the basket closes when the bar turns back (velocity flip +
  opposing tick pressure or a wick against + >= 30 % retrace). Pullbacks inside the M1 trend (no structure
  failure, < 60 % of the peak) are held; looser trail 0.65 / 0.40 / 0.22 x ATR, >= 2 x spread.
* **Risk (AGGRESSIVE, owner request, DEMO only)**: basket stop risk <= 50 % of equity x score scale (0.6-1.0),
  all open baskets (all symbols) <= 100 %, used margin <= 90 % of equity, lots compound with equity, safe mode only
  at a 50 % drawdown, basket loss backup cap 60 %. Hard broker SL on every order, emergency exit, execution and
  duplicate protection stay. All are runtime-tunable (`config.TUNABLE`).
* **Multi-asset**: every selected symbol (UI: up to 6) trades at the same time, sharing margin and the total cap.
* **Learning never resets**: setup = symbol | side vs M1 trend (+swing) | tape | spread regime; no time decay;
  proven setups get more orders/size (x up to 2), losing setups are mostly skipped (Thompson re-exploration at
  min lot). Memory is stored locally AND in a Render Key Value instance (`RB_LEARN_REDIS_URL`, internal URL
  only, never logged) so it survives redeploys; the newer copy (more trades) wins on start.
* MT5 app indicators: TickerAll exposes candles/ticks/orders only - indicators added inside the MT5 app cannot
  be read; any named indicator can be computed from the broker candles instead.

## Trading engine (t29u base): Immediate Continuous Score Momentum Scalper (microburst.py + mtf.py + trader.py)

One engine, refactored in place (Rapid Micro-Burst -> Pre-Burst -> Continuous Directional -> Fast Candle
Momentum Batch Scalper -> this). START evaluates after ~1 s / 3 live ticks (warm-up reduced from 2.5 s).
 Loop
(~0.1 s, per selected symbol): new ticks -> scanners (updated per tick) -> ONE decision -> risk & execution
checks -> one batch -> basket managed tick by tick (momentum hold, break-even, dynamic trailing) -> verified
exit -> immediate re-evaluation.

**Inputs, all read on the FORMING bar (no candle-close wait, no RSI/MACD cross wait):**
* **M5 direction (E)** and **M1 intrabar trigger (D)** from REAL broker candles (TickerAll `GET /candles`,
  bid OHLC + tickVolume; MT5 `copy_rates`), polled M1 every 20 s / M5 every 60 s in a background thread,
  the forming bar built from live ticks between polls. EMA(8/21), MACD(12,26,9) and **RSI(14, Wilder)** are
  incremental (advance once per closed bar, O(1) peek for the forming bar).
  * E, M5 bias (-1..1): EMA trend, body/range expansion, MACD direction + histogram + momentum change, RSI
    level, tick-volume, HH/HL vs LH/LL, supported vs exhausting. A filter, not a gate: ambiguous or weak
    opposite M5 only lowers the score; only a clear, supported opposite M5 vetoes.
  * D, M1 trigger (0..1 per side): forming-bar direction and body expansion, MACD histogram expansion and
    slope change, **RSI as a soft feature** (level vs 50, slope, momentum change; damped when stretched and
    turning back), tick-volume expansion (relative volume), continuation in the M5 direction.
* **A, price velocity & acceleration** (ticks): tick rate, velocity/acceleration in ATR, runs, directional
  efficiency, early acceleration, exhaustion.
* **B, activity & candle pressure**: tick activity (= number of quotes, **not** exchange volume) counted
  only when price responds, up/down-tick and bid/ask **proxies**, body/range, wick rejection, absorption.
* **C, micro-structure**: micro high/low, compression -> expansion, breakout, failed breakout, structure
  break, pullback recovery, rebound / re-acceleration, anti-chase distance from the swing.
Tick volume is compared like-for-like and never called exchange volume. Market depth is not used.

**Decision = two independent scores**: every loop BUY and SELL each get a strength (regime-weighted mean of
A/B/C/D/E; families without data drop out) and are checked on their OWN evidence: strength >= adaptive
threshold (0.55 base; higher in quiet/chop, wide spread, bad slippage), >= 0.08 edge over the other side,
>= 2 families agree (was 3) and one of them is C (micro-structure) or D (M1 intrabar trigger), and no veto
(anti-chase, exhaustion, candle already travelled, spread vs expected move, M5 clearly against). A weak or
vetoed side never implies the opposite side and never blocks it. A+B (speed + activity) or B+E alone can
never enter: no single mandatory indicator, no speed-only or volume-only entry. The score also sets the
order count and the risk (below).

**Continuous intrabar entry / re-entry**: unlimited entry/exit cycles per M1 bar. There is NO cooldown, NO
candle lock, NO one-trade-per-candle rule and NO re-entry delay. A closed basket is booked in the same loop
(`_adopt_positions` right after the verified close) and the next loop (~0.1 s) re-evaluates; a new basket can
open in the same bar. Only these are blocked: an order in flight (in-flight lock), the exact same signal
(same side + same swing origin + same trigger level, or the same tick), a setup the broker just rejected,
and an unverified execution (the engine stops). A losing streak raises the threshold (+0.03 per losing
basket, max +0.09) - never the size. 30 baskets/min is only an API guard. Re-entry delay and entries per
M1 bar are measured and shown.

**Batch entry (max 8 orders)**: q = (strength - threshold) / (1 - threshold); n = 1 + round(7 q), capped at 8
(2 in quiet/chop, 1 during safe-mode probing); risk scale 0.6 + 0.4 q; then cut by the batch risk budget
(1 % of equity x scale, per order min(0.5 %, budget/n)), batch lots <= 1.0, total open risk <= 3 %, margin use <= 50 %, exposure, positions per
symbol (8) and in total (10) - every order is validated up front including the batch's own orders. Same lot
per order; orders sent in parallel (one round trip), each fill verified; never added to later (no
martingale, no averaging down, no adding to a losing basket, no size-up after a loss).
*Tiny accounts (min-lot floor):* on a ~$12 account the broker minimum lot (0.01) already risks more than
0.5 %/order (BTCUSDm ~0.44-0.51 USD = ~4 % per stop, XAUUSDm ~1.4 USD = ~12 %). Such accounts may open
**several min-lot orders** as long as the stop risk of ALL open bot orders stays <= `small_account_batch_risk_pct`
(10 % of equity) x the score's risk scale, and free margin covers them. On ~$11.7: BTCUSDm 1-2 orders
(~0.5-1.0 USD = 4-9 %); XAUUSDm is refused (one min lot alone > the cap). Safe mode on such accounts trips at
max(8 % of peak, min(1.5 x the largest basket risk, 30 % of peak)), i.e. ~1.5 USD for a 2-order BTC basket.

**Duplicate protection is not an entry lock**: only an order in flight, an unconfirmed execution, and the
SAME signal (side + origin + level) whose previous basket LOST or was rejected are blocked. A winning signal
may re-enter at once. A basket closed by the broker (SL/trail) is booked with its REAL P/L as soon as the exit
deals appear in the history (re-read twice a second, max 120 s, flagged `pl_incomplete` otherwise).

**Runtime tuning (no redeploy, the MT5 login stays)**: `POST /api/cmd {"cmd":"tune","args":{"values":{...}}}`
(UI login + CSRF required) or `{"args":{"reset":true}}`. Only keys in `config.TUNABLE` with their min/max
bounds are accepted (thresholds, agreement, exits, BE/trail, stop sizing, order count, risk %, safe mode,
learning); `live`, `broker_sl`, magic and credentials are never tunable. A running engine applies them on the
next loop and logs `TUNE old -> new`; they are saved to `tune.json` in the data dir (re-applied when the engine
restarts; a redeploy resets to defaults). The active overrides are shown in the engine panel.

**Momentum hold & exit** (net basket P/L after spread and commission; NO fixed take-profit):
1. hard risk first (always wins): safe mode, broker/engine stop mirror, hard stop, basket loss cap, exposure
   breach, emergency (no fresh price), failed protection (below);
2. in profit: held while momentum continues; closed when >= 3 exhaustion signals agree - RSI momentum turn,
   MACD histogram turn, candle momentum loss, volume weakening, directional reversal, micro-structure
   failure, tick/M5 exhaustion - or on a **meaningful** retracement (>= max(0.25 ATR, 2 x spread) and >= 35 %
   of the peak) with fading momentum, or reversal + structure failure. Small retracements are held;
3. not in profit: held inside the hard stop (the old 4 s "no follow-through" cut is gone) unless the move
   clearly turned (reversal + structure failure + 1 more signal, or >= 0.5 of the stop with >= 2 signals),
   or stale after 180 s with momentum gone.

**Break-even & dynamic trailing (basket level)**: once NET basket profit >= max(0.35 x M1 ATR, 3 x spread,
stops level + lock + spread) the stop of EVERY order in the basket moves to the basket average + costs +
0.3 x spread (net profit after costs). Then it trails at 0.40 / 0.25 / 0.12 x ATR (strong / normal / fading
momentum), never closer than 1.5 x spread or the broker stops/freeze level, only forward, in steps >= 0.3 x
the distance, at most one basket update every max(1 s, 0.25 s x orders) (TickerAll write budget). Each change
uses the clamped PATCH and is **verified on the live MT5 position** (re-read after the PATCH, and again from
the positions feed). Until verified the order counts as unprotected at the broker: the engine enforces the
same level on the broker price and retries; 3 failed/unconfirmed changes in a row close the basket. The
initial hard stop (max(0.8 x M1 ATR, 4 x spread, stops level)) is always on the order.

**Latency** is measured on real clocks for every order and shown in the UI (p50/p90). Limits: TickerAll
tick frequency is set by TickerAll/broker; account/positions REST cache 1 s; fill verification polls
positions; Render free plan latency to the broker API.

## Risk & execution (risk.py, trader.py, broker_mt5.py / broker_tickerall.py)
Defaults (all in `worker/rapidburst/config.py`, overridable by env / `RB_CONFIG_JSON`):
* risk per order 0.5 % of equity (SL distance x tick value; min-lot floor for tiny accounts) –
  `RB_RISK_PER_TRADE_PCT`;
* total open risk <= 3 % of equity (`RB_MAX_TOTAL_RISK_PCT`); 1.5x breach closes the newest group;
* **margin use cap 50 % of equity** (`RB_MAX_MARGIN_USE_PCT`) and min margin level 200 % – the earlier
  100 % margin setting was replaced by this risk-based cap;
* batch: max 8 orders (`RB_MAX_ORDERS_PER_BATCH`, a cap, never forced), batch risk 1 %
  (`RB_RISK_PER_BATCH_PCT`), 1.0 lots per batch; 8 per symbol, 10 in total, 2.0 lots total, max 1.0 lot per order, exposure <= 500x equity notional;
* basket loss cap 2 % of equity (`RB_GROUP_LOSS_PCT`, never inside 3x the spread cost);
* **no daily-loss all-day stop** (removed; `RB_DAILY_LOSS_PCT` is ignored). **Equity drawdown 8 %** from the
  session peak (`RB_MAX_DD_PCT`) -> **SAFE MODE**: bot positions flattened, new entries paused 5 min
  (doubling on repeats, max 30 min, `RB_SAFE_PAUSE_S`), then entries resume from a new reference peak with
  3 single min-lot batches. The bot keeps running after START;
* execution protection: 3 rejected opens within 60 s pause the symbol 60 s; 5 failed close attempts halt new entries;
  15 s without a fresh price = emergency exit (REST quote used first when the stream lags);
* pacing: no cooldowns or candle locks (continuous intrabar re-entry on fresh signals); 30 baskets/min API
  guard; a loss streak raises the signal threshold, never the size; never size up after a losing basket.
* **min-lot floor accounts** (the broker minimum lot already risks more than the % caps, e.g. XAUUSDm 0.01
  lot on a ~$13 account = ~18-20 % per stop): one min-lot position at a time (no multi-order batch), that
  position is not "exposure-breach" closed (which only paid the spread in the first real demo run), and
  safe mode needs a drawdown of at least 1.25 single stop losses (capped at 25 % of the peak). BTCUSDm has
  a much smaller min-lot stop risk than XAUUSDm on such an account.
* batch orders (and basket closes) go out in parallel on TickerAll: one round trip per batch instead of one
  per order; every fill/close is still verified.
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
