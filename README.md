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

## t32u: conflict fixes, indicator confluence, structure stop (current)

Conflicts fixed (from the t31u live audit):
- **Safe mode** measures drawdown from the **closed-balance peak** (min(balance, equity)), never a floating spike, and
  only **pauses new entries**: open baskets keep their own structure/BE/trail stops. After the pause the first entry
  needs a **fresh M1 structure confirmation** (swing break, or HH/HL / LH/LL with the trade). Last resort only:
  equity `safe_flatten_pct` (60 %) below the session peak flattens bot positions and pauses 30 min, then restarts
  from the new balance (no all-day stop).
- **Losers are no longer cut on momentum noise** (`mb_cut_frac` 0 = off, the 180 s stale exit is off). A loser is held
  inside its structure stop and closed early only when the structure is invalidated: the M1 swing is broken against
  the basket, or micro-structure fails with a broad reversal against the M1 trend (or before M1 bars exist).
- **All exits use the basket's money P/L** (broker profit + swap of every leg, including adds); the peak restarts after
  an add (new average price).
- **Adds only share the basket's BE/trail stop**; if that stop cannot be set that close to price the add is skipped.
- **Trailing in an intact M1 trend is left to the trail** (no spike / exhaustion close unless the structure breaks).
- **Never a single order**: if `min_orders_per_batch` (2) min lots do not fit the risk/margin budget the symbol is
  skipped (with the reason shown) until equity grows.
- **M1 trigger (D) is required** once M1 bars exist; entering into an M1 swing already broken against the side is
  vetoed (the exit would invalidate it at once).
- Per-symbol feed check: the allowed tick gap adapts to the symbol's own tick spacing (FX pairs tick less than BTC).
- Dead M5 code and labels removed (`m5_bias`, the M5 series and poll, "M5 aligned" labels -> "M1 trend").

Indicator confluence (indicators.py, family F), from real broker M1 candles + the live price, computed once per closed
bar: EMA ribbon 8/13/21/34/55, session VWAP, Bollinger(20,2)/Keltner(20,1.5) squeeze + breakout, ADX/DI(14),
Stochastic RSI(14,14,3), Supertrend(10,3), ATR(14)/ATR(50) regime, OBV slope, Donchian(20) break, Heikin-Ashi trend,
pivot support/resistance. Votes are weighted per symbol by **learned weights** (learner.py `ind`: vote x outcome R of
every real closed basket, persisted with the learning memory, never decays). Vetoes: confluence < `ind_min_score`
against the side, entering within `ind_sr_near_atr` ATR of support/resistance without a breakout, chop (ADX < 18 with
no breakout, or a squeeze without the break).

Structure stop: beyond the last M1 swing low (BUY) / high (SELL) + spread + 0.1 ATR, clamped to 0.5-1.6 ATR, never
inside the broker stops level or 3x spread; ATR stop when no swing is on that side. BE + dynamic trailing unchanged
(every SL change verified on MT5).

Risk defaults (all tunable at runtime): basket stop risk 10 % of equity (`risk_per_batch_pct`, scaled 0.6-1.0 by the
score), all open stops <= 25 % (`max_total_risk_pct`), min-lot-floor accounts <= 15 %
(`small_account_batch_risk_pct`), safe mode 30 % below the closed-balance peak. Why: the t30u 50 %/100 % limits lost
the account twice in one day; at 10 %/25 % a run of 4-5 full stop-outs costs ~35-45 % instead of the whole account.
With ~$56 equity two BTCUSDm min lots fit; XAUUSDm (often $3-4 of structure-stop risk per 0.01 lot) only trades when two min lots fit the 15 % budget, otherwise it is skipped with the reason shown.
No martingale, no averaging down, no grid; re-entry stays immediate (no cooldown) and learning keeps every result.

### Replay on REAL market data (tools/replay_real.py) - a replay, not live results
Binance public data for 2026-11-23 (BKK): BTCUSDT aggTrades (<= 5 ticks/s) + 1m klines for BTCUSDm, 09:53-13:53;
PAXGUSDT (gold-backed token, thin: ~1 trade / 3 s) for XAUUSDm, 05:53-13:53. Broker conditions MODELLED: spread
BTC $6.0 / XAU $0.24 (as seen on the demo), fills at the quote 0.9 s after the send, Exness-like contract specs,
1:2000 leverage, start equity $56.60. Same data, each version with its own default settings:

| replay | baskets | win rate | profit factor | net | max drawdown (of peak) | safe-mode trips |
|---|---|---|---|---|---|---|
| BTC before (18f7c1a) | 114 | 44.7 % | 1.01 | +79.81 | 5,757.94 (97.7 %: peaked near $5.9k, ended $136) | 9 |
| BTC after (t32u) | 114 | 65.8 % | 1.28 | +50.63 | 39.38 (26.9 %) | 0 |
| XAU proxy before | 128 | 43.0 % | 0.84 | -52.23 | 77.37 (94.7 %) | 9 |
| XAU proxy after | 35 | 54.3 % | 0.65 | -23.49 | 29.47 (48.3 %) | 3 |

Read it honestly: one short window per symbol, a proxy for gold, modelled costs; it says nothing certain about future
results. The gold proxy still lost - small accounts on XAU min lots carry large per-order risk.

## t30u: M1 Rapid Basket Scalper (base engine, refactored in place)

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

## t34u — M5 momentum, auto-reversal, continuous re-entry

The M5 forming bar is now the main frame (family E = M5 momentum/direction, no close wait); M1 + ticks time the entry.
**Auto-reversal**: when the opposite side passes its own checks and beats the held side by `reverse_edge` (0.10),
the basket is closed, every close is verified on MT5, and the opposite batch (2–8 orders, risk-sized) opens in the
same loop. If a close fails, the opposite side is not opened (no hedge, no blind resend).
After any close the engine is back in SCAN immediately: no cooldown, no candle lock, no same-burst lock (only a
broker-rejected signal is blocked for 3 s), the learner sizes (min lot) instead of skipping, no post-safe-mode confirm.
Hard vetoes on M1 trend, swing-break, S/R and chop became penalties; "no live force" only vetoes price moving against.
Still no martingale/averaging; hard stop, BE, trailing, margin and safe-mode protection unchanged (safe pause 60 s).

Root causes of slow/missing re-entry (at e3f6ea1): microburst.py:707 velocity veto (28–50 hits/min live, 25–40 s gaps),
:672 M1 trend veto, :681 swing-break veto, :748 D-only trigger, trader.py:937 open basket ignored opposite signals,
:973 losing-basket duplicate lock, :988 learner skip, :991 confirm gate, 180 s safe pause.

REPLAY on real Binance 1m/tick data (simulated broker double, NOT live results), $56.60 start:

| window | version | baskets | win % | PF | net | max DD % | re-entry p50 / p90 | reversals |
|---|---|---|---|---|---|---|---|---|
| BTCUSDT 4h | before | 114 | 65.8 | 1.28 | +50.63 | 26.9 | 14.8 s / 161.5 s | 0 |
| BTCUSDT 4h | after | 224 | 51.8 | 1.10 | +16.90 | 34.7 | 8.3 s / 56.0 s | 37 |
| PAXGUSDT 8h (gold proxy) | before | 35 | 54.3 | 0.65 | −23.49 | 48.3 | 313 s / 2650 s | 0 |
| PAXGUSDT 8h (gold proxy) | after | 261 | 51.3 | 1.26 | +113.67 | 52.4 | 13.9 s / 132.8 s | 41 |

More frequent trading also means more exposure; replay results are not a profit guarantee.

## t35u–t39u — immediate entries, anti-jitter exits, owner no-stop full-batch mode, zones, learner off

**Per-order stop loss is OFF by owner request (DEMO, risk accepted).** `no_stop_mode=1` (default):
* at a signal ONE full batch goes in at once (up to `max_orders_per_batch` = 16, tunable to 40), same lot, sized by
  MARGIN: all bot positions together use <= `nostop_margin_use_pct` (50%) of equity, with a 5% free-margin buffer;
* no broker SL, no hard-stop mirror, no group-loss cut. The ONLY emergency guards: margin level below
  `emergency_margin_level_pct` (110% since t41u) closes the bot's positions, and data-feed loss (`emergency_stale_s`).
  BE/trail on winning baskets stays as engine-side profit protection; never martingale or averaging losers
  (adds only into winning baskets);
* exits: ZONE TARGET (price reaches an opposing supply/demand zone, FVG or S/R in profit) or a confirmed
  pullback/reversal from volume + momentum + indicators; a pullback INTO the basket's own demand/supply zone or FVG
  is held (`pullback_hold`).
* `no_stop_mode=0` restores stop-based sizing with a broker SL on every order.

Entries (t35u): `mb_threshold` 0.40, `mb_edge` 0.03, `min_orders_per_batch` 1. Live 15:27–15:32 BKK showed
"below threshold" ~40–54 of 60 samples/min, START→first entry 91 s and close→re-entry 151–206 s; after the runtime
tune entries came 1–5 s later and reversal re-entries 0.00 s after the verified close. A broker reject blocks resends
on that symbol for 3 s (no blind resend).

Anti-jitter (t36u): soft exits (reversal, spike-turn, momentum, structure, profit protection, auto-reverse) must
persist >= `exit_confirm_s` (0.8 s) over >= `exit_confirm_ticks` (2) clean ticks; outlier ticks (> 0.8 ATR from the
median of the last 5 mids), feed gaps, stale quotes and spread spikes never count; emergency guards are never delayed.

Price action (t37u, `zones.py`): demand/supply zones, fair value gaps, confirmation candles and wick-tip entries
from real M5/M1 candles add up to ±0.10 strength (never decide alone).

Learner (t39u): `learning_enabled=0` - closed trades are still recorded, but the learner no longer skips,
resizes, reweights or exits anything.

## t40u — owner mode: no stop loss (locked), UP/DOWN/SIDEWAYS + phases, recovery hedge + averaging, profit-only exits

All of this is the **account owner's explicit request on a DEMO account, risk accepted after a stop-out warning.**

* **Per-order stop loss is permanently off**: `no_stop_mode` is locked on - removed from the runtime TUNE list and from
  the env overrides (`RB_BROKER_SL` no longer exists); no broker SL is ever sent or modified (BE/trail are
  engine-side profit levels only).
* **Market state** (`regime.py`): only UP / DOWN / SIDEWAYS from M5 swings (HH+HL / LH+LL) on the forming bar,
  ADX + range for sideways; a structure break flips it at once. **Phase counter**: legs since the trend started
  (odd = impulse, even = pullback) with the expected next phase (impulse -> pullback -> continuation; phase >=
  `regime_max_phase` (5) = exhaustion / reversal risk). Shown in the UI and logged as `REGIME ...` on every change.
* **Entries**: with the trend at the pullback wick / zone / FVG (+`regime_bonus`); **anti-chase** veto when price is
  more than `regime_chase_atr` (2.0) M5 ATR beyond the pullback origin without a wick; SIDEWAYS fades the range edges
  at wicks. Scanning never stops.
* **Recovery (owner-requested)**: a basket not green after `recovery_after_s` (60 s) or red beyond
  `recovery_loss_pct` (2% equity) while the opposite side has a valid signal opens the **opposite basket** (hedging
  account). A losing basket gets **averaging layers** every `avg_spacing_atr` (1.0) ATR at better prices, at most
  `avg_max_layers` (4), each layer = the batch's lots in <= 4 orders x `avg_lot_mult`^layer (default 1.0 = no
  martingale; the owner may raise it to 3.0).
* **Exits: close only in profit** - a basket closes on its trail level / zone target / confirmed momentum or reversal
  only while its net is > `tp_min_money`; a hedged/recovery cycle closes ALL its legs once the combined net >=
  `cycle_tp_pct` (0.3%) of equity. No loss close except the emergency guards: margin level < 110% and data-feed loss.
* **Margin budget** (t41u): one batch / hedge / layer <= `batch_margin_pct` (10%) of equity; ALL bot positions <=
  `nostop_margin_use_pct` (75%) -> at zero P/L the margin level stays >= ~133%, above the 110% emergency close.
  Worst case: total margin 75% of equity (cap); the emergency close then fires after a ~17.5% equity drop at full
  load (~89% drop with a single 10% batch).

## t41u / t42u — fast profit lock, never self-stopping, loss guardian

* **PROFIT LOCK**: once a basket's net > max(`profit_lock_min_money` 0.03, 2x its spread cost) the profit is locked;
  when the net gives back 40% of its peak or falls to 25% of the lock level it closes at once (0.3 s confirm) -
  before it turns red. A basket that never goes green is handed to the loss guardian after `recovery_after_s` (20 s).
* **No auto-stop / pause in owner mode**: no safe mode, no drawdown halt, no catastrophic flatten, no execution
  pauses after rejects or close failures, no self-stop on an unverified entry. Only the user's STOP and the
  margin-level / data-feed emergency closes act, and scanning continues after an emergency close.
* **Loss guardian** (`guardian.py`): every loop it watches each symbol's red baskets (net per side, combined net),
  opens the opposite basket / recovery layers per t40u and logs `GUARD ...` every 15 s; the combined net is closed
  in profit (`CYCLE TAKE PROFIT`).
* REPLAY on real Binance data (simulated broker, t40u settings before the t41u lock): BTCUSDT 4h - 75 baskets, 90.7%
  winners but profit factor 0.22 and equity 56.60 -> 0.42 (-99%): small wins, a few huge losses with no stop.

## t43u — profit target, fast grab, no break-even close

* **Profit target** per basket: net >= max(`tp_target_money` 0.10, `tp_target_atr` 0.4 x M1 ATR on its lots, the lock
  level). Once reached the profit is grabbed when it gives back `tp_target_giveback` (20%) of its peak, or at once at
  `tp_grab_x` (1.5x) the target. Below the target the t41u PROFIT LOCK still closes a fading profit before red.
* **EARLY CLOSE**: a green basket closes at once (0.3 s confirm) when the opposite side signals or price turns
  against it while the profit is thin - before it turns red.
* **No break-even close** in owner mode (it ended at ~0 / slightly negative after costs). Engine BE/trail levels no
  longer close anything; every soft exit requires the basket net > 0.
* **Loss-closing rules that still exist (all modes of owner mode):** only (1) the user's STOP, (2) the emergency
  close at margin level < `emergency_margin_level_pct` (110%), (3) the data-feed-loss emergency
  (`emergency_stale_s`). A red basket is never closed by the strategy: the loss guardian hedges / layers it and the
  cycle closes when the combined net is in profit. This cannot guarantee the account never decreases: open losses
  can grow without a stop until the margin emergency closes them.

## t44u — recovery cancelled, sets close together, longer targets, stricter entries (owner request)

* **Hedge basket and averaging layers are CANCELLED.** Live logs showed the guardian opening an opposite basket
  every time a basket was red for ~20 s (dozens in 25 minutes), leaving one side green and the other red.
  `hedge_enabled` / `avg_enabled` default to False, `EngineConfig.load()` forces them off (config file,
  `RB_CONFIG_JSON`) and they, `avg_*` and `recovery_*` are no longer runtime-tunable. A red basket is now simply
  HELD: no new orders, closed only when it returns to profit (or by the margin / data-feed emergencies above).
* **Mixed sets close together.** If both sides are still open on a symbol (an old hedged set), a green basket is
  never taken alone: the whole set closes when its combined net >= `cycle_tp_pct` of equity (`CYCLE TAKE PROFIT`).
* **Longer profit capture:** `tp_target_atr` 0.4 -> 0.6, `tp_grab_x` 1.5 -> 2.0 (immediate grab at 1.2 ATR instead
  of 0.6), `tp_target_giveback` 20% -> 25%. EARLY CLOSE now needs a real opposite signal that persists through the
  normal anti-jitter confirmation; a momentary velocity flip no longer closes a thin profit.
* **Entries** (no hedge safety net any more): `regime_counter_veto` - no entry against an intact M5 structure (SELL
  in UP / BUY in DOWN) unless a wick tip or demand/supply zone backs it; `regime_chase_atr` 2.0 -> 1.5;
  `regime_mid_penalty` 0.06 -> 0.10 (middle of a sideways range); `mb_edge` 0.03 -> 0.06 (a near-tie BUY/SELL is
  noise). These are reasoned changes, NOT back-tested on real data.

## t45u — Structure-to-Liquidity precision entry (owner spec)

Entry logic of the existing engine changed in place (`worker/rapidburst/precision.py`, wired into `microburst.decide` and the trader; no new engine, no new UI page).

* **Direction from M5 structure**: confirmed swings, BOS / CHOCH (closes beyond the last swing), displacement leg (origin -> impulse extreme, big range + big body). Not entered because RSI / MACD / volume spike.
* **Order block + liquidity map**: bullish / bearish OB tied to the displacement, equal highs / lows, swing liquidity, previous (range) high / low, internal vs external, liquidity sweeps. **Target LIQ** is chosen before entry; liquidity closer than `pe_tgt_min_atr` is skipped, liquidity right overhead (`pe_near_atr`) vetoes.
* **Fibo 0.50 / 0.618 / 0.705 / 0.786** of the displacement leg is only one confluence factor: an entry needs >= `pe_min_confluence` factors (fibo, OB, S/R, sweep, FVG-zone) and at least one that is not the fibo.
* **Pullback, not chasing**: price must be back in the zone (`pe_min_depth`, `pe_zone_chase_atr`), structure not lost (`pe_max_depth`), then a rejection / micro structure change / momentum re-activation on the live M1 triggers the order immediately (no bar-close wait).
* **Position Entry Score** (`Plan.score`, `pe_min`) is separate from the directional momentum score; right direction + bad location = `NO TRADE (location rejected)`. It weighs structure, OB, liquidity, fibo, S/R, pullback quality, rejection, re-activation, volume, target space, R:R, spread cost.
* **Stop / target**: broker SL at the structural invalidation (beyond the pullback low / OB / swept low + buffer), never widened (`pe_max_sl_atr`), stop distance + lot + R:R (`pe_rr_min`) re-checked on the live price in `_open_batch`. No fixed TP: the basket is held to its Target LIQ (`TARGET LIQ reached`), protected by the existing break-even + dynamic trailing, and closed at once on `STRUCTURE INVALIDATED`. Cents-level zone targets are off for these baskets.
* **Management**: no hedge, no averaging, no martingale, no pyramid adds (`add_enabled` default off), one entry per structure event.
* **`no_stop_mode` is now OFF** and `load()` keeps it off while `precision_enabled` (the precision entry is defined by its stop). `precision_enabled` is tunable (0 = previous behaviour, for A/B).

### Testing - what is and is not proven
`tests/test_t45u.py` (15 tests) checks the LOGIC on synthetic candles and the simulated broker. It is **not** market evidence. No real historical candles are in the repo and the sandbox had no market-data access, so **no old-vs-new profitability comparison has been run**. The defaults (`pe_*`) are untuned.
Real evidence comes from the DEMO forward test: the status API (`performance.precision`) reports, from real closed baskets after costs: win rate, profit factor, avg win / loss, Target-LIQ reach %, invalidation / broker-stop %, avg MAE / MFE in R, avg initial drawdown, planned vs realised R, entry depth / score and win rate per confluence tag; each closed basket carries its plan in `performance` rows. Compare it with the previous release on a **later** period than any tuning (run `precision_enabled=0` on the same demo for the old logic).

### t45u+ — entry-location correction (anti-chase BUY / SELL)
Applied to the same engine (`precision.py`), no new module, nothing unrelated touched:
* **Premium / discount**: the pullback path also checks where price sits in the loaded M5 range (`pe_range_prem_max`, default 0.85): no BUY in the top 15% / no SELL in the bottom 15% of the range; the position in the range is a score component (`location`) and is reported as `premium` in the plan. The leg-based discount (`pe_min_depth`) and the fibo / OB / S-R / sweep / FVG confluence were already there.
* **Breakout exception** (the alternative to waiting for a deep pullback): a confirmed breakout continuation or breakout retest is allowed only when the break is fresh (`pe_bo_max_bars`), the break candle is a real body that closed beyond the broken level with range expansion (`pe_bo_body_frac`, `pe_bo_beyond_atr`, `pe_bo_range_atr`) - a wick spike is not a breakout - price has not run beyond the level by more than `pe_bo_cont_ext_atr` (`pe_bo_retest_ext_atr` after a retest), momentum is pushing in the direction right now, and the same R:R / target-space / spread / structural-stop rules pass (the stop is the broken level: the breakout has failed when price is back through it). It has its own score weights (breakout quality replaces OB / fibo / S-R) and the same `pe_min` gate.
* Direction and location stay separate: a high momentum score never executes on its own (`location rejected` in the status reason).
* Higher-timeframe context is M5 only - the engine loads M1 and M5 candles, not M15 / H1.

