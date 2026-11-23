"use strict";
(() => {
  const $ = (id) => document.getElementById(id);
  let S = { worker_online: false, snapshot: {} };
  let logs = [];
  let lastSeq = 0;
  let picked = null; // pending symbol selection (array) or null
  let ws = null;
  let wsRetry = 1000;

  const fmt = (v, d = 2) => (v === null || v === undefined || Number.isNaN(Number(v))) ? "—" :
    Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  const pl = (el, v) => { el.textContent = (v === null || v === undefined) ? "—" : (v > 0 ? "+" : "") + fmt(v);
    el.className = v > 0 ? "pos-v" : v < 0 ? "neg-v" : ""; };
  const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const badge = (el, text, cls) => { el.textContent = text; el.className = "badge " + (cls || ""); };

  async function api(path, body) {
    const r = await fetch(path, {
      method: body ? "POST" : "GET", credentials: "same-origin",
      headers: body ? { "Content-Type": "application/json", "X-RB": "1" } : {},
      body: body ? JSON.stringify(body) : undefined,
    });
    if (r.status === 401) { showLogin(); throw new Error("login required"); }
    const t = await r.text();
    try { return JSON.parse(t); } catch { return { ok: r.ok, msg: t }; }
  }
  const cmd = (c, args) => api("/api/cmd", { cmd: c, args: args || {} });

  function showLogin() { $("login").classList.remove("hidden"); if (ws) { try { ws.close(); } catch {} } }
  function hideLogin() { $("login").classList.add("hidden"); }

  $("loginForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    $("loginMsg").textContent = "…";
    const r = await fetch("/api/login", { method: "POST", credentials: "same-origin",
      headers: { "Content-Type": "application/json", "X-RB": "1" }, body: JSON.stringify({ password: $("pw").value }) });
    const j = await r.json().catch(() => ({}));
    if (j.ok) { $("pw").value = ""; hideLogin(); boot(); } else $("loginMsg").textContent = j.msg || "failed";
  });
  $("btnLogout").addEventListener("click", async () => { await fetch("/api/logout", { method: "POST" }); showLogin(); });

  function addLogs(items) {
    for (const l of items || []) if (!l.seq || l.seq > lastSeq || l.seq < lastSeq - 1000) { logs.push(l); if (l.seq) lastSeq = l.seq; }
    if (logs.length > 300) logs = logs.slice(-300);
    const box = $("logs");
    const atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 30;
    box.innerHTML = logs.map((l) => `<div class="${esc(l.level)}">${new Date(l.ts * 1000).toLocaleTimeString("en-GB")} ${esc(l.msg)}</div>`).join("");
    if (atBottom) box.scrollTop = box.scrollHeight;
  }

  function render() {
    const s = S.snapshot || {};
    const conn = s.conn || {};
    const acct = s.account;
    const eng = s.engine || {};
    const val = s.validation || {};
    const online = !!S.worker_online;
    const L = s.launcher || {};
    const lc = L.last_connect || null;
    const cloud = S.mode === "cloud";
    badge($("bWorker"), cloud ? (online ? "CLOUD ENGINE ON" : "CLOUD ENGINE STARTING…") : (online ? "LAUNCHER ON" : "LAUNCHER WAKING…"),
      online ? "ok" : "warn");
    const st = online ? (conn.state || "—") : "UNKNOWN";
    badge($("bMt5"), st === "IDLE" ? "MT5 OFF (idle)" : "MT5 " + st,
      st === "CONNECTED" ? "ok" : (st === "CONNECTING" || st === "IDLE") ? "warn" : "bad");
    const mode = (acct && acct.mode) || (!acct && lc && lc.account && lc.account.mode) || null;
    badge($("bMode"), mode ? (mode === "REAL" ? "LIVE" : mode) : "NO ACCOUNT", mode === "DEMO" ? "ok" : mode ? "live" : "");
    const phase = L.phase || (eng.running ? "RUNNING" : "IDLE");
    const engTxt = !online ? "ENGINE —" : eng.halted ? "SAFE MODE" : phase === "IDLE" ? "ENGINE OFF" : phase;
    badge($("bEngine"), engTxt, eng.halted ? "warn" : phase === "RUNNING" ? "ok" : "warn");

    const ka = (S.cloud || {}).keepalive || {};
    const cloudNote = cloud ? ` · cloud (TickerAll)${(S.cloud || {}).api_key_set ? "" : " · TICKERALL_API_KEY NOT SET"}${ka.active ? ` · keep-awake ON (${ka.pings_ok || 0} pings)` : ""}` : "";
    $("connLine").textContent = (!online
      ? (cloud ? "Cloud engine starting on the server…" : "Waking the Windows launcher… (if this stays, the PC/VPS launcher is off or offline)")
      : st === "IDLE"
      ? `Engine off - nothing running. ${lc ? `Last check ${new Date(lc.ts * 1000).toLocaleString("en-GB")}: ${lc.validation_ok ? "validated ✓" : "not validated ✗ " + (lc.failed_checks || []).join(", ")}` : "Use CONNECT MT5 first."}`
      : `${conn.msg || ""}${conn.ping_ms != null ? ` · ping ${conn.ping_ms} ms` : ""}${conn.state === "CONNECTED" ? ` · algo trading ${conn.algo_trading ? "ON" : "OFF"}` : ""}`) + cloudNote;
    $("acctLine").textContent = acct ? `#${acct.login} · ${acct.server} · ${acct.company || ""} · ${acct.currency} · 1:${acct.leverage}`
      : (lc && lc.account ? `last: #${lc.account.login} · ${lc.account.server} · ${lc.account.mode}` : "");

    const cur = acct ? acct.currency : "";
    $("mBal").textContent = acct ? fmt(acct.balance) : "—";
    $("mEq").textContent = acct ? fmt(acct.equity) : "—";
    $("mFree").textContent = acct ? fmt(acct.margin_free) : "—";
    const p = s.pnl;
    pl($("mFloat"), p ? p.floating : null); pl($("mDaily"), p ? p.daily : null); pl($("mBatch"), p ? p.batch : null);

    const liveOk = conn.state === "CONNECTED" && val.ok;
    const storedOk = L.has_credentials && lc && lc.validation_ok;
    const canStart = online && (eng.selected || []).length > 0 && (liveOk || storedOk) && !eng.running && !eng.blocked &&
      !["STARTING", "RUNNING", "STOPPING"].includes(phase);
    $("btnStart").disabled = !canStart;
    const why = [];
    if (!online) why.push(cloud ? "The cloud engine is still starting on the server - wait a few seconds." : "The Windows launcher is not connected.");
    else {
      if (cloud && !(S.cloud || {}).api_key_set) why.push("TICKERALL_API_KEY is not set on Render (Render -> rapid-burst -> Environment).");
      if (eng.running || ["STARTING", "RUNNING"].includes(phase)) why.push("The engine is already running (use STOP first).");
      if (phase === "STOPPING") why.push("STOP is still in progress.");
      if (eng.blocked) why.push("Blocked: " + eng.blocked);
      if (!(eng.selected || []).length && !eng.running) why.push("No symbol selected - CONNECT MT5, then search in Symbols, tick the assets you want (up to 6) and press APPLY.");
      if (!liveOk && !storedOk && !eng.running) {
        if (!L.has_credentials && st !== "CONNECTED") why.push("CONNECT MT5 has not been done yet - press CONNECT MT5 and enter the demo login, password and server.");
        else {
          const det = (conn.state === "CONNECTED" ? (val.checks || []).filter((c) => !c.ok).map((c) => `${c.name}: ${c.detail}`) : (lc && !lc.validation_ok ? (lc.failed_details || lc.failed_checks || []) : []));
          if (det.length) det.forEach((d) => why.push("Check failed - " + d));
          else if (lc && !lc.ok) why.push("Last CONNECT failed: " + (lc.msg || "unknown error"));
          else if (conn.state && !["CONNECTED", "IDLE"].includes(conn.state)) why.push(`MT5 is ${conn.state}: ${conn.msg || ""}`);
          else why.push("The account has not been validated yet - press REFRESH or CONNECT MT5.");
        }
      }
    }
    const w = $("startWhy");
    w.classList.toggle("hidden", !why.length || canStart);
    w.innerHTML = (why.length && !canStart) ? "<b>START is disabled because:</b><br>" + why.map((x) => "• " + esc(x)).join("<br>") : "";
    $("btnStop").disabled = false;
    $("btnConnect").textContent = conn.state === "CONNECTED" ? "RECONNECT MT5" : "CONNECT MT5";

    $("checks").innerHTML = (val.checks || []).map((c) =>
      `<li><span>${c.ok ? "✅" : c.soft ? "⚠️" : "❌"}</span><span>${esc(c.name)}</span><span class="d">${esc(c.detail)}</span></li>`).join("")
      || `<li class="muted">${!online ? "Launcher not connected" : st === "IDLE" ? "Engine off - checks run on CONNECT MT5 / START" : "Waiting for MT5 connection"}</li>`;

    const sel = eng.selected || [];
    $("selSyms").textContent = sel.join(", ");
    const chosen = picked || sel;
    $("symPicked").innerHTML = chosen.map((x) => `<span class="chip">${esc(x)}</span>`).join("");
    $("btnApply").classList.toggle("hidden", !picked);

    const pos = s.positions || [];
    const lots = pos.reduce((a, x) => a + x.volume, 0);
    $("posSum").textContent = pos.length ? `${pos.length} pos · ${fmt(lots, 2)} lots · ${s.foreign_positions || 0} manual/other untouched` :
      (s.foreign_positions ? `${s.foreign_positions} manual/other (untouched)` : "");
    $("posBody").innerHTML = pos.length ? pos.map((x) =>
      `<tr><td>${esc(x.symbol)}</td><td class="${x.side === "BUY" ? "pos-v" : "neg-v"}">${x.side}</td><td>${fmt(x.volume, 2)}</td>` +
      `<td>${esc(x.price_open)}${x.batch ? `<div class="tiny muted">group ${esc(x.batch)}</div>` : ""}</td><td>${x.sl ? esc(x.sl) : "—"}<div class="tiny ${x.sl_state === "trail" || x.sl_state === "BE" ? "pos-v" : "muted"}">${esc(x.sl_state === "trail" ? "trailing" : x.sl_state === "BE" ? "break-even" : x.sl_state || "")}</div></td><td class="${x.profit >= 0 ? "pos-v" : "neg-v"}">${x.profit > 0 ? "+" : ""}${fmt(x.profit)}</td><td>${x.age_s ?? "—"}s</td></tr>`).join("")
      : `<tr><td colspan="7" class="muted">No bot positions</td></tr>`;

    const syms = eng.symbols || {};
    const bsl = (eng.risk_config || {}).broker_sl;
    $("engMeta").textContent = (eng.batch && eng.batch.id ? `session ${eng.batch.id} · ` : "") + (bsl ? `Broker SL: ${bsl}` + (bsl === "OFF" ? " (engine hard-close)" : "") : "") + ((eng.risk_config || {}).learning_enabled === false ? " · Learning: record only (no influence)" : "") + ((eng.risk_config || {}).recoveries ? ` · Recovery (owner-requested): hedges ${eng.risk_config.recoveries.hedges}, layers ${eng.risk_config.recoveries.layers} (max ${eng.risk_config.avg_max_layers}/basket, lot x${eng.risk_config.avg_lot_mult}), cycle TPs ${eng.risk_config.recoveries.cycle_tp} · close only in profit` : "");
    $("engine").innerHTML = Object.keys(syms).length ? Object.entries(syms).map(([k, v]) => {
      const f = v.features || {};
      const cls = v.signal === "BUY" ? "pos-v" : v.signal === "SELL" ? "neg-v" : "muted";
      const mk = v.market === "live" ? ' <span class="pos-v tiny">● quoting</span>' : v.market === "closed" ? ' <span class="neg-v tiny">● no quotes / market closed</span>' : v.market === "n/a" ? ' <span class="neg-v tiny">● not tradable</span>' : "";
      const mt = v.mtf;
      const arrow = (x) => x > 0 ? "▲" : x < 0 ? "▼" : "▬";
      const trHtml = mt ? ` <span class="tiny ${mt.bias >= 0.2 ? "pos-v" : mt.bias <= -0.2 ? "neg-v" : "muted"}">M1 trend ${arrow(mt.bias >= 0.2 ? 1 : mt.bias <= -0.2 ? -1 : 0)} ${esc(mt.label)} (${fmt(mt.bias, 2)})</span>` : "";
      const tfLine = (n, x) => !x || !x.ready ? `${n} waiting for bars` :
        `${n} ${esc(x.src)} ${x.bars} bars · MACD hist ${fmt(x.hist, 2)} slope ${x.hist_slope > 0 ? "+" : ""}${fmt(x.hist_slope, 2)}${x.macd_ready ? "" : " (warming)"} · body ${fmt(x.body, 2)} · tick-volume ${x.vol_x == null ? "n/a" : fmt(x.vol_x, 2) + "× (" + esc(x.vol_src) + ")"} · RSI ${x.rsi_ready ? fmt(x.rsi, 1) + " (" + (x.rsi_slope > 0 ? "+" : "") + fmt(x.rsi_slope, 1) + ")" : "warming"} · structure ${x.structure > 0 ? "HH/HL" : x.structure < 0 ? "LH/LL" : "mixed"}${x.exhaust ? ' · <span class="neg-v">exhausting</span>' : ""}`;
      const mtHtml = mt ? `<div class="tiny">📈 M5 momentum (forming bar) BUY ${fmt(mt.m5_up, 2)} / SELL ${fmt(mt.m5_dn, 2)} · ${esc(mt.m5_label || "")} · ${tfLine("M5", mt.m5)}</div><div class="tiny">⏱ M1 timing BUY ${fmt(mt.m1_up, 2)} / SELL ${fmt(mt.m1_dn, 2)} · ${tfLine("M1", mt.m1)}</div>` : "";
      const rg = v.trend;
      const rgHtml = rg && rg.ready ? `<div class="tiny">🧭 Market state: <b class="${rg.state === "UP" ? "pos-v" : rg.state === "DOWN" ? "neg-v" : "muted"}">${esc(rg.state)}</b>${rg.state !== "SIDEWAYS" ? ` · phase ${rg.phase} (${esc(rg.leg)})` : ""} · next: ${esc(rg.expect)}</div>` : "";
      const zv = v.zones;
      const zHtml = zv && zv.ready ? `<div class="tiny">🧱 Zones/FVG: BUY ${fmt(zv.buy, 2)} [${esc((zv.tags.BUY || []).join(", ") || "-")}] / SELL ${fmt(zv.sell, 2)} [${esc((zv.tags.SELL || []).join(", ") || "-")}] · confirmation candle ${zv.confirm > 0 ? "▲" : zv.confirm < 0 ? "▼" : "-"} · wick tip ${zv.wick > 0 ? "BUY (lower wick)" : zv.wick < 0 ? "SELL (upper wick)" : "-"}</div>` : "";
      const iv = v.ind;
      const ivHtml = iv && iv.ready ? `<div class="tiny">📊 indicators: ${Object.entries(iv.votes || {}).map(([k, x]) => `<span class="${x >= 0.2 ? "pos-v" : x <= -0.2 ? "neg-v" : "muted"}">${esc(k)} ${fmt(x, 2)}</span>`).join(" · ")} · ADX ${fmt(iv.adx, 1)}${iv.breakout ? " · breakout " + (iv.breakout > 0 ? "▲" : "▼") : ""}${iv.chop ? ' · <span class="neg-v">chop: ' + esc(iv.chop_why) + "</span>" : ""} · S/R ▲${iv.res_above ?? "—"} ▼${iv.sup_below ?? "—"}</div>` : "";
      const cy = v.m1_cycles;
      const cyHtml = cy ? `<div class="tiny">🔁 this M1 bar: ${cy.opened} basket(s) opened · ${cy.closed} closed (no per-candle limit, no candle lock)</div>` : "";
      const sk = v.skips && Object.keys(v.skips).length ? `<div class="tiny muted">why no trade (count): ${Object.entries(v.skips).map(([a, b]) => esc(a) + " " + b).join(" · ")}</div>` : "";
      const sg = v.strength || {};
      const bar = (x, c) => `<span class="${c}">${x == null ? "—" : fmt(x, 2)}</span>`;
      const sgHtml = sg.threshold != null ? `<div class="tiny">strength BUY ${bar(sg.buy, "pos-v")} · SELL ${bar(sg.sell, "neg-v")} · need ≥ ${fmt(sg.threshold, 2)} · regime <b>${esc(sg.regime || "—")}</b>` +
        (sg.scan ? ` · families BUY ${["A", "B", "C", "D", "E", "F"].map((k) => k + fmt(sg.scan.BUY?.[k], 2)).join(" ")} / SELL ${["A", "B", "C", "D", "E", "F"].map((k) => k + fmt(sg.scan.SELL?.[k], 2)).join(" ")}` : "") +
        (sg.vetoes && sg.vetoes.length ? ` · <span class="neg-v">${esc(sg.vetoes.join("; "))}</span>` : "") + `</div>` : "";
      return `<div class="eng"><b>${esc(k)}</b>${mk}${trHtml} <span class="sig ${cls}">${esc(v.signal)}</span> · <b>${v.signal === "HOLD" ? "waiting for: " : ""}</b>${esc(v.reason)}` + sgHtml + rgHtml + mtHtml + zHtml + ivHtml + cyHtml + sk +
        `<div class="tiny muted">vel ${f.vel_atr ?? "—"} ATR · acc ${f.acc_atr ?? "—"} · eff ${f.eff ?? "—"} · burst ${f.burst_z ?? "—"}× · tick-activity ${f.act_x ?? "—"}× (count, not real volume) · up/down-tick proxy ${f.imb ?? "—"} · bid/ask proxy ${f.bidask ?? "—"} · candle body ${f.body_atr ?? "—"} ATR · ${f.early ? '<span class="pos-v">early burst ' + (f.early > 0 ? "▲" : "▼") + "</span> · " : ""}${f.exhaust ? '<span class="neg-v">exhaustion</span> · ' : ""}${f.rebound ? "rebound " + (f.rebound > 0 ? "▲" : "▼") + " · " : ""}chase ▲${f.chase_up ?? "—"}/▼${f.chase_dn ?? "—"} ATR · target ${f.target ?? "—"} · spread ${v.spread_pts ?? "—"}pt · ${f.tps ?? "—"} t/s · tick ${v.tick_age ?? "—"}s ago</div></div>`;
    }).join("") : `<div class="muted small">${eng.halted ? esc(eng.halt_reason) : "Engine idle"}</div>`;
    const rc = eng.risk_config;
    if (rc) {
      const gs = Object.entries(eng.groups || {});
      const rowG = ([k, x]) => `<div class="tiny"><b>${esc(x.symbol)} ${esc(x.side)}</b> basket ${esc(k)} · ${x.orders}${x.planned ? "/" + x.planned : ""} order(s) (cap ${rc.max_orders_per_batch}) · ${fmt(x.volume, 2)} lots · ` +
        `net <b class="${x.money >= 0 ? "pos-v" : "neg-v"}">${x.money > 0 ? "+" : ""}${fmt(x.money)}</b> · peak ${x.peak_px == null ? "—" : fmt(x.peak_px, 2)} (price) · ` +
        `<span class="${x.protect === "BE" || x.protect === "trail" ? "pos-v" : "muted"}">${x.protect === "trail" ? "trailing" : x.protect === "BE" ? "break-even" : "hard stop only"}${x.protect_level ? " @ " + fmt(x.protect_level, 2) : ""}${x.protect === "BE" || x.protect === "trail" ? (x.sl_verified ? " ✔ on MT5" : ' <span class="neg-v">not yet confirmed on MT5 - engine enforcing</span>') : ""}</span>` +
        ` · BE at +${x.be_trigger_px == null ? "—" : fmt(x.be_trigger_px, 2)} · trail ${x.trail_px == null ? "—" : fmt(x.trail_px, 2)} (${esc(x.momentum || "—")} momentum, ${x.trail_moves || 0} move(s)) · ${esc(x.regime || "")} · ${x.age_s ?? "—"}s</div>`;
      const lt = eng.latency || {};
      const q = (o) => o && o.p50 != null ? `${o.p50}/${o.p90}ms` : "—";
      const latHtml = `<div class="tiny">⏱ Measured latency (p50/p90, real clocks, ${lt.entries || 0} order(s), ${lt.exits || 0} close(s)): tick→decision ${q(lt.tick_to_decision_ms)} · decision→send ${q(lt.decision_to_send_ms)} · send→ack ${q(lt.send_to_ack_ms)} · send→fill verified ${q(lt.send_to_fill_verified_ms)} · tick→fill ${q(lt.tick_to_fill_ms)} · feed (broker→worker) ${q(lt.feed_ms)} · close ${q(lt.close_ms)}${lt.entries ? "" : " · no orders yet - nothing measured"}</div>`;
      const pf = eng.performance || {};
      const att = (a) => Object.entries(a || {}).map(([k, g]) => `${esc(k)} ${g.n}× ${g.win_rate}% ${g.net > 0 ? "+" : ""}${fmt(g.net)}`).join(" · ");
      const pfHtml = pf.batches ? `<div class="tiny">📊 Real demo results this session (${esc(pf.source)}, after costs): ${pf.batches} basket(s), ${pf.orders} order(s) · ${pf.batches_per_hour ?? "—"}/h · win rate ${pf.win_rate}% · net ${pf.net > 0 ? "+" : ""}${fmt(pf.net)} · expectancy ${fmt(pf.expectancy)} · profit factor ${pf.profit_factor ?? "—"} · avg win ${fmt(pf.avg_win)} / avg loss ${fmt(pf.avg_loss)} · max drawdown ${fmt(pf.max_drawdown)} · re-entry p50 ${pf.reentry_s && pf.reentry_s.p50 != null ? pf.reentry_s.p50 + "s" : "—"} · spread ${pf.avg_spread_pts ?? "—"}pt · slippage ${pf.avg_slip_pts ?? "—"}pt</div>` +
        `<div class="tiny">orders/basket ${pf.orders_per_basket ?? "—"} · entries per M1 bar avg ${pf.entries_per_m1_bar?.avg ?? "—"} / max ${pf.entries_per_m1_bar?.max ?? "—"} (${pf.entries_per_m1_bar?.bars_with_2plus ?? 0} bar(s) with 2+) · re-entry p90 ${pf.reentry_s?.p90 ?? "—"}s · protection: ${pf.protection?.be ?? 0} break-even, ${pf.protection?.trail ?? 0} trail move(s), ${pf.protection?.modify_ok ?? 0} SL modify ok (${pf.protection?.verified ?? 0} verified on MT5), ${(pf.protection?.modify_fail ?? 0) + (pf.protection?.unverified ?? 0)} failed/unconfirmed, ${pf.protection?.protect_close ?? 0} basket(s) closed for failed protection · baskets protected ${pf.protection?.baskets_with_be ?? 0} (net ${fmt(pf.protection?.net_be_or_trail)})</div>` +
        `<div class="tiny muted">exits: ${Object.entries(pf.exits || {}).map(([a, b]) => esc(a) + " " + b).join(" · ")}</div>` +
        `<div class="tiny muted">by M1 trend at entry: ${att(pf.attribution?.trend)} | M1 MACD: ${att(pf.attribution?.macd)} | volume: ${att(pf.attribution?.volume)} | tick force: ${att(pf.attribution?.tick)}</div>`
        : `<div class="tiny muted">📊 No closed demo batches yet - no results to show (nothing simulated here).</div>`;
      const safeHtml = rc.safe_mode ? `<div class="tiny neg-v">🛡 ${esc(rc.safe_reason)} · resumes in ${rc.safe_resume_in_s ?? "—"}s (the bot keeps running)</div>`
        : rc.probe_batches_left ? `<div class="tiny muted">🛡 after safe mode: next ${rc.probe_batches_left} batch(es) single min-lot</div>` : "";
      $("engine").insertAdjacentHTML("beforeend", `<details class="small" open><summary>⚡ M1 Rapid Basket Scalper: ${gs.length} open basket(s) · margin used ${rc.margin_used ?? "—"} (${rc.margin_use_pct ?? "—"}% of equity, cap ${rc.max_margin_use_pct}%)${rc.exec_halt ? ' · <span class="neg-v">entries halted: close failures</span>' : ""}</summary>` +
        safeHtml + (gs.length ? gs.map(rowG).join("") : `<div class="tiny muted">no open basket - scanning live ticks for a move that is starting</div>`) + latHtml + pfHtml +
        `<div class="tiny muted">One decision engine, six families scored on the same moment: A tick velocity/acceleration · B tick-activity (count, not real volume) &amp; live candle pressure · C micro-structure · D M1 intrabar trigger (forming-bar body, MACD histogram expansion/turn, RSI level/slope as a soft feature, tick volume, continuation - no RSI/MACD cross or candle-close wait; a trigger family) · F indicator confluence on M5 bars (EMA ribbon 8-55, session VWAP, Bollinger/Keltner squeeze-breakout, ADX/DI, Stochastic RSI, Supertrend, ATR regime, OBV, Donchian break, Heikin-Ashi; weights learned per symbol from real results; no entry straight into a nearby support/resistance level or in chop) · E M5 MAIN MOMENTUM on the forming M5 bar (body expansion, MACD histogram change, relative volume, RSI direction/slope, M5 direction; broker M5 candles every ${rc.candle_poll_s?.M5 ?? "—"}s, M1 every ${rc.candle_poll_s?.M1 ?? "—"}s, forming bars from live ticks - no close or cross wait). AUTO-REVERSAL ${rc.reverse_enabled ? "ON" : "OFF"}: when the opposite side passes its own check and beats the held side by ≥ ${rc.reverse_edge ?? "—"}, the basket is closed, every close verified on MT5, and the opposite batch opens in the same loop (${rc.reversals ?? 0} so far); a failed close never opens the other side. Positive evidence is required for the side entered (absence of BUY is not SELL). BUY and SELL are scored independently every loop (a weak or vetoed side never implies or blocks the other); a side enters when its strength ≥ adaptive threshold ${rc.threshold}${rc.loss_streak_add ? " +" + rc.loss_streak_add + " (loss streak)" : ""} beats the other side by ≥ ${rc.mb_edge} and ≥ ${rc.min_agree} families agree (≥ ${rc.scan_support} each) including a trigger family (C micro-structure, D M1 or E M5 momentum): no single-indicator, speed-only or volume-only entries; anti-chase ≤ ${rc.max_chase_atr} ATR; exhaustion only lowers the score. Continuous intrabar: no cooldowns, no candle lock, no one-trade-per-candle; re-evaluated right after each basket closes and a new basket may open in the same M1 bar; only the exact same signal / order / unconfirmed execution is blocked. Immediate start: evaluated on START after ~1 s of live ticks. Every basket has ${rc.min_orders_per_batch}-${rc.max_orders_per_batch} same-lot orders (never a single order: a symbol whose ${rc.min_orders_per_batch} min lots do not fit the risk budget is skipped) sent in parallel (count grows with the score edge and proven setups), then cut by equity/lot/margin/exposure; batch risk scales 0.6-1.0 with the score; min-lot accounts: as many min lots as fit inside ${rc.small_account_batch_risk_pct}% of equity for ALL open stops, batch risk ≤ ${rc.risk_per_batch_pct}% · ≤ ${rc.max_batch_lots} lots, rest cancelled if price runs past the edge; pyramid ADD (same lot, no multiplier) only after the basket stop is at break-even and the add can share that stop, on a pullback that resumes with the trend${rc.add_enabled ? "" : " (adds OFF)"}; never averaging down, no martingale, no grid. Re-entry right after any close, including a break-even stop.</div>` +
        `<div class="tiny muted">Owner mode (demo, owner request): NO per-order stop loss (locked off). Exits close ONLY IN PROFIT - fast PROFIT LOCK (net gives back ${Math.round((rc.profit_lock_giveback || 0.4) * 100)}% of its peak or nears zero → close at once, before red), zone target, confirmed momentum/reversal (anti-jitter); a red basket is managed by the LOSS GUARDIAN (opposite basket after ${rc.recovery_after_s || "—"} s / ${rc.recovery_loss_pct || "—"}% equity, owner-requested averaging layers ≤ ${rc.avg_max_layers ?? "—"}, lot ×${rc.avg_lot_mult ?? "—"}), and a hedge/recovery cycle closes together once its combined net is in profit. No safe mode, no pauses, no self-stop; only emergency closes: margin level < ${rc.emergency_margin_level_pct || "—"}% or data-feed loss. Margin: ≤ ${rc.batch_margin_pct || "—"}% of equity per batch, ≤ ${rc.nostop_margin_use_pct || "—"}% total. Each close is verified; a closed ticket is never closed twice.</div>` +
        `<div class="tiny muted rc">Hard risk (overrides everything): broker SL ${esc(rc.broker_sl)} (${esc(rc.sl_rule || "")}) · group loss ${rc.group_loss_pct}% of equity · risk/order ${rc.risk_per_trade_pct}% · open risk ≤ ${rc.max_total_risk_pct}% · exposure ≤ ${rc.max_total_notional_x}× equity · max ${rc.max_positions_total} positions (${rc.max_positions_per_symbol}/symbol) · no daily-loss all-day stop · equity drawdown ${rc.equity_drawdown_pct ? rc.equity_drawdown_pct + "%" + (rc.safe_dd_limit != null ? " (now " + fmt(rc.safe_dd_limit) + ", AGGRESSIVE mode requested by the owner)" : "") + " → SAFE MODE (flatten, pause " + Math.round(rc.safe_pause_s / 60) + "+ min, resume with min lot)" : "OFF"} · ≥ ${rc.min_tick_rate} ticks/s · spread ≤ ${rc.max_spread_bps} bp. Depth: not available from this broker. Runtime tuning (no redeploy): ${rc.runtime_tuning && Object.keys(rc.runtime_tuning).length ? esc(Object.entries(rc.runtime_tuning).map(([k, v]) => k + "=" + v).join(", ")) : "defaults"}. No profit guarantee.</div></details>`);
    }
    const lr = eng.learning;
    if (lr) {
      const row = (r) => `<div class="tiny">${esc(r.bucket)} · n ${r.n} · win ${Math.round(r.win * 100)}% · exp ${r.exp_r >= 0 ? "+" : ""}${r.exp_r}R · <b>${esc(r.state)}</b></div>`;
      const last = Object.entries(lr.last || {}).map(([k, v]) => `<div class="tiny">${esc(k)}: ${esc(v)}</div>`).join("");
      $("engine").insertAdjacentHTML("beforeend", `<details class="small" open><summary>🧠 Learning: ${lr.trades} closed group(s) learned · ${lr.buckets} setup(s) · memory ${esc(lr.memory || "local")} · ${esc(lr.decay || "")}</summary>` +
        (last ? `<div class="tiny muted">current size multiplier / gate:</div>${last}` : `<div class="tiny muted">no entry decided yet</div>`) +
        (lr.top && lr.top.length ? `<div class="tiny muted">best buckets</div>${lr.top.map(row).join("")}` : "") +
        (lr.bottom && lr.bottom.length ? `<div class="tiny muted">worst buckets</div>${lr.bottom.map(row).join("")}` : "") +
        `<div class="tiny muted">Setup = symbol | side vs the M1 trend (+swing) | tape | spread. Proven setups get more orders/size, losing ones are mostly skipped. Learns from real closed trades only; no guarantee of profit.</div></details>`);
    }
    const dec = eng.decisions || [];
    if (dec.length) $("engine").insertAdjacentHTML("beforeend", `<details class="small"><summary>Recent decisions (${dec.length})</summary>` +
      dec.slice().reverse().map((d) => `<div class="tiny">${new Date(d.ts * 1000).toLocaleTimeString("en-GB")} ${esc(d.symbol)} ${esc(d.side)} ${d.score ?? ""} · trend ${esc(d.trend)} → ${esc(d.outcome)}</div>`).join("") + `</details>`);
    if (eng.blocked) $("engine").insertAdjacentHTML("afterbegin", `<div class="neg-v small">${esc(eng.blocked)}</div>`);
  }

  function connectWs() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}/api/ws`);
    ws.onopen = () => { wsRetry = 1000; };
    ws.onmessage = (ev) => {
      const m = JSON.parse(ev.data);
      if (m.type === "state") { S.worker_online = m.worker_online; S.snapshot = m.snapshot || {}; S.mode = m.mode; S.cloud = m.cloud; if (m.logs) { logs = []; lastSeq = 0; addLogs(m.logs); } render(); }
      else if (m.type === "logs") addLogs(m.logs);
      else if (m.type === "worker") { S.worker_online = m.worker_online; render(); }
    };
    ws.onclose = () => { setTimeout(async () => {
      try { await api("/api/state"); connectWs(); } catch { /* login shown */ }
    }, wsRetry); wsRetry = Math.min(wsRetry * 2, 15000); };
  }

  async function act(label, fn) {
    $("actMsg").textContent = label + "…";
    try { const r = await fn(); $("actMsg").textContent = (r.ok ? "✔ " : "✖ ") + (r.msg || (r.ok ? "done" : "failed")); return r; }
    catch (e) { $("actMsg").textContent = "✖ " + e.message; return { ok: false }; }
  }

  $("btnConnect").addEventListener("click", () => $("connectPanel").classList.toggle("hidden"));
  $("connectForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const pk = (S.snapshot || {}).pubkey;
    if (!pk) { $("actMsg").textContent = "✖ Launcher has not connected yet - cannot encrypt credentials"; return; }
    if (!window.crypto || !crypto.subtle) { $("actMsg").textContent = "✖ Secure context (HTTPS) required"; return; }
    const der = Uint8Array.from(atob(pk), (c) => c.charCodeAt(0));
    const key = await crypto.subtle.importKey("spki", der, { name: "RSA-OAEP", hash: "SHA-256" }, false, ["encrypt"]);
    const payload = new TextEncoder().encode(JSON.stringify({ login: $("cLogin").value.trim(), password: $("cPass").value, server: $("cServer").value.trim() }));
    const ct = new Uint8Array(await crypto.subtle.encrypt({ name: "RSA-OAEP" }, key, payload));
    $("cPass").value = "";
    const enc = btoa(String.fromCharCode(...ct));
    const r = await act("Connecting MT5", () => cmd("connect", { enc }));
    if (r.ok) $("connectPanel").classList.add("hidden");
  });
  $("btnStart").addEventListener("click", () => act("Starting engine + MT5 (fresh account check, ~60s tick warm-up)", () => cmd("start")));
  $("btnStop").addEventListener("click", () => act("Stopping (closing bot positions, then shutting engine + MT5 down)", () => cmd("stop")));
  $("btnRefresh").addEventListener("click", () => act("Refreshing", () => cmd("refresh")));

  let symOffset = 0, symQuery = "", symAll = [];
  function drawSyms(more) {
    const cur = picked || ((S.snapshot.engine || {}).selected || []);
    $("symList").innerHTML = symAll.map((x) => `<div data-s="${esc(x.name)}" class="${cur.includes(x.name) ? "sel" : ""}"><span><b>${esc(x.name)}</b> <span class="muted tiny">${esc(x.desc || "")}</span></span><span>${cur.includes(x.name) ? "✓" : "+"}</span></div>`).join("")
      + (more ? `<div id="symMore" class="center muted">Load more…</div>` : "") || `<div class="muted">No symbols found</div>`;
  }
  async function search(append) {
    if (!append) { symOffset = 0; symQuery = $("symQ").value.trim(); symAll = []; }
    const r = await act("Searching", () => cmd("search", { q: symQuery, offset: symOffset, limit: 50 }));
    if (!r.ok) { $("symList").innerHTML = `<div class="muted">${esc(r.msg || "search failed - CONNECT MT5 first")}</div>`; return; }
    symAll = symAll.concat(r.symbols || []);
    symOffset += (r.symbols || []).length;
    drawSyms(!!r.has_more);
    $("actMsg").textContent = `✔ ${symAll.length} of ${r.total} symbol(s) - tap to tick, then APPLY`;
  }
  $("btnSearch").addEventListener("click", () => search(false));
  $("symQ").addEventListener("keydown", (e) => { if (e.key === "Enter") search(false); });
  $("symList").addEventListener("click", (e) => {
    if (e.target.closest("#symMore")) { search(true); return; }
    const row = e.target.closest("[data-s]"); if (!row) return;
    const s = row.dataset.s;
    const base = picked || [...((S.snapshot.engine || {}).selected || [])];
    picked = base.includes(s) ? base.filter((x) => x !== s) : [...base, s].slice(0, 6);
    row.classList.toggle("sel"); row.lastElementChild.textContent = picked.includes(s) ? "✓" : "+";
    render();
  });
  $("btnApply").addEventListener("click", async () => {
    const r = await act("Selecting symbols", () => cmd("select", { symbols: picked }));
    if (r.ok) picked = null; render();
  });

  async function boot() {
    try {
      const st = await api("/api/state");
      S.worker_online = st.worker_online; S.snapshot = st.snapshot || {}; S.mode = st.mode; S.cloud = st.cloud;
      logs = []; lastSeq = 0; addLogs(st.logs); render(); hideLogin(); connectWs();
    } catch { /* login */ }
  }
  boot();
})();
