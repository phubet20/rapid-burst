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
    const engTxt = !online ? "ENGINE —" : eng.halted ? "HALTED" : phase === "IDLE" ? "ENGINE OFF" : phase;
    badge($("bEngine"), engTxt, eng.halted ? "bad" : phase === "RUNNING" ? "ok" : "warn");

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
    const canStart = online && (eng.selected || []).length > 0 && (liveOk || storedOk) && !eng.running && !eng.halted && !eng.blocked &&
      !["STARTING", "RUNNING", "STOPPING"].includes(phase);
    $("btnStart").disabled = !canStart;
    const why = [];
    if (!online) why.push(cloud ? "The cloud engine is still starting on the server - wait a few seconds." : "The Windows launcher is not connected.");
    else {
      if (cloud && !(S.cloud || {}).api_key_set) why.push("TICKERALL_API_KEY is not set on Render (Render -> rapid-burst -> Environment).");
      if (eng.running || ["STARTING", "RUNNING"].includes(phase)) why.push("The engine is already running (use STOP first).");
      if (phase === "STOPPING") why.push("STOP is still in progress.");
      if (eng.halted) why.push("Risk halt for today: " + (eng.halt_reason || "limit reached") + ".");
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
      `<td>${esc(x.price_open)}${x.batch ? `<div class="tiny muted">batch ${esc(x.batch)}</div>` : ""}</td><td>${x.sl ? esc(x.sl) : "—"}<div class="tiny ${x.sl_state === "trail" || x.sl_state === "BE+" ? "pos-v" : "muted"}">${esc(x.sl_state === "trail" ? "trailing" : x.sl_state === "BE+" ? "breakeven+" : x.sl_state || "")}</div></td><td class="${x.profit >= 0 ? "pos-v" : "neg-v"}">${x.profit > 0 ? "+" : ""}${fmt(x.profit)}</td><td>${x.age_s ?? "—"}s</td></tr>`).join("")
      : `<tr><td colspan="7" class="muted">No bot positions</td></tr>`;

    const syms = eng.symbols || {};
    const bsl = (eng.risk_config || {}).broker_sl;
    $("engMeta").textContent = (eng.batch && eng.batch.id ? `session ${eng.batch.id} · ` : "") + (bsl ? `Broker SL: ${bsl}` + (bsl === "OFF" ? " (engine hard-close)" : "") : "");
    $("engine").innerHTML = Object.keys(syms).length ? Object.entries(syms).map(([k, v]) => {
      const f = v.features || {};
      const cls = v.signal === "BUY" ? "pos-v" : v.signal === "SELL" ? "neg-v" : "muted";
      const mk = v.market === "live" ? ' <span class="pos-v tiny">● quoting</span>' : v.market === "closed" ? ' <span class="neg-v tiny">● no quotes / market closed</span>' : v.market === "n/a" ? ' <span class="neg-v tiny">● not tradable</span>' : "";
      const tr = v.trend;
      const trHtml = tr ? (tr.known ? ` <span class="tiny ${tr.dir > 0 ? "pos-v" : tr.dir < 0 ? "neg-v" : "muted"}">trend ${tr.dir > 0 ? "▲" : tr.dir < 0 ? "▼" : "▬"} ${esc(tr.label)}</span>`
                                    : ` <span class="tiny muted">trend ?</span>`) : "";
      const sk = v.skips && Object.keys(v.skips).length ? `<div class="tiny muted">why no trade (count): ${Object.entries(v.skips).map(([a, b]) => esc(a) + " " + b).join(" · ")}</div>` : "";
      const im = v.impulse;
      const imHtml = im ? `<div class="tiny ${im.dir && im.strength ? (im.dir > 0 ? "pos-v" : "neg-v") : "muted"}">⚡ ${esc(im.reason)} · bar ${im.bar_range_atr ?? "—"} ATR range, ${im.bar_move_atr ?? "—"} move, flow ${im.pressure ?? "—"} (${im.ticks_w ?? 0} ticks/${im.atr_src || ""} ATR ${im.atr ?? "—"})</div>` : "";
      return `<div class="eng"><b>${esc(k)}</b>${mk}${trHtml} <span class="sig ${cls}">${esc(v.signal)}</span> score ${fmt(v.score, 2)} · <b>${v.signal === "HOLD" ? "waiting for: " : ""}</b>${esc(v.reason)}` + imHtml + sk +
        `<div class="tiny muted">vel ${f.vel ?? "—"} acc ${f.acc ?? "—"} press ${f.press ?? "—"} brk ${f.brk ?? "—"} exp ${f.exp ?? "—"} z ${f.z ?? "—"} · spread ${v.spread_pts ?? "—"}pt · ${f.tps ?? "—"} t/s · tick ${v.tick_age ?? "—"}s ago</div></div>`;
    }).join("") : `<div class="muted small">${eng.halted ? esc(eng.halt_reason) : "Engine idle"}</div>`;
    const rc = eng.risk_config;
    if (rc) $("engine").insertAdjacentHTML("beforeend", `<div class="tiny muted rc">Active limits: broker SL ${rc.broker_sl} (${esc(rc.sl_rule || "")}) · exposure ≤ ${rc.max_total_notional_x}× equity · risk/trade ${rc.risk_per_trade_pct}% · min-lot floor (no minimum balance; max orders now ${rc.tier_legs_now}, tiers ${rc.batch_tiers}) · open risk ≤ ${rc.max_total_risk_pct}% · daily loss ${rc.daily_loss_limit_pct ? rc.daily_loss_limit_pct + "%" : "OFF"} · drawdown stop ${rc.equity_drawdown_pct ? rc.equity_drawdown_pct + "%" : "OFF"} · batch loss cap ${rc.batch_max_loss_pct}% · batch ${rc.batch} orders, TP ${rc.batch_tp_units} vu, max hold ${rc.batch_max_hold_s}s, cooldown ${rc.batch_cooldown_s}s · entry |score| ≥ ${rc.entry_threshold} (counter-trend ≥ ${rc.countertrend_min_score}) · ≥ ${rc.min_tick_rate} ticks/s · spread ≤ ${rc.max_spread_bps} bp</div>`);
    const sc = eng.scalp;
    if (sc && sc.enabled) {
      const its = Object.entries(sc.items || {});
      const rowS = ([k, x]) => `<div class="tiny"><b>${esc(x.symbol)} ${esc(x.side)}</b> group ${esc(k)} · ${x.orders}/${sc.max_orders} orders · ${fmt(x.volume, 2)} lots · ` +
        `floating <b class="${x.net >= 0 ? "pos-v" : "neg-v"}">${x.net > 0 ? "+" : ""}${fmt(x.net)}</b> · adds ${x.adds}` +
        (x.rev ? ` · reversal: ${["vel_flip", "decay", "press", "wick"].filter((q) => x.rev[q]).join("+") || "none"}` : "") + `</div>`;
      $("engine").insertAdjacentHTML("beforeend", `<details class="small" open><summary>⚡ Scalp: margin used ${sc.margin_used ?? "—"} (${sc.margin_use_pct ?? "—"}% of equity, cap ${sc.max_margin_use_pct}%) · up to ${sc.max_orders} small orders, ${sc.add_interval}s apart</summary>` +
        (its.length ? its.map(rowS).join("") : `<div class="tiny muted">no open scalp group - waiting for an impulse at its start</div>`) +
        `<div class="tiny muted">Early entry only (refuses if > ${sc.max_bar_atr} ATR from the bar open or > ${sc.max_swing_atr} ATR from the swing, or decelerating) · hold until in profit, then close on reversal / quick TP ${sc.tp_atr} ATR · no time stop · emergency: group loss stop ${sc.group_loss_pct}% of equity + far broker SL. Higher risk by choice.</div></details>`);
    }
    const bk = eng.baskets;
    if (bk && bk.enabled) {
      const its = Object.entries(bk.items || {});
      const rowB = ([k, x]) => `<div class="tiny"><b>${esc(x.symbol)} ${esc(x.side)}</b> basket ${esc(k)} · ${x.orders}/${bk.max_orders} orders · ${fmt(x.volume, 2)} lots · ` +
        `floating <b class="${x.net >= 0 ? "pos-v" : "neg-v"}">${x.net > 0 ? "+" : ""}${fmt(x.net)}</b> · target +${fmt(x.target)}` +
        (x.armed ? ` · <span class="pos-v">trailing: peak +${fmt(x.peak)}, closes ≤ +${fmt(x.trail_floor)}</span>` : "") +
        ` · loss stop −${fmt(x.loss_limit)} (${bk.loss_pct}% of equity at start ${fmt(x.eq0)}) · adds ${x.adds}</div>`;
      $("engine").insertAdjacentHTML("beforeend", `<details class="small" open><summary>🧺 Baskets: margin used ${bk.margin_used ?? "—"} / cap ${bk.margin_cap ?? "—"} (${bk.margin_use_pct ?? "—"}% of equity, max ${bk.max_margin_use_pct}%)</summary>` +
        (its.length ? its.map(rowB).join("") : `<div class="tiny muted">no open basket</div>`) +
        `<div class="tiny muted">Orders average against / pyramid with the move, all close together at +${bk.target_pct}% of equity with profit trailed (${Math.round(bk.trail_frac * 100)}% give-back); no tight SL (far broker SL only). Higher risk by choice.</div></details>`);
    }
    const lr = eng.learning;
    if (lr) {
      const row = (r) => `<div class="tiny">${esc(r.bucket)} · n ${r.n} · win ${Math.round(r.win * 100)}% · exp ${r.exp_r >= 0 ? "+" : ""}${r.exp_r}R · <b>${esc(r.state)}</b></div>`;
      const last = Object.entries(lr.last || {}).map(([k, v]) => `<div class="tiny">${esc(k)}: ${esc(v)}</div>`).join("");
      $("engine").insertAdjacentHTML("beforeend", `<details class="small" open><summary>🧠 Learning: ${lr.trades} closed burst(s) learned · ${lr.buckets} bucket(s) (cold until ${lr.min_samples} trades: min lot + fast cut)</summary>` +
        (last ? `<div class="tiny muted">current size multiplier / gate:</div>${last}` : `<div class="tiny muted">no entry decided yet</div>`) +
        (lr.top && lr.top.length ? `<div class="tiny muted">best buckets</div>${lr.top.map(row).join("")}` : "") +
        (lr.bottom && lr.bottom.length ? `<div class="tiny muted">worst buckets</div>${lr.bottom.map(row).join("")}` : "") +
        `<div class="tiny muted">Re-weights from real closed trades only; no guarantee of profit.</div></details>`);
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
