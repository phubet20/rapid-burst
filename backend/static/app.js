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
    const canStart = online && (liveOk || storedOk) && !eng.running && !eng.halted && !eng.blocked &&
      !["STARTING", "RUNNING", "STOPPING"].includes(phase);
    $("btnStart").disabled = !canStart;
    $("btnStop").disabled = false;
    $("btnConnect").textContent = conn.state === "CONNECTED" ? "RECONNECT MT5" : "CONNECT MT5";

    $("checks").innerHTML = (val.checks || []).map((c) =>
      `<li><span>${c.ok ? "✅" : "❌"}</span><span>${esc(c.name)}</span><span class="d">${esc(c.detail)}</span></li>`).join("")
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
      `<td>${esc(x.price_open)}</td><td class="${x.profit >= 0 ? "pos-v" : "neg-v"}">${x.profit > 0 ? "+" : ""}${fmt(x.profit)}</td><td>${x.age_s ?? "—"}s</td></tr>`).join("")
      : `<tr><td colspan="6" class="muted">No bot positions</td></tr>`;

    const syms = eng.symbols || {};
    $("engMeta").textContent = eng.batch && eng.batch.id ? `batch ${eng.batch.id}` : "";
    $("engine").innerHTML = Object.keys(syms).length ? Object.entries(syms).map(([k, v]) => {
      const f = v.features || {};
      const cls = v.signal === "BUY" ? "pos-v" : v.signal === "SELL" ? "neg-v" : "muted";
      return `<div class="eng"><b>${esc(k)}</b> <span class="sig ${cls}">${esc(v.signal)}</span> score ${fmt(v.score, 2)} · ${esc(v.reason)}` +
        `<div class="tiny muted">vel ${f.vel ?? "—"} acc ${f.acc ?? "—"} press ${f.press ?? "—"} brk ${f.brk ?? "—"} exp ${f.exp ?? "—"} z ${f.z ?? "—"} · spread ${v.spread_pts ?? "—"}pt · ${f.tps ?? "—"} t/s · tick ${v.tick_age ?? "—"}s ago</div></div>`;
    }).join("") : `<div class="muted small">${eng.halted ? esc(eng.halt_reason) : "Engine idle"}</div>`;
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

  async function search() {
    const r = await act("Searching", () => cmd("search", { q: $("symQ").value.trim() }));
    const list = r.symbols || [];
    const cur = picked || ((S.snapshot.engine || {}).selected || []);
    $("symList").innerHTML = list.map((x) => `<div data-s="${esc(x.name)}" class="${cur.includes(x.name) ? "sel" : ""}"><span><b>${esc(x.name)}</b> <span class="muted tiny">${esc(x.desc)}</span></span><span>${cur.includes(x.name) ? "✓" : "+"}</span></div>`).join("")
      || `<div class="muted">${r.ok ? "No symbols found" : ""}</div>`;
    if (r.ok) $("actMsg").textContent = `✔ ${list.length} symbol(s)`;
  }
  $("btnSearch").addEventListener("click", search);
  $("symQ").addEventListener("keydown", (e) => { if (e.key === "Enter") search(); });
  $("symList").addEventListener("click", (e) => {
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
