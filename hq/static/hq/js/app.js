/* The S&E Partners HQ — dashboard client */
(() => {
  "use strict";

  // ---------- helpers ----------
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
  const fmtInt = (n) => (n || 0).toLocaleString("en-US");
  const pad2 = (n) => String(n).padStart(2, "0");
  const tsLog = (d = new Date()) => `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;

  // ---------- parallax on mesh (mouse + scroll) ----------
  const meshes = $$(".parallax-stage .mesh");
  let mx = 0, my = 0, sy = 0;
  const applyParallax = () => {
    meshes.forEach((m, i) => {
      const depth = (i + 1) * 6;
      m.style.transform = `translate3d(${mx * depth}px, ${(my * depth) + (sy * -0.02 * depth)}px, 0)`;
    });
  };
  window.addEventListener("mousemove", (e) => {
    mx = (e.clientX / window.innerWidth - 0.5);
    my = (e.clientY / window.innerHeight - 0.5);
    applyParallax();
  }, { passive: true });
  window.addEventListener("scroll", () => {
    sy = window.scrollY;
    applyParallax();
  }, { passive: true });

  // ---------- clock ----------
  const clockEl = $("#server-clock");
  if (clockEl) {
    setInterval(() => {
      const d = new Date();
      clockEl.textContent = `${d.toISOString().slice(0,10)} · ${tsLog(d)}`;
    }, 1000);
  }

  // only dashboard beyond this point
  if (!document.body.classList.contains("page-dashboard")) return;

  // ---------- state ----------
  const state = {
    running: false,
    workers: {},
    metrics: {
      queries_total: 0, queries_done: 0,
      pages_fetched: 0, pages_ok: 0, pages_error: 0,
      people_found: 0, people_unique: 0, leads_final: 0,
      by_role: {}, by_engine: {}, by_source: {},
    },
    logCount: 0,
    leads: [],
  };

  const els = {
    terminal: $("#terminal"),
    logCount: $("#log-count"),
    mQueries: $("#m-queries"),
    mQueriesTotal: $("#m-queries-total"),
    mPages: $("#m-pages"),
    mPagesOk: $("#m-pages-ok"),
    mPagesErr: $("#m-pages-err"),
    mPeople: $("#m-people"),
    mPeopleUniq: $("#m-people-unique"),
    mLeadsFinal: $("#m-leads-final"),
    workersGrid: $("#workers-grid"),
    bdRoles: $("#bd-roles"),
    bdEngines: $("#bd-engines"),
    bdSources: $("#bd-sources"),
    leadsBody: $("#leads-body"),
    leadsCount: $("#leads-count"),
    runState: $("#run-state"),
    btnStart: $("#btn-start"),
    btnStop: $("#btn-stop"),
    btnClearLog: $("#btn-clear-log"),
    connPill: $("#connection-pill"),
    connLabel: $("#connection-label"),
  };

  // ---------- counter animation ----------
  const animate = (el, from, to, dur = 450) => {
    if (!el) return;
    if (from === to) { el.textContent = fmtInt(to); return; }
    const t0 = performance.now();
    const step = (t) => {
      const p = Math.min(1, (t - t0) / dur);
      const e = 1 - Math.pow(1 - p, 3);
      el.textContent = fmtInt(Math.round(from + (to - from) * e));
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  };

  const setMetric = (el, val) => {
    if (!el) return;
    const prev = parseInt(el.textContent.replace(/,/g, "")) || 0;
    animate(el, prev, val || 0);
  };

  // ---------- terminal ----------
  const MAX_LOG_LINES = 400;
  const appendLog = (tag, msg, ts) => {
    const line = document.createElement("div");
    line.className = "log-line";
    const tsStr = ts ? tsLog(new Date(ts * 1000)) : tsLog();
    const tagCls = (tag || "system").toLowerCase();
    line.innerHTML = `
      <span class="log-ts">${tsStr}</span>
      <span class="log-tag ${tagCls}">${tag || "system"}</span>
      <span class="log-msg"></span>
    `;
    line.querySelector(".log-msg").textContent = msg;
    els.terminal.appendChild(line);
    while (els.terminal.children.length > MAX_LOG_LINES) {
      els.terminal.removeChild(els.terminal.firstChild);
    }
    state.logCount += 1;
    els.logCount.textContent = state.logCount;
    els.terminal.scrollTop = els.terminal.scrollHeight;
  };
  els.btnClearLog.addEventListener("click", () => {
    els.terminal.innerHTML = "";
    state.logCount = 0;
    els.logCount.textContent = "0";
  });

  // ---------- render ----------
  const renderMetrics = (m) => {
    Object.assign(state.metrics, m);
    setMetric(els.mQueries, m.queries_done);
    els.mQueriesTotal.textContent = fmtInt(m.queries_total);
    setMetric(els.mPages, m.pages_fetched);
    els.mPagesOk.textContent = fmtInt(m.pages_ok);
    els.mPagesErr.textContent = fmtInt(m.pages_error);
    setMetric(els.mPeople, m.people_found);
    els.mPeopleUniq.textContent = fmtInt(m.people_unique);
    els.mLeadsFinal.textContent = fmtInt(m.leads_final);

    renderBreakdown(els.bdRoles, m.by_role);
    renderBreakdown(els.bdEngines, m.by_engine);
    renderBreakdown(els.bdSources, m.by_source);
  };

  const renderBreakdown = (ul, counts) => {
    if (!ul) return;
    const entries = Object.entries(counts || {}).sort((a, b) => b[1] - a[1]).slice(0, 8);
    if (!entries.length) { ul.innerHTML = `<li class="bd-item mono">—</li>`; return; }
    ul.innerHTML = entries.map(([k, v]) => `<li class="bd-item"><span>${escapeHtml(k)}</span><b>${fmtInt(v)}</b></li>`).join("");
  };

  const renderWorkers = (workers) => {
    state.workers = workers || {};
    const ids = Object.keys(state.workers);
    if (!ids.length) {
      els.workersGrid.innerHTML = `<div class="worker-placeholder mono">Waiting for run…</div>`;
      return;
    }
    els.workersGrid.innerHTML = ids.map((id) => {
      const w = state.workers[id];
      const busy = w.status === "busy";
      return `
        <div class="worker-card ${busy ? "busy" : "idle"}">
          <div class="worker-head">
            <span>${escapeHtml(id)}</span>
            <span class="worker-stage">${escapeHtml(w.stage || "—")}</span>
          </div>
          <div class="worker-current" title="${escapeHtml(w.current || "")}">${escapeHtml(w.current || "idle")}</div>
          <div class="worker-sub">processed ${fmtInt(w.processed || 0)}</div>
        </div>
      `;
    }).join("");
  };

  const renderLeads = (leads) => {
    state.leads = leads || [];
    if (!state.leads.length) {
      els.leadsBody.innerHTML = `<tr class="empty"><td colspan="6" class="mono">No leads yet. Start a run.</td></tr>`;
      els.leadsCount.textContent = "0";
      return;
    }
    els.leadsBody.innerHTML = state.leads.slice(0, 50).map((l) => {
      const email = (l.emails && l.emails[0]) || (l.email_candidates && l.email_candidates[0]) || "—";
      const linkedin = l.linkedin ? `<a href="${escapeAttr(l.linkedin)}" target="_blank" rel="noopener">view</a>` : "—";
      const score = typeof l.lead_score === "number" ? l.lead_score.toFixed(2) : "—";
      return `
        <tr>
          <td><span class="score-pill">${score}</span></td>
          <td>${escapeHtml(l.name || "")}</td>
          <td>${escapeHtml(l.role || "")}</td>
          <td>${escapeHtml(l.company || "")}</td>
          <td class="mono">${escapeHtml(email)}</td>
          <td>${linkedin}</td>
        </tr>
      `;
    }).join("");
    els.leadsCount.textContent = fmtInt(state.leads.length);
  };

  const setRunState = (running, label) => {
    state.running = !!running;
    els.btnStart.disabled = state.running;
    els.btnStop.disabled = !state.running;
    els.runState.textContent = label || (running ? "running" : "idle");
    els.runState.className = `run-state mono ${label || (running ? "running" : "idle")}`;
  };

  const escapeHtml = (s) => String(s == null ? "" : s)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&#39;");
  const escapeAttr = (s) => escapeHtml(s);

  // ---------- WebSocket ----------
  const wsProto = location.protocol === "https:" ? "wss" : "ws";
  const wsUrl = `${wsProto}://${location.host}/ws/events`;
  let ws = null;
  let reconnectDelay = 1000;

  const setConn = (label, cls) => {
    if (els.connLabel) els.connLabel.textContent = label;
    if (els.connPill) {
      els.connPill.classList.remove("live", "err");
      if (cls) els.connPill.classList.add(cls);
    }
  };

  const connect = () => {
    setConn("connecting…");
    ws = new WebSocket(wsUrl);
    ws.onopen = () => {
      setConn("live", "live");
      reconnectDelay = 1000;
    };
    ws.onclose = () => {
      setConn("reconnecting", "err");
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 1.5, 8000);
    };
    ws.onerror = () => {};
    ws.onmessage = (ev) => {
      let evt;
      try { evt = JSON.parse(ev.data); } catch { return; }
      handleEvent(evt);
    };
  };

  // merge/prune lead into state.leads (dedup on name|company|role)
  const leadKey = (l) => `${(l.name || "").toLowerCase()}|${(l.company || "").toLowerCase()}|${(l.role || "").toLowerCase()}`;
  const upsertLead = (row) => {
    const k = leadKey(row);
    const i = state.leads.findIndex((l) => leadKey(l) === k);
    if (i >= 0) state.leads[i] = Object.assign({}, state.leads[i], row);
    else state.leads.push(row);
    state.leads.sort((a, b) => (b.lead_score || 0) - (a.lead_score || 0));
    if (state.leads.length > 200) state.leads = state.leads.slice(0, 200);
  };

  const parseLogLine = (line) => {
    // Format: "[HH:MM:SS] message"
    const m = String(line || "").match(/^\[(\d\d:\d\d:\d\d)\]\s*(.*)$/);
    if (m) return { ts: m[1], msg: m[2] };
    return { ts: null, msg: String(line || "") };
  };

  const tagFromMsg = (msg, level) => {
    const s = String(msg || "").toLowerCase();
    if (level === "warn" || s.includes("error")) return "error";
    if (s.includes("search")) return "search";
    if (s.includes("fetch")) return "fetch";
    if (s.includes("extract")) return "extract";
    if (s.includes("enrich")) return "enrich";
    if (s.startsWith("▶") || s.includes("démarr") || s.includes("start")) return "system";
    if (s.startsWith("✅") || s.includes("termin") || s.includes("finished")) return "system";
    return "system";
  };

  const handleEvent = (evt) => {
    const t = evt.type;
    if (t === "snapshot") {
      const snap = evt.snapshot || {};
      if (snap.metrics) renderMetrics(snap.metrics);
      if (snap.workers) renderWorkers(snap.workers);
      if (snap.leads) renderLeads(snap.leads);
      setRunState(!!snap.running, snap.running ? "running" : "idle");
      return;
    }
    if (t === "metrics") { renderMetrics(evt.metrics || {}); return; }
    if (t === "worker") {
      const w = evt.worker || {};
      if (w.id) {
        state.workers[w.id] = w;
        renderWorkers(state.workers);
      }
      return;
    }
    if (t === "person") {
      const row = evt.person || {};
      upsertLead(row);
      renderLeads(state.leads);
      return;
    }
    if (t === "log") {
      const { ts, msg } = parseLogLine(evt.line);
      const tag = tagFromMsg(msg, evt.level);
      appendLog(tag, msg, evt.ts);
      return;
    }
    if (t === "done") {
      setRunState(false, "idle");
      appendLog("system", `Run finished · ${evt.run_id || ""}`);
      return;
    }
    if (t === "error") {
      appendLog("error", evt.msg || "unknown error");
      return;
    }
  };

  connect();

  // ---------- controls ----------
  const selected = (sel) => Array.from(sel.selectedOptions).map((o) => o.value);

  $("#start-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    if (state.running) return;
    const payload = {
      categories: selected($("#f-categories")),
      min_priority: +$("#f-min-priority").value || 5,
      max_results_per_query: +$("#f-max-results").value || 10,
      recency_months: +$("#f-recency").value || 12,
      fetch_workers: +$("#f-fetch-workers").value || 3,
      extract_workers: +$("#f-extract-workers").value || 2,
      enrich_workers: +$("#f-enrich-workers").value || 2,
      use_llm: $("#f-use-llm").checked,
      use_team_crawl: $("#f-team-crawl").checked,
      use_email_enrich: $("#f-email-enrich").checked,
      recency_required: $("#f-recency-required").checked,
      pdf_only: $("#f-pdf-only").checked,
      platforms_only: $("#f-platforms-only").checked,
      exclude_platforms: $("#f-exclude-platforms").checked,
    };
    if (!payload.categories.length) delete payload.categories;
    setRunState(true, "starting");
    try {
      const r = await fetch("/api/start", {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRFToken": window.HQ_CSRF || "" },
        body: JSON.stringify(payload),
      });
      if (!r.ok) {
        const j = await r.json().catch(() => ({}));
        appendLog("error", `Start failed: ${j.error || r.status}`);
        setRunState(false, "error");
        return;
      }
      const j = await r.json();
      appendLog("system", `Run ${j.run_id} launched`);
    } catch (err) {
      appendLog("error", `Network error: ${err.message}`);
      setRunState(false, "error");
    }
  });

  $("#btn-stop").addEventListener("click", async () => {
    if (!state.running) return;
    setRunState(true, "stopping");
    try {
      await fetch("/api/stop", {
        method: "POST",
        headers: { "X-CSRFToken": window.HQ_CSRF || "" },
      });
      appendLog("system", "Stop requested");
    } catch (err) {
      appendLog("error", `Stop failed: ${err.message}`);
    }
  });

  // initial disabled state
  setRunState(false, "idle");
})();
