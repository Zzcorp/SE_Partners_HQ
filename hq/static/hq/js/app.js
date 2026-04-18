/* The S&E Partners HQ — console client */
(() => {
  "use strict";

  // ---------- helpers ----------
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
  const fmtInt = (n) => (n || 0).toLocaleString("en-US");
  const pad2 = (n) => String(n).padStart(2, "0");
  const tsLog = (d = new Date()) => `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
  const escapeHtml = (s) => String(s == null ? "" : s)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&#39;");
  const escapeAttr = (s) => escapeHtml(s);

  // ---------- parallax on mesh ----------
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
  window.addEventListener("scroll", () => { sy = window.scrollY; applyParallax(); }, { passive: true });

  // ---------- clock ----------
  const clockEl = $("#server-clock");
  if (clockEl) {
    setInterval(() => {
      const d = new Date();
      clockEl.textContent = `${d.toISOString().slice(0,10)} · ${tsLog(d)}`;
    }, 1000);
  }

  // Hide connection pill on non-console pages
  if (!document.body.classList.contains("page-console")) {
    const pill = document.getElementById("connection-pill");
    if (pill) pill.style.display = "none";
    return;
  }

  // ---------- state ----------
  const state = {
    running: false,
    startTs: null,
    workers: {},
    metrics: {
      queries_total: 0, queries_done: 0,
      pages_fetched: 0, pages_ok: 0, pages_error: 0,
      people_found: 0, people_unique: 0, leads_final: 0,
      by_role: {}, by_engine: {}, by_source: {},
    },
    // throughput windows (pages & people timestamps in last 60s)
    tsPages: [],
    tsPeople: [],
    // coverage
    domains: new Set(),
    countries: new Set(),
    emails: 0,
    seniors: 0,
    highValue: 0,
    linkedin: 0,
    logCount: 0,
    leads: [],
    narrationLast: 0,
  };

  const els = {
    terminal: $("#terminal"),
    logCount: $("#log-count"),
    // pipeline stages
    stageSearch: $("#stage-search"), stageSearchTotal: $("#stage-search-total"), stageSearchFill: $("#stage-search-fill"),
    stageFetch: $("#stage-fetch"), stageFetchOk: $("#stage-fetch-ok"), stageFetchErr: $("#stage-fetch-err"), stageFetchFill: $("#stage-fetch-fill"),
    stageExtract: $("#stage-extract"), stageExtractFill: $("#stage-extract-fill"),
    stageEnrich: $("#stage-enrich"), stageFinal: $("#stage-final"), stageEnrichFill: $("#stage-enrich-fill"),
    // progress
    progressFill: $("#progress-fill"), progressPct: $("#progress-pct"), progressDone: $("#progress-done"), progressTotal: $("#progress-total"),
    // narration + throughput
    narration: $("#narration"),
    thrPages: $("#thr-pages"), thrPeople: $("#thr-people"), thrEta: $("#thr-eta"),
    // coverage
    covSources: $("#cov-sources"), covCountries: $("#cov-countries"),
    covEmails: $("#cov-emails"), covSeniors: $("#cov-seniors"),
    covHv: $("#cov-hv"), covLinkedin: $("#cov-linkedin"),
    // workers / breakdown / leads
    workersGrid: $("#workers-grid"),
    bdRoles: $("#bd-roles"), bdEngines: $("#bd-engines"), bdSources: $("#bd-sources"),
    leadsBody: $("#leads-body"), leadsCount: $("#leads-count"),
    // controls
    runState: $("#run-state"),
    btnStart: $("#btn-start"), btnStop: $("#btn-stop"), btnClearLog: $("#btn-clear-log"),
    connPill: $("#connection-pill"), connLabel: $("#connection-label"),
  };

  // ---------- counter tween ----------
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
  const setText = (el, v) => { if (el) el.textContent = v; };
  const setFill = (el, pct) => { if (el) el.style.setProperty("--p", clamp(pct, 0, 100).toFixed(1)); };

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
    while (els.terminal.children.length > MAX_LOG_LINES) els.terminal.removeChild(els.terminal.firstChild);
    state.logCount += 1;
    els.logCount.textContent = state.logCount;
    els.terminal.scrollTop = els.terminal.scrollHeight;
  };
  els.btnClearLog.addEventListener("click", () => {
    els.terminal.innerHTML = "";
    state.logCount = 0;
    els.logCount.textContent = "0";
  });

  // ---------- narration (prominent, above log) ----------
  const setNarration = (tag, msg) => {
    if (!els.narration) return;
    const tagCls = (tag || "system").toLowerCase();
    els.narration.innerHTML = `
      <span class="narr-tag mono narr-tag-${tagCls}"></span>
      <span class="narr-msg"></span>
    `;
    els.narration.querySelector(".narr-tag").textContent = tag || "system";
    els.narration.querySelector(".narr-msg").textContent = msg;
    els.narration.classList.remove("narr-pulse");
    void els.narration.offsetWidth;  // reflow to restart CSS animation
    els.narration.classList.add("narr-pulse");
    state.narrationLast = Date.now();
  };

  // ---------- pipeline / progress / throughput render ----------
  const pruneWindow = (arr, ms = 60000) => {
    const cutoff = Date.now() - ms;
    while (arr.length && arr[0] < cutoff) arr.shift();
  };
  const ratePerMin = (arr) => {
    pruneWindow(arr);
    if (arr.length < 2) return 0;
    const spanMs = Math.max(1, Date.now() - arr[0]);
    return Math.round((arr.length * 60000) / spanMs);
  };

  const fmtDuration = (ms) => {
    if (ms == null || !isFinite(ms) || ms <= 0) return "—";
    const s = Math.round(ms / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ${s % 60}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
  };

  const renderPipeline = () => {
    const m = state.metrics;
    setMetric(els.stageSearch, m.queries_done);
    setText(els.stageSearchTotal, fmtInt(m.queries_total));
    setFill(els.stageSearchFill, m.queries_total ? (m.queries_done * 100 / m.queries_total) : 0);

    setMetric(els.stageFetch, m.pages_fetched);
    setText(els.stageFetchOk, fmtInt(m.pages_ok));
    setText(els.stageFetchErr, fmtInt(m.pages_error));
    const fetchGoal = Math.max(m.queries_total * 10, 50); // visual reference
    setFill(els.stageFetchFill, Math.min(100, m.pages_fetched * 100 / fetchGoal));

    setMetric(els.stageExtract, m.people_found);
    setFill(els.stageExtractFill, m.pages_fetched ? Math.min(100, (m.people_found / Math.max(1, m.pages_fetched)) * 300) : 0);

    setMetric(els.stageEnrich, m.people_unique);
    setText(els.stageFinal, fmtInt(m.leads_final));
    setFill(els.stageEnrichFill, m.people_unique ? Math.min(100, (m.leads_final * 100) / m.people_unique) : 0);

    // Overall progress (queries_done / queries_total)
    const pct = m.queries_total ? (m.queries_done * 100 / m.queries_total) : 0;
    setFill(els.progressFill, pct);
    setText(els.progressPct, Math.round(pct));
    setText(els.progressDone, fmtInt(m.queries_done));
    setText(els.progressTotal, fmtInt(m.queries_total));
  };

  const renderThroughput = () => {
    const pagesPerMin = ratePerMin(state.tsPages);
    const peoplePerMin = ratePerMin(state.tsPeople);
    setText(els.thrPages, pagesPerMin);
    setText(els.thrPeople, peoplePerMin);
    const remaining = Math.max(0, state.metrics.queries_total - state.metrics.queries_done);
    if (!state.running || !state.startTs || state.metrics.queries_done <= 0 || !remaining) {
      setText(els.thrEta, "—");
      return;
    }
    const elapsed = Date.now() - state.startTs;
    const perQuery = elapsed / state.metrics.queries_done;
    setText(els.thrEta, fmtDuration(remaining * perQuery));
  };
  setInterval(renderThroughput, 2000);

  const renderCoverage = () => {
    setMetric(els.covSources, state.domains.size);
    setMetric(els.covCountries, state.countries.size);
    setMetric(els.covEmails, state.emails);
    setMetric(els.covSeniors, state.seniors);
    setMetric(els.covHv, state.highValue);
    setMetric(els.covLinkedin, state.linkedin);
  };

  const renderBreakdown = (ul, counts) => {
    if (!ul) return;
    const entries = Object.entries(counts || {}).sort((a, b) => b[1] - a[1]).slice(0, 8);
    if (!entries.length) { ul.innerHTML = `<li class="bd-item mono">—</li>`; return; }
    ul.innerHTML = entries.map(([k, v]) => `<li class="bd-item"><span>${escapeHtml(k)}</span><b>${fmtInt(v)}</b></li>`).join("");
  };

  const renderMetrics = (m) => {
    Object.assign(state.metrics, m);
    renderPipeline();
    renderBreakdown(els.bdRoles, m.by_role);
    renderBreakdown(els.bdEngines, m.by_engine);
    renderBreakdown(els.bdSources, m.by_source);
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
          <div class="worker-current" title="${escapeAttr(w.current || "")}">${escapeHtml(w.current || "idle")}</div>
          <div class="worker-sub">processed ${fmtInt(w.processed || 0)}</div>
        </div>
      `;
    }).join("");
  };

  const renderLeads = (leads) => {
    state.leads = leads || [];
    if (!state.leads.length) {
      els.leadsBody.innerHTML = `<tr class="empty"><td colspan="7" class="mono">No leads yet. Launch a run.</td></tr>`;
      els.leadsCount.textContent = "0";
      return;
    }
    els.leadsBody.innerHTML = state.leads.slice(0, 50).map((l) => {
      const email = (l.emails && l.emails[0]) || (l.email_candidates && l.email_candidates[0]) || "—";
      const score = typeof l.lead_score === "number" ? l.lead_score.toFixed(2) : "—";
      const llm = (typeof l.llm_score === "number") ? Math.round(l.llm_score) : "—";
      const llmClass = (typeof l.llm_score === "number")
        ? (l.llm_score >= 80 ? "hi" : l.llm_score >= 50 ? "mid" : "lo") : "na";
      const geo = l.country
        ? `<span class="chip mono">${escapeHtml(l.country)}</span>${l.city ? ` <span class="lead-city">${escapeHtml(l.city)}</span>` : ""}`
        : "—";
      const company = l.company_description
        ? `<span title="${escapeAttr(l.company_description)}">${escapeHtml(l.company || "—")}</span>`
        : escapeHtml(l.company || "—");
      return `
        <tr>
          <td><span class="score-pill">${score}</span></td>
          <td><span class="llm-pill llm-${llmClass}">${llm}</span></td>
          <td>${escapeHtml(l.name || "")}</td>
          <td>${escapeHtml(l.role || "")}</td>
          <td>${company}</td>
          <td>${geo}</td>
          <td class="mono">${escapeHtml(email)}</td>
        </tr>
      `;
    }).join("");
    els.leadsCount.textContent = fmtInt(state.leads.length);
  };

  const setRunState = (running, label) => {
    state.running = !!running;
    if (running && !state.startTs) state.startTs = Date.now();
    if (!running) state.startTs = null;
    els.btnStart.disabled = state.running;
    els.btnStop.disabled = !state.running;
    els.runState.textContent = label || (running ? "running" : "idle");
    els.runState.className = `run-state mono ${label || (running ? "running" : "idle")}`;
  };

  // ---------- domain extraction helper ----------
  const domainOf = (url) => {
    try { return new URL(url).hostname.replace(/^www\./, ""); } catch { return ""; }
  };

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
    ws.onopen = () => { setConn("live", "live"); reconnectDelay = 1000; };
    ws.onclose = () => {
      setConn("reconnecting", "err");
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 1.5, 8000);
    };
    ws.onerror = () => {};
    ws.onmessage = (ev) => {
      let evt; try { evt = JSON.parse(ev.data); } catch { return; }
      handleEvent(evt);
    };
  };

  const leadKey = (l) => `${(l.name || "").toLowerCase()}|${(l.company || "").toLowerCase()}|${(l.role || "").toLowerCase()}`;
  const seenLeadKeys = new Set();
  const upsertLead = (row) => {
    const k = leadKey(row);
    const i = state.leads.findIndex((l) => leadKey(l) === k);
    if (i >= 0) state.leads[i] = Object.assign({}, state.leads[i], row);
    else state.leads.push(row);
    state.leads.sort((a, b) => (b.lead_score || 0) - (a.lead_score || 0));
    if (state.leads.length > 200) state.leads = state.leads.slice(0, 200);
    // coverage (count only first time we see this key)
    if (!seenLeadKeys.has(k)) {
      seenLeadKeys.add(k);
      if (row.country) state.countries.add(String(row.country).toUpperCase());
      const dom = domainOf(row.source_url || "");
      if (dom) state.domains.add(dom);
      if ((row.emails && row.emails.length) || (row.email_candidates && row.email_candidates.length)) state.emails += 1;
      if (row.linkedin) state.linkedin += 1;
      const sr = (row.seniority || "").toLowerCase();
      if (sr === "senior" || sr === "exec" || sr === "founder") state.seniors += 1;
      if (typeof row.llm_score === "number" && row.llm_score >= 80) state.highValue += 1;
      state.tsPeople.push(Date.now());
      renderCoverage();
    }
  };

  const parseLogLine = (line) => {
    const m = String(line || "").match(/^\[(\d\d:\d\d:\d\d)\]\s*(.*)$/);
    if (m) return { ts: m[1], msg: m[2] };
    return { ts: null, msg: String(line || "") };
  };

  // Narration ranks: which log tags take over the "NOW" panel
  const NARR_PRIO = { enrich: 5, extract: 4, fetch: 3, search: 2, system: 1, error: 6 };

  const tagFromMsg = (msg, level) => {
    const s = String(msg || "").toLowerCase();
    if (level === "warn" || s.includes("error")) return "error";
    if (s.includes("enrich") || s.includes("llm") || s.includes("claude")) return "enrich";
    if (s.includes("extract") || s.includes("parsed")) return "extract";
    if (s.includes("fetch") || s.includes("fetching") || s.includes("page")) return "fetch";
    if (s.includes("search") || s.includes("query") || s.includes("serp")) return "search";
    return "system";
  };

  const handleEvent = (evt) => {
    const t = evt.type;
    if (t === "snapshot") {
      const snap = evt.snapshot || {};
      if (snap.metrics) renderMetrics(snap.metrics);
      if (snap.workers) renderWorkers(snap.workers);
      if (snap.leads) {
        renderLeads(snap.leads);
        snap.leads.forEach((l) => upsertLead(l));
      }
      setRunState(!!snap.running, snap.running ? "running" : "idle");
      return;
    }
    if (t === "metrics") {
      const prevPages = state.metrics.pages_fetched;
      renderMetrics(evt.metrics || {});
      const newPages = state.metrics.pages_fetched - prevPages;
      for (let i = 0; i < newPages; i++) state.tsPages.push(Date.now());
      return;
    }
    if (t === "worker") {
      const w = evt.worker || {};
      if (w.id) { state.workers[w.id] = w; renderWorkers(state.workers); }
      if (w.current) setNarration(w.stage || "system", w.current);
      return;
    }
    if (t === "person") {
      const row = evt.person || {};
      upsertLead(row);
      renderLeads(state.leads);
      setNarration("extract", `Found ${row.name || "lead"}${row.company ? " · " + row.company : ""}`);
      window.dispatchEvent(new CustomEvent("hq:person", { detail: row }));
      return;
    }
    if (t === "log") {
      const { msg } = parseLogLine(evt.line);
      const tag = tagFromMsg(msg, evt.level);
      appendLog(tag, msg, evt.ts);
      // promote to narration if it's a more specific stage than current
      const lastTag = els.narration && els.narration.querySelector(".narr-tag")
        ? els.narration.querySelector(".narr-tag").textContent : "idle";
      const nowPrio = NARR_PRIO[tag] || 0;
      const lastPrio = NARR_PRIO[lastTag] || 0;
      const stale = Date.now() - state.narrationLast > 6000;
      if (nowPrio >= lastPrio || stale) setNarration(tag, msg);
      return;
    }
    if (t === "done") {
      setRunState(false, "idle");
      appendLog("system", `Run finished · ${evt.run_id || ""}`);
      setNarration("system", `Run finished · ${evt.run_id || ""}`);
      window.dispatchEvent(new CustomEvent("hq:run-done", { detail: { run_id: evt.run_id } }));
      return;
    }
    if (t === "error") {
      appendLog("error", evt.msg || "unknown error");
      setNarration("error", evt.msg || "unknown error");
      return;
    }
  };

  connect();

  // ---------- launcher ----------
  const selected = (sel) => Array.from(sel.selectedOptions).map((o) => o.value);

  const PRESETS = {
    scout:     { min_priority: 7, max_results_per_query: 6,  fetch_workers: 2, extract_workers: 1, enrich_workers: 1 },
    sweep:     { min_priority: 5, max_results_per_query: 10, fetch_workers: 3, extract_workers: 2, enrich_workers: 2 },
    deep:      { min_priority: 3, max_results_per_query: 18, fetch_workers: 5, extract_workers: 3, enrich_workers: 3 },
    sovereign: { min_priority: 2, max_results_per_query: 25, fetch_workers: 9, extract_workers: 6, enrich_workers: 5 },
  };
  let activePreset = "scout";
  const applyPreset = (name) => {
    const p = PRESETS[name] || PRESETS.scout;
    const set = (id, v) => { const el = $(id); if (el) el.value = v; };
    set("#f-min-priority", p.min_priority);
    set("#f-max-results", p.max_results_per_query);
    set("#f-fetch-workers", p.fetch_workers);
    set("#f-extract-workers", p.extract_workers);
    set("#f-enrich-workers", p.enrich_workers);
    activePreset = name;
  };
  applyPreset("scout");
  $$(".preset").forEach((btn) => {
    btn.addEventListener("click", () => {
      const name = btn.dataset.preset || "scout";
      $$(".preset").forEach((b) => {
        const on = b === btn;
        b.classList.toggle("is-active", on);
        b.setAttribute("aria-checked", on ? "true" : "false");
      });
      applyPreset(name);
    });
  });

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
    // reset coverage counters on new run
    state.domains.clear(); state.countries.clear();
    state.emails = 0; state.seniors = 0; state.highValue = 0; state.linkedin = 0;
    seenLeadKeys.clear();
    state.tsPages.length = 0; state.tsPeople.length = 0;
    renderCoverage();
    setNarration("system", `Starting ${activePreset} run…`);
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
        setNarration("error", `Start failed: ${j.error || r.status}`);
        setRunState(false, "error");
        return;
      }
      const j = await r.json();
      appendLog("system", `Run ${j.run_id} launched`);
      setNarration("system", `Run ${j.run_id.slice(0,8)} launched · ${activePreset} preset`);
    } catch (err) {
      appendLog("error", `Network error: ${err.message}`);
      setNarration("error", `Network error: ${err.message}`);
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
      setNarration("system", "Stop requested");
    } catch (err) {
      appendLog("error", `Stop failed: ${err.message}`);
    }
  });

  setRunState(false, "idle");
})();
