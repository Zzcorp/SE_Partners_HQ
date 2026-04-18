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

  // ---------- tab switching ----------
  const TABS = ["overview", "workers", "intel", "map", "log", "leads", "history"];
  const activateTab = (name, opts = {}) => {
    if (!TABS.includes(name)) name = "overview";
    $$(".console-tab").forEach((btn) => {
      const on = btn.dataset.tab === name;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-selected", on ? "true" : "false");
    });
    $$(".console-tab-panel").forEach((p) => {
      p.classList.toggle("is-active", p.dataset.tabPanel === name);
    });
    if (!opts.silent) {
      if (history.replaceState) history.replaceState(null, "", `#${name}`);
      window.dispatchEvent(new CustomEvent("hq:tab-change", { detail: { tab: name } }));
    }
  };
  $$(".console-tab").forEach((btn) => {
    btn.addEventListener("click", () => activateTab(btn.dataset.tab || "overview"));
    btn.addEventListener("keydown", (e) => {
      if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
      const cur = TABS.indexOf(btn.dataset.tab);
      const nxt = (cur + (e.key === "ArrowRight" ? 1 : -1) + TABS.length) % TABS.length;
      const nxtBtn = document.querySelector(`.console-tab[data-tab="${TABS[nxt]}"]`);
      if (nxtBtn) { nxtBtn.focus(); activateTab(TABS[nxt]); }
    });
  });
  const hashTab = (location.hash || "").replace("#", "");
  if (hashTab) activateTab(hashTab, { silent: true });

  // ---------- state ----------
  const state = {
    running: false,
    startTs: null,
    workers: {},
    metrics: {
      queries_total: 0, queries_done: 0,
      pages_fetched: 0, pages_ok: 0, pages_error: 0,
      people_raw: 0, people_unique: 0, leads_final: 0,
      by_role: {}, by_engine: {}, by_source: {}, by_kind: {},
    },
    tsPages: [],
    tsPeople: [],
    sparkPages: new Array(60).fill(0),
    sparkPeople: new Array(60).fill(0),
    // coverage
    domains: new Set(),
    countries: new Set(),
    cities: new Set(),
    countryCounts: {},
    emails: 0,
    seniors: 0,
    highValue: 0,
    linkedin: 0,
    fundsize: 0,
    founders: 0,
    execs: 0,
    // llm
    llmSum: 0,
    llmN: 0,
    logCount: 0,
    leads: [],
    intelCards: [],
    tickerDomains: [],
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
    // status ribbon
    srState: $("#sr-state"), srElapsed: $("#sr-elapsed"),
    srProgressFill: $("#sr-progress-fill"), srPct: $("#sr-pct"),
    srDone: $("#sr-done"), srTotal: $("#sr-total"), srEta: $("#sr-eta"),
    srWorkers: $("#sr-workers"), srPagesRate: $("#sr-pages-rate"), srPeopleRate: $("#sr-people-rate"),
    // tab badges
    ctabWorkers: $("#ctab-workers-badge"), ctabIntel: $("#ctab-intel-badge"),
    ctabMap: $("#ctab-map-badge"), ctabLog: $("#ctab-log-badge"), ctabLeads: $("#ctab-leads-badge"),
    // hero metrics
    hmPages: $("#hm-pages"), hmPagesRate: $("#hm-pages-rate"),
    hmPeople: $("#hm-people"), hmPeopleRate: $("#hm-people-rate"),
    hmUnique: $("#hm-unique"), hmYield: $("#hm-yield"), hmFinal: $("#hm-final"), hmDup: $("#hm-dup"),
    hmLlm: $("#hm-llm"), hmLlmN: $("#hm-llm-n"), hmLlmFill: $("#hm-llm-fill"),
    hmHv: $("#hm-hv"), hmFounder: $("#hm-founder"), hmExec: $("#hm-exec"),
    hmCountries: $("#hm-countries"), hmCities: $("#hm-cities"), hmTopCountry: $("#hm-top-country"),
    sparkPages: $("#spark-pages"), sparkPeople: $("#spark-people"),
    // narration + throughput
    narrTag: $("#narr-tag"), narrMsg: $("#narr-msg"),
    thrPages: $("#thr-pages"), thrPeople: $("#thr-people"), thrEta: $("#thr-eta"), thrElapsed: $("#thr-elapsed"),
    // coverage
    covSources: $("#cov-sources"), covCountries: $("#cov-countries"),
    covEmails: $("#cov-emails"), covSeniors: $("#cov-seniors"),
    covLinkedin: $("#cov-linkedin"), covFundsize: $("#cov-fundsize"),
    // workers / breakdown / leads
    workerMesh: $("#worker-mesh"),
    workersActive: $("#workers-active"), workersIdle: $("#workers-idle"), workersTotal: $("#workers-total"),
    intelStream: $("#intel-stream"), intelCount: $("#intel-count"),
    bdRoles: $("#bd-roles"), bdEngines: $("#bd-engines"), bdSources: $("#bd-sources"), bdKinds: $("#bd-kinds"),
    leadsBody: $("#leads-body"), leadsCount: $("#leads-count"),
    domainTicker: $("#domain-ticker"),
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
    const prev = parseInt(el.textContent.replace(/[^\d-]/g, "")) || 0;
    animate(el, prev, val || 0);
  };
  const setText = (el, v) => { if (el) el.textContent = v; };
  const setPctWidth = (el, pct) => { if (el) el.style.width = clamp(pct, 0, 100).toFixed(1) + "%"; };

  // ---------- terminal ----------
  const MAX_LOG_LINES = 500;
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
    setText(els.logCount, state.logCount);
    setText(els.ctabLog, state.logCount > 999 ? "999+" : state.logCount);
    els.terminal.scrollTop = els.terminal.scrollHeight;
  };
  if (els.btnClearLog) els.btnClearLog.addEventListener("click", () => {
    els.terminal.innerHTML = "";
    state.logCount = 0;
    setText(els.logCount, "0");
    setText(els.ctabLog, "0");
  });

  // ---------- narration ----------
  const setNarration = (tag, msg) => {
    if (!els.narrTag || !els.narrMsg) return;
    els.narrTag.textContent = tag || "system";
    els.narrTag.className = `narr-tag mono narr-tag-${(tag || "system").toLowerCase()}`;
    els.narrMsg.textContent = msg;
    const wrap = els.narrTag.closest(".narration");
    if (wrap) {
      wrap.classList.remove("narr-pulse");
      void wrap.offsetWidth;
      wrap.classList.add("narr-pulse");
    }
    state.narrationLast = Date.now();
  };

  // ---------- throughput windows ----------
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
  const fmtElapsed = (ms) => {
    if (!ms || ms < 0) return "00:00";
    const s = Math.floor(ms / 1000);
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sc = s % 60;
    return (h ? `${pad2(h)}:` : "") + `${pad2(m)}:${pad2(sc)}`;
  };

  // ---------- sparklines ----------
  const drawSpark = (canvas, buf, color) => {
    if (!canvas) return;
    const w = canvas.width, h = canvas.height;
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0, 0, w, h);
    const n = buf.length;
    const max = Math.max(1, ...buf);
    ctx.beginPath();
    for (let i = 0; i < n; i++) {
      const x = (i / (n - 1)) * w;
      const y = h - (buf[i] / max) * (h - 2) - 1;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.4;
    ctx.stroke();
    // fill below
    ctx.lineTo(w, h); ctx.lineTo(0, h); ctx.closePath();
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, color + "55");
    grad.addColorStop(1, color + "00");
    ctx.fillStyle = grad;
    ctx.fill();
  };
  // advance spark buffers every second — count new events in last 1s
  let lastSparkCheck = Date.now();
  let lastPagesCount = 0, lastPeopleCount = 0;
  setInterval(() => {
    const now = Date.now();
    // Count events in the last second window from our ts arrays.
    pruneWindow(state.tsPages);
    pruneWindow(state.tsPeople);
    const pagesLastSec = state.tsPages.filter((t) => t >= now - 1000).length;
    const peopleLastSec = state.tsPeople.filter((t) => t >= now - 1000).length;
    state.sparkPages.push(pagesLastSec); state.sparkPages.shift();
    state.sparkPeople.push(peopleLastSec); state.sparkPeople.shift();
    drawSpark(els.sparkPages, state.sparkPages, "#d4af6c");
    drawSpark(els.sparkPeople, state.sparkPeople, "#7dd3ae");
    lastSparkCheck = now;
  }, 1000);

  // ---------- pipeline / progress / hero / status ribbon render ----------
  const renderPipeline = () => {
    const m = state.metrics;
    setMetric(els.stageSearch, m.queries_done);
    setText(els.stageSearchTotal, fmtInt(m.queries_total));
    setPctWidth(els.stageSearchFill, m.queries_total ? (m.queries_done * 100 / m.queries_total) : 0);

    setMetric(els.stageFetch, m.pages_fetched);
    setText(els.stageFetchOk, fmtInt(m.pages_ok));
    setText(els.stageFetchErr, fmtInt(m.pages_error));
    const fetchGoal = Math.max(m.queries_total * 10, 50);
    setPctWidth(els.stageFetchFill, Math.min(100, m.pages_fetched * 100 / fetchGoal));

    setMetric(els.stageExtract, m.people_raw || 0);
    setPctWidth(els.stageExtractFill, m.pages_fetched ? Math.min(100, ((m.people_raw || 0) / Math.max(1, m.pages_fetched)) * 300) : 0);

    setMetric(els.stageEnrich, m.people_unique);
    setText(els.stageFinal, fmtInt(m.leads_final));
    setPctWidth(els.stageEnrichFill, m.people_unique ? Math.min(100, (m.leads_final * 100) / m.people_unique) : 0);

    const pct = m.queries_total ? (m.queries_done * 100 / m.queries_total) : 0;
    setPctWidth(els.progressFill, pct);
    setText(els.progressPct, Math.round(pct));
    setText(els.progressDone, fmtInt(m.queries_done));
    setText(els.progressTotal, fmtInt(m.queries_total));

    // status ribbon mirror
    setPctWidth(els.srProgressFill, pct);
    setText(els.srPct, Math.round(pct));
    setText(els.srDone, fmtInt(m.queries_done));
    setText(els.srTotal, fmtInt(m.queries_total));
  };

  const renderHeroMetrics = () => {
    const m = state.metrics;
    setMetric(els.hmPages, m.pages_fetched);
    setMetric(els.hmPeople, m.people_raw || 0);
    setMetric(els.hmUnique, m.people_unique);
    const yield_pct = (m.people_raw || 0) > 0
      ? Math.round((m.people_unique / m.people_raw) * 100) : 0;
    setText(els.hmYield, (m.people_raw ? yield_pct : "—"));
    setText(els.hmFinal, fmtInt(m.leads_final));
    setText(els.hmDup, fmtInt(Math.max(0, (m.people_raw || 0) - m.people_unique)));
    const llmAvg = state.llmN ? Math.round(state.llmSum / state.llmN) : null;
    setText(els.hmLlm, llmAvg == null ? "—" : llmAvg);
    setText(els.hmLlmN, fmtInt(state.llmN));
    setPctWidth(els.hmLlmFill, llmAvg == null ? 0 : llmAvg);
    setMetric(els.hmHv, state.highValue);
    setText(els.hmFounder, fmtInt(state.founders));
    setText(els.hmExec, fmtInt(state.execs));
    setMetric(els.hmCountries, state.countries.size);
    setText(els.hmCities, fmtInt(state.cities.size));
    // top country
    const top = Object.entries(state.countryCounts).sort((a, b) => b[1] - a[1])[0];
    setText(els.hmTopCountry, top ? `top · ${top[0]} (${top[1]})` : "—");
  };

  const renderThroughput = () => {
    const pagesPerMin = ratePerMin(state.tsPages);
    const peoplePerMin = ratePerMin(state.tsPeople);
    setText(els.thrPages, pagesPerMin);
    setText(els.thrPeople, peoplePerMin);
    setText(els.hmPagesRate, `${pagesPerMin} /min`);
    setText(els.hmPeopleRate, `${peoplePerMin} /min`);
    setText(els.srPagesRate, pagesPerMin);
    setText(els.srPeopleRate, peoplePerMin);

    const remaining = Math.max(0, state.metrics.queries_total - state.metrics.queries_done);
    if (!state.running || !state.startTs || state.metrics.queries_done <= 0 || !remaining) {
      setText(els.thrEta, "—");
      setText(els.srEta, "—");
    } else {
      const elapsed = Date.now() - state.startTs;
      const perQuery = elapsed / state.metrics.queries_done;
      const eta = fmtDuration(remaining * perQuery);
      setText(els.thrEta, eta);
      setText(els.srEta, eta);
    }

    if (state.running && state.startTs) {
      const e = Date.now() - state.startTs;
      setText(els.thrElapsed, fmtElapsed(e));
      setText(els.srElapsed, fmtElapsed(e));
    } else if (!state.running) {
      setText(els.thrElapsed, "—");
    }
  };
  setInterval(renderThroughput, 1000);

  const renderCoverage = () => {
    setMetric(els.covSources, state.domains.size);
    setMetric(els.covCountries, state.countries.size);
    setMetric(els.covEmails, state.emails);
    setMetric(els.covSeniors, state.seniors);
    setMetric(els.covLinkedin, state.linkedin);
    setMetric(els.covFundsize, state.fundsize);
  };

  const renderBreakdown = (ul, counts) => {
    if (!ul) return;
    const entries = Object.entries(counts || {}).sort((a, b) => b[1] - a[1]).slice(0, 8);
    if (!entries.length) { ul.innerHTML = `<li class="bd-item mono">—</li>`; return; }
    const max = entries[0][1] || 1;
    ul.innerHTML = entries.map(([k, v]) => {
      const pct = Math.max(6, Math.round((v / max) * 100));
      return `<li class="bd-item">
        <span class="bd-k">${escapeHtml(k)}</span>
        <span class="bd-bar-wrap"><span class="bd-bar" style="width:${pct}%"></span></span>
        <b>${fmtInt(v)}</b>
      </li>`;
    }).join("");
  };

  const renderMetrics = (m) => {
    Object.assign(state.metrics, m);
    renderPipeline();
    renderHeroMetrics();
    renderBreakdown(els.bdRoles, m.by_role);
    renderBreakdown(els.bdEngines, m.by_engine);
    renderBreakdown(els.bdSources, m.by_source);
    renderBreakdown(els.bdKinds, m.by_kind);
  };

  // ---------- worker mesh ----------
  const STAGE_ICON = { search: "🔍", fetch: "⬇", extract: "◎", enrich: "✦" };
  const truncate = (s, n) => !s ? "" : (s.length > n ? s.slice(0, n - 1) + "…" : s);

  const renderWorkers = (workers) => {
    state.workers = workers || {};
    const ids = Object.keys(state.workers);
    let busy = 0;
    ids.forEach((id) => { if (state.workers[id].status === "busy" || state.workers[id].status === "working") busy += 1; });
    const idle = Math.max(0, ids.length - busy);

    setText(els.workersActive, busy);
    setText(els.workersIdle, idle);
    setText(els.workersTotal, ids.length);
    setText(els.ctabWorkers, ids.length);
    setText(els.srWorkers, busy);

    if (!els.workerMesh) return;
    if (!ids.length) {
      els.workerMesh.innerHTML = `<div class="worker-placeholder mono">Waiting for run…</div>`;
      return;
    }

    // Stable sort: stage order then id
    const ORDER = { search: 0, fetch: 1, extract: 2, enrich: 3 };
    ids.sort((a, b) => {
      const sa = ORDER[(state.workers[a].stage || "").toLowerCase()] ?? 99;
      const sb = ORDER[(state.workers[b].stage || "").toLowerCase()] ?? 99;
      return sa - sb || a.localeCompare(b);
    });

    els.workerMesh.innerHTML = ids.map((id) => {
      const w = state.workers[id];
      const stage = (w.stage || "").toLowerCase();
      const isBusy = (w.status === "busy" || w.status === "working");
      const currentFull = w.current || "idle";
      const icon = STAGE_ICON[stage] || "·";
      return `
        <article class="wcard ${isBusy ? "is-busy" : "is-idle"}" data-stage="${escapeAttr(stage)}">
          <header class="wcard-head">
            <span class="wcard-id mono">${escapeHtml(id)}</span>
            <span class="wcard-stage mono">
              <span class="wcard-stage-icon">${icon}</span>
              <span>${escapeHtml(stage || "—")}</span>
            </span>
          </header>
          <div class="wcard-current" title="${escapeAttr(currentFull)}">${escapeHtml(truncate(currentFull, 120))}</div>
          <footer class="wcard-foot mono">
            <span class="wcard-status-dot" aria-hidden="true"></span>
            <span>${isBusy ? "working" : "idle"}</span>
            <span class="wcard-processed">processed <b>${fmtInt(w.processed || 0)}</b></span>
          </footer>
        </article>
      `;
    }).join("");
  };

  // ---------- leads table ----------
  const renderLeads = (leads) => {
    state.leads = leads || [];
    setText(els.ctabLeads, state.leads.length);
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

  // ---------- company intel stream ----------
  const SENIORITY_CLS = { founder: "founder", exec: "exec", senior: "senior", mid: "mid", junior: "junior" };
  const MAX_INTEL_CARDS = 40;

  const buildIntelCard = (l) => {
    const hasLlm = typeof l.llm_score === "number";
    const llm = hasLlm ? Math.round(l.llm_score) : null;
    const llmCls = hasLlm ? (llm >= 80 ? "hi" : llm >= 50 ? "mid" : "lo") : "na";
    const sen = (l.seniority || "").toLowerCase();
    const senCls = SENIORITY_CLS[sen] || "mid";
    const domain = (() => {
      try { return l.source_url ? new URL(l.source_url).hostname.replace(/^www\./, "") : ""; } catch { return ""; }
    })();
    const chips = [];
    if (sen) chips.push(`<span class="chip chip-${senCls} mono">${escapeHtml(sen)}</span>`);
    if (l.country) chips.push(`<span class="chip mono">${escapeHtml(l.country)}${l.city ? " · " + escapeHtml(l.city) : ""}</span>`);
    if (l.fund_size) chips.push(`<span class="chip chip-fund mono">${escapeHtml(l.fund_size)}</span>`);
    if (l.fund_close_step) chips.push(`<span class="chip mono">${escapeHtml(l.fund_close_step)}</span>`);

    const reasoning = l.llm_score_reasoning || l.llm_reasoning || "";
    const desc = l.company_description || "";

    return `
      <article class="intel-card intel-card-new" data-intel-key="${escapeAttr((l.name || "") + "|" + (l.company || ""))}">
        <header class="intel-head">
          <div class="intel-who">
            <div class="intel-name">${escapeHtml(l.name || "Unknown")}</div>
            <div class="intel-role mono">${escapeHtml(l.role || "")}${l.company ? " · " + escapeHtml(l.company) : ""}</div>
          </div>
          ${hasLlm ? `<div class="intel-score llm-pill llm-${llmCls}">${llm}</div>` : ""}
        </header>
        ${desc ? `<div class="intel-desc">${escapeHtml(truncate(desc, 220))}</div>` : ""}
        ${reasoning ? `<blockquote class="intel-quote">${escapeHtml(truncate(reasoning, 180))}</blockquote>` : ""}
        <div class="intel-chips">${chips.join("")}</div>
        ${domain ? `<footer class="intel-foot mono"><span class="intel-src">src · ${escapeHtml(domain)}</span>${l.recency_months != null ? `<span>${Math.round(l.recency_months)} mo</span>` : ""}</footer>` : ""}
      </article>
    `;
  };

  const pushIntelCard = (l) => {
    if (!els.intelStream) return;
    if (typeof l.llm_score !== "number" && !l.company_description && !l.llm_score_reasoning) return;
    const key = (l.name || "") + "|" + (l.company || "");
    // De-dupe: if card with same key already exists, move it to top (updated)
    const existingIdx = state.intelCards.findIndex((c) => c.key === key);
    if (existingIdx >= 0) state.intelCards.splice(existingIdx, 1);
    state.intelCards.unshift({ key, data: l });
    if (state.intelCards.length > MAX_INTEL_CARDS) state.intelCards = state.intelCards.slice(0, MAX_INTEL_CARDS);
    // Re-render (simple — list stays short)
    els.intelStream.innerHTML = state.intelCards.map(({ data }) => buildIntelCard(data)).join("");
    setText(els.intelCount, state.intelCards.length);
    setText(els.ctabIntel, state.intelCards.length);
  };

  // ---------- domain ticker ----------
  const MAX_TICKER = 30;
  const pushTickerDomain = (dom) => {
    if (!dom || !els.domainTicker) return;
    // Don't push consecutive duplicates
    if (state.tickerDomains[state.tickerDomains.length - 1] === dom) return;
    state.tickerDomains.push(dom);
    if (state.tickerDomains.length > MAX_TICKER) state.tickerDomains = state.tickerDomains.slice(-MAX_TICKER);
    els.domainTicker.innerHTML = state.tickerDomains
      .map((d) => `<span class="dom-tick">${escapeHtml(d)}</span>`).join(`<span class="dom-sep">·</span>`);
  };

  // ---------- state / run ----------
  const STATE_LABEL = { idle: "IDLE", running: "RUNNING", starting: "STARTING", stopping: "STOPPING", error: "ERROR" };
  const setRunState = (running, label) => {
    state.running = !!running;
    if (running && !state.startTs) state.startTs = Date.now();
    if (!running) state.startTs = null;
    if (els.btnStart) els.btnStart.disabled = state.running;
    if (els.btnStop) els.btnStop.disabled = !state.running;
    const lbl = label || (running ? "running" : "idle");
    setText(els.runState, lbl);
    if (els.runState) els.runState.className = `run-state mono ${lbl}`;
    if (els.srState) {
      els.srState.textContent = STATE_LABEL[lbl] || lbl.toUpperCase();
      els.srState.className = `sr-state-pill sr-state-${lbl}`;
    }
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
  const llmScoredKeys = new Set();

  const upsertLead = (row) => {
    const k = leadKey(row);
    const i = state.leads.findIndex((l) => leadKey(l) === k);
    const prev = i >= 0 ? state.leads[i] : null;
    if (i >= 0) state.leads[i] = Object.assign({}, state.leads[i], row);
    else state.leads.push(row);
    state.leads.sort((a, b) => (b.lead_score || 0) - (a.lead_score || 0));
    if (state.leads.length > 200) state.leads = state.leads.slice(0, 200);

    // coverage on first-seen
    if (!seenLeadKeys.has(k)) {
      seenLeadKeys.add(k);
      if (row.country) {
        const c = String(row.country).toUpperCase();
        state.countries.add(c);
        state.countryCounts[c] = (state.countryCounts[c] || 0) + 1;
      }
      if (row.city) state.cities.add(`${row.country || "??"}:${row.city}`);
      const dom = domainOf(row.source_url || "");
      if (dom) { state.domains.add(dom); pushTickerDomain(dom); }
      if ((row.emails && row.emails.length) || (row.email_candidates && row.email_candidates.length)) state.emails += 1;
      if (row.linkedin) state.linkedin += 1;
      if (row.fund_size) state.fundsize += 1;
      const sr = (row.seniority || "").toLowerCase();
      if (sr === "senior" || sr === "exec" || sr === "founder") state.seniors += 1;
      if (sr === "founder") state.founders += 1;
      if (sr === "exec") state.execs += 1;
      if (typeof row.llm_score === "number" && row.llm_score >= 80) state.highValue += 1;
      state.tsPeople.push(Date.now());
    } else if (prev) {
      // update derived counters if the row upgraded in-place (e.g. enrichment added country or seniority)
      const prevCountry = prev.country;
      const newCountry = row.country;
      if (!prevCountry && newCountry) {
        const c = String(newCountry).toUpperCase();
        state.countries.add(c);
        state.countryCounts[c] = (state.countryCounts[c] || 0) + 1;
      }
      if (!prev.fund_size && row.fund_size) state.fundsize += 1;
      const prevSen = (prev.seniority || "").toLowerCase();
      const newSen = (row.seniority || "").toLowerCase();
      if (prevSen !== newSen) {
        const counted = (s) => ["senior", "exec", "founder"].includes(s);
        if (counted(newSen) && !counted(prevSen)) state.seniors += 1;
        if (newSen === "founder" && prevSen !== "founder") state.founders += 1;
        if (newSen === "exec" && prevSen !== "exec") state.execs += 1;
      }
      const prevHv = typeof prev.llm_score === "number" && prev.llm_score >= 80;
      const newHv = typeof row.llm_score === "number" && row.llm_score >= 80;
      if (newHv && !prevHv) state.highValue += 1;
    }

    // Track LLM avg — count each key once
    if (typeof row.llm_score === "number" && !llmScoredKeys.has(k)) {
      llmScoredKeys.add(k);
      state.llmSum += row.llm_score;
      state.llmN += 1;
    }

    renderCoverage();
    renderHeroMetrics();
    pushIntelCard(row);
  };

  const parseLogLine = (line) => {
    const m = String(line || "").match(/^\[(\d\d:\d\d:\d\d)\]\s*(.*)$/);
    if (m) return { ts: m[1], msg: m[2] };
    return { ts: null, msg: String(line || "") };
  };

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
      // Surface the URL's domain on the ticker if fetch stage
      if ((w.stage || "").toLowerCase() === "fetch" && w.current) {
        const dom = domainOf(w.current);
        if (dom) pushTickerDomain(dom);
      }
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
      const lastTag = els.narrTag ? els.narrTag.textContent : "idle";
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

  const resetCoverage = () => {
    state.domains.clear(); state.countries.clear(); state.cities.clear();
    state.countryCounts = {};
    state.emails = 0; state.seniors = 0; state.highValue = 0;
    state.linkedin = 0; state.fundsize = 0;
    state.founders = 0; state.execs = 0;
    state.llmSum = 0; state.llmN = 0;
    state.intelCards = [];
    state.tickerDomains = [];
    seenLeadKeys.clear();
    llmScoredKeys.clear();
    state.tsPages.length = 0; state.tsPeople.length = 0;
    state.sparkPages.fill(0); state.sparkPeople.fill(0);
    if (els.intelStream) {
      els.intelStream.innerHTML = `<div class="intel-placeholder mono">When the engine enriches a lead, it streams here with full company context.</div>`;
    }
    if (els.domainTicker) els.domainTicker.innerHTML = "";
    setText(els.intelCount, "0");
    setText(els.ctabIntel, "0");
    renderCoverage();
    renderHeroMetrics();
  };

  const startForm = $("#start-form");
  if (startForm) startForm.addEventListener("submit", async (e) => {
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
    resetCoverage();
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
      setNarration("system", `Run ${(j.run_id || "").slice(0,8)} launched · ${activePreset} preset`);
    } catch (err) {
      appendLog("error", `Network error: ${err.message}`);
      setNarration("error", `Network error: ${err.message}`);
      setRunState(false, "error");
    }
  });

  if (els.btnStop) els.btnStop.addEventListener("click", async () => {
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
  renderHeroMetrics();
})();
