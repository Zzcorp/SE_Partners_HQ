/* The S&E Partners HQ — shared live pipe
   =========================================================================
   Connects every authenticated page to /ws/events so data grows in real time
   across the whole connected-user side: dashboard KPIs, database counters,
   per-run row stats, and a thin global LIVE ribbon while something is firing.

   The /console/ page has its own dedicated client (app.js) — on that page we
   stand down and do nothing, so the two never fight over the same socket.

   Public DOM contract (other scripts can listen):
     window.addEventListener("hq:person",   (e) => e.detail = lead row)
     window.addEventListener("hq:metrics",  (e) => e.detail = metrics obj)
     window.addEventListener("hq:worker",   (e) => e.detail = worker obj)
     window.addEventListener("hq:log",      (e) => e.detail = { line, ts, level })
     window.addEventListener("hq:done",     (e) => e.detail = { run_id })
     window.addEventListener("hq:snapshot", (e) => e.detail = snapshot)
     window.addEventListener("hq:running",  (e) => e.detail = { running })

   Elements the pipe auto-updates if present on the current page:
     [data-live-kpi="total_leads"]      —   numeric KPI cell
     [data-live-kpi="high_value"]       —   numeric KPI cell
     [data-live-kpi="avg_llm"]          —   decimal KPI cell
     [data-live-kpi="total_runs"]       —   numeric KPI cell
     [data-live-kpi="with_email"] …     —   numeric KPI cell
     [data-run-id="<run>"] [data-run-stat="queries_total|people_unique|leads_final"]

   The pipe is side-effect-free until it actually receives data; the server
   snapshot tells us whether a run is active on connect, so counters are
   always aligned with the truth on the wire. */
(() => {
  "use strict";

  // Stand down entirely on the console page — app.js owns the socket there.
  if (document.body.classList.contains("page-console")) return;

  // Require an authenticated user (presence of the nav pill is our signal,
  // same as base.html — anonymous visitors never see it).
  const pill      = document.getElementById("connection-pill");
  const pillLabel = document.getElementById("connection-label");
  if (!pill) return;

  // ---------- helpers ----------
  const fmtInt = (n) => (n || 0).toLocaleString("en-US");
  const toNum  = (v) => {
    const n = typeof v === "number" ? v : parseFloat(String(v).replace(/,/g, ""));
    return Number.isFinite(n) ? n : 0;
  };

  const setPill = (label, cls) => {
    if (pillLabel) pillLabel.textContent = label;
    pill.classList.remove("live", "err");
    if (cls) pill.classList.add(cls);
  };

  /* ---------- KPI glow-on-change -----------------------------------------
     All live-updated numbers pulse a gold halo once so operators see the
     delta without needing a full repaint. CSS is appended below, scoped to
     [data-live-bump]. */
  const bumpCell = (el) => {
    if (!el) return;
    el.setAttribute("data-live-bump", "1");
    // Restart the animation reliably by removing & re-adding on the next frame.
    // eslint-disable-next-line no-unused-expressions
    el.offsetWidth;
    clearTimeout(el._liveBumpT);
    el._liveBumpT = setTimeout(() => el.removeAttribute("data-live-bump"), 900);
  };

  const setKpi = (name, value, { format = "int" } = {}) => {
    const els = document.querySelectorAll(`[data-live-kpi="${name}"]`);
    if (!els.length) return;
    const v = toNum(value);
    const pretty = format === "decimal"
      ? (Math.round(v * 10) / 10).toFixed(1)
      : fmtInt(Math.round(v));
    els.forEach((el) => {
      const prev = el.getAttribute("data-live-value");
      if (prev === pretty) return;
      el.textContent = pretty;
      el.setAttribute("data-live-value", pretty);
      bumpCell(el);
    });
  };

  const setRunStat = (runId, key, value) => {
    if (!runId) return;
    // Match both the full and the 8-char-truncated rendering.
    const short = String(runId).slice(0, 8);
    const selectors = [
      `[data-run-id="${runId}"] [data-run-stat="${key}"]`,
      `[data-run-id="${short}"] [data-run-stat="${key}"]`,
    ];
    selectors.forEach((sel) => {
      document.querySelectorAll(sel).forEach((el) => {
        const pretty = fmtInt(toNum(value));
        if (el.textContent === pretty) return;
        el.textContent = pretty;
        bumpCell(el);
      });
    });
  };

  /* ---------- global LIVE ribbon ------------------------------------------
     A thin strip that slides in while a run is firing and shows
     "LIVE · run xxxxxxxx · N leads · M people". Injected once and reused. */
  let ribbon = null;
  const ensureRibbon = () => {
    if (ribbon) return ribbon;
    ribbon = document.createElement("div");
    ribbon.className = "live-ribbon";
    ribbon.setAttribute("role", "status");
    ribbon.setAttribute("aria-live", "polite");
    ribbon.innerHTML = `
      <span class="live-ribbon-dot" aria-hidden="true"></span>
      <span class="live-ribbon-label mono">LIVE</span>
      <span class="live-ribbon-run mono"></span>
      <span class="live-ribbon-stats mono"></span>
      <a class="live-ribbon-cta mono" href="/console/">open console →</a>
    `;
    document.body.appendChild(ribbon);
    return ribbon;
  };
  const setRibbon = ({ running, runId, leads, people }) => {
    const r = ensureRibbon();
    r.classList.toggle("is-on", !!running);
    const runEl   = r.querySelector(".live-ribbon-run");
    const statsEl = r.querySelector(".live-ribbon-stats");
    if (runEl)   runEl.textContent   = runId ? `· run ${String(runId).slice(0, 8)}` : "";
    const bits = [];
    if (typeof leads  === "number") bits.push(`${fmtInt(leads)} leads`);
    if (typeof people === "number") bits.push(`${fmtInt(people)} ppl`);
    if (statsEl) statsEl.textContent = bits.length ? `· ${bits.join(" · ")}` : "";
  };

  /* ---------- live session state -----------------------------------------
     We track the currently running run locally so per-run row counters can
     tick even when a metrics event doesn't include a run_id payload. */
  const live = {
    running: false,
    runId: null,
    metrics: {},
    seenLeadKeys: new Set(),
    // Running deltas applied to whatever the page was rendered with:
    // the server snapshot is the ground truth we start from, deltas only
    // decorate it so KPIs don't double-count rows we already have.
    deltaLeads: 0,
    deltaHighValue: 0,
    llmSum: 0,
    llmN:   0,
  };

  const leadKey = (l) =>
    `${(l.name || "").toLowerCase()}|${(l.company || "").toLowerCase()}|${(l.role || "").toLowerCase()}`;

  /* ---------- base values read from the server-rendered page -------------
     KPI cells include the value as a data attribute so we can add deltas
     to whatever was there on first paint. We snapshot the baselines ONCE on
     load so subsequent updates don't read back the value we just wrote and
     double-count. */
  const _baselines = {};
  const baselineOf = (name, format = "int") => {
    if (name in _baselines) return _baselines[name];
    const el = document.querySelector(`[data-live-kpi="${name}"]`);
    if (!el) { _baselines[name] = null; return null; }
    const raw = el.getAttribute("data-live-value");
    const val = raw == null ? null : (format === "decimal" ? parseFloat(raw) : toNum(raw));
    _baselines[name] = val;
    return val;
  };

  const applyLeadDelta = (row) => {
    const k = leadKey(row);
    if (live.seenLeadKeys.has(k)) return;  // only count a lead once
    live.seenLeadKeys.add(k);

    live.deltaLeads += 1;
    const baseLeads = baselineOf("total_leads");
    if (baseLeads != null) setKpi("total_leads", baseLeads + live.deltaLeads);

    const baseWeek = baselineOf("leads_this_week");
    if (baseWeek != null) setKpi("leads_this_week", baseWeek + live.deltaLeads);

    if (typeof row.llm_score === "number" && row.llm_score >= 80) {
      live.deltaHighValue += 1;
      const baseHv = baselineOf("high_value");
      if (baseHv != null) setKpi("high_value", baseHv + live.deltaHighValue);
    }
    if (typeof row.llm_score === "number") {
      live.llmSum += row.llm_score; live.llmN += 1;
      const baseAvg = baselineOf("avg_llm", "decimal");
      if (baseAvg != null && live.llmN > 0) {
        // Weighted blend: combine pre-existing avg with streaming contribution.
        // We don't know the original denominator, so we bias toward the stream
        // once it's meaningful (>= 8 scored leads this session).
        const streamAvg = live.llmSum / live.llmN;
        const blended = live.llmN >= 8 ? streamAvg : (baseAvg * 0.7 + streamAvg * 0.3);
        setKpi("avg_llm", blended, { format: "decimal" });
      }
    }

    // Per-run row counters on the database page: bump leads_final optimistically.
    if (live.runId) {
      const short = String(live.runId).slice(0, 8);
      const bEl = document.querySelector(
        `[data-run-id="${live.runId}"] [data-run-stat="leads_final"], [data-run-id="${short}"] [data-run-stat="leads_final"]`
      );
      if (bEl) setRunStat(live.runId, "leads_final", toNum(bEl.textContent) + 1);
    }
  };

  /* ---------- event dispatch ---------------------------------------------
     Every incoming frame is also re-emitted as a CustomEvent so page-specific
     scripts (like database.js) can listen without having to touch the socket. */
  const emit = (name, detail) => {
    window.dispatchEvent(new CustomEvent(name, { detail }));
  };

  const handleEvent = (evt) => {
    const t = evt && evt.type;
    if (!t) return;

    if (t === "snapshot") {
      emit("hq:snapshot", evt.snapshot || {});
      const snap = evt.snapshot || {};
      live.running = !!snap.running;
      live.runId   = snap.run_id || snap.runId || live.runId;
      if (snap.metrics) live.metrics = snap.metrics;
      setRibbon({
        running: live.running,
        runId:   live.runId,
        leads:   snap.metrics && snap.metrics.leads_final,
        people:  snap.metrics && snap.metrics.people_unique,
      });
      emit("hq:running", { running: live.running });
      return;
    }

    if (t === "metrics") {
      const m = evt.metrics || {};
      live.metrics = Object.assign({}, live.metrics, m);
      if (m.run_id) live.runId = m.run_id;
      emit("hq:metrics", m);
      // Per-run row counters on the database page
      if (live.runId) {
        if (typeof m.queries_total === "number") setRunStat(live.runId, "queries_total", m.queries_total);
        if (typeof m.people_unique === "number") setRunStat(live.runId, "people_unique", m.people_unique);
        if (typeof m.leads_final   === "number") setRunStat(live.runId, "leads_final",   m.leads_final);
      }
      setRibbon({
        running: live.running,
        runId:   live.runId,
        leads:   m.leads_final,
        people:  m.people_unique,
      });
      return;
    }

    if (t === "worker") {
      emit("hq:worker", evt.worker || {});
      if (!live.running) {
        live.running = true;
        setRibbon({ running: true, runId: live.runId });
        emit("hq:running", { running: true });
      }
      return;
    }

    if (t === "person") {
      const row = evt.person || {};
      // The publisher doesn't stamp run_id on the row itself; decorate it
      // from the snapshot-tracked run_id so downstream listeners can target
      // the right row on the database page.
      if (row.run_id) live.runId = row.run_id;
      else if (live.runId) row.run_id = live.runId;
      if (!live.running) {
        live.running = true;
        emit("hq:running", { running: true });
      }
      applyLeadDelta(row);
      emit("hq:person", row);
      setRibbon({
        running: true,
        runId: live.runId,
        leads: (live.metrics && live.metrics.leads_final) || null,
        people: (live.metrics && live.metrics.people_unique) || null,
      });
      return;
    }

    if (t === "log") {
      emit("hq:log", { line: evt.line, ts: evt.ts, level: evt.level });
      return;
    }

    if (t === "done") {
      live.running = false;
      setRibbon({ running: false });
      emit("hq:done",    { run_id: evt.run_id || live.runId });
      emit("hq:running", { running: false });
      return;
    }

    if (t === "error") {
      emit("hq:error", { msg: evt.msg || "unknown error" });
      return;
    }
  };

  /* ---------- socket ------------------------------------------------------ */
  const wsProto = location.protocol === "https:" ? "wss" : "ws";
  const wsUrl   = `${wsProto}://${location.host}/ws/events`;
  let ws = null;
  let reconnectDelay = 1000;

  const connect = () => {
    setPill("connecting…");
    try {
      ws = new WebSocket(wsUrl);
    } catch {
      setPill("reconnecting", "err");
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 1.5, 8000);
      return;
    }
    ws.onopen    = () => { setPill("live", "live"); reconnectDelay = 1000; };
    ws.onclose   = () => {
      setPill("reconnecting", "err");
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 1.5, 8000);
    };
    ws.onerror   = () => {};
    ws.onmessage = (ev) => {
      let evt; try { evt = JSON.parse(ev.data); } catch { return; }
      try { handleEvent(evt); } catch (e) { /* never let a handler crash the pipe */ }
    };
  };

  connect();
})();
