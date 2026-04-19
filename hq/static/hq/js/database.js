/* S&E Partners HQ — Database page */
(() => {
  "use strict";
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const esc = (s) => String(s == null ? "" : s)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&#39;");
  const fmt = (n) => (n == null ? "—" : Number(n).toLocaleString("en-US"));

  const cache = new Map();

  const domainOf = (url) => {
    try { return url ? new URL(url).hostname.replace(/^www\./, "") : ""; } catch { return ""; }
  };

  const emailCell = (l) => {
    const e = (l.emails && l.emails.length) ? l.emails : (l.email_candidates || []);
    if (!e.length) return '<span class="db-dim mono">—</span>';
    const first = e[0];
    const rest = e.length > 1 ? ` <span class="db-dim mono">+${e.length - 1}</span>` : "";
    const prov = (l.emails && l.emails.length) ? "confirmed" : "candidate";
    return `<span class="db-email ${prov}">${esc(first)}</span>${rest}`;
  };

  const llmPill = (v) => {
    if (typeof v !== "number") return '<span class="llm-pill llm-na mono">—</span>';
    const cls = v >= 80 ? "hi" : v >= 50 ? "mid" : "lo";
    return `<span class="llm-pill llm-${cls} mono">${Math.round(v)}</span>`;
  };

  const geoCell = (l) => {
    if (!l.country) return '<span class="db-dim mono">—</span>';
    const name = l.country_name || l.country;
    const city = l.city ? ` <span class="db-dim">${esc(l.city)}</span>` : "";
    return `<span class="chip mono">${esc(name)}</span>${city}`;
  };

  const fundCell = (l) => {
    if (!l.fund_size && !l.fund_close_step) return '<span class="db-dim mono">—</span>';
    const size = l.fund_size ? `<b>${esc(l.fund_size)}</b>` : "";
    const step = l.fund_close_step ? ` <span class="db-dim mono">${esc(l.fund_close_step)}</span>` : "";
    return `${size}${step}`;
  };

  const linkedinCell = (l) => {
    if (!l.linkedin) return '<span class="db-dim mono">—</span>';
    return `<a class="db-link mono" href="${esc(l.linkedin)}" target="_blank" rel="noopener">in/…</a>`;
  };

  const sourceCell = (l) => {
    const d = domainOf(l.source_url);
    if (!d) return '<span class="db-dim mono">—</span>';
    return `<a class="db-link mono" href="${esc(l.source_url)}" target="_blank" rel="noopener" title="${esc(l.source_title || '')}">${esc(d)}</a>`;
  };

  const leadRowHtml = (l) => {
    const score = typeof l.lead_score === "number" ? l.lead_score.toFixed(2) : "—";
    return `<tr class="db-lead-row" data-lead-id="${l.id}" data-search="${esc(((l.name || '') + ' ' + (l.company || '') + ' ' + (l.role || '')).toLowerCase())}">
      <td><span class="score-pill">${score}</span></td>
      <td>${llmPill(l.llm_score)}</td>
      <td class="db-name">${esc(l.name || '—')}</td>
      <td>${esc(l.role || '—')}</td>
      <td class="db-company" title="${esc(l.company_description || '')}">${esc(l.company || '—')}</td>
      <td>${geoCell(l)}</td>
      <td>${emailCell(l)}</td>
      <td>${linkedinCell(l)}</td>
      <td>${fundCell(l)}</td>
      <td>${sourceCell(l)}</td>
    </tr>`;
  };

  const loadRunLeads = async (runId, runEl) => {
    const tbody = $("[data-db-leads]", runEl);
    const ph = $("[data-db-placeholder]", runEl);
    const countEl = $("[data-db-count]", runEl);
    if (ph) ph.textContent = "Loading…";

    try {
      let data = cache.get(runId);
      if (!data) {
        const r = await fetch(`/api/runs/${encodeURIComponent(runId)}/leads`, { headers: { "Accept": "application/json" } });
        if (!r.ok) throw new Error("HTTP " + r.status);
        data = await r.json();
        cache.set(runId, data);
      }
      const leads = data.leads || [];
      if (!leads.length) {
        if (tbody) tbody.innerHTML = "";
        if (ph) ph.textContent = "No leads stored for this run.";
        if (countEl) countEl.textContent = "0";
        return;
      }
      if (tbody) tbody.innerHTML = leads.map(leadRowHtml).join("");
      if (ph) ph.hidden = true;
      if (countEl) countEl.textContent = leads.length;
    } catch (e) {
      if (ph) ph.textContent = `Failed to load leads: ${e.message}`;
    }
  };

  // Toggle row expansion
  $$(".db-run").forEach((runEl) => {
    const btn = $("[data-db-toggle]", runEl);
    const body = $(".db-run-body", runEl);
    if (!btn || !body) return;
    btn.addEventListener("click", () => {
      const open = !runEl.classList.contains("is-open");
      runEl.classList.toggle("is-open", open);
      body.hidden = !open;
      if (open && !runEl.dataset.loaded) {
        runEl.dataset.loaded = "1";
        loadRunLeads(runEl.dataset.runId, runEl);
      }
    });
  });

  // Lead filter (per-run)
  $$("[data-db-filter]").forEach((inp) => {
    inp.addEventListener("input", () => {
      const runEl = inp.closest(".db-run");
      const q = inp.value.trim().toLowerCase();
      $$(".db-lead-row", runEl).forEach((tr) => {
        tr.hidden = q ? !(tr.dataset.search || "").includes(q) : false;
      });
    });
  });

  // Click lead row → open detail drawer
  const drawer = $("#lead-drawer");
  const drawerClose = () => {
    if (!drawer) return;
    drawer.hidden = true;
    drawer.setAttribute("aria-hidden", "true");
    document.body.classList.remove("drawer-open");
  };
  const openDrawer = async (leadId) => {
    if (!drawer) return;
    drawer.hidden = false;
    drawer.setAttribute("aria-hidden", "false");
    document.body.classList.add("drawer-open");
    const set = (id, v, wrap) => {
      const el = document.getElementById(id);
      if (el) el.textContent = v == null || v === "" ? "—" : v;
      if (wrap) {
        const w = document.getElementById(wrap);
        if (w) w.hidden = !(v != null && v !== "");
      }
    };
    // Clear first
    ["ld-name","ld-role","ld-company","ld-lead-score","ld-llm-score","ld-seniority",
     "ld-emails","ld-email-candidates","ld-phones","ld-linkedin",
     "ld-country","ld-city","ld-coords","ld-fund-size","ld-fund-step","ld-recency",
     "ld-source","ld-source-title","ld-run","ld-id","ld-created"].forEach((id) => set(id, "…"));

    try {
      const r = await fetch(`/api/leads/${leadId}/`, { headers: { "Accept": "application/json" } });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const l = await r.json();

      set("ld-name", l.name || "—");
      set("ld-role", l.role || "—");
      set("ld-company", l.company || "—");
      set("ld-lead-score", typeof l.lead_score === "number" ? l.lead_score.toFixed(2) : "—");

      const llmEl = document.getElementById("ld-llm-score");
      if (llmEl) llmEl.innerHTML = typeof l.llm_score === "number" ? llmPill(l.llm_score) : "—";

      set("ld-seniority", l.seniority || "—");

      const cp = l.company_profile || {};
      const descText = cp.description || l.company_description || "";
      const descSec = document.getElementById("ld-desc-sec");
      if (descSec) {
        const grid = document.getElementById("ld-company-grid");
        const setCF = (id, v) => {
          const el = document.getElementById(id);
          if (!el) return false;
          const row = el.closest("[data-cf]");
          const has = v != null && v !== "" && v !== 0;
          if (row) row.hidden = !has;
          if (has) el.textContent = v;
          return has;
        };
        document.getElementById("ld-company-desc").textContent = descText;
        document.getElementById("ld-company-desc").hidden = !descText;

        let anyCF = false;
        anyCF = setCF("ld-cf-industry", cp.industry) || anyCF;
        const hq = [cp.hq_city, cp.hq_country_name || cp.hq_country].filter(Boolean).join(", ");
        anyCF = setCF("ld-cf-hq", hq) || anyCF;
        anyCF = setCF("ld-cf-size", cp.size) || anyCF;
        anyCF = setCF("ld-cf-founded", cp.founded) || anyCF;
        anyCF = setCF("ld-cf-aum", cp.aum) || anyCF;
        anyCF = setCF("ld-cf-specialties", cp.specialties) || anyCF;
        const webEl = document.getElementById("ld-cf-website");
        const webRow = webEl ? webEl.closest("[data-cf]") : null;
        if (webEl) {
          if (cp.website) {
            webEl.textContent = cp.website;
            webEl.href = cp.website;
            if (webRow) webRow.hidden = false;
            anyCF = true;
          } else {
            webEl.textContent = "—";
            webEl.removeAttribute("href");
            if (webRow) webRow.hidden = true;
          }
        }
        if (grid) grid.hidden = !anyCF;
        descSec.hidden = !(descText || anyCF);
      }
      const reasonSec = document.getElementById("ld-reason-sec");
      if (reasonSec) {
        reasonSec.hidden = !l.llm_score_reasoning;
        document.getElementById("ld-llm-reason").textContent = l.llm_score_reasoning || "";
      }

      set("ld-emails", (l.emails && l.emails.length) ? l.emails.join(", ") : "—");
      set("ld-email-candidates", (l.email_candidates && l.email_candidates.length) ? l.email_candidates.join(", ") : "—");
      set("ld-phones", (l.phones && l.phones.length) ? l.phones.join(", ") : "—");
      const liEl = document.getElementById("ld-linkedin");
      if (liEl) {
        if (l.linkedin) { liEl.innerHTML = `<a href="${esc(l.linkedin)}" target="_blank" rel="noopener">${esc(l.linkedin)}</a>`; }
        else { liEl.textContent = "—"; }
      }

      set("ld-country", l.country ? `${l.country_name || l.country} (${l.country})` : "—");
      set("ld-city", l.city || "—");
      set("ld-coords", (l.lat != null && l.lng != null) ? `${l.lat.toFixed(3)}, ${l.lng.toFixed(3)}` : "—");

      set("ld-fund-size", l.fund_size || "—");
      set("ld-fund-step", l.fund_close_step || "—");
      set("ld-recency", l.recency_months != null ? `${l.recency_months} mo` : "—");

      set("ld-source", l.source || "—");
      set("ld-source-title", l.source_title || "—");
      const urlEl = document.getElementById("ld-source-url");
      if (urlEl) {
        if (l.source_url) { urlEl.textContent = l.source_url; urlEl.href = l.source_url; }
        else { urlEl.textContent = "—"; urlEl.removeAttribute("href"); }
      }

      const evSec = document.getElementById("ld-evidence-sec");
      if (evSec) {
        evSec.hidden = !l.evidence;
        document.getElementById("ld-evidence").textContent = l.evidence || "";
      }

      set("ld-run", (l.run_id || "").slice(0, 8) || "—");
      set("ld-id", l.id);
      set("ld-created", l.created_at ? new Date(l.created_at).toLocaleString() : "");

      const csvA = document.getElementById("ld-export-csv");
      const xlsxA = document.getElementById("ld-export-xlsx");
      if (l.run_id) {
        if (csvA)  csvA.href  = `/api/runs/${encodeURIComponent(l.run_id)}/export.csv`;
        if (xlsxA) xlsxA.href = `/api/runs/${encodeURIComponent(l.run_id)}/export.xlsx`;
      } else {
        if (csvA)  csvA.removeAttribute("href");
        if (xlsxA) xlsxA.removeAttribute("href");
      }
    } catch (e) {
      set("ld-name", `Failed to load: ${e.message}`);
    }
  };

  document.body.addEventListener("click", (e) => {
    const row = e.target.closest(".db-lead-row");
    if (row && row.dataset.leadId) { openDrawer(row.dataset.leadId); return; }
    if (e.target.closest("[data-drawer-close]")) { drawerClose(); return; }
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && drawer && !drawer.hidden) drawerClose();
  });

  /* ------- live pipe integration ----------------------------------------
     live.js re-emits every incoming WebSocket frame as a DOM CustomEvent.
     Here we use three of them:
       hq:person  → prepend the new lead into its run's open tbody (if any)
                    so operators watch the list grow as enrichment finishes
       hq:done    → invalidate the cached run so the next expand hits the DB
       hq:running → toggle a "live" decoration on the matching run row */
  const findRunEl = (runId) => {
    if (!runId) return null;
    const short = String(runId).slice(0, 8);
    return document.querySelector(`.db-run[data-run-id="${runId}"]`)
        || document.querySelector(`.db-run[data-run-id="${short}"]`);
  };

  const leadKeyOf = (l) =>
    `${(l.name || "").toLowerCase()}|${(l.company || "").toLowerCase()}|${(l.role || "").toLowerCase()}`;

  window.addEventListener("hq:person", (ev) => {
    const l = ev.detail || {};
    if (!l.run_id) return;
    const runEl = findRunEl(l.run_id);
    if (!runEl) return;

    runEl.classList.add("is-live");
    const tbody = $("[data-db-leads]", runEl);
    const ph = $("[data-db-placeholder]", runEl);
    const countEl = $("[data-db-count]", runEl);

    if (!runEl.dataset.loaded || !tbody) return;

    // Dedup by (name|company|role) — enrichment can fire twice for the same lead.
    const key = leadKeyOf(l);
    const existing = $$(".db-lead-row", tbody).find((tr) => {
      const n = (tr.children[2] && tr.children[2].textContent || "").toLowerCase();
      const c = (tr.children[4] && tr.children[4].textContent || "").toLowerCase();
      const r = (tr.children[3] && tr.children[3].textContent || "").toLowerCase();
      return `${n}|${c}|${r}` === key;
    });
    if (existing) {
      // Replace row in place with the freshest render (e.g. gained a score).
      existing.outerHTML = leadRowHtml(Object.assign({ id: existing.dataset.leadId }, l));
    } else {
      const frag = document.createElement("tbody");
      frag.innerHTML = leadRowHtml(Object.assign({ id: l.id || "live" }, l));
      const tr = frag.firstElementChild;
      tr.classList.add("db-lead-row--fresh");
      tbody.insertBefore(tr, tbody.firstChild);
      setTimeout(() => tr.classList.remove("db-lead-row--fresh"), 1600);
      if (countEl) countEl.textContent = String($$(".db-lead-row", tbody).length);
      if (ph) ph.hidden = true;
    }

    // Burn the cache so the next cold expand fetches the full DB row.
    cache.delete(runEl.dataset.runId);
  });

  window.addEventListener("hq:running", (ev) => {
    const running = !!(ev.detail && ev.detail.running);
    if (!running) $$(".db-run.is-live").forEach((el) => el.classList.remove("is-live"));
  });

  window.addEventListener("hq:done", (ev) => {
    const runId = ev.detail && ev.detail.run_id;
    if (!runId) return;
    const runEl = findRunEl(runId);
    if (runEl) {
      runEl.classList.remove("is-live");
      cache.delete(runEl.dataset.runId);
      // If it's currently expanded, refresh with the final server-of-truth.
      if (runEl.classList.contains("is-open")) {
        runEl.dataset.loaded = "";
        loadRunLeads(runEl.dataset.runId, runEl);
      }
    }
  });
})();
