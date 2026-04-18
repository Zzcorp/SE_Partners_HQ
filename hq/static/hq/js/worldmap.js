/* S&E Partners HQ — world vision map */
(() => {
  "use strict";
  const el = document.getElementById("world-map");
  if (!el || typeof L === "undefined") return;

  const GEO_URL = window.HQ_GEO_URL || "/api/leads/geo";

  const map = L.map(el, {
    zoomControl: true,
    worldCopyJump: true,
    attributionControl: false,
    minZoom: 2,
    maxZoom: 9,
    zoomSnap: 0.25,
  }).setView([28, 12], 2.25);

  // CartoDB dark-matter : dark tiles, minimal label noise, good for our palette
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png", {
    subdomains: "abcd",
    maxZoom: 9,
  }).addTo(map);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png", {
    subdomains: "abcd",
    maxZoom: 9,
    opacity: 0.45,
  }).addTo(map);

  const pointsLayer = L.layerGroup().addTo(map);
  const countryLayer = L.layerGroup().addTo(map);

  const tooltip = document.getElementById("map-tooltip");
  const $placed = document.getElementById("map-placed");
  const $countries = document.getElementById("map-countries");

  const fmt = (n) => (n == null ? "—" : (Number.isFinite(+n) ? (+n).toLocaleString("en-US") : n));
  const esc = (s) => String(s == null ? "" : s)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

  const showTooltip = (ev, html) => {
    if (!tooltip) return;
    tooltip.innerHTML = html;
    tooltip.hidden = false;
    const rect = el.getBoundingClientRect();
    const x = ev.originalEvent ? ev.originalEvent.clientX : ev.clientX;
    const y = ev.originalEvent ? ev.originalEvent.clientY : ev.clientY;
    const px = Math.min(Math.max(x - rect.left + 14, 8), rect.width - 280);
    const py = Math.min(Math.max(y - rect.top + 14, 8), rect.height - 140);
    tooltip.style.transform = `translate(${px}px, ${py}px)`;
  };
  const hideTooltip = () => { if (tooltip) tooltip.hidden = true; };
  el.addEventListener("mouseleave", hideTooltip);

  const scoreClass = (s) => {
    if (typeof s !== "number") return "lo";
    if (s >= 80) return "hi";
    if (s >= 50) return "mid";
    return "lo";
  };

  const dotIcon = (klass) => L.divIcon({
    className: `map-dot map-dot-${klass}`,
    iconSize: [14, 14],
    iconAnchor: [7, 7],
  });

  const seenPointKey = new Set();
  const keyOf = (p) => `${(p.name || "").toLowerCase()}|${(p.company || "").toLowerCase()}|${p.lat}|${p.lng}`;

  const dropPoint = (p) => {
    if (p.lat == null || p.lng == null) return;
    const k = keyOf(p);
    if (seenPointKey.has(k)) return;
    seenPointKey.add(k);

    const klass = scoreClass(p.llm_score);
    // Jitter lat/lng slightly when it's a country-centroid fallback (many points collide).
    const jitter = (p.city ? 0 : (Math.random() - 0.5) * 0.6);
    const lat = p.lat + jitter;
    const lng = p.lng + jitter;
    const m = L.marker([lat, lng], { icon: dotIcon(klass), keyboard: false });

    m.on("mouseover", (ev) => {
      showTooltip(ev, `
        <div class="tt-head">
          <span class="tt-name">${esc(p.name || "—")}</span>
          ${p.llm_score != null ? `<span class="tt-score tt-score-${klass}">${Math.round(p.llm_score)}</span>` : ""}
        </div>
        <div class="tt-role">${esc(p.role || "")}${p.company ? ` · <b>${esc(p.company)}</b>` : ""}</div>
        ${p.company_description ? `<div class="tt-desc">${esc(p.company_description)}</div>` : ""}
        <div class="tt-meta mono">
          ${p.seniority ? `<span class="chip chip-${esc(p.seniority)}">${esc(p.seniority)}</span>` : ""}
          ${p.country ? `<span class="chip">${esc(p.country_name || p.country)}</span>` : ""}
          ${p.city ? `<span class="chip">${esc(p.city)}</span>` : ""}
          ${p.fund_size ? `<span class="chip">${esc(p.fund_size)}</span>` : ""}
        </div>
        ${p.llm_score_reasoning ? `<div class="tt-reason">${esc(p.llm_score_reasoning)}</div>` : ""}
      `);
    });
    m.on("mousemove", (ev) => showTooltip(ev, tooltip.innerHTML));
    m.on("mouseout", hideTooltip);
    m.addTo(pointsLayer);
  };

  const radiusFor = (count) => 6 + Math.min(28, Math.sqrt(count) * 3);

  const renderCountries = (countries) => {
    countryLayer.clearLayers();
    let rendered = 0;
    countries.forEach((c) => {
      if (c.lat == null || c.lng == null) return;
      rendered += 1;
      const circle = L.circleMarker([c.lat, c.lng], {
        radius: radiusFor(c.count),
        weight: 1,
        color: "rgba(212,175,108,0.55)",
        fillColor: "rgba(212,175,108,0.18)",
        fillOpacity: 0.65,
        interactive: true,
      });
      circle.on("mouseover", (ev) => {
        showTooltip(ev, `
          <div class="tt-head"><span class="tt-name">${esc(c.name)}</span><span class="tt-score">${c.count}</span></div>
          <div class="tt-meta mono">
            ${c.avg_llm != null ? `<span class="chip">avg LLM ${c.avg_llm}</span>` : ""}
            ${c.top_llm != null ? `<span class="chip">top ${Math.round(c.top_llm)}</span>` : ""}
            ${c.avg_lead != null ? `<span class="chip">lead ${c.avg_lead}</span>` : ""}
          </div>
        `);
      });
      circle.on("mousemove", (ev) => showTooltip(ev, tooltip.innerHTML));
      circle.on("mouseout", hideTooltip);
      circle.addTo(countryLayer);
    });
    if ($countries) $countries.textContent = rendered;
  };

  const renderPoints = (points) => {
    pointsLayer.clearLayers();
    seenPointKey.clear();
    points.forEach(dropPoint);
    if ($placed) $placed.textContent = seenPointKey.size;
  };

  const load = async () => {
    try {
      const r = await fetch(GEO_URL, { headers: { "Accept": "application/json" } });
      if (!r.ok) return;
      const j = await r.json();
      renderCountries(j.countries || []);
      renderPoints(j.points || []);
    } catch (e) {
      // silent: map stays empty
    }
  };

  // Live updates during a run
  window.addEventListener("hq:person", (ev) => {
    const p = ev.detail || {};
    // The scraper pipeline gives us country/city, not lat/lng directly — we'd need
    // a centroid lookup client-side for the live feed. For now we only drop the
    // point if lat/lng already resolved. A full refresh happens on "done".
    if (p.lat != null && p.lng != null) {
      dropPoint(p);
      if ($placed) $placed.textContent = seenPointKey.size;
    }
  });

  // Refresh after a run finishes (person rows persisted with centroids server-side)
  let refreshTimer = null;
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") load();
  });
  window.addEventListener("hq:run-done", () => {
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(load, 1200);
  });

  // When the Map tab becomes active, Leaflet needs to remeasure the container
  // (it was rendered at size 0 while hidden).
  window.addEventListener("hq:tab-change", (ev) => {
    if ((ev.detail || {}).tab === "map") {
      setTimeout(() => { try { map.invalidateSize(); } catch {} }, 60);
    }
  });

  load();
})();
