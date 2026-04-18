// The S&E Partners — landing page motion system
// - hero word-split + staggered load-in
// - reveal-on-scroll with stagger children
// - metric counters
// - scroll parallax + pointer drift on hero backdrop
(function () {
  "use strict";

  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // ----- Split hero title into animatable words --------------------------
  // Walks text nodes so <em> / inline spans are preserved.
  function splitWords(container, startIndex) {
    let wi = startIndex;
    const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT, null);
    const textNodes = [];
    let n;
    while ((n = walker.nextNode())) textNodes.push(n);
    textNodes.forEach((tn) => {
      if (!/\S/.test(tn.nodeValue)) return;
      const frag = document.createDocumentFragment();
      tn.nodeValue.split(/(\s+)/).forEach((part) => {
        if (!part) return;
        if (/^\s+$/.test(part)) {
          frag.appendChild(document.createTextNode(part));
          return;
        }
        const word = document.createElement("span");
        word.className = "word";
        const inner = document.createElement("span");
        inner.className = "word-i";
        inner.style.setProperty("--w", wi);
        inner.textContent = part;
        word.appendChild(inner);
        frag.appendChild(word);
        wi++;
      });
      tn.replaceWith(frag);
    });
    return wi;
  }

  const titleLines = document.querySelectorAll(".lnd-title .lnd-title-line");
  if (titleLines.length) {
    let wi = 0;
    titleLines.forEach((line) => { wi = splitWords(line, wi); });
    document.body.classList.add("lnd-title-split");
  }

  // Flip the hero into its ready state — CSS drives the staggered entrance.
  requestAnimationFrame(() => {
    document.documentElement.classList.add("lnd-loaded");
  });

  // ----- Reveal on scroll (single + stagger containers) ------------------
  const revealTargets = document.querySelectorAll("[data-reveal], [data-reveal-stagger]");
  if ("IntersectionObserver" in window && revealTargets.length) {
    const io = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        const el = entry.target;
        if (el.hasAttribute("data-reveal-stagger")) {
          Array.from(el.children).forEach((child, i) => {
            child.style.setProperty("--i", i);
          });
        }
        el.classList.add("is-in");
        io.unobserve(el);
      });
    }, { threshold: 0.12, rootMargin: "0px 0px -6% 0px" });
    revealTargets.forEach((el) => io.observe(el));
  } else {
    revealTargets.forEach((el) => el.classList.add("is-in"));
  }

  // ----- Metric counters -------------------------------------------------
  const metrics = document.querySelectorAll(".metric-n[data-count]");
  if ("IntersectionObserver" in window && metrics.length) {
    const format = (n, decimals) => {
      const s = decimals > 0 ? n.toFixed(decimals) : Math.round(n).toString();
      return s;
    };
    const mio = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        const el = entry.target;
        mio.unobserve(el);
        const target = parseFloat(el.getAttribute("data-count")) || 0;
        const prefix = el.getAttribute("data-prefix") || "";
        const suffix = el.getAttribute("data-suffix") || "";
        const decimals = (el.getAttribute("data-decimals") | 0) || 0;
        const duration = 1600;
        if (reduceMotion) {
          el.innerHTML = prefix + format(target, decimals) + (suffix ? `<span>${suffix}</span>` : "");
          return;
        }
        const start = performance.now();
        const ease = (t) => 1 - Math.pow(1 - t, 3);
        function tick(now) {
          const t = Math.min((now - start) / duration, 1);
          const v = target * ease(t);
          el.innerHTML = prefix + format(v, decimals) + (suffix ? `<span>${suffix}</span>` : "");
          if (t < 1) requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
      });
    }, { threshold: 0.4 });
    metrics.forEach((el) => mio.observe(el));
  }

  if (reduceMotion) return;

  // ----- Scroll parallax -------------------------------------------------
  const depthNodes = Array.from(document.querySelectorAll("[data-depth]")).map((el) => {
    const section = el.closest("[data-parallax-section]") || el.parentElement;
    return { el, section, depth: parseFloat(el.getAttribute("data-depth")) || 0 };
  });

  // ----- Pointer drift on hero backdrop ---------------------------------
  const hero = document.querySelector(".lnd-hero");
  const heroBg = hero ? hero.querySelector(".lnd-hero-bg") : null;
  const backdropNodes = heroBg
    ? Array.from(heroBg.querySelectorAll("[data-depth]")).map((el) => ({
        el,
        depth: parseFloat(el.getAttribute("data-depth")) || 0,
      }))
    : [];
  let pointerX = 0, pointerY = 0, targetPX = 0, targetPY = 0;
  const viewportH = () => window.innerHeight || document.documentElement.clientHeight;
  let ticking = false;
  const backdropSet = new Set(backdropNodes.map((b) => b.el));

  function schedule() {
    if (!ticking) { requestAnimationFrame(frame); ticking = true; }
  }

  if (hero && backdropNodes.length) {
    hero.addEventListener("pointermove", (e) => {
      const r = hero.getBoundingClientRect();
      targetPX = ((e.clientX - r.left) / r.width - 0.5) * 2;
      targetPY = ((e.clientY - r.top) / r.height - 0.5) * 2;
      schedule();
    }, { passive: true });
    hero.addEventListener("pointerleave", () => {
      targetPX = 0; targetPY = 0;
      schedule();
    });
  }

  function frame() {
    // smooth pointer damping
    pointerX += (targetPX - pointerX) * 0.08;
    pointerY += (targetPY - pointerY) * 0.08;

    const vh = viewportH();
    for (let i = 0; i < depthNodes.length; i++) {
      const node = depthNodes[i];
      const section = node.section;
      if (!section) continue;
      const rect = section.getBoundingClientRect();
      const total = rect.height + vh;
      const progress = (vh - rect.top) / total;
      const y = (progress - 0.5) * node.depth * 140;
      node.el.style.setProperty("--py", y.toFixed(2) + "px");

      // pointer drift only on hero backdrop nodes
      if (backdropSet.has(node.el)) {
        const px = pointerX * node.depth * 28;
        const py = pointerY * node.depth * 22;
        node.el.style.setProperty("--px", px.toFixed(2) + "px");
        node.el.style.setProperty("--py", (y + py).toFixed(2) + "px");
      }
    }
    ticking = false;

    // keep animating while pointer damping still settling
    if (Math.abs(targetPX - pointerX) > 0.001 || Math.abs(targetPY - pointerY) > 0.001) {
      requestAnimationFrame(frame);
      ticking = true;
    }
  }

  window.addEventListener("scroll", schedule, { passive: true });
  window.addEventListener("resize", schedule);
  frame();
})();
