// The S&E Partners — landing page: reveal-on-scroll + parallax depth
(function () {
  "use strict";

  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // ------ Reveal on scroll -----------------------------------------------
  const reveals = document.querySelectorAll("[data-reveal]");
  if ("IntersectionObserver" in window && reveals.length) {
    const io = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-in");
          io.unobserve(entry.target);
        }
      });
    }, { threshold: 0.12, rootMargin: "0px 0px -8% 0px" });
    reveals.forEach((el) => io.observe(el));
  } else {
    reveals.forEach((el) => el.classList.add("is-in"));
  }

  // ------ Parallax -------------------------------------------------------
  if (reduceMotion) return;

  const depthNodes = Array.from(document.querySelectorAll("[data-depth]"));
  if (!depthNodes.length) return;

  // Cache each node's section-relative center
  const nodes = depthNodes.map((el) => {
    const section = el.closest("[data-parallax-section]") || el.parentElement;
    return {
      el,
      section,
      depth: parseFloat(el.getAttribute("data-depth")) || 0,
    };
  });

  let ticking = false;
  const viewportH = () => window.innerHeight || document.documentElement.clientHeight;

  function update() {
    const vh = viewportH();
    for (let i = 0; i < nodes.length; i++) {
      const { el, section, depth } = nodes[i];
      if (!section) continue;
      const rect = section.getBoundingClientRect();
      // progress: 0 when section top aligns bottom of viewport, 1 when bottom aligns top
      const total = rect.height + vh;
      const progress = (vh - rect.top) / total; // 0..1 range centered
      const offset = (progress - 0.5) * depth * 120; // pixels
      el.style.transform = `translate3d(0, ${offset.toFixed(2)}px, 0)`;
    }
    ticking = false;
  }

  function onScroll() {
    if (!ticking) {
      window.requestAnimationFrame(update);
      ticking = true;
    }
  }

  window.addEventListener("scroll", onScroll, { passive: true });
  window.addEventListener("resize", onScroll);
  // Prime
  update();
})();
