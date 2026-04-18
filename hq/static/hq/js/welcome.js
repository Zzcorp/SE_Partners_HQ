/* S&E Partners HQ — welcome overlay (intro / login / dashboard) */
(() => {
  "use strict";

  const TZ_GREETINGS = {
    "Europe/Paris":     { hello: "Bonjour",   city: "Paris" },
    "Europe/Monaco":    { hello: "Bonjour",   city: "Monaco" },
    "Europe/London":    { hello: "Welcome",   city: "London" },
    "Europe/Berlin":    { hello: "Guten Tag", city: "Berlin" },
    "Europe/Madrid":    { hello: "Hola",      city: "Madrid" },
    "Europe/Rome":      { hello: "Ciao",      city: "Rome" },
    "Europe/Zurich":    { hello: "Grüezi",    city: "Zurich" },
    "Europe/Amsterdam": { hello: "Welkom",    city: "Amsterdam" },
    "Europe/Brussels":  { hello: "Bonjour",   city: "Brussels" },
    "Europe/Lisbon":    { hello: "Olá",       city: "Lisbon" },
    "Europe/Dublin":    { hello: "Welcome",   city: "Dublin" },
    "Europe/Stockholm": { hello: "Välkommen", city: "Stockholm" },
    "Europe/Oslo":      { hello: "Velkommen", city: "Oslo" },
    "Europe/Helsinki":  { hello: "Tervetuloa",city: "Helsinki" },
    "Europe/Warsaw":    { hello: "Witaj",     city: "Warsaw" },
    "Europe/Athens":    { hello: "Γειά σας",   city: "Athens" },
    "Europe/Istanbul":  { hello: "Merhaba",   city: "Istanbul" },
    "Europe/Moscow":    { hello: "Добро пожаловать", city: "Moscow" },
    "America/New_York":     { hello: "Welcome",  city: "New York" },
    "America/Los_Angeles":  { hello: "Welcome",  city: "Los Angeles" },
    "America/Chicago":      { hello: "Welcome",  city: "Chicago" },
    "America/Toronto":      { hello: "Welcome",  city: "Toronto" },
    "America/Sao_Paulo":    { hello: "Olá",       city: "São Paulo" },
    "America/Mexico_City":  { hello: "Hola",      city: "Mexico City" },
    "America/Buenos_Aires": { hello: "Hola",      city: "Buenos Aires" },
    "Asia/Tokyo":        { hello: "ようこそ",  city: "Tokyo" },
    "Asia/Shanghai":     { hello: "欢迎",      city: "Shanghai" },
    "Asia/Hong_Kong":    { hello: "歡迎",      city: "Hong Kong" },
    "Asia/Singapore":    { hello: "Welcome",   city: "Singapore" },
    "Asia/Seoul":        { hello: "환영합니다", city: "Seoul" },
    "Asia/Dubai":        { hello: "مرحبا",      city: "Dubai" },
    "Asia/Jerusalem":    { hello: "ברוך הבא",  city: "Tel Aviv" },
    "Asia/Kolkata":      { hello: "नमस्ते",    city: "Mumbai" },
    "Asia/Bangkok":      { hello: "สวัสดี",    city: "Bangkok" },
    "Australia/Sydney":  { hello: "Welcome",   city: "Sydney" },
    "Africa/Johannesburg":{ hello: "Welcome",  city: "Johannesburg" },
  };
  const LANG_FALLBACK = {
    fr: "Bonjour", es: "Hola", it: "Ciao", de: "Guten Tag", pt: "Olá",
    nl: "Welkom", sv: "Välkommen", no: "Velkommen", fi: "Tervetuloa",
    pl: "Witaj", ja: "ようこそ", zh: "欢迎", ko: "환영합니다",
    ar: "مرحبا", he: "ברוך הבא", ru: "Добро пожаловать", en: "Welcome",
  };
  const INTRO_ROTATIONS = [
    "From the sovereign data floor.",
    "Where intelligence compounds.",
    "Every partner. Everywhere.",
    "The world, in the third dimension.",
  ];

  const setText = (el, v) => { if (el) el.textContent = v; };

  function initWelcome(overlay) {
    if (!overlay || overlay.dataset.welcomeInit === "1") return;
    overlay.dataset.welcomeInit = "1";

    const mode = overlay.dataset.welcomeMode || "intro";
    const username = overlay.dataset.welcomeName || "";

    if (mode === "intro") {
      try {
        if (sessionStorage.getItem("hq_intro_seen") === "1") {
          overlay.remove();
          return;
        }
        sessionStorage.setItem("hq_intro_seen", "1");
      } catch { /* private mode — fall through */ }
    }

    overlay.classList.add("is-active");
    overlay.setAttribute("aria-hidden", "false");
    document.documentElement.classList.add("welcome-lock");

    const $greeting = overlay.querySelector("#welcome-greeting");
    const $subtitle = overlay.querySelector("#welcome-subtitle");
    const $eyebrow  = overlay.querySelector("#welcome-eyebrow");
    const $hint     = overlay.querySelector("#welcome-hint");
    const $bar      = overlay.querySelector("#welcome-bar-fill");

    const tz = (() => {
      try { return Intl.DateTimeFormat().resolvedOptions().timeZone || ""; }
      catch { return ""; }
    })();
    const lang = (navigator.language || "en").slice(0, 2).toLowerCase();

    const tzMatch = TZ_GREETINGS[tz];
    const cityLabel = tzMatch
      ? tzMatch.city
      : (tz.split("/")[1] || "").replace(/_/g, " ") || null;
    const helloWord = tzMatch ? tzMatch.hello : (LANG_FALLBACK[lang] || "Welcome");

    const hour = new Date().getHours();
    const timeOfDay = hour < 6 ? "early" : hour < 12 ? "morning" : hour < 18 ? "afternoon" : "evening";

    let durationMs = 2400;
    let rotate = false;

    if (mode === "intro") {
      setText($greeting, cityLabel ? `${helloWord}, ${cityLabel}.` : `${helloWord}.`);
      setText($eyebrow, `— connecting from ${tz || "the open web"} · ${timeOfDay} —`);
      setText($hint, "initializing intelligence layer…");
      durationMs = 2600;
      rotate = true;
    } else if (mode === "login") {
      const nm = username ? username.charAt(0).toUpperCase() + username.slice(1) : "";
      setText($greeting, nm ? `${helloWord}, ${nm}.` : `${helloWord}.`);
      setText($subtitle, "Unlocking sovereign console…");
      setText($eyebrow, "— access granted —");
      setText($hint, "loading metrics · leads · pipeline");
      durationMs = 2400;
    } else if (mode === "dashboard") {
      const nm = username ? username.charAt(0).toUpperCase() + username.slice(1) : "";
      setText($greeting, nm ? `${helloWord}, ${nm}.` : `${helloWord}.`);
      setText($subtitle, cityLabel ? `${timeOfDay} in ${cityLabel}.` : "The floor is yours.");
      setText($eyebrow, "— command center online —");
      setText($hint, "briefing ready");
      durationMs = 2200;
    }

    if (rotate && $subtitle) {
      let idx = 0;
      const rot = setInterval(() => {
        idx = (idx + 1) % INTRO_ROTATIONS.length;
        $subtitle.classList.add("is-out");
        setTimeout(() => {
          setText($subtitle, INTRO_ROTATIONS[idx]);
          $subtitle.classList.remove("is-out");
        }, 360);
      }, 1200);
      overlay.addEventListener("welcome:close", () => clearInterval(rot), { once: true });
    }

    const startTs = performance.now();
    const tickBar = () => {
      const p = Math.min(1, (performance.now() - startTs) / durationMs);
      if ($bar) $bar.style.width = (p * 100).toFixed(1) + "%";
      if (p < 1) requestAnimationFrame(tickBar);
    };
    requestAnimationFrame(tickBar);

    let closed = false;
    const close = () => {
      if (closed) return;
      closed = true;
      overlay.dispatchEvent(new CustomEvent("welcome:close"));
      overlay.classList.remove("is-active");
      overlay.classList.add("is-closing");
      document.documentElement.classList.remove("welcome-lock");
      setTimeout(() => {
        overlay.remove();
        if (mode === "dashboard" && location.search.includes("welcome=")) {
          const url = new URL(location.href);
          url.searchParams.delete("welcome");
          history.replaceState(null, "", url.pathname + (url.search || "") + url.hash);
        }
      }, 560);
    };

    // In login mode, we let the page navigation close the overlay naturally — no click / keyboard / auto-close.
    if (mode !== "login") {
      overlay.addEventListener("click", close);
      const onKey = (e) => {
        if (e.key === "Escape" || e.key === " " || e.key === "Enter") { close(); window.removeEventListener("keydown", onKey); }
      };
      window.addEventListener("keydown", onKey);
      setTimeout(close, durationMs + 200);
    }

    window.HQ_WELCOME_TRIGGER = { close };
  }

  window.HQ_WELCOME_INIT = initWelcome;

  // Auto-init any overlay already in the DOM.
  const existing = document.getElementById("welcome-overlay");
  if (existing) initWelcome(existing);
})();
