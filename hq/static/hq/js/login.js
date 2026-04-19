/* S&E Partners HQ — stepped login (username → password → PIN) with shrink/fade on success */
(() => {
  "use strict";
  const form = document.getElementById("login-form");
  const card = document.getElementById("login-card");
  if (!form || !card) return;

  const stages = Array.from(form.querySelectorAll(".login-stage"));
  const dots = Array.from(document.querySelectorAll(".login-step"));
  const btnNext = document.getElementById("btn-step-next");
  const btnSubmit = document.getElementById("btn-step-submit");

  const $user = document.getElementById("f-username");
  const $pass = document.getElementById("f-password");
  const $pinHidden = document.getElementById("f-pin");
  const pinBoxes = Array.from(form.querySelectorAll(".pin-box"));

  const getCsrf = () => {
    const el = form.querySelector("input[name=csrfmiddlewaretoken]");
    return el ? el.value : "";
  };

  const removeError = () => {
    const e = form.querySelector(".form-error");
    if (e) e.remove();
  };
  const showError = (msg) => {
    removeError();
    const div = document.createElement("div");
    div.className = "form-error";
    div.textContent = msg || "Identifiants invalides.";
    const actions = form.querySelector(".login-actions");
    if (actions) form.insertBefore(div, actions);
    else form.appendChild(div);
  };

  // ---- Step state ------------------------------------------------------
  let current = 1;
  const TOTAL = stages.length;

  const focusStage = (n) => {
    const stage = stages[n - 1];
    if (!stage) return;
    if (n === 3) {
      const firstEmpty = pinBoxes.find((b) => !b.value) || pinBoxes[0];
      firstEmpty && firstEmpty.focus();
    } else {
      const input = stage.querySelector("input");
      input && input.focus();
    }
  };

  const updateDots = () => {
    dots.forEach((dot, i) => {
      const stepNum = i + 1;
      dot.classList.toggle("is-current", stepNum === current);
      dot.classList.toggle("is-done", stepNum < current);
    });
  };

  const updateButtons = () => {
    const onLast = current === TOTAL;
    btnNext.hidden = onLast;
    btnSubmit.hidden = !onLast;
  };

  const goToStep = (n, opts = {}) => {
    if (n < 1 || n > TOTAL || n === current) return;
    const back = opts.back || n < current;
    const prev = stages[current - 1];
    const next = stages[n - 1];
    if (!prev || !next) return;

    removeError();

    prev.classList.remove("is-current");
    prev.classList.add(back ? "is-leaving-right" : "is-leaving-left");
    // Re-enable the incoming stage's display (it was `hidden` by markup on first load)
    next.removeAttribute("hidden");
    // Force reflow so transition triggers
    // eslint-disable-next-line no-unused-expressions
    next.offsetWidth;
    next.classList.remove("is-leaving-left", "is-leaving-right");
    next.classList.add("is-current");

    // Clear leaving class after the transition so hidden stages don't linger
    setTimeout(() => {
      prev.classList.remove("is-leaving-left", "is-leaving-right");
      prev.setAttribute("hidden", "");
    }, 520);

    card.setAttribute("data-step", String(n));
    current = n;
    updateDots();
    updateButtons();
    // Wait a tick so the browser focuses the right field after the transition starts
    requestAnimationFrame(() => focusStage(n));
  };

  // ---- Step validation -------------------------------------------------
  const validateStep = (n) => {
    if (n === 1) {
      const v = ($user.value || "").trim();
      if (!v) { showError("Nom d'utilisateur requis."); $user.focus(); return false; }
      return true;
    }
    if (n === 2) {
      const v = $pass.value || "";
      if (!v) { showError("Mot de passe requis."); $pass.focus(); return false; }
      return true;
    }
    if (n === 3) {
      const pin = pinBoxes.map((b) => b.value).join("");
      if (pin.length !== 4 || !/^\d{4}$/.test(pin)) {
        showError("PIN à 4 chiffres requis.");
        const firstEmpty = pinBoxes.find((b) => !b.value) || pinBoxes[0];
        firstEmpty && firstEmpty.focus();
        return false;
      }
      $pinHidden.value = pin;
      return true;
    }
    return true;
  };

  const advance = () => {
    if (!validateStep(current)) return;
    if (current < TOTAL) goToStep(current + 1);
  };

  // ---- Next / Back wiring ---------------------------------------------
  btnNext.addEventListener("click", advance);

  form.querySelectorAll("[data-step-back]").forEach((btn) => {
    btn.addEventListener("click", () => {
      if (current > 1) goToStep(current - 1, { back: true });
    });
  });

  // Enter key on step 1 & 2 inputs advances without submitting the form
  [$user, $pass].forEach((inp, i) => {
    if (!inp) return;
    inp.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        advance();
      }
    });
  });

  // ---- PIN box behaviour ----------------------------------------------
  const syncPin = () => {
    $pinHidden.value = pinBoxes.map((b) => b.value).join("");
    pinBoxes.forEach((b) => b.classList.toggle("is-filled", !!b.value));
  };

  pinBoxes.forEach((box, i) => {
    box.addEventListener("input", (e) => {
      // Keep only digits
      const raw = box.value || "";
      const digit = raw.replace(/\D/g, "").slice(-1);
      box.value = digit;
      syncPin();
      if (digit && i < pinBoxes.length - 1) {
        pinBoxes[i + 1].focus();
        pinBoxes[i + 1].select && pinBoxes[i + 1].select();
      }
      // Auto-submit when last box filled & all digits present
      if (i === pinBoxes.length - 1 && $pinHidden.value.length === 4) {
        // leave Enter/click to trigger submission, but focus the submit button
        btnSubmit && btnSubmit.focus();
      }
    });

    box.addEventListener("keydown", (e) => {
      if (e.key === "Backspace") {
        if (!box.value && i > 0) {
          e.preventDefault();
          const prev = pinBoxes[i - 1];
          prev.focus();
          prev.value = "";
          syncPin();
        }
      } else if (e.key === "ArrowLeft" && i > 0) {
        e.preventDefault();
        pinBoxes[i - 1].focus();
      } else if (e.key === "ArrowRight" && i < pinBoxes.length - 1) {
        e.preventDefault();
        pinBoxes[i + 1].focus();
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (validateStep(3)) form.requestSubmit ? form.requestSubmit(btnSubmit) : form.submit();
      }
    });

    box.addEventListener("focus", () => box.select && box.select());

    box.addEventListener("paste", (e) => {
      const text = (e.clipboardData || window.clipboardData).getData("text") || "";
      const digits = text.replace(/\D/g, "").slice(0, pinBoxes.length - i);
      if (!digits) return;
      e.preventDefault();
      for (let k = 0; k < digits.length; k++) {
        const target = pinBoxes[i + k];
        if (target) target.value = digits[k];
      }
      syncPin();
      const landing = Math.min(i + digits.length, pinBoxes.length - 1);
      pinBoxes[landing].focus();
    });
  });

  // ---- Submission ------------------------------------------------------

  // Build the welcome overlay in "login" mode on the fly, then let welcome.js pick it up.
  const buildOverlay = (username) => {
    let existing = document.getElementById("welcome-overlay");
    if (existing) {
      existing.dataset.welcomeMode = "login";
      existing.dataset.welcomeName = username || "";
      return existing;
    }
    const wrap = document.createElement("div");
    wrap.innerHTML = `
<div class="welcome-overlay" id="welcome-overlay" data-welcome-mode="login" data-welcome-name="${(username || "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]))}" aria-hidden="true">
  <div class="welcome-lights" aria-hidden="true">
    <div class="welcome-orb welcome-orb-1"></div>
    <div class="welcome-orb welcome-orb-2"></div>
    <div class="welcome-orb welcome-orb-3"></div>
    <div class="welcome-orb welcome-orb-4"></div>
    <div class="welcome-beam welcome-beam-1"></div>
    <div class="welcome-beam welcome-beam-2"></div>
    <div class="welcome-noise"></div>
  </div>
  <div class="welcome-content">
    <div class="welcome-mark">
      <span class="welcome-mark-text">S&amp;E</span>
      <span class="welcome-mark-ring"></span>
    </div>
    <div class="welcome-eyebrow mono" id="welcome-eyebrow">— access granted —</div>
    <h1 class="welcome-title">
      <span class="welcome-line" id="welcome-greeting">Welcome.</span>
      <span class="welcome-line welcome-line-accent" id="welcome-subtitle">Unlocking sovereign console…</span>
    </h1>
    <div class="welcome-bar-wrap" aria-hidden="true">
      <div class="welcome-bar"><span class="welcome-bar-fill" id="welcome-bar-fill"></span></div>
    </div>
    <div class="welcome-hint mono" id="welcome-hint">loading metrics · leads · pipeline</div>
  </div>
</div>`;
    const node = wrap.firstElementChild;
    document.body.appendChild(node);
    return node;
  };

  const activateOverlay = (overlay) => {
    if (typeof window.HQ_WELCOME_INIT === "function") {
      window.HQ_WELCOME_INIT(overlay);
    } else {
      overlay.classList.add("is-active");
      overlay.setAttribute("aria-hidden", "false");
      document.documentElement.classList.add("welcome-lock");
    }
  };

  form.addEventListener("submit", (ev) => {
    ev.preventDefault();
    removeError();

    // Final guard — must have all three credentials before we even try.
    if (!validateStep(1)) { goToStep(1, { back: true }); return; }
    if (!validateStep(2)) { goToStep(2); return; }
    if (!validateStep(3)) { goToStep(3); return; }

    if (btnSubmit) btnSubmit.disabled = true;

    const username = ($user.value || "").trim();
    const fd = new FormData(form);

    fetch(form.action || window.location.pathname, {
      method: "POST",
      body: fd,
      credentials: "same-origin",
      redirect: "follow",
      headers: {
        "X-CSRFToken": getCsrf(),
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then((resp) => {
      const redirectedAway = resp.redirected && !/\/login\/?$/.test(new URL(resp.url).pathname);
      if (redirectedAway || (resp.ok && resp.url && !/\/login\/?/.test(new URL(resp.url).pathname))) {
        card.classList.add("is-shrinking");
        const overlay = buildOverlay(username);
        activateOverlay(overlay);
        const target = resp.url && !/\/login\/?$/.test(new URL(resp.url).pathname)
          ? resp.url
          : "/dashboard/?welcome=1";
        setTimeout(() => { window.location.href = target; }, 2000);
        return;
      }
      return resp.text().then((html) => {
        if (btnSubmit) btnSubmit.disabled = false;
        const m = html.match(/<div class="form-error"[^>]*>([\s\S]*?)<\/div>/i);
        showError(m ? m[1].trim() : "Identifiants invalides.");
        // Clear the PIN so the user re-enters it
        pinBoxes.forEach((b) => { b.value = ""; b.classList.remove("is-filled"); });
        $pinHidden.value = "";
        pinBoxes[0] && pinBoxes[0].focus();
      });
    }).catch(() => {
      if (btnSubmit) btnSubmit.disabled = false;
      showError("Connexion interrompue. Réessayez.");
    });
  });

  // ---- Boot ------------------------------------------------------------
  updateDots();
  updateButtons();
  // Make sure the non-current stages are truly hidden
  stages.forEach((s, i) => {
    if (i === 0) { s.removeAttribute("hidden"); s.classList.add("is-current"); }
    else         { s.setAttribute("hidden", ""); }
  });
  requestAnimationFrame(() => focusStage(1));
})();
