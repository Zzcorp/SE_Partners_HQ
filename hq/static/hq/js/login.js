/* S&E Partners HQ — login-form shrink/fade + welcome overlay on success */
(() => {
  "use strict";
  const form = document.getElementById("login-form");
  const card = document.getElementById("login-card");
  if (!form || !card) return;

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
    const submit = form.querySelector("button[type=submit]");
    const div = document.createElement("div");
    div.className = "form-error";
    div.textContent = msg || "Identifiants invalides.";
    if (submit) form.insertBefore(div, submit);
    else form.appendChild(div);
  };

  // Build the welcome overlay in "login" mode on the fly, then let welcome.js pick it up.
  const buildOverlay = (username) => {
    // If the server already rendered one, reuse it.
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
      // welcome.js hasn't loaded yet — fall back to a visible style so the user isn't left on the old card.
      overlay.classList.add("is-active");
      overlay.setAttribute("aria-hidden", "false");
      document.documentElement.classList.add("welcome-lock");
    }
  };

  form.addEventListener("submit", (ev) => {
    ev.preventDefault();
    removeError();
    const submit = form.querySelector("button[type=submit]");
    if (submit) submit.disabled = true;

    const username = (form.querySelector("input[name=username]") || {}).value || "";
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
        // Success — play the shrink/fade, mount the overlay, then navigate.
        card.classList.add("is-shrinking");
        const overlay = buildOverlay(username);
        activateOverlay(overlay);
        const target = resp.url && !/\/login\/?$/.test(new URL(resp.url).pathname)
          ? resp.url
          : "/dashboard/?welcome=1";
        setTimeout(() => { window.location.href = target; }, 2000);
        return;
      }
      // Failure — parse HTML for server-rendered error message.
      return resp.text().then((html) => {
        if (submit) submit.disabled = false;
        const m = html.match(/<div class="form-error">([\s\S]*?)<\/div>/i);
        showError(m ? m[1].trim() : "Identifiants invalides.");
      });
    }).catch(() => {
      if (submit) submit.disabled = false;
      showError("Connexion interrompue. Réessayez.");
    });
  });
})();
