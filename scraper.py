"""Fetcher intelligent et résilient.

Stratégie :
 1. Respecte un min-interval par domaine (anti-ban).
 2. Tente d'abord `requests` (rapide, peu coûteux).
 3. Détecte : status anti-bot, challenge Cloudflare, SPA vide,
    texte < seuil, captcha → retente avec Selenium si activé.
 4. Cache disque par URL hashée.
 5. Parse HTML (bs4/lxml) et PDF (pypdf).
 6. Backoff exponentiel + jitter sur erreurs réseau.
"""
from __future__ import annotations
import hashlib
import io
import logging
import random
import re
import threading
import time
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type,
)

from config import (
    CACHE_DIR, ENABLE_SELENIUM, JS_TRIGGER_PATTERNS,
    MAX_PAGE_BYTES, MIN_TEXT_LENGTH_FOR_STATIC,
    PER_DOMAIN_MIN_INTERVAL, REQUEST_TIMEOUT, SPA_HINTS, USER_AGENTS,
)

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# per-domain throttle
# --------------------------------------------------------------------------
_last_hit: dict[str, float] = {}
_lock = threading.Lock()


def _throttle(domain: str) -> None:
    with _lock:
        last = _last_hit.get(domain, 0.0)
        wait = PER_DOMAIN_MIN_INTERVAL - (time.time() - last)
        if wait > 0:
            time.sleep(wait + random.uniform(0.0, 0.4))
        _last_hit[domain] = time.time()


# --------------------------------------------------------------------------
# cache
# --------------------------------------------------------------------------
def _cache_path(url: str) -> Path:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{h}.txt"


# --------------------------------------------------------------------------
# requests helpers
# --------------------------------------------------------------------------
def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml,"
                  "application/pdf;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8,fr;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Upgrade-Insecure-Requests": "1",
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    reraise=True,
)
def _get(url: str) -> requests.Response:
    return requests.get(
        url,
        headers=_headers(),
        timeout=REQUEST_TIMEOUT,
        stream=True,
        allow_redirects=True,
    )


# --------------------------------------------------------------------------
# shared browser (lazy)
# --------------------------------------------------------------------------
_shared_browser = None
_browser_lock = threading.Lock()


def _get_browser():
    global _shared_browser
    if not ENABLE_SELENIUM:
        return None
    with _browser_lock:
        if _shared_browser is None:
            try:
                from browser import BrowserSession
                _shared_browser = BrowserSession()
                log.info("Selenium session initialisée.")
            except Exception as e:
                log.warning("Impossible d'initialiser Selenium : %s", e)
                _shared_browser = False
        return _shared_browser or None


def shutdown_browser():
    global _shared_browser
    with _browser_lock:
        if _shared_browser:
            try:
                _shared_browser.close()
            except Exception:
                pass
        _shared_browser = None


# --------------------------------------------------------------------------
# smart fetch
# --------------------------------------------------------------------------
def fetch(
    url: str,
    use_cache: bool = True,
    allow_js: bool = True,
) -> Tuple[str, str]:
    """Retourne (kind, text). kind ∈ {html, pdf, cached, error, http_XXX, js}."""
    kind, text, _ = fetch_full(url, use_cache=use_cache, allow_js=allow_js)
    return kind, text


def fetch_full(
    url: str,
    use_cache: bool = True,
    allow_js: bool = True,
) -> Tuple[str, str, str]:
    """Retourne (kind, text_extrait, raw_html).

    raw_html est vide pour PDF et pour les lectures depuis cache.
    """
    cache = _cache_path(url)
    if use_cache and cache.exists():
        return "cached", cache.read_text(encoding="utf-8", errors="ignore"), ""

    domain = domain_of(url)
    _throttle(domain)

    kind, text, raw_html = _fetch_static(url)

    # Décide si un rendu JS est nécessaire
    if allow_js and _needs_js(kind, text, raw_html):
        js_html = _fetch_via_selenium_raw(url)
        if js_html:
            js_text = _html_to_text(js_html)
            if len(js_text) > len(text):
                text = js_text
                raw_html = js_html
                kind = "js"

    if text:
        try:
            cache.write_text(text, encoding="utf-8")
        except Exception:
            pass
    return kind, text, raw_html


def fetch_raw_html(url: str) -> str:
    """Retourne le HTML brut (pour discovery de liens).

    N'utilise pas le cache texte; préfère Selenium si ENABLE_SELENIUM.
    """
    try:
        resp = _get(url)
        if resp.status_code >= 400:
            return ""
        body = b""
        for chunk in resp.iter_content(chunk_size=65536):
            body += chunk
            if len(body) > MAX_PAGE_BYTES:
                break
        try:
            return body.decode(resp.encoding or "utf-8", errors="ignore")
        except Exception:
            return body.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _fetch_static(url: str) -> Tuple[str, str, str]:
    """Retourne (kind, text_extrait, raw_html). raw_html vide pour PDF."""
    try:
        resp = _get(url)
    except Exception as e:
        log.debug("requests fetch failed %s: %s", url, e)
        return "error", "", ""

    # 429 / 403 → bot wall probable
    if resp.status_code in (403, 429, 503):
        log.info("Status %s sur %s → candidat Selenium", resp.status_code, url)
        return f"http_{resp.status_code}", "", ""

    if resp.status_code >= 400:
        return f"http_{resp.status_code}", "", ""

    ctype = resp.headers.get("Content-Type", "").lower()
    body = b""
    for chunk in resp.iter_content(chunk_size=65536):
        body += chunk
        if len(body) > MAX_PAGE_BYTES:
            break

    if "pdf" in ctype or url.lower().endswith(".pdf"):
        return "pdf", _pdf_to_text(body), ""

    try:
        raw = body.decode(resp.encoding or "utf-8", errors="ignore")
    except Exception:
        raw = body.decode("utf-8", errors="ignore")
    return "html", _html_to_text(raw), raw


def _needs_js(kind: str, text: str, raw_html: str) -> bool:
    if not ENABLE_SELENIUM:
        return False
    if kind.startswith("http_") or kind == "error":
        return True
    if kind == "pdf":
        return False
    low = (raw_html or "")[:12000].lower()
    if any(p in low for p in JS_TRIGGER_PATTERNS):
        return True
    if any(h.lower() in low for h in SPA_HINTS):
        return True
    if len(text) < MIN_TEXT_LENGTH_FOR_STATIC:
        return True
    return False


def _fetch_via_selenium_raw(url: str) -> str:
    """Retourne le HTML brut via Selenium (ou vide en cas d'échec)."""
    b = _get_browser()
    if not b:
        return ""
    try:
        return b.fetch(url) or ""
    except Exception as e:
        log.debug("selenium fetch failed %s: %s", url, e)
        return ""


def _fetch_via_selenium(url: str) -> str:
    html = _fetch_via_selenium_raw(url)
    return _html_to_text(html) if html else ""


# --------------------------------------------------------------------------
# parsers
# --------------------------------------------------------------------------
def _html_to_text(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _pdf_to_text(body: bytes) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(body))
        out = []
        for page in reader.pages:
            try:
                out.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(out)
    except Exception as e:
        log.debug("pdf parse error: %s", e)
        return ""


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""
