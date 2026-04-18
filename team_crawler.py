"""Crawler de pages /team /about /people /partners sur le domaine d'un fonds.

Le search renvoie souvent une annonce de fonds → on suit les liens du site
pour trouver la page équipe et y extraire les partners/GP/IR.
"""
from __future__ import annotations
import logging
from typing import List, Set, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from config import TEAM_PATH_HINTS, TEAM_CRAWL_MAX_PAGES_PER_DOMAIN
from scraper import fetch_full, domain_of

log = logging.getLogger(__name__)


def _same_domain(a: str, b: str) -> bool:
    return domain_of(a) == domain_of(b) and domain_of(a) != ""


def discover_team_urls(seed_url: str, html: str) -> List[str]:
    """À partir d'une page déjà chargée, trouve les URLs équipe/about/people."""
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    found: List[str] = []
    seen: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        abs_url = urljoin(seed_url, href)
        if not _same_domain(abs_url, seed_url):
            continue
        path = urlparse(abs_url).path.lower()
        anchor = (a.get_text(" ", strip=True) or "").lower()
        match_path = any(h in path for h in TEAM_PATH_HINTS)
        match_text = any(
            kw in anchor for kw in (
                "team", "about", "people", "partners", "leadership",
                "équipe", "equipe", "à propos", "notre équipe",
            )
        )
        if match_path or match_text:
            norm = abs_url.split("#")[0].rstrip("/")
            if norm not in seen:
                seen.add(norm)
                found.append(norm)
    # Priorité : paths équipe en premier
    found.sort(key=lambda u: (
        0 if any(h in urlparse(u).path.lower() for h in ("/team", "/our-team", "/people")) else 1
    ))
    return found[:TEAM_CRAWL_MAX_PAGES_PER_DOMAIN]


def crawl_team_pages(
    seed_url: str,
    allow_js: bool = True,
) -> List[dict]:
    """Charge le seed puis explore les pages team/about du même domaine.

    Retourne : liste de {url, kind, text, html}
    """
    results: List[dict] = []
    # On bypass le cache texte pour récupérer le raw HTML nécessaire au discover
    kind, text, raw_html = fetch_full(seed_url, use_cache=False, allow_js=allow_js)
    if not raw_html and not text:
        return results

    team_urls = discover_team_urls(seed_url, raw_html)
    for url in team_urls:
        if url == seed_url:
            continue
        k, t, h = fetch_full(url, allow_js=allow_js)
        if t:
            results.append({"url": url, "kind": k, "text": t, "html": h})
            log.info("  ↳ team page scraped: %s", url)
    return results
