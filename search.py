"""Wrapper de recherche multi-moteurs avec rotation et fallback.

Priorité d'appel (selon clés disponibles) :
  1. SerpAPI (Google)       — payant, le plus large
  2. Brave Search API       — freemium (~2k/mo)
  3. Bing Web Search API    — payant Microsoft
  4. DuckDuckGo (ddgs)      — gratuit mais limité

Si un moteur échoue ou rate-limit, on tombe sur le suivant.
"""
from __future__ import annotations
import logging
import time
from typing import Iterator, List, Dict, Callable
from urllib.parse import urlparse

try:
    from ddgs import DDGS  # type: ignore
except ImportError:  # pragma: no cover
    try:
        from duckduckgo_search import DDGS  # type: ignore
    except ImportError:
        DDGS = None  # type: ignore
        logging.getLogger(__name__).warning(
            "ddgs/duckduckgo_search absent — moteur DuckDuckGo désactivé."
        )

import requests

from config import (
    BLOCKED_DOMAINS, BRAVE_API_KEY, BING_API_KEY,
    DEFAULT_REGION, DEFAULT_SAFESEARCH,
    REQUEST_DELAY_SECONDS, SERPAPI_KEY,
)

log = logging.getLogger(__name__)


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def _is_blocked(url: str) -> bool:
    d = _domain(url)
    return any(d == b or d.endswith("." + b) for b in BLOCKED_DOMAINS)


def _dedup_clean(items: List[Dict]) -> List[Dict]:
    seen: set[str] = set()
    out = []
    for r in items:
        url = r.get("url", "")
        if not url or _is_blocked(url) or url in seen:
            continue
        seen.add(url)
        out.append(r)
    return out


# -------------------------------------------------------------------------
# DuckDuckGo
# -------------------------------------------------------------------------
def search_duckduckgo(query: str, max_results: int = 15) -> List[Dict]:
    results: List[Dict] = []
    if DDGS is None:
        return results
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(
                query,
                region=DEFAULT_REGION,
                safesearch=DEFAULT_SAFESEARCH,
                max_results=max_results,
            ):
                url = r.get("href") or r.get("url") or r.get("link")
                if not url:
                    continue
                results.append({
                    "title": r.get("title", ""),
                    "url": url,
                    "snippet": r.get("body") or r.get("snippet", ""),
                    "engine": "ddg",
                })
    except Exception as e:
        log.warning("DDG error for %r: %s", query, e)
    return results


# -------------------------------------------------------------------------
# Brave Search API
# -------------------------------------------------------------------------
def search_brave(query: str, max_results: int = 15) -> List[Dict]:
    if not BRAVE_API_KEY:
        return []
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={
                "X-Subscription-Token": BRAVE_API_KEY,
                "Accept": "application/json",
            },
            params={"q": query, "count": min(max_results, 20)},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        out = []
        for r in data.get("web", {}).get("results", [])[:max_results]:
            out.append({
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("description", ""),
                "engine": "brave",
            })
        return out
    except Exception as e:
        log.warning("Brave error for %r: %s", query, e)
        return []


# -------------------------------------------------------------------------
# Bing Web Search
# -------------------------------------------------------------------------
def search_bing(query: str, max_results: int = 15) -> List[Dict]:
    if not BING_API_KEY:
        return []
    try:
        resp = requests.get(
            "https://api.bing.microsoft.com/v7.0/search",
            headers={"Ocp-Apim-Subscription-Key": BING_API_KEY},
            params={"q": query, "count": min(max_results, 50), "mkt": "en-US"},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        out = []
        for r in data.get("webPages", {}).get("value", [])[:max_results]:
            out.append({
                "title": r.get("name", ""),
                "url": r.get("url", ""),
                "snippet": r.get("snippet", ""),
                "engine": "bing",
            })
        return out
    except Exception as e:
        log.warning("Bing error for %r: %s", query, e)
        return []


# -------------------------------------------------------------------------
# SerpAPI (Google)
# -------------------------------------------------------------------------
def search_serpapi(query: str, max_results: int = 15) -> List[Dict]:
    if not SERPAPI_KEY:
        return []
    try:
        resp = requests.get(
            "https://serpapi.com/search.json",
            params={
                "engine": "google",
                "q": query,
                "num": max_results,
                "api_key": SERPAPI_KEY,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        out = []
        for r in data.get("organic_results", [])[:max_results]:
            out.append({
                "title": r.get("title", ""),
                "url": r.get("link", ""),
                "snippet": r.get("snippet", ""),
                "engine": "serpapi",
            })
        return out
    except Exception as e:
        log.warning("SerpAPI error for %r: %s", query, e)
        return []


# -------------------------------------------------------------------------
# Orchestration : rotation avec fallback
# -------------------------------------------------------------------------
def _engines_in_order() -> List[Callable[[str, int], List[Dict]]]:
    engines = []
    if SERPAPI_KEY:
        engines.append(search_serpapi)
    if BRAVE_API_KEY:
        engines.append(search_brave)
    if BING_API_KEY:
        engines.append(search_bing)
    engines.append(search_duckduckgo)  # toujours dispo
    return engines


def search(query: str, max_results: int = 15) -> List[Dict]:
    """Tente chaque moteur jusqu'à obtenir des résultats."""
    for engine_fn in _engines_in_order():
        results = engine_fn(query, max_results)
        results = _dedup_clean(results)
        if results:
            log.debug("Engine %s → %d results", engine_fn.__name__, len(results))
            return results
    return []


def run_queries(
    queries: List[Dict],
    max_results_per_query: int = 15,
    delay: float = REQUEST_DELAY_SECONDS,
) -> Iterator[Dict]:
    """Exécute toutes les requêtes et yield chaque résultat enrichi."""
    seen_urls: set[str] = set()
    for idx, q in enumerate(queries, 1):
        log.info("[%d/%d] %s", idx, len(queries), q["query"])
        results = search(q["query"], max_results_per_query)
        for r in results:
            if r["url"] in seen_urls:
                continue
            seen_urls.add(r["url"])
            yield {
                **r,
                "query": q["query"],
                "group": q["group"],
                "priority": q["priority"],
            }
        time.sleep(delay)
