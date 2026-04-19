"""Deep company enrichment.

Given a lead row that mentions a company, try to resolve the company's primary
website and extract a richer structured profile (description, industry, HQ,
size band, founded year, specialties) via Claude.

Design notes:
- Best-effort: if no domain can be resolved, or if the fetch/LLM fails, we
  return an empty dict and the caller continues.
- The HTTP fetch reuses `scraper.fetch_full()` (with its cache) so we don't
  pay twice for pages the pipeline already saw.
- One LLM call per company. The system prompt is cached.
"""
from __future__ import annotations
import logging
import re
from typing import Dict, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field

from config import (
    ANTHROPIC_API_KEY, LLM_ENABLED, LLM_MAX_CHARS_PER_PAGE,
    LLM_MAX_TOKENS, LLM_MODEL,
)

log = logging.getLogger(__name__)

# Hosts that are never a company's own domain
_NON_CORPORATE = {
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "bloomberg.com", "ft.com", "reuters.com", "wsj.com", "nytimes.com",
    "forbes.com", "techcrunch.com", "crunchbase.com", "pitchbook.com",
    "businesswire.com", "prnewswire.com", "medium.com", "substack.com",
    "youtube.com", "wikipedia.org", "google.com", "github.com",
}


class CompanyProfile(BaseModel):
    description: Optional[str] = Field(
        None,
        description=(
            "One paragraph (<=500 chars) describing the firm: what it does, "
            "primary strategy, AUM if stated, flagship funds, geography focus. "
            "Factual, no marketing fluff. Null if not inferable."
        ),
    )
    industry: Optional[str] = Field(
        None,
        description=(
            "Short label of the firm's vertical: Venture Capital, Private Equity, "
            "Growth Equity, Family Office, Pension Fund, Sovereign Wealth, "
            "Fund of Funds, Asset Manager, Corporate VC, Endowment, etc."
        ),
    )
    hq_city: Optional[str] = Field(None, description="Primary HQ city, if identifiable")
    hq_country_iso2: Optional[str] = Field(None, description="HQ country as ISO 3166-1 alpha-2")
    size: Optional[str] = Field(
        None,
        description=(
            "Rough headcount band: '1-10', '11-50', '51-200', '201-500', "
            "'501-1000', '1000+'. Null if not inferable."
        ),
    )
    founded: Optional[int] = Field(
        None, ge=1800, le=2100,
        description="Year the firm was founded, if stated.",
    )
    aum: Optional[str] = Field(
        None,
        description="Assets under management if explicitly stated (e.g. '€2Bn AUM').",
    )
    specialties: Optional[str] = Field(
        None,
        description="Comma-separated specialties (e.g. 'SaaS, Climate, Series A-B').",
    )
    website: Optional[str] = Field(
        None,
        description="The firm's primary website URL.",
    )


# ---- Prompt système (caché) -----------------------------------------------
_SYSTEM_PROMPT = """\
You are an analyst profiling investment firms (VC, PE, family offices, \
LP institutions). Given the homepage content of a firm, extract a structured \
profile.

Rules:
1. Be factual. Never invent figures (AUM, founded year, headcount). Only fill \
   a field when the evidence is in the text.
2. Description: one paragraph <=500 chars; cover strategy, stage, AUM if any, \
   sectors, geography.
3. Industry: pick the single best label (Venture Capital, Private Equity, \
   Family Office, Pension Fund, Sovereign Wealth, Asset Manager, Growth Equity, \
   Corporate VC, Fund of Funds, Endowment, Credit Fund). Null if it's clearly \
   not an investment firm.
4. Country as ISO 3166-1 alpha-2 (FR, US, GB, SG, AE, HK, DE, etc.).
5. Size bands: 1-10, 11-50, 51-200, 201-500, 501-1000, 1000+. Null if unclear.
6. Specialties: comma-separated, short tokens (SaaS, Climate, Series A, B2B, \
   Infrastructure, etc.). Max ~6 tokens.
7. Respond ONLY with valid JSON matching the schema.\
"""


# ---- Client Claude (lazy) -------------------------------------------------
_client = None


def _get_client():
    global _client
    if not LLM_ENABLED:
        return None
    if _client is None:
        try:
            import anthropic
            _client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        except Exception as e:
            log.warning("company_enricher: could not init Claude: %s", e)
            _client = False
    return _client or None


# ---- Domain resolution ----------------------------------------------------
def _domain_of(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        net = urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return None
    if not net or "." not in net:
        return None
    root = ".".join(net.split(".")[-2:])
    if root in _NON_CORPORATE or net in _NON_CORPORATE:
        return None
    return net


def _resolve_company_domain(row: Dict) -> Optional[str]:
    """Try to find the company's own domain from the lead row."""
    # 1) explicit email domain (strongest signal)
    for e in (row.get("emails") or []):
        if isinstance(e, str) and "@" in e:
            d = e.split("@", 1)[-1].lower().strip()
            if d and d not in _NON_CORPORATE:
                return d
    # 2) email_domain already computed by enrich_person
    d = (row.get("email_domain") or "").strip().lower()
    if d and d not in _NON_CORPORATE:
        return d
    # 3) source URL — only if it's NOT a press / aggregator host
    src = _domain_of(row.get("source_url") or "")
    if src:
        return src
    return None


_COMPANY_PATHS = ["", "/about", "/about-us", "/team", "/company", "/who-we-are"]


def _fetch_company_text(domain: str) -> str:
    """Grab homepage + one /about-style page. Best-effort; short-circuit on error."""
    try:
        from scraper import fetch_full  # lazy import to avoid cycles
    except Exception as e:
        log.debug("company_enricher: scraper unavailable: %s", e)
        return ""
    combined = []
    for path in _COMPANY_PATHS[:3]:  # homepage + about + about-us
        url = f"https://{domain}{path}"
        try:
            _kind, text, _raw = fetch_full(url, use_cache=True, allow_js=False)
            if text:
                combined.append(text)
                if sum(len(t) for t in combined) > LLM_MAX_CHARS_PER_PAGE:
                    break
        except Exception as e:
            log.debug("company_enricher: fetch %s failed: %s", url, e)
            continue
    return "\n\n---\n\n".join(combined)[:LLM_MAX_CHARS_PER_PAGE]


_JSON_RE = re.compile(r"\{[\s\S]*\}")


def _extract_json(s: str) -> str:
    m = _JSON_RE.search(s or "")
    return m.group(0) if m else "{}"


def enrich_company(row: Dict) -> Dict:
    """Return a dict of company_* enrichment fields (may be empty).

    Keys (all optional):
      - company_website
      - company_description_full  (richer than the per-lead company_description)
      - company_industry
      - company_hq_city
      - company_hq_country (ISO2)
      - company_size
      - company_founded
      - company_aum
      - company_specialties
    """
    client = _get_client()
    if client is None:
        return {}

    domain = _resolve_company_domain(row)
    if not domain:
        return {}

    text = _fetch_company_text(domain)
    if not text or len(text) < 120:
        # Not enough content to reason about; at least return the website.
        return {"company_website": f"https://{domain}"}

    company_hint = (row.get("company") or "").strip()
    user_content = (
        f"Domain: {domain}\n"
        f"Company (from lead row, may be noisy): {company_hint or '(unknown)'}\n\n"
        f"--- HOMEPAGE / ABOUT TEXT ---\n{text}\n--- END ---\n\n"
        "Extract the firm profile. Only fill fields the text actually supports."
    )

    profile = None
    try:
        response = client.messages.parse(
            model=LLM_MODEL,
            max_tokens=LLM_MAX_TOKENS,
            system=[{
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user_content}],
            output_format=CompanyProfile,
        )
        profile = response.parsed_output
    except Exception as e:
        # Fallback: json_schema via create()
        log.debug("company_enricher: .parse() failed (%s), falling back", e)
        try:
            resp = client.messages.create(
                model=LLM_MODEL,
                max_tokens=LLM_MAX_TOKENS,
                system=[{
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_content}],
                output_config={
                    "format": {
                        "type": "json_schema",
                        "schema": CompanyProfile.model_json_schema(),
                    }
                },
            )
            txt = next(
                (b.text for b in resp.content if getattr(b, "type", "") == "text"),
                "",
            )
            import json as _json
            data = _json.loads(_extract_json(txt))
            profile = CompanyProfile.model_validate(data)
        except Exception as e2:
            log.info("company_enricher: LLM failed for %s: %s", domain, e2)
            return {"company_website": f"https://{domain}"}

    if not profile:
        return {"company_website": f"https://{domain}"}

    out: Dict = {"company_website": profile.website or f"https://{domain}"}
    if profile.description:
        out["company_description_full"] = profile.description[:500]
    if profile.industry:
        out["company_industry"] = profile.industry[:60]
    if profile.hq_city:
        out["company_hq_city"] = profile.hq_city[:80]
    if profile.hq_country_iso2:
        out["company_hq_country"] = profile.hq_country_iso2.upper()[:2]
    if profile.size:
        out["company_size"] = profile.size[:16]
    if profile.founded:
        out["company_founded"] = int(profile.founded)
    if profile.aum:
        out["company_aum"] = profile.aum[:60]
    if profile.specialties:
        out["company_specialties"] = profile.specialties[:200]
    return out
