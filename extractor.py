"""Extraction de personnes ciblées (GP/IR) — orchestre 3 stratégies.

Stratégie en cascade :
 1. JSON-LD schema.org Person → très fiable quand dispo
 2. LLM (Claude) → rattrape les phrases libres ("X, partner at Y")
 3. Regex → filet de sécurité (vieux sites, PDFs, snippets courts)

Chaque source est taggée (`source = jsonld | llm | regex`) pour que le
scoring et l'utilisateur sachent d'où vient le lead.
"""
from __future__ import annotations
import logging
import re
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

from jsonld import extract_people_from_jsonld
from llm_extractor import extract_with_llm, llm_to_common

log = logging.getLogger(__name__)

# Rôles ciblés — GP (fonds qui lèvent) + LP (investisseurs institutionnels)
TARGET_ROLES: List[str] = [
    # GP side
    "Head of Investor Relations",
    "Head of IR",
    "Managing Partner",
    "General Partner",
    "Investment Partner",
    "Partner",
    "Principal",
    "Director",
    # LP side
    "Chief Investment Officer",
    "Head of Investments",
    "Investment Director",
    "Portfolio Manager",
    "Investment Manager",
    "PE Manager",
    "VC Manager",
]

# Regex de rôle : trie du plus spécifique au plus générique (ordre important).
# Un match "Head of Investments" doit gagner contre un simple "Director".
_ROLE_PATTERNS = [
    # Très spécifiques d'abord
    ("Head of Investor Relations", r"Head\s+of\s+Investor\s+Relations"),
    ("Head of IR", r"Head\s+of\s+IR\b"),
    ("Head of Investments", r"Head\s+of\s+Investments?"),
    ("Chief Investment Officer", r"Chief\s+Investment\s+Officer|\bCIO\b"),
    ("Managing Partner", r"Managing\s+Partner"),
    ("General Partner", r"General\s+Partner"),
    ("Investment Partner", r"Investment\s+Partner"),
    ("Investment Director", r"Investment\s+Director"),
    ("Investment Manager", r"Investment\s+Manager"),
    ("Portfolio Manager", r"Portfolio\s+Manager"),
    ("PE Manager", r"\bPE\s+Manager\b"),
    ("VC Manager", r"\bVC\s+Manager\b"),
    # Génériques en dernier
    ("Partner", r"\bPartner\b"),
    ("Principal", r"\bPrincipal\b"),
    ("Director", r"\bDirector\b"),
]

NAME_RE = (
    r"(?:[A-Z][a-zA-ZÀ-ÖØ-öø-ÿ'\-]+\s){1,2}"
    r"[A-Z][a-zA-ZÀ-ÖØ-öø-ÿ'\-]+"
)

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
LINKEDIN_RE = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(?:in|pub)/[A-Za-z0-9\-_%]+",
    re.IGNORECASE,
)
# Téléphones internationaux — E.164 ou format local avec séparateurs.
# On valide ensuite par count de chiffres pour limiter les faux positifs.
PHONE_RE = re.compile(
    r"(?:\+|00)\d{1,3}[\s.\-]?(?:\(\d{1,4}\)[\s.\-]?)?\d{1,4}(?:[\s.\-]?\d{1,4}){1,5}"
    r"|\b\d{2,4}[\s.\-]\d{2,4}[\s.\-]\d{2,4}(?:[\s.\-]\d{2,4})?\b"
)


def _clean_phones(text: str) -> List[str]:
    """Trouve les numéros plausibles — garde ceux avec 9-15 chiffres."""
    out = []
    seen = set()
    for m in PHONE_RE.findall(text):
        digits = re.sub(r"\D", "", m)
        if not (9 <= len(digits) <= 15):
            continue
        # Normalise affichage : strip espaces multiples
        cleaned = re.sub(r"\s+", " ", m).strip(" .-")
        if cleaned.lower() in seen:
            continue
        seen.add(cleaned.lower())
        out.append(cleaned)
        if len(out) >= 5:
            break
    return out



FUND_MONEY_RE = re.compile(
    r"(?:€|EUR|USD|\$|£)\s?\d{1,4}(?:[\.,]\d+)?\s?(?:M|Mn|Bn|B|billion|million)",
    re.IGNORECASE,
)
FUND_CLOSE_RE = re.compile(
    r"(first|second|third|final)\s+close",
    re.IGNORECASE,
)

COMPANY_HINTS = [
    "Capital", "Ventures", "Partners", "Fund", "Management",
    "Equity", "Holdings", "Group", "Advisors", "Investments",
]


@dataclass
class Person:
    name: str
    role: str
    company: Optional[str] = None
    source_url: str = ""
    source_title: str = ""
    query_group: str = ""
    priority: int = 5
    emails: List[str] = field(default_factory=list)
    phones: List[str] = field(default_factory=list)
    linkedin: Optional[str] = None
    fund_size: Optional[str] = None
    fund_close_step: Optional[str] = None
    context_snippet: str = ""
    evidence: str = ""
    source: str = "regex"  # regex | jsonld | llm


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _detect_company(snippet: str, page_title: str = "") -> Optional[str]:
    for source in (snippet, page_title):
        if not source:
            continue
        for hint in COMPANY_HINTS:
            m = re.search(
                rf"([A-Z][A-Za-zÀ-ÖØ-öø-ÿ&'\-]+(?:\s+[A-Z][A-Za-zÀ-ÖØ-öø-ÿ&'\-]+){{0,3}}\s+{hint})",
                source,
            )
            if m:
                return _clean(m.group(1))
    return None


def _build_role_patterns() -> List:
    patterns = []
    for role_label, role_rx in _ROLE_PATTERNS:
        p_a = re.compile(
            rf"(?P<name>{NAME_RE})\s*[,\-–|:]\s*(?P<role>{role_rx})",
        )
        p_b = re.compile(
            rf"(?P<role>{role_rx})\s*[:\-–]?\s*(?P<name>{NAME_RE})",
        )
        patterns.append((role_label, p_a))
        patterns.append((role_label, p_b))
    return patterns


_COMPILED_PATTERNS = _build_role_patterns()


def _extract_context(text: str, pos: int, width: int = 180) -> str:
    start = max(0, pos - width)
    end = min(len(text), pos + width)
    return _clean(text[start:end])


def _is_false_positive(name: str, role: str) -> bool:
    blacklist = {
        "Venture Capital", "Private Equity", "Fund Launch",
        "New Fund", "First Close", "Final Close", "Second Close",
        "Managing Director", "General Partner", "Investor Relations",
        "Limited Partner", "Family Office", "United States", "New York",
        "Southeast Asia", "Asia Pacific", "European Union",
    }
    if name in blacklist:
        return True
    if role.lower() in name.lower():
        return True
    if len(name.split()) < 2:
        return True
    return False


def _match_linkedin(urls: List[str], name: str) -> Optional[str]:
    slug = name.lower().replace(" ", "-")
    slug_alt = name.lower().replace(" ", "")
    for u in urls:
        low = u.lower()
        if slug in low or slug_alt in low:
            return u
    return urls[0] if urls else None


def _match_emails(emails: List[str], name: str) -> List[str]:
    parts = [p.lower() for p in name.split() if len(p) > 1]
    return [e for e in emails if any(p in e.lower() for p in parts)]


def extract_regex(
    text: str,
    source_url: str,
    source_title: str = "",
    query_group: str = "",
    priority: int = 5,
    snippet: str = "",
) -> List[Person]:
    """Extraction regex (filet de sécurité)."""
    if not text:
        return []
    emails = list(set(EMAIL_RE.findall(text)))[:10]
    linkedin_urls = list(set(LINKEDIN_RE.findall(text)))
    phones = _clean_phones(text)
    fund_size_match = FUND_MONEY_RE.search(text)
    fund_close_match = FUND_CLOSE_RE.search(text)
    company_guess = _detect_company(snippet, source_title)

    found: Dict[tuple, Person] = {}
    for role_label, pat in _COMPILED_PATTERNS:
        for m in pat.finditer(text):
            name = _clean(m.group("name"))
            if _is_false_positive(name, role_label):
                continue
            key = (name.lower(), role_label)
            if key in found:
                continue
            found[key] = Person(
                name=name,
                role=role_label,
                company=company_guess,
                source_url=source_url,
                source_title=source_title,
                query_group=query_group,
                priority=priority,
                emails=_match_emails(emails, name),
                phones=phones,
                linkedin=_match_linkedin(linkedin_urls, name),
                fund_size=fund_size_match.group(0) if fund_size_match else None,
                fund_close_step=(
                    fund_close_match.group(0).lower() if fund_close_match else None
                ),
                context_snippet=_extract_context(text, m.start()),
                source="regex",
            )
    return list(found.values())


def extract_jsonld_people(
    html: str,
    source_url: str,
    source_title: str = "",
    query_group: str = "",
    priority: int = 5,
) -> List[Person]:
    """Extraction JSON-LD schema.org."""
    if not html:
        return []
    people = []
    for p in extract_people_from_jsonld(html, base_url=source_url):
        emails = [p["email"]] if p.get("email") else []
        phones = [p["phone"]] if p.get("phone") else []
        people.append(Person(
            name=p["name"],
            role=p["role"],
            company=p.get("company"),
            source_url=source_url,
            source_title=source_title,
            query_group=query_group,
            priority=priority,
            emails=emails,
            phones=phones,
            linkedin=p.get("linkedin"),
            context_snippet="",
            source="jsonld",
        ))
    return people


def extract_llm_people(
    text: str,
    source_url: str,
    source_title: str = "",
    query_group: str = "",
    priority: int = 5,
) -> List[Person]:
    """Extraction LLM (Claude)."""
    if not text:
        return []
    extraction = extract_with_llm(text, source_url=source_url, source_title=source_title)
    if not extraction:
        return []
    rows = llm_to_common(extraction, source_url=source_url, source_title=source_title)
    out = []
    for r in rows:
        out.append(Person(
            name=r["name"],
            role=r["role"],
            company=r.get("company"),
            source_url=source_url,
            source_title=source_title,
            query_group=query_group,
            priority=priority,
            fund_size=r.get("fund_size"),
            fund_close_step=r.get("fund_close_step"),
            evidence=r.get("evidence", ""),
            source="llm",
        ))
    return out


def extract_people(
    text: str,
    source_url: str,
    source_title: str = "",
    query_group: str = "",
    priority: int = 5,
    snippet: str = "",
    html: str = "",
    use_llm: bool = True,
) -> List[Person]:
    """Orchestre JSON-LD + LLM + Regex. Retourne la concat (dedup en aval)."""
    all_people: List[Person] = []

    # 1. JSON-LD (si on a le HTML brut)
    if html:
        js = extract_jsonld_people(
            html, source_url, source_title, query_group, priority,
        )
        if js:
            log.debug("  jsonld: %d personne(s)", len(js))
            all_people.extend(js)

    # 2. LLM (si activé)
    if use_llm:
        llm = extract_llm_people(
            text, source_url, source_title, query_group, priority,
        )
        if llm:
            log.debug("  llm: %d personne(s)", len(llm))
            all_people.extend(llm)

    # 3. Regex (toujours, en filet)
    rx = extract_regex(
        text, source_url, source_title, query_group, priority, snippet,
    )
    if rx:
        log.debug("  regex: %d personne(s)", len(rx))
        all_people.extend(rx)

    return all_people


def person_to_row(p: Person) -> Dict:
    row = asdict(p)
    row["emails"] = ";".join(p.emails)
    return row
