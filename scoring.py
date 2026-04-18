"""Entity resolution + lead scoring.

Deux responsabilités :
 1. Fusionner les doublons (même personne citée dans plusieurs sources).
 2. Scorer chaque lead → priorité × rôle × close_stage × recency × signaux.

Un bon lead = (rôle senior) + (fonds en close actif) + (source fraîche)
           + (LinkedIn et/ou email).
"""
from __future__ import annotations
import logging
import re
import unicodedata
from typing import Dict, Iterable, List, Optional

from config import (
    CLOSE_STAGE_WEIGHTS, MIN_LEAD_SCORE,
    PRIORITY_WEIGHTS, ROLE_WEIGHTS,
)

log = logging.getLogger(__name__)


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )


def _norm_name(name: str) -> str:
    """Clé de dédup : minuscule, sans accents, sans ponctuation, trié."""
    s = _strip_accents(name or "").lower()
    s = re.sub(r"[^a-z\s]", " ", s)
    parts = [p for p in s.split() if p]
    # un nom court (<2 parts) est rarement fiable → renvoie vide
    if len(parts) < 2:
        return ""
    # prénom + dernier mot suffit pour dédupliquer la plupart des cas
    return f"{parts[0]} {parts[-1]}"


_COMPANY_STOPWORDS = {
    "capital", "partners", "ventures", "fund", "funds", "management",
    "equity", "holdings", "group", "advisors", "investments", "ltd",
    "inc", "llc", "llp", "sas", "sa", "gmbh", "co", "corp", "the",
}


def _norm_company(company: Optional[str]) -> str:
    """Clé fonds : on ne garde que le premier mot distinctif.

    'Alpha Capital' et 'Alpha Capital Partners' matchent → 'alpha'.
    """
    if not company:
        return ""
    s = _strip_accents(company).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = [t for t in s.split() if t and t not in _COMPANY_STOPWORDS]
    if not tokens:
        return ""
    return tokens[0]


def _dedup_key(row: dict) -> str:
    name = _norm_name(row.get("name", ""))
    company = _norm_company(row.get("company"))
    role = (row.get("role") or "").lower()
    if not name:
        return ""
    return f"{name}||{company}||{role}"


def _close_stage_weight(stage: Optional[str]) -> float:
    if not stage:
        return 0.80
    low = stage.lower()
    for k, w in CLOSE_STAGE_WEIGHTS.items():
        if k in low:
            return w
    if "launch" in low or "raising" in low:
        return 0.88
    return 0.80


def _merge(a: dict, b: dict) -> dict:
    """Fusionne b dans a (conservant les infos les plus riches)."""
    out = dict(a)
    # Listes : emails, candidates
    def _union_list(k: str) -> None:
        ea = a.get(k) or []
        eb = b.get(k) or []
        if isinstance(ea, str):
            ea = [x for x in ea.split(";") if x]
        if isinstance(eb, str):
            eb = [x for x in eb.split(";") if x]
        merged = list({*ea, *eb})
        out[k] = merged

    _union_list("emails")
    _union_list("email_candidates")
    _union_list("phones")

    # Valeurs scalaires : garde la 1re non-vide, sinon celle de b
    for k in (
        "linkedin", "company", "fund_size", "fund_close_step",
        "geography", "image", "evidence", "email_domain",
    ):
        if not out.get(k) and b.get(k):
            out[k] = b[k]

    # Sources multiples → on garde la mieux scorée comme "source principale",
    # mais on empile les URLs
    sources = out.get("sources") or []
    if not sources and a.get("source_url"):
        sources = [{
            "url": a["source_url"],
            "title": a.get("source_title", ""),
            "kind": a.get("kind", ""),
            "source": a.get("source", ""),
        }]
    if b.get("source_url"):
        sources.append({
            "url": b["source_url"],
            "title": b.get("source_title", ""),
            "kind": b.get("kind", ""),
            "source": b.get("source", ""),
        })
    # dédup urls
    seen = set()
    dedup = []
    for s in sources:
        u = s.get("url", "")
        if u and u not in seen:
            seen.add(u)
            dedup.append(s)
    out["sources"] = dedup
    out["n_sources"] = len(dedup)

    # Priorité : min (plus fort)
    pa = a.get("priority") or 5
    pb = b.get("priority") or 5
    out["priority"] = min(pa, pb)

    # Recency : la plus récente
    ra = a.get("recency_months")
    rb = b.get("recency_months")
    if ra is None:
        out["recency_months"] = rb
    elif rb is None:
        out["recency_months"] = ra
    else:
        out["recency_months"] = min(ra, rb)

    # Conserve la meilleure evidence (la plus longue)
    ea = a.get("evidence") or ""
    eb = b.get("evidence") or ""
    out["evidence"] = ea if len(ea) >= len(eb) else eb

    return out


def resolve_entities(rows: Iterable[dict]) -> List[dict]:
    """Fusionne les doublons (même personne, même rôle, même fonds)."""
    bucket: Dict[str, dict] = {}
    orphans: List[dict] = []
    for row in rows:
        key = _dedup_key(row)
        if not key:
            orphans.append(row)
            continue
        if key in bucket:
            bucket[key] = _merge(bucket[key], row)
        else:
            # Normalise au premier passage
            row = dict(row)
            row.setdefault("sources", [])
            if row.get("source_url") and not row["sources"]:
                row["sources"] = [{
                    "url": row["source_url"],
                    "title": row.get("source_title", ""),
                    "kind": row.get("kind", ""),
                    "source": row.get("source", ""),
                }]
            row["n_sources"] = len(row["sources"]) or 1
            bucket[key] = row
    return list(bucket.values()) + orphans


def recency_multiplier(months: Optional[float]) -> float:
    if months is None:
        return 0.6
    if months <= 3:
        return 1.20
    if months <= 6:
        return 1.00
    if months <= 12:
        return 0.80
    if months <= 24:
        return 0.50
    return 0.25


def score_lead(row: dict) -> float:
    """Retourne un score dans [0, ~1.5]."""
    role = row.get("role", "")
    role_w = ROLE_WEIGHTS.get(role, 0.30)

    priority = row.get("priority") or 5
    prio_w = PRIORITY_WEIGHTS.get(priority, 0.30)

    stage_w = _close_stage_weight(row.get("fund_close_step"))
    rec_w = recency_multiplier(row.get("recency_months"))

    # Bonus signaux
    has_email = bool(row.get("emails"))
    has_candidates = bool(row.get("email_candidates"))
    has_linkedin = bool(row.get("linkedin"))
    n_sources = max(1, int(row.get("n_sources") or 1))

    signal_bonus = 1.0
    if has_email:
        signal_bonus *= 1.15
    elif has_candidates:
        signal_bonus *= 1.05
    if has_linkedin:
        signal_bonus *= 1.10
    if n_sources >= 2:
        signal_bonus *= 1.08
    if n_sources >= 3:
        signal_bonus *= 1.05

    # Source "llm" ou "jsonld" = plus fiable que "regex"
    src = (row.get("source") or "").lower()
    if src == "jsonld":
        signal_bonus *= 1.05
    elif src == "llm":
        signal_bonus *= 1.08

    base = role_w * prio_w * stage_w * rec_w
    return round(base * signal_bonus, 4)


def score_and_filter(rows: Iterable[dict]) -> List[dict]:
    """Score chaque lead et filtre < MIN_LEAD_SCORE."""
    scored = []
    for row in rows:
        s = score_lead(row)
        row["lead_score"] = s
        if s >= MIN_LEAD_SCORE:
            scored.append(row)
    scored.sort(key=lambda r: r.get("lead_score", 0.0), reverse=True)
    return scored
