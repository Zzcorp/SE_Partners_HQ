"""Extraction de données structurées schema.org depuis le HTML.

La plupart des sites VC utilisent schema.org Person / Organization
en JSON-LD. C'est une mine d'or structurée qu'on rate avec le regex.
"""
from __future__ import annotations
import json
import logging
import re
from typing import List, Dict, Any, Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Rôles qu'on cherche (en minuscule pour comparaison)
TARGET_ROLES_LC = {
    # GP
    "head of investor relations": "Head of Investor Relations",
    "head of ir": "Head of IR",
    "managing partner": "Managing Partner",
    "general partner": "General Partner",
    "investment partner": "Investment Partner",
    "partner": "Partner",
    "principal": "Principal",
    "director": "Director",
    # LP
    "head of investments": "Head of Investments",
    "chief investment officer": "Chief Investment Officer",
    "cio": "Chief Investment Officer",
    "investment director": "Investment Director",
    "investment manager": "Investment Manager",
    "portfolio manager": "Portfolio Manager",
    "pe manager": "PE Manager",
    "vc manager": "VC Manager",
}


def extract_jsonld(html: str, base_url: str = "") -> List[Dict]:
    """Retourne tous les blocs JSON-LD parsés (flat list)."""
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    blocks: List[Dict] = []
    for tag in soup.find_all("script", type="application/ld+json"):
        raw = tag.string or tag.text or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            # certains sites ont du JSON mal échappé
            try:
                data = json.loads(raw.encode("utf-8").decode("unicode_escape"))
            except Exception:
                continue
        blocks.extend(_flatten(data))
    return blocks


def _flatten(obj: Any) -> List[Dict]:
    """Applati @graph et listes imbriquées."""
    out: List[Dict] = []
    if isinstance(obj, list):
        for item in obj:
            out.extend(_flatten(item))
    elif isinstance(obj, dict):
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for item in obj["@graph"]:
                out.extend(_flatten(item))
        else:
            out.append(obj)
    return out


def _type_of(block: Dict) -> List[str]:
    t = block.get("@type", [])
    if isinstance(t, str):
        return [t]
    if isinstance(t, list):
        return [str(x) for x in t]
    return []


def _normalize_role(raw: str) -> Optional[str]:
    low = (raw or "").strip().lower()
    # recherche le rôle le plus long qui matche (ex: "Managing Partner")
    matches = [(k, v) for k, v in TARGET_ROLES_LC.items() if k in low]
    if not matches:
        return None
    matches.sort(key=lambda kv: len(kv[0]), reverse=True)
    return matches[0][1]


def extract_people_from_jsonld(
    html: str,
    base_url: str = "",
) -> List[Dict]:
    """Retourne une liste de {name, role, company, linkedin, email, image}."""
    blocks = extract_jsonld(html)
    out: List[Dict] = []

    # Chercher l'organisation (pour assigner une company par défaut)
    org_name: Optional[str] = None
    for b in blocks:
        if any(t in _type_of(b) for t in ("Organization", "Corporation", "LocalBusiness")):
            org_name = org_name or b.get("name")

    for b in blocks:
        types = _type_of(b)
        if "Person" not in types:
            continue
        name = (b.get("name") or "").strip()
        if not name:
            given = b.get("givenName", "")
            family = b.get("familyName", "")
            name = f"{given} {family}".strip()
        if not name:
            continue

        role_raw = b.get("jobTitle") or b.get("role") or ""
        if isinstance(role_raw, list):
            role_raw = " ".join(str(x) for x in role_raw)
        role = _normalize_role(str(role_raw))
        if not role:
            continue

        # company : works_for > default org
        company = None
        works_for = b.get("worksFor") or b.get("affiliation")
        if isinstance(works_for, dict):
            company = works_for.get("name")
        elif isinstance(works_for, list) and works_for:
            if isinstance(works_for[0], dict):
                company = works_for[0].get("name")
        company = company or org_name

        # sameAs / url → LinkedIn
        linkedin = None
        same_as = b.get("sameAs", [])
        if isinstance(same_as, str):
            same_as = [same_as]
        for u in same_as or []:
            if isinstance(u, str) and "linkedin.com/in/" in u.lower():
                linkedin = u
                break

        email = b.get("email")
        if isinstance(email, list):
            email = email[0] if email else None

        phone = b.get("telephone") or b.get("phone")
        if isinstance(phone, list):
            phone = phone[0] if phone else None

        image = b.get("image")
        if isinstance(image, dict):
            image = image.get("url")

        out.append({
            "name": name,
            "role": role,
            "company": company,
            "linkedin": linkedin,
            "email": email,
            "phone": phone,
            "image": image,
            "source": "jsonld",
        })
    return out
