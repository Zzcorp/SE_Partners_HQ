"""Génération de candidats emails + vérification MX du domaine.

On ne brute-force pas le SMTP (VRFY est souvent filtré et ça pue le spam).
On se contente de :
 1. extraire le domaine du fonds (source_url → netloc),
 2. vérifier qu'il a un MX record (dnspython),
 3. générer les patterns courants (firstname.lastname, f.lastname, etc.).

Résultat : pour chaque personne, une liste de candidats emails **plausibles**
(pas 100% confirmés — c'est au commercial de valider).
"""
from __future__ import annotations
import logging
import re
import unicodedata
from functools import lru_cache
from typing import List, Optional, Tuple
from urllib.parse import urlparse

log = logging.getLogger(__name__)

try:
    import dns.resolver  # type: ignore
    _DNS_OK = True
except Exception:  # pragma: no cover
    _DNS_OK = False
    log.debug("dnspython indisponible — vérif MX désactivée.")

# Domaines d'hébergement/presse à ignorer (pas le domaine du fonds)
NON_CORPORATE = {
    "medium.com", "substack.com", "wordpress.com", "blogspot.com",
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "techcrunch.com", "bloomberg.com", "reuters.com", "forbes.com",
    "ft.com", "wsj.com", "pehub.com", "businessinsider.com",
    "crunchbase.com", "pitchbook.com", "preqin.com", "private-equity-insights.com",
    "axios.com", "cnbc.com", "financialtimes.com", "altassets.net",
    "dealstreetasia.com", "efinancialcareers.com", "pionline.com",
    "privatefundscfo.com", "institutionalinvestor.com",
}


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


def _slug(s: str) -> str:
    s = _strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_name_parts(full_name: str) -> Tuple[str, str]:
    """Retourne (first_slug, last_slug). Vide si parsing impossible."""
    parts = [p for p in re.split(r"\s+", full_name.strip()) if p]
    if len(parts) < 2:
        return "", ""
    first = _slug(parts[0])
    last = _slug(parts[-1])
    return first, last


def corporate_domain(url: str) -> Optional[str]:
    """Renvoie le netloc du fonds si l'URL semble corporate, sinon None."""
    try:
        net = urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return None
    if not net or "." not in net:
        return None
    if net in NON_CORPORATE:
        return None
    # filtre extensions bizarres
    if net.endswith((".pdf",)):
        return None
    return net


@lru_cache(maxsize=512)
def has_mx(domain: str) -> bool:
    """True si le domaine a au moins un enregistrement MX."""
    if not _DNS_OK or not domain:
        return False
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=5.0)
        return bool(list(answers))
    except Exception as e:
        log.debug("MX lookup failed for %s: %s", domain, e)
        return False


def generate_candidates(full_name: str, domain: str) -> List[str]:
    """Génère une liste de patterns email courants."""
    first, last = normalize_name_parts(full_name)
    if not (first and last and domain):
        return []
    patterns = [
        f"{first}.{last}@{domain}",
        f"{first[0]}.{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{first}@{domain}",
        f"{last}@{domain}",
        f"{first}_{last}@{domain}",
        f"{first[0]}{last}@{domain}",
    ]
    # dédup en conservant l'ordre
    seen = set()
    out = []
    for p in patterns:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def enrich_person(
    full_name: str,
    source_url: str,
    company_site: Optional[str] = None,
    existing_emails: Optional[List[str]] = None,
) -> dict:
    """Retourne {domain, mx_ok, candidates, confirmed_from_page}.

    - `company_site` prioritaire sur `source_url` si fourni.
    - `existing_emails` = emails déjà trouvés dans la page (plus fiables).
    """
    confirmed = list(existing_emails or [])
    domain = None
    if company_site:
        domain = corporate_domain(company_site)
    if not domain:
        domain = corporate_domain(source_url)

    # Si un email confirmé matche le nom, on garde son domaine
    if not domain and confirmed:
        for e in confirmed:
            d = e.split("@", 1)[-1].lower()
            if d and d not in NON_CORPORATE:
                domain = d
                break

    if not domain:
        return {
            "domain": None, "mx_ok": False,
            "candidates": [], "confirmed": confirmed,
        }

    mx_ok = has_mx(domain)
    candidates = generate_candidates(full_name, domain) if mx_ok else []
    # Ne pas dupliquer les emails déjà confirmés
    confirmed_lc = {e.lower() for e in confirmed}
    candidates = [c for c in candidates if c.lower() not in confirmed_lc]
    return {
        "domain": domain,
        "mx_ok": mx_ok,
        "candidates": candidates,
        "confirmed": confirmed,
    }
