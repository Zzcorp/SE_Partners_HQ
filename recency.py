"""Détection de la date de publication d'une page + filtrage par fraîcheur."""
from __future__ import annotations
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from bs4 import BeautifulSoup

try:
    import dateparser  # type: ignore
except Exception:  # pragma: no cover
    dateparser = None

log = logging.getLogger(__name__)

# Meta tags courants où se trouve la date de publication
META_PROPS = [
    ("meta", {"property": "article:published_time"}),
    ("meta", {"property": "og:article:published_time"}),
    ("meta", {"property": "og:updated_time"}),
    ("meta", {"name": "pubdate"}),
    ("meta", {"name": "publishdate"}),
    ("meta", {"name": "publish_date"}),
    ("meta", {"name": "date"}),
    ("meta", {"name": "DC.date"}),
    ("meta", {"itemprop": "datePublished"}),
]

# Regex date visible dans le texte (fallback)
DATE_TEXT_RE = re.compile(
    r"\b(?:"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}"
    r"|\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}"
    r"|\d{4}-\d{2}-\d{2}"
    r")\b",
)


def _parse(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    if dateparser:
        try:
            parsed = dateparser.parse(dt_str)
            if parsed:
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
        except Exception:
            pass
    # Fallback ISO
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def detect_publish_date(html: str, text: str = "") -> Optional[datetime]:
    """Retourne la date de publication détectée, ou None."""
    if html:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")

        # 1. meta tags
        for tag, attrs in META_PROPS:
            el = soup.find(tag, attrs=attrs)
            if el:
                val = el.get("content") or el.get("datetime")
                dt = _parse(val or "")
                if dt:
                    return dt

        # 2. <time datetime="...">
        for t in soup.find_all("time"):
            val = t.get("datetime") or t.text
            dt = _parse(val or "")
            if dt:
                return dt

        # 3. JSON-LD datePublished
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.text or ""
            try:
                data = json.loads(raw)
            except Exception:
                continue
            dt = _find_date_published(data)
            if dt:
                return dt

    # 4. Fallback regex sur le texte extrait
    if text:
        m = DATE_TEXT_RE.search(text[:4000])
        if m:
            dt = _parse(m.group(0))
            if dt:
                return dt
    return None


def _find_date_published(obj) -> Optional[datetime]:
    if isinstance(obj, dict):
        for key in ("datePublished", "dateCreated", "uploadDate", "dateModified"):
            if key in obj:
                dt = _parse(str(obj[key]))
                if dt:
                    return dt
        for v in obj.values():
            dt = _find_date_published(v)
            if dt:
                return dt
    elif isinstance(obj, list):
        for v in obj:
            dt = _find_date_published(v)
            if dt:
                return dt
    return None


def months_ago(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    return delta.days / 30.44


def is_recent(
    dt: Optional[datetime],
    max_months: int,
    required: bool = False,
) -> bool:
    """True si la page doit être gardée."""
    if dt is None:
        return not required
    return months_ago(dt) <= max_months


def recency_multiplier(dt: Optional[datetime]) -> float:
    """Facteur à appliquer au lead score selon la fraîcheur."""
    if dt is None:
        return 0.6  # prudence par défaut
    m = months_ago(dt)
    if m <= 3:
        return 1.20
    if m <= 6:
        return 1.00
    if m <= 12:
        return 0.80
    if m <= 24:
        return 0.50
    return 0.25
