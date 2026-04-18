"""Extraction d'entités basée LLM (Anthropic Claude).

- Claude API via SDK officiel `anthropic`
- Réponses structurées via `client.messages.parse()` + Pydantic
- Prompt caching : le système prompt fixe est mis en cache (≈90% d'économie
  sur les appels suivants car on envoie les mêmes instructions à chaque page)
- Mode dégradé : si pas d'ANTHROPIC_API_KEY, on no-op proprement

Objectif : boucher les trous du regex → "according to Jane Doe, partner at...",
noms/rôles séparés dans des <div> distincts, structure de presse non triviale.
"""
from __future__ import annotations
import logging
import re
from typing import List, Optional

from pydantic import BaseModel, Field

from config import (
    ANTHROPIC_API_KEY, LLM_ENABLED, LLM_MAX_CHARS_PER_PAGE,
    LLM_MAX_TOKENS, LLM_MODEL,
)

log = logging.getLogger(__name__)

# ---- Import paresseux de l'SDK Anthropic -------------------------------
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
            log.warning("Impossible d'initialiser Claude : %s", e)
            _client = False
    return _client or None


# ---- Schéma Pydantic pour la réponse structurée ------------------------
class LLMPerson(BaseModel):
    name: str = Field(..., description="Nom complet prénom + nom")
    role: str = Field(..., description="Rôle tel que mentionné (Partner, Managing Partner, etc.)")
    company: Optional[str] = Field(None, description="Nom du fonds / société")
    evidence: str = Field(..., description="Phrase exacte de la source qui prouve le rôle")


class LLMExtraction(BaseModel):
    people: List[LLMPerson] = Field(default_factory=list)
    fund_name: Optional[str] = Field(None, description="Nom du fonds mentionné, s'il y a")
    fund_stage: Optional[str] = Field(None, description="first close | second close | final close | raising | launched | null")
    fund_size: Optional[str] = Field(None, description="Taille cible du fonds (€100M, $50M, etc.)")
    geography: Optional[str] = Field(None, description="Focus géographique (Europe, SEA, Singapore, etc.)")


# ---- Prompt système (fixe → mis en cache) -----------------------------
SYSTEM_PROMPT = """\
Tu es un analyste spécialisé dans l'écosystème Venture Capital / Private Equity.
Ton rôle : extraire avec précision des profils humains présents dans des \
contenus d'actualité financière ou de sites de fonds.

RÔLES QUI T'INTÉRESSENT (ne rapporter QUE ceux-là) :

Côté GP (fonds qui lèvent) :
- Head of Investor Relations
- Head of IR
- Managing Partner
- General Partner
- Investment Partner
- Partner
- Principal
- Director (uniquement si clairement investment/fund director, pas board director)

Côté LP (investisseurs institutionnels — family office, pension fund, \
endowment, sovereign wealth, fund-of-funds, corporate VC allouant à des GPs) :
- Chief Investment Officer (CIO)
- Head of Investments
- Investment Director
- Portfolio Manager
- Investment Manager
- PE Manager
- VC Manager

RÈGLES STRICTES :
1. Ne rapporter une personne QUE si son rôle est clairement associé à un fonds \
   d'investissement, un family office, un endowment, une pension fund, un \
   sovereign wealth fund, ou une structure équivalente allouant à des fonds.
2. Pour chaque personne : fournir la phrase exacte (`evidence`) qui prouve le \
   lien nom ↔ rôle ↔ société.
3. Si le texte parle de "X, partner at Y" mais que Y est un cabinet d'avocats, \
   une banque d'affaires traditionnelle ou autre → NE PAS inclure.
4. Ignorer les rôles board/advisory sauf si explicitement GP.
5. Ignorer les mentions génériques ("general partner in the fund" sans nom).
6. Pour les rôles LP (Portfolio Manager, Investment Director, etc.), vérifier \
   que la société gère un portefeuille d'investissements dans des fonds \
   (PE/VC/alternatifs). Un "Portfolio Manager" d'une banque retail ne compte PAS.
7. Pour `fund_stage` : normaliser en minuscule, exactement l'une de ces valeurs \
   ou null : first close, second close, final close, raising, launched.
8. Si aucune personne pertinente → retourner une liste `people` vide (ne pas \
   inventer).

Répondre UNIQUEMENT au format JSON conforme au schéma demandé.\
"""


# -------------------------------------------------------------------------
# Extraction
# -------------------------------------------------------------------------
def extract_with_llm(
    text: str,
    source_url: str = "",
    source_title: str = "",
) -> Optional[LLMExtraction]:
    """Appelle Claude sur le texte et retourne une LLMExtraction (ou None)."""
    client = _get_client()
    if client is None or not text:
        return None

    # Tronquer pour éviter de gaspiller du token sur des pages énormes
    truncated = text[:LLM_MAX_CHARS_PER_PAGE]

    user_content = (
        f"Source URL : {source_url}\n"
        f"Source title : {source_title}\n\n"
        f"--- TEXTE BRUT ---\n{truncated}\n--- FIN ---\n\n"
        "Extrais les personnes correspondant aux rôles ciblés et les "
        "métadonnées du fonds mentionné. Retourne strictement le JSON "
        "conforme au schéma."
    )

    try:
        # Prompt caching sur le system prompt → ~90% d'économie sur les
        # appels suivants (le system est identique à chaque requête).
        response = client.messages.parse(
            model=LLM_MODEL,
            max_tokens=LLM_MAX_TOKENS,
            system=[{
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{"role": "user", "content": user_content}],
            output_format=LLMExtraction,
        )
    except Exception as e:
        # fallback via create() + json_schema si .parse() pas dispo dans
        # la version installée du SDK
        log.debug("messages.parse failed (%s), fallback create+json_schema", e)
        try:
            resp = client.messages.create(
                model=LLM_MODEL,
                max_tokens=LLM_MAX_TOKENS,
                system=[{
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_content}],
                output_config={
                    "format": {
                        "type": "json_schema",
                        "schema": LLMExtraction.model_json_schema(),
                    }
                },
            )
            txt = next(
                (b.text for b in resp.content if getattr(b, "type", "") == "text"),
                "",
            )
            import json as _json
            data = _json.loads(_extract_json(txt))
            return LLMExtraction.model_validate(data)
        except Exception as e2:
            log.warning("LLM extraction failed for %s: %s", source_url, e2)
            return None

    # Log utile sur le cache hit
    try:
        usage = response.usage
        cached = getattr(usage, "cache_read_input_tokens", 0) or 0
        if cached:
            log.debug("LLM cache hit: %d tokens read from cache", cached)
    except Exception:
        pass

    return response.parsed_output


def _extract_json(s: str) -> str:
    """Extrait le premier objet JSON valide d'une string (fallback)."""
    m = re.search(r"\{.*\}", s, re.DOTALL)
    return m.group(0) if m else s


# -------------------------------------------------------------------------
# Conversion vers le schéma Person commun
# -------------------------------------------------------------------------
def llm_to_common(
    extraction: LLMExtraction,
    source_url: str,
    source_title: str,
) -> List[dict]:
    """Convertit une LLMExtraction en liste de dicts Person compatibles."""
    rows = []
    fund_size = extraction.fund_size
    stage = extraction.fund_stage
    geo = extraction.geography
    for p in extraction.people:
        rows.append({
            "name": p.name,
            "role": p.role,
            "company": p.company or extraction.fund_name,
            "fund_size": fund_size,
            "fund_close_step": stage,
            "geography": geo,
            "evidence": p.evidence,
            "source_url": source_url,
            "source_title": source_title,
            "source": "llm",
        })
    return rows
