"""Requêtes Google/DuckDuckGo organisées par catégorie.

Basé sur la stratégie de prospection GP/LP (Venture Capital / Private Equity).
Chaque catégorie a une priorité (1 = top, 5 = signal faible).
"""
from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class QueryGroup:
    name: str
    priority: int
    description: str
    queries: List[str] = field(default_factory=list)


# 🔥 1. GPs EN LEVÉE (TOP PRIORITÉ)
GP_NEW_FUNDS = QueryGroup(
    name="gp_new_funds",
    priority=1,
    description="Annonces de nouveaux fonds",
    queries=[
        '"launches new fund" venture capital Europe',
        '"announces new fund" private equity 2026',
        '"VC firm launches €" fund',
        '"private equity fund launch" Europe',
        '"new fund targeting" million venture capital',
    ],
)

GP_ACTIVE_FUNDRAISING = QueryGroup(
    name="gp_active_fundraising",
    priority=1,
    description="Fundraising actif",
    queries=[
        '"currently raising" venture capital fund',
        '"raising fund II" venture capital',
        '"raising fund III" private equity',
        '"fundraising for new fund" VC',
        '"open for LP discussions" fund',
        '"targets €" fund',
    ],
)

GP_FUND_STEPS = QueryGroup(
    name="gp_fund_steps",
    priority=1,
    description="Étapes de levée (ULTRA QUALIFIÉ)",
    queries=[
        '"first close" venture capital fund 2026',
        '"final close" private equity fund Europe',
        '"second close" VC fund',
        '"anchor investor" venture capital fund',
        '"cornerstone investor" private equity',
    ],
)

GP_DISCRETE = QueryGroup(
    name="gp_discrete",
    priority=2,
    description="Version avancée (plus discrète)",
    queries=[
        '"fund in market" private equity',
        '"capital raising" venture capital Europe',
        '"roadshow" LP venture capital',
        '"marketing period" private equity fund',
        '"we are raising"',
        '"currently fundraising"',
        '"open for LP discussions"',
        '"looking for investors"',
    ],
)

# 💰 2. LPs ACTIFS
LP_RECENT_INVESTMENTS = QueryGroup(
    name="lp_recent_investments",
    priority=2,
    description="Investissements récents de LPs",
    queries=[
        '"commits to fund" venture capital',
        '"invests in private equity fund"',
        '"backs venture capital fund"',
        '"family office invests in VC"',
        '"institution commits to fund"',
    ],
)

LP_STRATEGIC_MOVES = QueryGroup(
    name="lp_strategic_moves",
    priority=2,
    description="LPs en mouvement stratégique",
    queries=[
        '"increasing allocation to private equity"',
        '"expanding alternatives portfolio"',
        '"new commitments private equity"',
        '"allocation to venture capital increased"',
        '"new investment strategy"',
        '"portfolio rebalancing"',
    ],
)

LP_FAMILY_OFFICES = QueryGroup(
    name="lp_family_offices",
    priority=1,
    description="Family offices (très important)",
    queries=[
        '"family office invests in fund"',
        '"family office backs venture capital"',
        '"family office private equity allocation"',
    ],
)

# 🔗 3. Requêtes hybrides (GP + LP)
HYBRID_GP_LP = QueryGroup(
    name="hybrid_gp_lp",
    priority=1,
    description="GP + LP en même temps (Jackpot)",
    queries=[
        '"LP invests in venture capital fund"',
        '"family office backs fund"',
        '"pension fund commits to private equity"',
        '"sovereign wealth fund invests in VC"',
    ],
)

# 🌏 4. Filtres géographiques
GEO_SEA = QueryGroup(
    name="geo_sea",
    priority=2,
    description="Focus Asie du Sud-Est",
    queries=[
        '"Southeast Asia" venture capital fund',
        '"Singapore" private equity fund raising',
        '"Vietnam" venture capital investment',
        '"Thailand" private equity fund',
    ],
)

GEO_EU_ASIA = QueryGroup(
    name="geo_eu_asia",
    priority=2,
    description="Europe → Asie (positionnement idéal)",
    queries=[
        '"European fund raising Asia capital"',
        '"European VC Asia investors"',
        '"EU private equity Asia LP"',
    ],
)

# 🧠 5. Signaux faibles
WEAK_BUSINESS = QueryGroup(
    name="weak_business",
    priority=3,
    description="Activité business (corrélé levée)",
    queries=[
        '"hiring investment analyst" venture capital',
        '"expanding investment team" private equity',
        '"opening Singapore office" fund',
    ],
)

WEAK_PRESSURE = QueryGroup(
    name="weak_pressure",
    priority=3,
    description="Pression marché (GPs ouverts)",
    queries=[
        '"fundraising environment challenging" venture capital',
        '"difficult fundraising market" private equity',
        '"LP appetite declining" VC',
    ],
)

# ⚡ 6. Ultra ciblé
TICKET_POSITION = QueryGroup(
    name="ticket_position",
    priority=2,
    description="Ticket / positioning",
    queries=[
        '"mid-market private equity fund raising"',
        '"growth equity fund raising Europe"',
        '"early stage VC fund raising"',
    ],
)

FUND_SIZE = QueryGroup(
    name="fund_size",
    priority=2,
    description="Taille de fonds",
    queries=[
        '"€100M fund" venture capital',
        '"€200M private equity fund first close"',
        '"targeting €300M fund"',
    ],
)

# 🌐 7. Plateformes ciblées (via SERP — on lit le snippet indexé, pas le site)
# Note : on ne scrape PAS LinkedIn/Crunchbase directement (ToS + anti-bot).
# On exploite ce que les moteurs de recherche indexent publiquement.
PLATFORM_LINKEDIN = QueryGroup(
    name="platform_linkedin",
    priority=1,
    description="LinkedIn profiles & posts publics (via SERP)",
    queries=[
        'site:linkedin.com/in "Managing Partner" "venture capital"',
        'site:linkedin.com/in "Managing Partner" "private equity"',
        'site:linkedin.com/in "General Partner" "fund"',
        'site:linkedin.com/in "Head of Investor Relations" fund',
        'site:linkedin.com/in "Head of IR" venture capital',
        'site:linkedin.com/in "Investment Partner" fund',
        'site:linkedin.com/in "Principal" venture capital "fund"',
        'site:linkedin.com/in "family office" Partner',
        'site:linkedin.com/posts "first close" venture capital',
        'site:linkedin.com/posts "final close" fund',
        'site:linkedin.com/posts "announces new fund"',
        'site:linkedin.com/posts "currently raising" VC',
        'site:linkedin.com/posts "we are raising" fund',
        'site:linkedin.com/pulse "first close" fund',
    ],
)

PLATFORM_CRUNCHBASE = QueryGroup(
    name="platform_crunchbase",
    priority=2,
    description="Crunchbase organizations & people (SERP-indexed)",
    queries=[
        'site:crunchbase.com/organization "venture capital" fund',
        'site:crunchbase.com/organization private equity Europe',
        'site:crunchbase.com/organization "family office"',
        'site:crunchbase.com/person "Managing Partner"',
        'site:crunchbase.com/person "General Partner"',
        'site:crunchbase.com/person "Head of Investor Relations"',
        'site:crunchbase.com "new fund" venture capital',
        'site:crunchbase.com "raised" "fund" 2026',
    ],
)

PLATFORM_DATABASES = QueryGroup(
    name="platform_databases",
    priority=2,
    description="Pitchbook / Dealroom / AngelList / Preqin / Wellfound",
    queries=[
        'site:pitchbook.com "fund" "first close"',
        'site:pitchbook.com venture capital Europe raising',
        'site:pitchbook.com "Managing Partner" fund',
        'site:dealroom.co fund launch',
        'site:dealroom.co "raising" venture capital',
        'site:dealroom.co "General Partner"',
        'site:wellfound.com fund Partner',
        'site:angel.co "Managing Partner"',
        'site:preqin.com "fund" private equity',
    ],
)

PLATFORM_PRESS = QueryGroup(
    name="platform_press",
    priority=2,
    description="Presse VC/PE spécialisée (DealStreetAsia, Tech.eu, EU-Startups, Sifted)",
    queries=[
        'site:dealstreetasia.com "new fund"',
        'site:dealstreetasia.com "first close"',
        'site:dealstreetasia.com "final close"',
        'site:tech.eu "venture capital" fund',
        'site:tech.eu "raises" fund',
        'site:eu-startups.com fund "first close"',
        'site:eu-startups.com "new fund" venture capital',
        'site:sifted.eu "new fund" venture capital',
        'site:sifted.eu "first close"',
        'site:techinasia.com "new fund"',
        'site:e27.co "raises fund"',
    ],
)

PLATFORM_REGULATORY = QueryGroup(
    name="platform_regulatory",
    priority=2,
    description="Dépôts réglementaires (SEC Form D, EDGAR)",
    queries=[
        'site:sec.gov "Form D" venture capital',
        'site:sec.gov "Form D" private equity',
        'site:sec.gov/cgi-bin/browse-edgar private fund',
        'site:efts.sec.gov "Form D" "Managing Partner"',
    ],
)

# 🧠 Bonus ultra différenciants
BONUS_TIMING = QueryGroup(
    name="bonus_timing",
    priority=3,
    description="Timing / pain GP",
    queries=[
        '"deployment pace" venture capital',
        '"dry powder" private equity',
        '"capital deployment" fund',
        '"fundraising environment challenging"',
        '"difficult fundraising market"',
    ],
)

ALL_GROUPS: List[QueryGroup] = [
    GP_NEW_FUNDS, GP_ACTIVE_FUNDRAISING, GP_FUND_STEPS, GP_DISCRETE,
    LP_RECENT_INVESTMENTS, LP_STRATEGIC_MOVES, LP_FAMILY_OFFICES,
    HYBRID_GP_LP,
    GEO_SEA, GEO_EU_ASIA,
    WEAK_BUSINESS, WEAK_PRESSURE,
    TICKET_POSITION, FUND_SIZE,
    PLATFORM_LINKEDIN, PLATFORM_CRUNCHBASE, PLATFORM_DATABASES,
    PLATFORM_PRESS, PLATFORM_REGULATORY,
    BONUS_TIMING,
]

PLATFORM_GROUP_NAMES = {
    "platform_linkedin", "platform_crunchbase", "platform_databases",
    "platform_press", "platform_regulatory",
}


def build_queries(
    categories: List[str] | None = None,
    min_priority: int = 5,
    pdf_only: bool = False,
    extra_geo: str | None = None,
) -> List[Dict]:
    """Retourne la liste plate des requêtes à exécuter.

    - categories : noms de QueryGroup à conserver (None = toutes)
    - min_priority : 1 top, 5 large (on garde priority <= min_priority)
    - pdf_only : ajoute filetype:pdf à chaque requête
    - extra_geo : append géographique ex: "Singapore", "Europe"
    """
    out: List[Dict] = []
    for g in ALL_GROUPS:
        if categories and g.name not in categories:
            continue
        if g.priority > min_priority:
            continue
        for q in g.queries:
            query = q
            if extra_geo:
                query = f"{query} {extra_geo}"
            if pdf_only:
                query = f"{query} filetype:pdf"
            out.append({
                "group": g.name,
                "priority": g.priority,
                "query": query,
            })
    return out


def list_categories() -> List[str]:
    return [g.name for g in ALL_GROUPS]
