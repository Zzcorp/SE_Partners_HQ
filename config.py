"""Configuration centrale du scraper SE_Parteners."""
from pathlib import Path
import os

# Charge .env local si présent (clés API, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = BASE_DIR / "cache"
LOG_DIR = BASE_DIR / "logs"

for d in (OUTPUT_DIR, CACHE_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

REQUEST_TIMEOUT = 20
REQUEST_DELAY_SECONDS = 2.0
MAX_RESULTS_PER_QUERY = 15
MAX_PAGE_BYTES = 6_000_000

# Selenium / rendu JS
ENABLE_SELENIUM = True
SELENIUM_HEADLESS = True
SELENIUM_PAGE_TIMEOUT = 25
SELENIUM_WAIT_AFTER_LOAD = 2.0
SELENIUM_MAX_PAGES_PER_SESSION = 40  # restart driver après N pages

# Per-domain throttling
PER_DOMAIN_MIN_INTERVAL = 3.0  # secondes min entre deux hits même domaine

# Signaux "il faut le JS" / bot wall
JS_TRIGGER_PATTERNS = [
    "checking your browser", "just a moment", "cloudflare",
    "enable javascript", "please enable js", "attention required",
    "captcha", "are you human", "access denied",
    "ddos protection", "perimeterx", "incapsula", "distil",
]
MIN_TEXT_LENGTH_FOR_STATIC = 400  # sous ce seuil → retente en JS
SPA_HINTS = ["<div id=\"root\"></div>", "<div id=\"app\"></div>", "window.__NUXT__"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

SERPAPI_KEY = os.getenv("SERPAPI_KEY")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BING_API_KEY = os.getenv("BING_API_KEY")

DEFAULT_REGION = "wt-wt"
DEFAULT_SAFESEARCH = "moderate"

BLOCKED_DOMAINS = {
    "youtube.com", "facebook.com", "twitter.com", "x.com",
    "instagram.com", "tiktok.com", "pinterest.com",
}

# -------------------------------------------------------------------------
# LLM extraction (Claude API)
# -------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
# Modèle par défaut = Opus 4.7 (le plus précis, plus cher).
# Alternative pour extraction en bulk peu coûteuse : "claude-haiku-4-5"
LLM_MODEL = os.getenv("LLM_MODEL", "claude-opus-4-7")
LLM_ENABLED = bool(ANTHROPIC_API_KEY)
LLM_MAX_CHARS_PER_PAGE = 18_000  # tronque le texte envoyé au LLM
LLM_MAX_TOKENS = 4096

# -------------------------------------------------------------------------
# Recency
# -------------------------------------------------------------------------
RECENCY_MAX_MONTHS = 12  # ignore les sources > 12 mois
RECENCY_REQUIRED = False  # si True, rejette toute source sans date

# -------------------------------------------------------------------------
# Team-page crawling
# -------------------------------------------------------------------------
TEAM_PATH_HINTS = [
    "/team", "/about", "/about-us", "/people", "/our-team",
    "/leadership", "/partners", "/our-people", "/who-we-are",
    "/equipe", "/notre-equipe", "/a-propos",
]
TEAM_CRAWL_MAX_PAGES_PER_DOMAIN = 6

# -------------------------------------------------------------------------
# Scoring
# -------------------------------------------------------------------------
ROLE_WEIGHTS = {
    # GP side (fonds qui lèvent)
    "Managing Partner": 1.00,
    "General Partner": 0.95,
    "Head of Investor Relations": 0.95,
    "Head of IR": 0.90,
    "Investment Partner": 0.85,
    "Partner": 0.70,
    "Principal": 0.55,
    "Director": 0.50,
    # LP side (investisseurs institutionnels)
    "Chief Investment Officer": 0.95,
    "Head of Investments": 0.90,
    "Investment Director": 0.80,
    "Portfolio Manager": 0.75,
    "Investment Manager": 0.70,
    "PE Manager": 0.65,
    "VC Manager": 0.65,
}
PRIORITY_WEIGHTS = {1: 1.00, 2: 0.70, 3: 0.45}
CLOSE_STAGE_WEIGHTS = {
    "final close": 1.00,
    "second close": 0.90,
    "first close": 0.85,
    "anchor": 0.95,
    "cornerstone": 0.95,
}
MIN_LEAD_SCORE = 0.15  # filtre sortie finale
