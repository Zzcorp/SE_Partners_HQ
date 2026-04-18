"""Orchestrateur du scraper SE_Parteners.

Pipeline :
  search → fetch_full → recency filter → extract (regex + JSON-LD + LLM)
  → team-page crawl → email enrich (MX + patterns)
  → entity resolution → lead scoring → CSV/JSONL triés

Usage basique :
    python main.py
    python main.py --max-results 10 --pdf
    python main.py --categories gp_new_funds,lp_family_offices --geo "Singapore"
    python main.py --list-categories
    python main.py --no-llm        # skip Claude (coût/latence)
    python main.py --no-team-crawl # skip deep crawl des sites de fonds
"""
from __future__ import annotations
import argparse
import csv
import json
import logging
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import config
from config import (
    LOG_DIR, OUTPUT_DIR, REQUEST_DELAY_SECONDS,
    RECENCY_MAX_MONTHS, RECENCY_REQUIRED,
)
from queries import build_queries, list_categories, PLATFORM_GROUP_NAMES
from search import run_queries
from scraper import fetch_full, shutdown_browser, domain_of
from extractor import extract_people, person_to_row, TARGET_ROLES, Person
from recency import detect_publish_date, is_recent, months_ago
from team_crawler import crawl_team_pages
from email_finder import enrich_person
from scoring import resolve_entities, score_and_filter


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    log_file = LOG_DIR / f"scrape_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scraper GP/LP pour SE_Parteners (pipeline multi-sources)",
    )
    p.add_argument("--categories", default="",
        help="Catégories séparées par virgules (défaut: toutes). Voir --list-categories.")
    p.add_argument("--min-priority", type=int, default=5,
        help="Garde priorité <= N (1=top, 5=large). Défaut 5.")
    p.add_argument("--max-results", type=int, default=15,
        help="Résultats max par requête. Défaut 15.")
    p.add_argument("--pdf", action="store_true", help="Ajoute filetype:pdf aux requêtes.")
    p.add_argument("--geo", default="", help="Filtre géo. Ex: 'Singapore', 'Europe'.")
    p.add_argument("--delay", type=float, default=REQUEST_DELAY_SECONDS,
        help="Délai entre requêtes en secondes.")
    p.add_argument("--out", default="", help="Préfixe du fichier de sortie.")
    p.add_argument("--no-cache", action="store_true", help="Désactive le cache pages.")
    p.add_argument("--no-js", action="store_true", help="Désactive le fallback Selenium.")
    p.add_argument("--no-llm", action="store_true", help="Désactive l'extraction LLM (Claude).")
    p.add_argument("--no-team-crawl", action="store_true",
        help="Désactive le deep crawl des pages team/about.")
    p.add_argument("--no-email-enrich", action="store_true",
        help="Désactive la génération de candidats email + MX check.")
    p.add_argument("--exclude-platforms", action="store_true",
        help="Skip les requêtes site: (LinkedIn, Crunchbase, Pitchbook…).")
    p.add_argument("--platforms-only", action="store_true",
        help="N'exécute QUE les requêtes plateformes ciblées.")
    p.add_argument("--headed", action="store_true", help="Chrome visible (debug).")
    p.add_argument("--list-categories", action="store_true",
        help="Affiche les catégories disponibles et quitte.")
    p.add_argument("--recency-months", type=int, default=RECENCY_MAX_MONTHS,
        help=f"Rejette les sources > N mois (défaut: {RECENCY_MAX_MONTHS}).")
    p.add_argument("--recency-required", action="store_true",
        help="Rejette aussi toute source sans date détectable.")
    p.add_argument("-v", "--verbose", action="store_true", help="Logging debug.")
    return p.parse_args()


def _process_page(
    *,
    text: str,
    html: str,
    source_url: str,
    source_title: str,
    query_group: str,
    priority: int,
    snippet: str,
    kind: str,
    use_llm: bool,
    recency_months_max: int,
    recency_required: bool,
    log: logging.Logger,
) -> List[Dict]:
    """Extrait → recency filter → enrichit, retourne des dicts."""
    # Recency (tente d'abord le texte snippet si la page est vide — ex: login wall)
    dt = detect_publish_date(html, text) or detect_publish_date("", snippet)
    rec_m = months_ago(dt) if dt else None
    if not is_recent(dt, recency_months_max, required=recency_required):
        log.debug("  skipped (vieux: %s mo) → %s", rec_m, source_url)
        return []

    # Construit le texte d'extraction : snippet + titre devant le texte.
    # Nécessaire pour LinkedIn/Crunchbase : la page retournée est souvent
    # un login wall, mais le snippet SERP contient "Name - Role at Company".
    composite_text = "\n\n".join(
        [p for p in (source_title, snippet, text) if p]
    )

    # Extraction multi-sources
    people = extract_people(
        text=composite_text,
        source_url=source_url,
        source_title=source_title,
        query_group=query_group,
        priority=priority,
        snippet=snippet,
        html=html,
        use_llm=use_llm,
    )
    if not people:
        return []

    rows: List[Dict] = []
    for p in people:
        row = person_to_row(p)
        row["recency_months"] = rec_m
        row["kind"] = kind
        rows.append(row)
    return rows


def _maybe_team_crawl(
    seed_url: str,
    query_group: str,
    priority: int,
    use_llm: bool,
    recency_months_max: int,
    recency_required: bool,
    allow_js: bool,
    log: logging.Logger,
) -> List[Dict]:
    """Crawl profond des pages équipe du site si ça ressemble à un site de fonds."""
    try:
        pages = crawl_team_pages(seed_url, allow_js=allow_js)
    except Exception as e:
        log.debug("team crawl failed %s: %s", seed_url, e)
        return []
    rows: List[Dict] = []
    for page in pages:
        rows.extend(_process_page(
            text=page.get("text", ""),
            html=page.get("html", ""),
            source_url=page.get("url", ""),
            source_title="",
            query_group=query_group + ":team",
            priority=priority,
            snippet="",
            kind=page.get("kind", ""),
            use_llm=use_llm,
            recency_months_max=recency_months_max,
            recency_required=recency_required,
            log=log,
        ))
    return rows


def _enrich_emails(row: Dict) -> Dict:
    """Ajoute email_candidates + email_domain + mx_ok."""
    existing = row.get("emails") or []
    if isinstance(existing, str):
        existing = [e for e in existing.split(";") if e]
    info = enrich_person(
        full_name=row.get("name", ""),
        source_url=row.get("source_url", ""),
        existing_emails=existing,
    )
    row["email_domain"] = info["domain"]
    row["email_mx_ok"] = info["mx_ok"]
    row["email_candidates"] = info["candidates"]
    return row


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)
    log = logging.getLogger("main")

    if args.list_categories:
        print("Catégories disponibles :")
        for c in list_categories():
            print("  -", c)
        return 0

    if args.no_js:
        config.ENABLE_SELENIUM = False
    if args.headed:
        config.SELENIUM_HEADLESS = False

    use_llm = not args.no_llm and config.LLM_ENABLED
    if args.no_llm:
        log.info("LLM extraction désactivée (--no-llm).")
    elif not config.LLM_ENABLED:
        log.info("LLM extraction désactivée (ANTHROPIC_API_KEY absent).")

    cats = [c.strip() for c in args.categories.split(",") if c.strip()] or None
    if args.platforms_only:
        cats = list(PLATFORM_GROUP_NAMES)
    queries = build_queries(
        categories=cats,
        min_priority=args.min_priority,
        pdf_only=args.pdf,
        extra_geo=args.geo or None,
    )
    if args.exclude_platforms and not args.platforms_only:
        before = len(queries)
        queries = [q for q in queries if q["group"] not in PLATFORM_GROUP_NAMES]
        log.info("--exclude-platforms : %d requêtes retirées.", before - len(queries))
    log.info("%d requêtes planifiées.", len(queries))
    log.info("Rôles ciblés : %s", ", ".join(TARGET_ROLES))

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = args.out or f"leads_{stamp}"
    csv_path = OUTPUT_DIR / f"{prefix}.csv"
    jsonl_path = OUTPUT_DIR / f"{prefix}.jsonl"
    raw_path = OUTPUT_DIR / f"{prefix}_raw_results.jsonl"

    all_rows: List[Dict] = []
    total_pages = 0
    domains_crawled: set = set()

    with open(raw_path, "w", encoding="utf-8") as raw_f:
        try:
            for result in run_queries(
                queries,
                max_results_per_query=args.max_results,
                delay=args.delay,
            ):
                raw_f.write(json.dumps(result, ensure_ascii=False) + "\n")
                raw_f.flush()

                url = result["url"]
                snippet = result.get("snippet", "")
                title = result.get("title", "")
                try:
                    kind, text, html = fetch_full(
                        url, use_cache=not args.no_cache,
                    )
                except Exception as e:
                    log.debug("fetch failed %s: %s", url, e)
                    kind, text, html = "error", "", ""
                total_pages += 1
                # Pas de texte ET pas de snippet → rien à extraire
                if not text and not snippet and not title:
                    continue

                rows = _process_page(
                    text=text,
                    html=html,
                    source_url=url,
                    source_title=title,
                    query_group=result.get("group", ""),
                    priority=result.get("priority", 5),
                    snippet=snippet,
                    kind=kind,
                    use_llm=use_llm,
                    recency_months_max=args.recency_months,
                    recency_required=args.recency_required,
                    log=log,
                )
                if rows:
                    all_rows.extend(rows)
                    for r in rows:
                        log.info(
                            "→ [%s] %s | %s | %s",
                            r.get("source", "?"),
                            r.get("role", ""),
                            r.get("name", ""),
                            r.get("company") or url,
                        )

                # Deep crawl si on détecte un domaine de fonds non-presse
                if not args.no_team_crawl:
                    dom = domain_of(url)
                    if dom and dom not in domains_crawled and _looks_like_fund_site(url, result):
                        domains_crawled.add(dom)
                        team_rows = _maybe_team_crawl(
                            seed_url=url,
                            query_group=result.get("group", ""),
                            priority=result.get("priority", 5),
                            use_llm=use_llm,
                            recency_months_max=args.recency_months,
                            recency_required=args.recency_required,
                            allow_js=config.ENABLE_SELENIUM,
                            log=log,
                        )
                        if team_rows:
                            all_rows.extend(team_rows)
                            log.info(
                                "  ↳ team crawl %s → %d personne(s)",
                                dom, len(team_rows),
                            )

                time.sleep(args.delay * 0.5)
        except KeyboardInterrupt:
            log.warning("Interruption clavier — sauvegarde des résultats courants.")
        finally:
            shutdown_browser()

    log.info("Extraction terminée. %d personnes brutes avant dedup.", len(all_rows))

    # Entity resolution
    merged = resolve_entities(all_rows)
    log.info("Après dedup : %d personnes uniques.", len(merged))

    # Email enrichment
    if not args.no_email_enrich:
        for row in merged:
            _enrich_emails(row)

    # Lead scoring + filter
    final_rows = score_and_filter(merged)
    log.info("Après filtre score (>= %s) : %d leads.",
             config.MIN_LEAD_SCORE, len(final_rows))

    _write_outputs(final_rows, csv_path, jsonl_path)

    log.info("Pages visitées=%d, leads finaux=%d", total_pages, len(final_rows))
    log.info("Sortie CSV   : %s", csv_path)
    log.info("Sortie JSONL : %s", jsonl_path)
    log.info("Résultats bruts (debug) : %s", raw_path)
    return 0


def _looks_like_fund_site(url: str, result: Dict) -> bool:
    """Heuristique : URL racine/short path sur un site qui n'est PAS de la presse."""
    from email_finder import NON_CORPORATE
    dom = domain_of(url)
    if not dom or dom in NON_CORPORATE:
        return False
    # Évite les blogposts/articles longs → on veut la home ou /team ou /about
    path = url.split(dom, 1)[-1]
    # Accepte path court (≤ 2 segments non vides)
    segs = [s for s in path.split("/") if s]
    return len(segs) <= 2


def _write_outputs(rows: List[Dict], csv_path: Path, jsonl_path: Path) -> None:
    csv_fields = [
        "lead_score", "name", "role", "company",
        "priority", "query_group",
        "emails", "email_candidates", "email_domain", "email_mx_ok",
        "phones", "linkedin",
        "fund_size", "fund_close_step",
        "recency_months", "n_sources", "source", "source_url",
        "source_title", "evidence", "context_snippet",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as csv_f, \
         open(jsonl_path, "w", encoding="utf-8") as jsonl_f:
        writer = csv.DictWriter(csv_f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flat = dict(row)
            for list_field in ("emails", "email_candidates", "phones"):
                if isinstance(flat.get(list_field), list):
                    flat[list_field] = ";".join(flat[list_field])
            writer.writerow(flat)
            jsonl_f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
