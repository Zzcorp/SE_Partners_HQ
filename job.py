"""Orchestrateur de scraping pipeliné pour le webapp HQ.

Architecture en 4 étages (chaque étage a N workers async) :

    query_q → [SEARCH x1]   → url_q
    url_q   → [FETCH   xN1] → page_q
    page_q  → [EXTRACT xN2] → enrich_q
    enrich_q→ [ENRICH  xN3] → results (in-memory) + WS broadcast

Dédup :
 - URL vue une seule fois (shared set guarded by lock)
 - Personne (name|company|role) émise une seule fois en live

Broadcast :
 - Chaque événement (log, worker state, person, metric) est poussé dans un
   pub/sub in-memory ; le webapp abonne chaque WebSocket connectée.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Set

import config
from queries import build_queries, PLATFORM_GROUP_NAMES
from search import search as run_search
from scraper import fetch_full, shutdown_browser, domain_of
from extractor import extract_people, person_to_row
from recency import detect_publish_date, is_recent, months_ago
from email_finder import enrich_person
from scoring import resolve_entities, score_lead, _dedup_key


log = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# Pub/Sub broadcaster (in-process)
# -----------------------------------------------------------------------
class Broadcaster:
    """Fan-out asyncio.Queue → N subscribers."""

    def __init__(self, buffer: int = 500) -> None:
        self._subs: List[asyncio.Queue] = []
        self._backlog: Deque[Dict] = deque(maxlen=buffer)
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        async with self._lock:
            self._subs.append(q)
            # Rejoue le backlog pour les nouveaux clients
            for evt in list(self._backlog):
                await q.put(evt)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        async with self._lock:
            if q in self._subs:
                self._subs.remove(q)

    async def publish(self, evt: Dict) -> None:
        evt = dict(evt)
        evt.setdefault("ts", time.time())
        self._backlog.append(evt)
        async with self._lock:
            dead = []
            for q in self._subs:
                try:
                    q.put_nowait(evt)
                except asyncio.QueueFull:
                    dead.append(q)
            for q in dead:
                if q in self._subs:
                    self._subs.remove(q)


# -----------------------------------------------------------------------
# Worker state (visible dans l'UI)
# -----------------------------------------------------------------------
@dataclass
class WorkerState:
    id: str
    stage: str               # search | fetch | extract | enrich
    status: str = "idle"     # idle | working | done
    current: str = ""        # description courte de la tâche
    processed: int = 0
    started_at: float = field(default_factory=time.time)

    def snapshot(self) -> Dict:
        return asdict(self)


# -----------------------------------------------------------------------
# Metrics agrégées live
# -----------------------------------------------------------------------
@dataclass
class Metrics:
    queries_total: int = 0
    queries_done: int = 0
    pages_fetched: int = 0
    pages_ok: int = 0
    people_raw: int = 0
    people_unique: int = 0
    leads_final: int = 0
    by_role: Counter = field(default_factory=Counter)
    by_engine: Counter = field(default_factory=Counter)
    by_source: Counter = field(default_factory=Counter)  # regex/jsonld/llm
    by_kind: Counter = field(default_factory=Counter)

    def snapshot(self) -> Dict:
        return {
            "queries_total": self.queries_total,
            "queries_done": self.queries_done,
            "pages_fetched": self.pages_fetched,
            "pages_ok": self.pages_ok,
            "people_raw": self.people_raw,
            "people_unique": self.people_unique,
            "leads_final": self.leads_final,
            "by_role": dict(self.by_role.most_common(15)),
            "by_engine": dict(self.by_engine),
            "by_source": dict(self.by_source),
            "by_kind": dict(self.by_kind),
        }


# -----------------------------------------------------------------------
# JobRunner
# -----------------------------------------------------------------------
class JobRunner:
    """Pipeline de scraping. Une instance = un run."""

    def __init__(
        self,
        *,
        broadcaster: Broadcaster,
        categories: Optional[List[str]] = None,
        min_priority: int = 5,
        max_results_per_query: int = 10,
        use_llm: bool = True,
        use_team_crawl: bool = True,
        use_email_enrich: bool = True,
        exclude_platforms: bool = False,
        platforms_only: bool = False,
        extra_geo: Optional[str] = None,
        pdf_only: bool = False,
        recency_months_max: int = 12,
        recency_required: bool = False,
        fetch_workers: int = 3,
        extract_workers: int = 2,
        enrich_workers: int = 2,
    ) -> None:
        self.id = uuid.uuid4().hex[:12]
        self.broadcaster = broadcaster
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.running = False

        self.categories = categories
        self.min_priority = min_priority
        self.max_results_per_query = max_results_per_query
        self.use_llm = use_llm and config.LLM_ENABLED
        self.use_team_crawl = use_team_crawl
        self.use_email_enrich = use_email_enrich
        self.exclude_platforms = exclude_platforms
        self.platforms_only = platforms_only
        self.extra_geo = extra_geo
        self.pdf_only = pdf_only
        self.recency_months_max = recency_months_max
        self.recency_required = recency_required
        self.n_fetch = fetch_workers
        self.n_extract = extract_workers
        self.n_enrich = enrich_workers

        # Pipeline queues
        self.query_q: asyncio.Queue = asyncio.Queue()
        self.url_q: asyncio.Queue = asyncio.Queue(maxsize=500)
        self.page_q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.enrich_q: asyncio.Queue = asyncio.Queue(maxsize=200)

        # Shared state
        self.seen_urls: Set[str] = set()
        self.seen_people: Set[str] = set()
        self.domains_team_crawled: Set[str] = set()
        self.url_lock = asyncio.Lock()
        self.people_lock = asyncio.Lock()
        self.dom_lock = asyncio.Lock()

        self.results_all: List[Dict] = []   # après dedup final
        self.metrics = Metrics()
        self.workers: Dict[str, WorkerState] = {}
        self._tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

    # ----- helpers ---------------------------------------------------
    async def _log(self, line: str, level: str = "info") -> None:
        stamp = datetime.utcnow().strftime("%H:%M:%S")
        await self.broadcaster.publish({
            "type": "log",
            "line": f"[{stamp}] {line}",
            "level": level,
        })

    async def _worker_evt(self, w: WorkerState) -> None:
        await self.broadcaster.publish({"type": "worker", "worker": w.snapshot()})

    async def _metric_evt(self) -> None:
        await self.broadcaster.publish({"type": "metrics", "metrics": self.metrics.snapshot()})

    async def _person_evt(self, row: Dict) -> None:
        await self.broadcaster.publish({"type": "person", "person": row})

    # ----- stages ----------------------------------------------------
    async def _stage_search(self, w: WorkerState) -> None:
        while not self._stop.is_set():
            try:
                qitem = await asyncio.wait_for(self.query_q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            w.status = "working"
            w.current = qitem["query"][:80]
            await self._worker_evt(w)
            await self._log(f"🔎 search: {qitem['query']}")

            try:
                loop = asyncio.get_running_loop()
                results = await loop.run_in_executor(
                    None, run_search, qitem["query"], self.max_results_per_query,
                )
            except Exception as e:
                await self._log(f"search error: {e}", "warn")
                results = []

            for r in results:
                url = r.get("url") or ""
                if not url:
                    continue
                async with self.url_lock:
                    if url in self.seen_urls:
                        continue
                    self.seen_urls.add(url)
                self.metrics.by_engine[r.get("engine", "?")] += 1
                await self.url_q.put({
                    "url": url,
                    "title": r.get("title", ""),
                    "snippet": r.get("snippet", ""),
                    "engine": r.get("engine", ""),
                    "group": qitem.get("group", ""),
                    "priority": qitem.get("priority", 5),
                })

            w.processed += 1
            self.metrics.queries_done += 1
            await self._worker_evt(w)
            await self._metric_evt()
            self.query_q.task_done()
            await asyncio.sleep(0.1)
        w.status = "done"
        w.current = ""
        await self._worker_evt(w)

    async def _stage_fetch(self, w: WorkerState) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                item = await asyncio.wait_for(self.url_q.get(), timeout=0.8)
            except asyncio.TimeoutError:
                if self.query_q.empty() and self._search_done:
                    break
                continue
            w.status = "working"
            w.current = item["url"][:100]
            await self._worker_evt(w)

            try:
                kind, text, html = await loop.run_in_executor(
                    None, fetch_full, item["url"], True, False,
                )
            except Exception as e:
                kind, text, html = "error", "", ""
                await self._log(f"fetch error {item['url']}: {e}", "warn")

            self.metrics.pages_fetched += 1
            self.metrics.by_kind[kind] += 1
            if text or html:
                self.metrics.pages_ok += 1

            await self.page_q.put({
                **item, "kind": kind, "text": text, "html": html,
            })
            w.processed += 1
            await self._worker_evt(w)
            await self._metric_evt()
            self.url_q.task_done()
        w.status = "done"
        w.current = ""
        await self._worker_evt(w)

    async def _stage_extract(self, w: WorkerState) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                page = await asyncio.wait_for(self.page_q.get(), timeout=0.8)
            except asyncio.TimeoutError:
                if self._fetch_done and self.url_q.empty():
                    break
                continue
            url = page["url"]
            title = page.get("title", "")
            snippet = page.get("snippet", "")
            text = page.get("text", "")
            html = page.get("html", "")

            w.status = "working"
            w.current = f"extract: {url[:80]}"
            await self._worker_evt(w)

            if not text and not snippet and not title:
                self.page_q.task_done()
                continue

            dt = detect_publish_date(html, text) or detect_publish_date("", snippet)
            if not is_recent(dt, self.recency_months_max, required=self.recency_required):
                self.page_q.task_done()
                continue
            rec_m = months_ago(dt) if dt else None

            composite = "\n\n".join([p for p in (title, snippet, text) if p])

            try:
                people = await loop.run_in_executor(
                    None, lambda: extract_people(
                        text=composite,
                        source_url=url,
                        source_title=title,
                        query_group=page.get("group", ""),
                        priority=page.get("priority", 5),
                        snippet=snippet,
                        html=html,
                        use_llm=self.use_llm,
                    ),
                )
            except Exception as e:
                people = []
                await self._log(f"extract error {url}: {e}", "warn")

            for p in people:
                row = person_to_row(p)
                row["recency_months"] = rec_m
                row["kind"] = page.get("kind", "")
                await self.enrich_q.put(row)
                self.metrics.people_raw += 1
                self.metrics.by_source[row.get("source", "?")] += 1
                self.metrics.by_role[row.get("role", "?")] += 1

            w.processed += 1
            await self._worker_evt(w)
            await self._metric_evt()
            self.page_q.task_done()

            # Team-page deep crawl (une fois par domaine "corporate")
            if self.use_team_crawl:
                dom = domain_of(url)
                from email_finder import NON_CORPORATE
                if (
                    dom and dom not in NON_CORPORATE
                    and dom not in self.domains_team_crawled
                    and len(url.split(dom, 1)[-1].strip("/").split("/")) <= 2
                ):
                    async with self.dom_lock:
                        if dom in self.domains_team_crawled:
                            continue
                        self.domains_team_crawled.add(dom)
                    try:
                        from team_crawler import crawl_team_pages
                        team = await loop.run_in_executor(
                            None, crawl_team_pages, url, False,
                        )
                    except Exception as e:
                        team = []
                        await self._log(f"team crawl {dom}: {e}", "warn")
                    for tp in team:
                        turl = tp.get("url", "")
                        async with self.url_lock:
                            if turl in self.seen_urls:
                                continue
                            self.seen_urls.add(turl)
                        await self.page_q.put({
                            "url": turl,
                            "title": "",
                            "snippet": "",
                            "engine": "team-crawl",
                            "group": page.get("group", "") + ":team",
                            "priority": page.get("priority", 5),
                            "kind": tp.get("kind", ""),
                            "text": tp.get("text", ""),
                            "html": tp.get("html", ""),
                        })
        w.status = "done"
        w.current = ""
        await self._worker_evt(w)

    async def _stage_enrich(self, w: WorkerState) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                row = await asyncio.wait_for(self.enrich_q.get(), timeout=0.8)
            except asyncio.TimeoutError:
                if self._extract_done and self.page_q.empty():
                    break
                continue
            w.status = "working"
            w.current = f"enrich: {row.get('name', '?')}"
            await self._worker_evt(w)

            key = _dedup_key(row)
            if key:
                async with self.people_lock:
                    if key in self.seen_people:
                        self.enrich_q.task_done()
                        continue
                    self.seen_people.add(key)

            if self.use_email_enrich:
                existing = row.get("emails") or []
                if isinstance(existing, str):
                    existing = [e for e in existing.split(";") if e]
                try:
                    info = await loop.run_in_executor(
                        None, lambda: enrich_person(
                            full_name=row.get("name", ""),
                            source_url=row.get("source_url", ""),
                            existing_emails=existing,
                        ),
                    )
                    row["email_domain"] = info["domain"]
                    row["email_mx_ok"] = info["mx_ok"]
                    row["email_candidates"] = info["candidates"]
                except Exception as e:
                    await self._log(f"enrich error: {e}", "warn")

            row["lead_score"] = score_lead(row)
            row["_run_id"] = self.id
            self.results_all.append(row)
            self.metrics.people_unique = len(self.seen_people)
            if row["lead_score"] >= config.MIN_LEAD_SCORE:
                self.metrics.leads_final += 1

            await self._person_evt(row)
            w.processed += 1
            await self._worker_evt(w)
            await self._metric_evt()
            self.enrich_q.task_done()
        w.status = "done"
        w.current = ""
        await self._worker_evt(w)

    # ----- lifecycle -------------------------------------------------
    def _build_queries(self) -> List[Dict]:
        cats = list(self.categories) if self.categories else None
        if self.platforms_only:
            cats = list(PLATFORM_GROUP_NAMES)
        queries = build_queries(
            categories=cats,
            min_priority=self.min_priority,
            pdf_only=self.pdf_only,
            extra_geo=self.extra_geo,
        )
        if self.exclude_platforms and not self.platforms_only:
            queries = [q for q in queries if q["group"] not in PLATFORM_GROUP_NAMES]
        return queries

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.started_at = time.time()
        self._stop.clear()
        self._search_done = False
        self._fetch_done = False
        self._extract_done = False

        queries = self._build_queries()
        self.metrics.queries_total = len(queries)
        await self._log(f"▶ run {self.id} démarré — {len(queries)} requêtes", "info")

        for q in queries:
            self.query_q.put_nowait(q)

        # Spawn workers
        search_w = WorkerState(id="S1", stage="search")
        self.workers[search_w.id] = search_w
        self._tasks.append(asyncio.create_task(self._stage_search(search_w)))

        for i in range(self.n_fetch):
            w = WorkerState(id=f"F{i+1}", stage="fetch")
            self.workers[w.id] = w
            self._tasks.append(asyncio.create_task(self._stage_fetch(w)))
        for i in range(self.n_extract):
            w = WorkerState(id=f"X{i+1}", stage="extract")
            self.workers[w.id] = w
            self._tasks.append(asyncio.create_task(self._stage_extract(w)))
        for i in range(self.n_enrich):
            w = WorkerState(id=f"E{i+1}", stage="enrich")
            self.workers[w.id] = w
            self._tasks.append(asyncio.create_task(self._stage_enrich(w)))

        self._tasks.append(asyncio.create_task(self._supervisor()))
        await self._metric_evt()

    async def _supervisor(self) -> None:
        """Ferme proprement les étages l'un après l'autre."""
        try:
            # Attendre que toutes les queries soient consommées
            await self.query_q.join()
            self._search_done = True
            await self.url_q.join()
            self._fetch_done = True
            await self.page_q.join()
            self._extract_done = True
            await self.enrich_q.join()
            await self._log("✔ pipeline drainé", "info")
        except asyncio.CancelledError:
            return
        finally:
            await self._finalize()

    async def _finalize(self) -> None:
        self.running = False
        self.finished_at = time.time()
        # Dedup + score + filter final
        merged = resolve_entities(self.results_all)
        for row in merged:
            row["lead_score"] = score_lead(row)
        merged.sort(key=lambda r: r.get("lead_score", 0.0), reverse=True)
        self.results_all = merged
        self.metrics.leads_final = sum(
            1 for r in merged if r.get("lead_score", 0) >= config.MIN_LEAD_SCORE
        )
        await self._log(
            f"✅ run terminé — {len(merged)} uniques, "
            f"{self.metrics.leads_final} leads",
            "info",
        )
        await self.broadcaster.publish({"type": "done", "run_id": self.id})
        await self._metric_evt()
        try:
            shutdown_browser()
        except Exception:
            pass

    async def stop(self) -> None:
        if not self.running:
            return
        self._stop.set()
        await self._log("⏹ arrêt demandé", "warn")
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._finalize()

    def snapshot(self) -> Dict:
        return {
            "id": self.id,
            "running": self.running,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "metrics": self.metrics.snapshot(),
            "workers": {wid: w.snapshot() for wid, w in self.workers.items()},
        }

    def top_leads(self, limit: int = 50) -> List[Dict]:
        return self.results_all[:limit]
