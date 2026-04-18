"""Pont entre Django et le JobRunner pipeliné.

Un seul run actif à la fois (cohérent avec un webservice à une instance).
Le JobRunner utilise un `Broadcaster` in-memory ; chaque WebSocket consumer
s'y abonne via `subscribe()`.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Dict, Optional

from django.utils import timezone

from job import Broadcaster, JobRunner

log = logging.getLogger(__name__)


class JobManager:
    """Singleton-esque : un run actif + le dernier run terminé."""

    def __init__(self) -> None:
        self.broadcaster = Broadcaster(buffer=500)
        self.current: Optional[JobRunner] = None
        self._persist_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self, **params) -> JobRunner:
        async with self._lock:
            if self.current and self.current.running:
                return self.current
            runner = JobRunner(broadcaster=self.broadcaster, **params)
            self.current = runner
            await runner.start()
            # Persistance après drainage (fire-and-forget)
            self._persist_task = asyncio.create_task(self._persist_when_done(runner, params))
            return runner

    async def stop(self) -> None:
        async with self._lock:
            if self.current and self.current.running:
                await self.current.stop()

    def snapshot(self) -> Dict:
        if not self.current:
            return {"running": False, "current": None}
        return {"running": self.current.running, "current": self.current.snapshot()}

    def top_leads(self, limit: int = 50):
        if not self.current:
            return []
        return self.current.top_leads(limit=limit)

    async def _persist_when_done(self, runner: JobRunner, params: Dict) -> None:
        """Attend la fin puis persiste run + leads en DB."""
        # Poll simple (le runner met `running=False` dans _finalize)
        while runner.running:
            await asyncio.sleep(2.0)

        from asgiref.sync import sync_to_async
        from hq.models import Lead, ScrapeRun

        def _save():
            run = ScrapeRun.objects.create(
                run_id=runner.id,
                categories=params.get("categories") or [],
                params={
                    "min_priority": params.get("min_priority"),
                    "max_results_per_query": params.get("max_results_per_query"),
                    "use_llm": params.get("use_llm"),
                    "platforms_only": params.get("platforms_only"),
                    "exclude_platforms": params.get("exclude_platforms"),
                    "geo": params.get("extra_geo"),
                },
                queries_total=runner.metrics.queries_total,
                queries_done=runner.metrics.queries_done,
                pages_fetched=runner.metrics.pages_fetched,
                people_unique=runner.metrics.people_unique,
                leads_final=runner.metrics.leads_final,
                status="done",
                finished_at=timezone.now(),
            )
            bulk = []
            for row in runner.top_leads(limit=1000):
                bulk.append(Lead(
                    run=run,
                    name=(row.get("name") or "")[:200],
                    role=(row.get("role") or "")[:80],
                    company=(row.get("company") or "")[:200],
                    emails=_as_list(row.get("emails")),
                    email_candidates=_as_list(row.get("email_candidates")),
                    phones=_as_list(row.get("phones")),
                    linkedin=(row.get("linkedin") or "")[:500],
                    fund_size=(row.get("fund_size") or "")[:80],
                    fund_close_step=(row.get("fund_close_step") or "")[:40],
                    recency_months=row.get("recency_months"),
                    lead_score=float(row.get("lead_score") or 0.0),
                    source=(row.get("source") or "")[:16],
                    source_url=(row.get("source_url") or "")[:1000],
                    source_title=(row.get("source_title") or "")[:500],
                    evidence=(row.get("evidence") or ""),
                    data={k: v for k, v in row.items() if k not in {
                        "name", "role", "company", "emails", "email_candidates",
                        "phones", "linkedin", "fund_size", "fund_close_step",
                        "recency_months", "lead_score", "source", "source_url",
                        "source_title", "evidence",
                    }},
                ))
            Lead.objects.bulk_create(bulk, batch_size=200)
            return run

        try:
            await sync_to_async(_save, thread_sensitive=True)()
        except Exception as e:
            log.exception("persist failed: %s", e)


def _as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        return [x for x in v.split(";") if x]
    return [v]


# Singleton global
manager = JobManager()
