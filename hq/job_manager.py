"""Pont entre Django et le JobRunner pipeliné.

Un seul run actif à la fois (cohérent avec un webservice à une instance).
Le JobRunner utilise un `Broadcaster` in-memory ; chaque WebSocket consumer
s'y abonne via `subscribe()`.

Persistance :
- Un `ScrapeRun` est créé en base dès le démarrage (status="running") afin
  qu'il apparaisse immédiatement dans l'historique, même si le process meurt
  en cours de route.
- Les leads sont bulk-insérés par vagues toutes les `CHECKPOINT_EVERY_SECONDS`
  pendant le run (dédup en mémoire sur une clé stable pour éviter les doublons
  en base).
- À la fin du run, on fait un dernier checkpoint + on met à jour le statut
  (done / stopped / error) + `finished_at` + compteurs.
"""
from __future__ import annotations
import asyncio
import hashlib
import logging
from typing import Dict, Optional, Set

from django.utils import timezone

from job import Broadcaster, JobRunner

log = logging.getLogger(__name__)

# Cadence du checkpoint pendant un run. Assez rapide pour ne rien perdre si le
# process est tué, assez lent pour ne pas saturer la DB sous charge.
CHECKPOINT_EVERY_SECONDS = 8.0


def _lead_key(row: Dict) -> str:
    """Clé stable pour dédup en base : (source_url, name, company, role)."""
    parts = "|".join([
        (row.get("source_url") or "").strip().lower(),
        (row.get("name") or "").strip().lower(),
        (row.get("company") or "").strip().lower(),
        (row.get("role") or "").strip().lower(),
    ])
    return hashlib.sha1(parts.encode("utf-8")).hexdigest()[:32]


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
            # Pipeline de persistance (fire-and-forget) : création + checkpoints + finalize.
            self._persist_task = asyncio.create_task(self._lifecycle(runner, params))
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

    # ------------------------------------------------------------------
    # Lifecycle : create → checkpoint loop → finalize
    # ------------------------------------------------------------------
    async def _lifecycle(self, runner: JobRunner, params: Dict) -> None:
        from asgiref.sync import sync_to_async

        try:
            run = await sync_to_async(self._create_run, thread_sensitive=True)(runner, params)
        except Exception as e:
            log.exception("create_run failed: %s", e)
            return

        persisted: Set[str] = set()

        try:
            # Boucle de checkpoints pendant que le runner tourne
            while runner.running:
                await asyncio.sleep(CHECKPOINT_EVERY_SECONDS)
                try:
                    await sync_to_async(self._checkpoint, thread_sensitive=True)(run, runner, persisted)
                except Exception as e:
                    log.warning("checkpoint failed: %s", e)

            # Dernier checkpoint (leads ajoutés entre la dernière itération et la sortie)
            try:
                await sync_to_async(self._checkpoint, thread_sensitive=True)(run, runner, persisted)
            except Exception as e:
                log.warning("final checkpoint failed: %s", e)

            # Si le runner a été stoppé proprement via stop(), status = stopped
            status = "stopped" if runner._stop.is_set() else "done"
            await sync_to_async(self._finalize_status, thread_sensitive=True)(run, runner, status)
        except asyncio.CancelledError:
            try:
                await sync_to_async(self._finalize_status, thread_sensitive=True)(run, runner, "stopped")
            except Exception:
                pass
            raise
        except Exception as e:
            log.exception("lifecycle failed: %s", e)
            try:
                await sync_to_async(self._finalize_status, thread_sensitive=True)(run, runner, "error")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # DB helpers (sync; called via sync_to_async)
    # ------------------------------------------------------------------
    @staticmethod
    def _create_run(runner: JobRunner, params: Dict):
        from hq.models import ScrapeRun
        return ScrapeRun.objects.create(
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
            status="running",
        )

    @staticmethod
    def _checkpoint(run, runner: JobRunner, persisted: Set[str]) -> int:
        """Insère les leads apparus depuis le dernier checkpoint. Retourne le count."""
        from hq.models import Lead
        from hq.geo import resolve_centroid, country_from_url

        promoted = {
            "name", "role", "company", "emails", "email_candidates",
            "phones", "linkedin", "fund_size", "fund_close_step",
            "recency_months", "lead_score", "source", "source_url",
            "source_title", "evidence",
            "country", "city", "lat", "lng",
            "company_description", "llm_score", "llm_score_reasoning", "seniority",
        }

        to_create = []
        # Snapshot : copie rapide des rows actuels (résultats du runner en RAM)
        snapshot = list(runner.top_leads(limit=5000))
        for row in snapshot:
            key = _lead_key(row)
            if key in persisted:
                continue
            persisted.add(key)

            country = (row.get("country") or "").upper()[:2]
            city = (row.get("city") or "")[:120]
            lat = row.get("lat")
            lng = row.get("lng")
            if not country:
                country = country_from_url(row.get("source_url") or "")
            if (lat is None or lng is None) and country:
                centroid = resolve_centroid(country)
                if centroid:
                    lat = lat if lat is not None else centroid[0]
                    lng = lng if lng is not None else centroid[1]
            llm_score = row.get("llm_score")
            try:
                llm_score = float(llm_score) if llm_score is not None else None
            except (TypeError, ValueError):
                llm_score = None

            data = {k: v for k, v in row.items() if k not in promoted}
            data["_lead_key"] = key  # recorded in data for future dedup/debug

            to_create.append(Lead(
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
                country=country,
                city=city,
                lat=lat,
                lng=lng,
                company_description=(row.get("company_description") or "")[:1200],
                llm_score=llm_score,
                llm_score_reasoning=(row.get("llm_score_reasoning") or "")[:800],
                seniority=(row.get("seniority") or "")[:16],
                data=data,
            ))

        if to_create:
            Lead.objects.bulk_create(to_create, batch_size=200)

        # Met à jour les compteurs + garde le run en "running"
        m = runner.metrics
        run.queries_total = m.queries_total
        run.queries_done = m.queries_done
        run.pages_fetched = m.pages_fetched
        run.people_unique = m.people_unique
        run.leads_final = m.leads_final
        run.save(update_fields=[
            "queries_total", "queries_done", "pages_fetched",
            "people_unique", "leads_final",
        ])
        return len(to_create)

    @staticmethod
    def _finalize_status(run, runner: JobRunner, status: str) -> None:
        from hq.models import Lead
        m = runner.metrics
        run.status = status
        run.queries_total = m.queries_total
        run.queries_done = m.queries_done
        run.pages_fetched = m.pages_fetched
        run.people_unique = m.people_unique
        # Recompute leads_final from DB (authoritative post-checkpoints)
        run.leads_final = Lead.objects.filter(run=run).count() or m.leads_final
        run.finished_at = timezone.now()
        run.save(update_fields=[
            "status", "queries_total", "queries_done", "pages_fetched",
            "people_unique", "leads_final", "finished_at",
        ])


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
