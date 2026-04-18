"""WebSocket consumer — forwarde les events du Broadcaster vers le navigateur."""
from __future__ import annotations
import asyncio
import json
import logging

from channels.generic.websocket import AsyncWebsocketConsumer

from hq.job_manager import manager

log = logging.getLogger(__name__)


class EventsConsumer(AsyncWebsocketConsumer):
    """Subscribe au Broadcaster in-memory, relaie chaque event au client."""

    async def connect(self) -> None:
        # Auth via session Django : le scope contient la session
        session = self.scope.get("session")
        if not session or not session.get("hq_auth"):
            await self.close(code=4401)
            return
        await self.accept()
        self._sub_queue = await manager.broadcaster.subscribe()
        self._pump_task = asyncio.create_task(self._pump())
        # Envoie un snapshot immédiat pour remplir l'UI (aplati)
        snap = manager.snapshot()
        current = snap.get("current") or {}
        leads = manager.top_leads(limit=50)
        flat = {
            "running": snap.get("running", False),
            "metrics": current.get("metrics") or {},
            "workers": current.get("workers") or {},
            "leads": leads,
            "run_id": current.get("id"),
        }
        await self.send(text_data=json.dumps({
            "type": "snapshot", "snapshot": flat,
        }, default=str))

    async def disconnect(self, code: int) -> None:
        try:
            if getattr(self, "_pump_task", None):
                self._pump_task.cancel()
        except Exception:
            pass
        try:
            if getattr(self, "_sub_queue", None):
                await manager.broadcaster.unsubscribe(self._sub_queue)
        except Exception:
            pass

    async def _pump(self) -> None:
        try:
            while True:
                evt = await self._sub_queue.get()
                await self.send(text_data=json.dumps(evt, default=str))
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.debug("pump error: %s", e)

    async def receive(self, text_data: str | None = None, bytes_data=None) -> None:
        # Le client peut envoyer des pings ; on ignore pour l'instant
        return
