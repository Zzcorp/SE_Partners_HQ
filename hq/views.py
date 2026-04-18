"""Vues HTTP (login + dashboard + API start/stop/status)."""
from __future__ import annotations
import json

from asgiref.sync import async_to_sync
from django.conf import settings
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

from hq.job_manager import manager
from hq.models import ScrapeRun
from queries import list_categories, PLATFORM_GROUP_NAMES


# -----------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------
@csrf_protect
def login_view(request):
    error = None
    if request.method == "POST":
        pin = (request.POST.get("pin") or "").strip()
        if pin and pin == settings.HQ_PIN:
            request.session["hq_auth"] = True
            request.session.set_expiry(60 * 60 * 12)  # 12h
            nxt = request.GET.get("next") or reverse("dashboard")
            return HttpResponseRedirect(nxt)
        error = "PIN invalide."
    return render(request, "hq/login.html", {"error": error})


def logout_view(request):
    request.session.flush()
    return HttpResponseRedirect(reverse("login"))


# -----------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------
def healthz(request):
    return HttpResponse("ok", content_type="text/plain")


# -----------------------------------------------------------------------
# Dashboard
# -----------------------------------------------------------------------
@require_GET
def dashboard(request):
    recent = ScrapeRun.objects.all()[:10]
    return render(request, "hq/dashboard.html", {
        "categories": list_categories(),
        "platform_groups": sorted(PLATFORM_GROUP_NAMES),
        "recent_runs": recent,
        "llm_enabled": _llm_enabled(),
    })


def _llm_enabled() -> bool:
    try:
        import config
        return bool(config.LLM_ENABLED)
    except Exception:
        return False


# -----------------------------------------------------------------------
# API
# -----------------------------------------------------------------------
@require_POST
def api_start(request):
    try:
        payload = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid json"}, status=400)

    categories = payload.get("categories") or None
    params = dict(
        categories=categories,
        min_priority=int(payload.get("min_priority", 5)),
        max_results_per_query=int(payload.get("max_results_per_query", 10)),
        use_llm=bool(payload.get("use_llm", True)),
        use_team_crawl=bool(payload.get("use_team_crawl", True)),
        use_email_enrich=bool(payload.get("use_email_enrich", True)),
        exclude_platforms=bool(payload.get("exclude_platforms", False)),
        platforms_only=bool(payload.get("platforms_only", False)),
        extra_geo=payload.get("geo") or None,
        pdf_only=bool(payload.get("pdf_only", False)),
        recency_months_max=int(payload.get("recency_months", 12)),
        recency_required=bool(payload.get("recency_required", False)),
        fetch_workers=int(payload.get("fetch_workers", 3)),
        extract_workers=int(payload.get("extract_workers", 2)),
        enrich_workers=int(payload.get("enrich_workers", 2)),
    )

    runner = async_to_sync(manager.start)(**params)
    return JsonResponse({"run_id": runner.id, "status": "running"})


@require_POST
def api_stop(request):
    async_to_sync(manager.stop)()
    return JsonResponse({"status": "stopped"})


@require_GET
def api_status(request):
    snap = manager.snapshot()
    snap["leads"] = manager.top_leads(limit=50)
    return JsonResponse(snap)


@require_GET
def api_runs(request):
    qs = ScrapeRun.objects.all()[:30]
    data = [{
        "run_id": r.run_id,
        "started_at": r.started_at.isoformat(),
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "queries_total": r.queries_total,
        "people_unique": r.people_unique,
        "leads_final": r.leads_final,
        "status": r.status,
    } for r in qs]
    return JsonResponse({"runs": data})
