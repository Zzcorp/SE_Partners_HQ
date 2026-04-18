"""Vues HTTP (login + dashboard + API start/stop/status + exports)."""
from __future__ import annotations
import csv
import io
import json

from asgiref.sync import async_to_sync
from django.conf import settings
from django.http import (
    Http404, HttpResponse, HttpResponseRedirect, JsonResponse, StreamingHttpResponse,
)
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

from hq.job_manager import manager
from hq.models import Lead, ScrapeRun
from queries import list_categories, PLATFORM_GROUP_NAMES


LEAD_FIELDS = [
    "lead_score", "name", "role", "company",
    "emails", "phones", "email_candidates", "linkedin",
    "fund_size", "fund_close_step", "recency_months",
    "source", "source_url", "source_title", "evidence",
]


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


# -----------------------------------------------------------------------
# Exports : CSV + XLSX par run
# -----------------------------------------------------------------------
def _leads_queryset(run_id: str):
    run = get_object_or_404(ScrapeRun, run_id=run_id)
    qs = Lead.objects.filter(run=run).order_by("-lead_score")
    return run, qs


def _flatten(row: Lead) -> list:
    def j(v):
        if v is None:
            return ""
        if isinstance(v, list):
            return "; ".join(str(x) for x in v if x)
        return str(v)
    return [
        round(row.lead_score or 0, 3),
        row.name or "",
        row.role or "",
        row.company or "",
        j(row.emails),
        j(row.phones),
        j(row.email_candidates),
        row.linkedin or "",
        row.fund_size or "",
        row.fund_close_step or "",
        row.recency_months if row.recency_months is not None else "",
        row.source or "",
        row.source_url or "",
        row.source_title or "",
        (row.evidence or "").replace("\n", " ").replace("\r", " ")[:2000],
    ]


@require_GET
def export_csv(request, run_id: str):
    run, qs = _leads_queryset(run_id)

    def stream():
        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(LEAD_FIELDS)
        yield buf.getvalue(); buf.seek(0); buf.truncate()
        for lead in qs.iterator(chunk_size=500):
            writer.writerow(_flatten(lead))
            yield buf.getvalue(); buf.seek(0); buf.truncate()

    resp = StreamingHttpResponse(stream(), content_type="text/csv; charset=utf-8")
    fname = f"se-partners-hq_{run.run_id[:8]}_{run.started_at:%Y%m%d-%H%M}.csv"
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp


@require_GET
def export_xlsx(request, run_id: str):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    run, qs = _leads_queryset(run_id)

    wb = Workbook()
    ws = wb.active
    ws.title = f"Leads {run.run_id[:8]}"

    header_fill = PatternFill("solid", fgColor="1A1407")
    header_font = Font(bold=True, color="D4AF6C", name="Calibri", size=11)
    ws.append(LEAD_FIELDS)
    for col_idx in range(1, len(LEAD_FIELDS) + 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="left", vertical="center")

    for lead in qs.iterator(chunk_size=500):
        ws.append(_flatten(lead))

    widths = [8, 24, 28, 28, 34, 22, 34, 40, 18, 18, 12, 10, 46, 46, 60]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    # Meta sheet
    meta = wb.create_sheet("Run")
    meta.append(["field", "value"])
    meta.append(["run_id", run.run_id])
    meta.append(["started_at", run.started_at.strftime("%Y-%m-%d %H:%M:%S UTC")])
    meta.append(["finished_at", run.finished_at.strftime("%Y-%m-%d %H:%M:%S UTC") if run.finished_at else ""])
    meta.append(["status", run.status])
    meta.append(["queries_total", run.queries_total])
    meta.append(["people_unique", run.people_unique])
    meta.append(["leads_final", run.leads_final])
    meta.append(["categories", ", ".join(run.categories or [])])
    meta.column_dimensions["A"].width = 18
    meta.column_dimensions["B"].width = 60

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    resp = HttpResponse(
        buf.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    fname = f"se-partners-hq_{run.run_id[:8]}_{run.started_at:%Y%m%d-%H%M}.xlsx"
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp
