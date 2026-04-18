"""Persistance : historique des runs + leads capturés."""
from django.db import models


class ScrapeRun(models.Model):
    run_id = models.CharField(max_length=32, unique=True, db_index=True)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    categories = models.JSONField(default=list, blank=True)
    params = models.JSONField(default=dict, blank=True)
    queries_total = models.IntegerField(default=0)
    queries_done = models.IntegerField(default=0)
    pages_fetched = models.IntegerField(default=0)
    people_unique = models.IntegerField(default=0)
    leads_final = models.IntegerField(default=0)
    status = models.CharField(max_length=16, default="running")  # running/done/stopped

    class Meta:
        ordering = ["-started_at"]


class Lead(models.Model):
    run = models.ForeignKey(
        ScrapeRun, related_name="leads", on_delete=models.CASCADE,
    )
    name = models.CharField(max_length=200)
    role = models.CharField(max_length=80)
    company = models.CharField(max_length=200, blank=True, default="")
    emails = models.JSONField(default=list, blank=True)
    email_candidates = models.JSONField(default=list, blank=True)
    phones = models.JSONField(default=list, blank=True)
    linkedin = models.URLField(blank=True, default="", max_length=500)
    fund_size = models.CharField(max_length=80, blank=True, default="")
    fund_close_step = models.CharField(max_length=40, blank=True, default="")
    recency_months = models.FloatField(null=True, blank=True)
    lead_score = models.FloatField(default=0.0, db_index=True)
    source = models.CharField(max_length=16, default="")
    source_url = models.URLField(blank=True, default="", max_length=1000)
    source_title = models.CharField(max_length=500, blank=True, default="")
    evidence = models.TextField(blank=True, default="")
    data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-lead_score"]
