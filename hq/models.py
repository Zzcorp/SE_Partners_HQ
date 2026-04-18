"""Persistance : users + profils, historique runs + leads, board & calendrier."""
from django.conf import settings
from django.contrib.auth.hashers import check_password, make_password
from django.db import models
from django.utils import timezone


# -----------------------------------------------------------------------
# User profile : PIN hashé + métadonnées UI
# -----------------------------------------------------------------------
class UserProfile(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="profile",
    )
    pin_hash = models.CharField(max_length=255, blank=True, default="")
    display_name = models.CharField(max_length=80, blank=True, default="")
    color = models.CharField(max_length=16, default="#d4af6c")  # pour l'agenda partagé
    bio = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    def set_pin(self, raw_pin: str) -> None:
        self.pin_hash = make_password(str(raw_pin))

    def check_pin(self, raw_pin: str) -> bool:
        if not self.pin_hash:
            return False
        return check_password(str(raw_pin), self.pin_hash)

    def __str__(self) -> str:
        return self.display_name or self.user.username


# -----------------------------------------------------------------------
# Scraper history (inchangé)
# -----------------------------------------------------------------------
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
    SENIORITY_CHOICES = [
        ("founder", "Founder"),
        ("exec", "Executive"),
        ("senior", "Senior"),
        ("mid", "Mid"),
        ("junior", "Junior"),
    ]

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

    # --- Geo --------------------------------------------------------
    country = models.CharField(max_length=2, blank=True, default="", db_index=True)  # ISO 3166-1 alpha-2
    city = models.CharField(max_length=120, blank=True, default="")
    lat = models.FloatField(null=True, blank=True)
    lng = models.FloatField(null=True, blank=True)

    # --- LLM enrichment --------------------------------------------
    company_description = models.TextField(blank=True, default="", max_length=1200)
    llm_score = models.FloatField(null=True, blank=True, db_index=True)
    llm_score_reasoning = models.TextField(blank=True, default="", max_length=800)
    seniority = models.CharField(
        max_length=16, blank=True, default="", choices=SENIORITY_CHOICES,
    )

    class Meta:
        ordering = ["-lead_score"]
        indexes = [
            models.Index(fields=["country", "-lead_score"]),
            models.Index(fields=["-created_at"]),
        ]


# -----------------------------------------------------------------------
# Board : tâches proposées par les utilisateurs + interactions
# -----------------------------------------------------------------------
class Task(models.Model):
    STATUS_CHOICES = [
        ("open", "Open"),
        ("in_progress", "In progress"),
        ("done", "Done"),
        ("archived", "Archived"),
    ]

    title = models.CharField(max_length=200)
    description = models.TextField(blank=True, default="")
    author = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="tasks_authored",
    )
    assignee = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL,
        null=True, blank=True, related_name="tasks_assigned",
    )
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="open")
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["start_date"]),
            models.Index(fields=["end_date"]),
            models.Index(fields=["status"]),
        ]

    def __str__(self) -> str:
        return self.title

    @property
    def score(self) -> int:
        return sum(v.value for v in self.votes.all())

    @property
    def like_count(self) -> int:
        return self.likes.count()

    @property
    def on_calendar(self) -> bool:
        return bool(self.start_date)

    def iter_days(self):
        """Yields each date the task spans (inclusive)."""
        if not self.start_date:
            return
        end = self.end_date or self.start_date
        d = self.start_date
        while d <= end:
            yield d
            d = d.fromordinal(d.toordinal() + 1)


class Comment(models.Model):
    task = models.ForeignKey(Task, on_delete=models.CASCADE, related_name="comments")
    author = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="comments",
    )
    body = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["created_at"]


class TaskLike(models.Model):
    task = models.ForeignKey(Task, on_delete=models.CASCADE, related_name="likes")
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="task_likes",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("task", "user")]


class TaskVote(models.Model):
    VALUE_CHOICES = [(1, "up"), (-1, "down")]
    task = models.ForeignKey(Task, on_delete=models.CASCADE, related_name="votes")
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="task_votes",
    )
    value = models.SmallIntegerField(choices=VALUE_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("task", "user")]


# -----------------------------------------------------------------------
# Contact (formulaire public du site vitrine)
# -----------------------------------------------------------------------
class ContactMessage(models.Model):
    INTENT_CHOICES = [
        ("info", "Information"),
        ("engagement", "Engagement"),
        ("partnership", "Partnership"),
        ("press", "Press"),
    ]
    name = models.CharField(max_length=120)
    email = models.EmailField(max_length=200)
    company = models.CharField(max_length=200, blank=True, default="")
    intent = models.CharField(max_length=20, choices=INTENT_CHOICES, default="info")
    message = models.TextField(max_length=5000)
    source_ip = models.GenericIPAddressField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    handled = models.BooleanField(default=False)

    class Meta:
        ordering = ["-created_at"]
        indexes = [models.Index(fields=["handled", "created_at"])]

    def __str__(self) -> str:
        return f"{self.name} <{self.email}> — {self.intent}"
