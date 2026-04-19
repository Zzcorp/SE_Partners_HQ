from django.urls import path

from hq import views

urlpatterns = [
    path("", views.landing_view, name="landing"),
    path("contact/", views.contact_submit, name="contact_submit"),
    path("dashboard/", views.dashboard, name="dashboard"),
    path("console/", views.console_view, name="console"),
    path("database/", views.database_view, name="database"),
    path("login/", views.login_view, name="login"),
    path("logout/", views.logout_view, name="logout"),
    path("healthz", views.healthz, name="healthz"),

    # Profile
    path("profile/", views.profile_view, name="profile"),

    # Board
    path("board/", views.board_view, name="board"),
    path("board/new", views.task_create, name="task_create"),
    path("board/t/<int:task_id>/", views.task_detail, name="task_detail"),
    path("board/t/<int:task_id>/like", views.task_like, name="task_like"),
    path("board/t/<int:task_id>/vote", views.task_vote, name="task_vote"),

    # Calendar
    path("calendar/", views.calendar_view, name="calendar"),
    path("calendar/new", views.calendar_create, name="calendar_create"),

    # API (scraper)
    path("api/start", views.api_start, name="api_start"),
    path("api/stop", views.api_stop, name="api_stop"),
    path("api/status", views.api_status, name="api_status"),
    path("api/runs", views.api_runs, name="api_runs"),
    path("api/runs/<str:run_id>/export.csv", views.export_csv, name="export_csv"),
    path("api/runs/<str:run_id>/export.xlsx", views.export_xlsx, name="export_xlsx"),
    path("api/leads/geo", views.api_leads_geo, name="api_leads_geo"),
    path("api/runs/<str:run_id>/leads", views.api_run_leads, name="api_run_leads"),
    path("api/leads/<int:lead_id>/", views.api_lead_detail, name="api_lead_detail"),
]
