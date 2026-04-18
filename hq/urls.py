from django.urls import path

from hq import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("login/", views.login_view, name="login"),
    path("logout/", views.logout_view, name="logout"),
    path("healthz", views.healthz, name="healthz"),
    path("api/start", views.api_start, name="api_start"),
    path("api/stop", views.api_stop, name="api_stop"),
    path("api/status", views.api_status, name="api_status"),
    path("api/runs", views.api_runs, name="api_runs"),
]
