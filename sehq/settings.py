"""Settings Django pour S&E Partners HQ."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-insecure-key-change-me")
DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"

_hosts = os.environ.get("DJANGO_ALLOWED_HOSTS", "*")
ALLOWED_HOSTS = [h.strip() for h in _hosts.split(",") if h.strip()]

CSRF_TRUSTED_ORIGINS = [
    o.strip() for o in os.environ.get("DJANGO_CSRF_TRUSTED_ORIGINS", "").split(",")
    if o.strip()
]

INSTALLED_APPS = [
    "daphne",  # doit être avant django.contrib.staticfiles
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "channels",
    "hq",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "hq.middleware.LoginRequiredMiddleware",
]

LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/"
LOGOUT_REDIRECT_URL = "/login/"
AUTH_USER_MODEL = "auth.User"

ROOT_URLCONF = "sehq.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

ASGI_APPLICATION = "sehq.asgi.application"
WSGI_APPLICATION = "sehq.wsgi.application"

import dj_database_url

_db_url = os.environ.get("DATABASE_URL")
if _db_url:
    DATABASES = {
        "default": dj_database_url.parse(
            _db_url,
            conn_max_age=600,
            ssl_require=os.environ.get("DATABASE_SSL", "1") == "1",
        ),
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "hq.sqlite3",
        }
    }

# Single-process in-memory channel layer (pas besoin de Redis sur un instance unique).
# Si tu scales horizontalement sur Render → bascule sur channels_redis.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

LANGUAGE_CODE = "fr-fr"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATICFILES_DIRS = [BASE_DIR / "hq" / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

SESSION_COOKIE_NAME = "hq_session"
SESSION_ENGINE = "django.contrib.sessions.backends.signed_cookies"
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = os.environ.get("DJANGO_SECURE_COOKIE", "0") == "1"

# Config HQ
HQ_PIN = os.environ.get("HQ_PIN", "1234")
if HQ_PIN == "1234":
    import warnings
    warnings.warn(
        "HQ_PIN n'est pas défini — fallback '1234'. "
        "Mets HQ_PIN dans l'environnement avant la prod.",
        stacklevel=2,
    )
