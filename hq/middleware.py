"""Auth par PIN.

Le PIN est dans l'env `HQ_PIN`. À la validation, on pose un flag en session.
Les routes de login, static et health sont publiques ; tout le reste exige le flag.
"""
from django.conf import settings
from django.http import HttpResponseRedirect
from django.urls import resolve, reverse


PUBLIC_URL_NAMES = {"login", "logout", "healthz"}


class PinAuthMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Static et health → toujours passer
        path = request.path_info
        if path.startswith(settings.STATIC_URL) or path == "/healthz":
            return self.get_response(request)

        # Routes publiques (login/logout)
        try:
            match = resolve(path)
            if match.url_name in PUBLIC_URL_NAMES:
                return self.get_response(request)
        except Exception:
            pass

        # WebSocket passe par son propre auth (dans le consumer)
        if request.session.get("hq_auth"):
            return self.get_response(request)

        login_url = reverse("login")
        return HttpResponseRedirect(f"{login_url}?next={path}")
