"""Auth : username + password + PIN (Django `auth` + PIN hashé sur le profil)."""
from django.conf import settings
from django.http import HttpResponseRedirect
from django.urls import resolve, reverse


PUBLIC_URL_NAMES = {"login", "logout", "healthz"}


class LoginRequiredMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        path = request.path_info

        if path.startswith(settings.STATIC_URL) or path == "/healthz":
            return self.get_response(request)

        try:
            match = resolve(path)
            if match.url_name in PUBLIC_URL_NAMES:
                return self.get_response(request)
        except Exception:
            pass

        if request.user.is_authenticated:
            return self.get_response(request)

        login_url = reverse("login")
        return HttpResponseRedirect(f"{login_url}?next={path}")
