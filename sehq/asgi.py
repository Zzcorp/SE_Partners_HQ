"""ASGI entrypoint avec Django Channels (HTTP + WebSocket)."""
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sehq.settings")

django_asgi_app = get_asgi_application()

from channels.routing import ProtocolTypeRouter, URLRouter  # noqa: E402
from channels.security.websocket import AllowedHostsOriginValidator  # noqa: E402
from channels.auth import AuthMiddlewareStack  # noqa: E402

import hq.routing  # noqa: E402


application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": AllowedHostsOriginValidator(
        AuthMiddlewareStack(URLRouter(hq.routing.websocket_urlpatterns))
    ),
})
