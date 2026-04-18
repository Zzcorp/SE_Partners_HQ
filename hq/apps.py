from django.apps import AppConfig


class HqConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "hq"
    verbose_name = "S&E Partners HQ"

    def ready(self) -> None:
        from hq import signals  # noqa: F401
