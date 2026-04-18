"""Seed les utilisateurs initiaux (Emilien + Reddreams) — idempotent.

Usage (sur Render Shell ou en local) :
    python manage.py seed_users
"""
from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from hq.models import UserProfile


SEED = [
    {
        "username": "Emilien",
        "password": "Elcabron2026!",
        "pin": "1995",
        "display_name": "Emilien",
        "color": "#d4af6c",
        "email": "",
    },
    {
        "username": "Reddreams",
        "password": "Elcamino2026!",
        "pin": "1995",
        "display_name": "Reddreams",
        "color": "#8eb5cf",
        "email": "",
    },
]


class Command(BaseCommand):
    help = "Crée / met à jour les utilisateurs Emilien et Reddreams."

    def add_arguments(self, parser):
        parser.add_argument(
            "--reset-passwords", action="store_true",
            help="Force la réinitialisation des mots de passe et PIN même si les comptes existent.",
        )

    def handle(self, *args, **opts):
        reset = bool(opts.get("reset_passwords"))

        for spec in SEED:
            user, created = User.objects.get_or_create(
                username=spec["username"],
                defaults={"email": spec["email"]},
            )
            # Set password on first creation, on explicit reset, or if the
            # account somehow ended up without a usable password hash.
            if created or reset or not user.has_usable_password():
                user.set_password(spec["password"])
            user.is_active = True
            user.email = spec["email"]
            user.save()

            profile, _ = UserProfile.objects.get_or_create(user=user)
            if created or reset or not profile.pin_hash:
                profile.set_pin(spec["pin"])
            profile.display_name = profile.display_name or spec["display_name"]
            profile.color = profile.color or spec["color"]
            profile.save()

            status = "created" if created else ("reset" if reset else "ok")
            self.stdout.write(self.style.SUCCESS(
                f"[{status}] {spec['username']} — password+PIN ready"
            ))

        self.stdout.write(self.style.SUCCESS("Seed complete."))
