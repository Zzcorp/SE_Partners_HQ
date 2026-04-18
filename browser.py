"""Gestion paresseuse d'un navigateur Selenium (undetected-chromedriver).

- Démarrage à la demande
- Restart périodique pour éviter les fuites mémoire
- Fallback sur selenium standard si undetected-chromedriver échoue
- Détection page challenge / captcha
"""
from __future__ import annotations
import logging
import random
import time
from typing import Optional

from config import (
    SELENIUM_HEADLESS, SELENIUM_PAGE_TIMEOUT, SELENIUM_WAIT_AFTER_LOAD,
    SELENIUM_MAX_PAGES_PER_SESSION, USER_AGENTS, JS_TRIGGER_PATTERNS,
)

log = logging.getLogger(__name__)


class BrowserSession:
    def __init__(self, headless: bool = SELENIUM_HEADLESS):
        self.headless = headless
        self._driver = None
        self._pages_served = 0

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def _new_driver(self):
        ua = random.choice(USER_AGENTS)
        try:
            import undetected_chromedriver as uc
            opts = uc.ChromeOptions()
            if self.headless:
                opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--window-size=1400,900")
            opts.add_argument(f"--user-agent={ua}")
            opts.add_argument("--lang=en-US,en;q=0.9,fr;q=0.8")
            driver = uc.Chrome(options=opts, use_subprocess=True)
            driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT)
            return driver
        except Exception as e:
            log.warning("undetected_chromedriver failed (%s); fallback Selenium standard.", e)

        # Fallback : selenium classique + webdriver-manager
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except Exception:
            service = Service()
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1400,900")
        opts.add_argument(f"--user-agent={ua}")
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT)
        return driver

    def _ensure_driver(self):
        if self._driver is None or self._pages_served >= SELENIUM_MAX_PAGES_PER_SESSION:
            self.close()
            self._driver = self._new_driver()
            self._pages_served = 0
        return self._driver

    def close(self):
        if self._driver is not None:
            try:
                self._driver.quit()
            except Exception:
                pass
            self._driver = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------
    def fetch(self, url: str) -> Optional[str]:
        """Charge la page et retourne le HTML rendu (ou None si échec)."""
        try:
            driver = self._ensure_driver()
            driver.get(url)
            time.sleep(SELENIUM_WAIT_AFTER_LOAD + random.uniform(0, 1.5))
            # petit scroll pour déclencher le lazy-loading
            try:
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight/2);"
                )
                time.sleep(0.5)
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);"
                )
                time.sleep(0.5)
            except Exception:
                pass
            html = driver.page_source or ""
            self._pages_served += 1
            if _looks_like_challenge(html):
                log.info("Challenge/captcha détecté sur %s", url)
                return None
            return html
        except Exception as e:
            log.warning("Selenium fetch failed %s: %s", url, e)
            # détruit le driver potentiellement coincé
            self.close()
            return None


def _looks_like_challenge(html: str) -> bool:
    low = html.lower()[:8000]
    return any(p in low for p in JS_TRIGGER_PATTERNS)
