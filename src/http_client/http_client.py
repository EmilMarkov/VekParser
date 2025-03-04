import requests
import logging
import time
import threading
from typing import Optional, Dict
from playwright.sync_api import sync_playwright


class HttpClient:
    """Обрабатывает HTTP-запросы с поддержкой JS-рендеринга и многопоточности."""

    def __init__(self, headers: Optional[Dict] = None, retries: int = 5,
                 request_interval: float = 0.5, render_js: bool = False,
                 disable_logging: bool = False):
        self.logger = logging.getLogger(self.__class__.__name__)
        if not disable_logging:
            self.logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)

        self.session = requests.Session()
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        self.session.headers.update(headers or default_headers)
        self.retries = retries
        self.request_interval = request_interval
        self.render_js = render_js
        self.thread_local = threading.local()

    def fetch(self, url: str) -> Optional[str]:
        """Получает содержимое страницы."""
        time.sleep(self.request_interval)
        if self.render_js:
            return self._render_js(url)
        return self._fetch_static(url)

    def _fetch_static(self, url: str) -> Optional[str]:
        """Обрабатывает статические запросы."""
        for attempt in range(self.retries):
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response.text
            except requests.RequestException:
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
        return None

    def _render_js(self, url: str) -> Optional[str]:
        """Рендерит JavaScript-контент."""
        try:
            browser = self._get_browser()
            context = browser.new_context()
            page = context.new_page()
            page.goto(url)
            content = page.content()
            context.close()
            return content
        except Exception as e:
            self.logger.error(f"Render error: {str(e)}")
            return None

    def _get_browser(self):
        """Управляет экземплярами браузера."""
        if not hasattr(self.thread_local, "playwright"):
            self.thread_local.playwright = sync_playwright().start()
        if not hasattr(self.thread_local, "browser"):
            self.thread_local.browser = self.thread_local.playwright.chromium.launch(
                headless=True,
                channel="chrome",
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
        return self.thread_local.browser

    def close(self):
        """Освобождает ресурсы."""
        self.session.close()
        if hasattr(self.thread_local, "browser"):
            self.thread_local.browser.close()
        if hasattr(self.thread_local, "playwright"):
            self.thread_local.playwright.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
