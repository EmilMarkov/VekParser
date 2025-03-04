import random
import requests
import logging
import yaml
import time
import json
from urllib.parse import urljoin
from selectorlib import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Lock
from typing import Optional, Dict, List
from playwright.sync_api import sync_playwright


class VekParser:
    """
    Класс для парсинга веб-сайтов с поддержкой многопоточности и JavaScript-рендеринга.
    
    Позволяет извлекать данные с веб-страниц на основе конфигурации в YAML формате.
    Поддерживает параллельную обработку, управление сессиями и обработку JavaScript.
    """

    def __init__(self, config_path: str,
                 base_url: Optional[str] = None,
                 headers: Optional[Dict] = None,
                 request_interval: float = 0.5,
                 max_workers: int = 20,
                 retries: int = 5,
                 render_js: bool = False):
        """
        Инициализация парсера.

        Args:
            config_path: Путь к файлу конфигурации YAML
            base_url: Базовый URL для относительных ссылок
            headers: Пользовательские заголовки HTTP
            request_interval: Интервал между запросами
            max_workers: Максимальное количество потоков
            retries: Количество попыток повторного запроса
            render_js: Флаг для включения JavaScript-рендеринга
        """

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

        with open(config_path, encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.base_url = base_url.rstrip('/') if base_url else None
        self.request_interval = request_interval
        self.retries = retries
        self.max_workers = max_workers
        self.extractor_cache = {}
        self.render_js = render_js
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        self.session = requests.Session()
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        self.session.headers.update(headers or default_headers)

        self.collected_data = []
        self.data_lock = Lock()
        self.shutdown_event = threading.Event()
        self.thread_local = threading.local()
        self._validate_config()

    def run(self, initial_context: Optional[Dict] = None):
        """
        Запускает процесс парсинга.

        Args:
            initial_context: Начальный контекст для парсинга
        """
        try:
            context = initial_context or {}
            self._execute_step(self.config['steps'][0], context)
        finally:
            self.wait_completion()

    def save_data(self, filename: str):
        """
        Сохраняет собранные данные в JSON файл.

        Args:
            filename: Имя файла для сохранения
        """
        with self.data_lock:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.collected_data, f, ensure_ascii=False, indent=4)

    def wait_completion(self):
        """Ожидает завершения всех запущенных задач."""
        self.executor.shutdown(wait=True)
        self.shutdown_event.set()

    def close(self):
        """Закрывает все ресурсы парсера."""
        self.wait_completion()
        self._close_browsers()
        self.session.close()

    def _get_browser(self):
        """
        Получает экземпляр браузера для текущего потока.
        
        Returns:
            Экземпляр браузера Playwright
        """
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

    def _close_browsers(self):
        """Закрывает все экземпляры браузеров."""
        if hasattr(self.thread_local, "browser"):
            self.thread_local.browser.close()
        if hasattr(self.thread_local, "playwright"):
            self.thread_local.playwright.stop()

    def _validate_config(self):
        """Проверяет корректность конфигурации."""
        required_fields = {
            'extract': ['url'],
            'list': ['source', 'steps']
        }
        for step in self.config.get('steps', []):
            step_type = step.get('type')
            if not step_type:
                raise ValueError("Missing step type")
            for field in required_fields.get(step_type, []):
                if field not in step:
                    raise ValueError(f"Missing {field} in {step.get('name')}")

    def _execute_step(self, step_config: Dict, context: Dict) -> Optional[Dict]:
        """
        Выполняет шаг парсинга.

        Args:
            step_config: Конфигурация шага
            context: Контекст выполнения

        Returns:
            Результат выполнения шага или None в случае ошибки
        """
        if self.shutdown_event.is_set():
            return None

        processor = getattr(self, f'_process_{step_config["type"]}', None)
        if not processor:
            raise ValueError(f"Unsupported step type: {step_config['type']}")

        try:
            result = processor(step_config, context)
            self._handle_next_steps(step_config, context, result)
            return result
        except Exception as e:
            self.logger.error(f"Step error: {str(e)}")
            return None

    def _process_static(self, step_config: Dict, context: Dict) -> Dict:
        """
        Обрабатывает статический шаг.

        Args:
            step_config: Конфигурация шага
            context: Контекст выполнения

        Returns:
            Статические значения из конфигурации
        """
        return step_config.get('values', {}).copy()

    def _process_extract(self, step_config: Dict, context: Dict) -> Dict:
        """
        Извлекает данные со страницы.

        Args:
            step_config: Конфигурация шага
            context: Контекст выполнения

        Returns:
            Извлеченные данные
        """
        time.sleep(self.request_interval)
        url = self._resolve_url(step_config.get('url', ''), context)
        html = self._fetch_url(url)
        if not html:
            return {}

        result = {}
        if 'data' in step_config:
            config_yaml = yaml.dump(step_config['data'])
            if config_yaml not in self.extractor_cache:
                self.extractor_cache[config_yaml] = Extractor.from_yaml_string(config_yaml)
            result.update(self.extractor_cache[config_yaml].extract(html))
        return result

    def _process_list(self, step_config: Dict, context: Dict) -> Dict:
        """
        Обрабатывает список элементов.

        Args:
            step_config: Конфигурация шага
            context: Контекст выполнения

        Returns:
            Результаты обработки списка
        """
        items = context.get(step_config.get('source'), [])
        futures = [self.executor.submit(self._process_item, step_config, item) for item in items]
        results = []
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                self.logger.error(f"Item error: {str(e)}")
        return {step_config['output']: results}

    def _process_item(self, step_config: Dict, item: Dict) -> List[Dict]:
        """
        Обрабатывает отдельный элемент списка.

        Args:
            step_config: Конфигурация шага
            item: Элемент для обработки

        Returns:
            Список результатов обработки элемента
        """
        if self.shutdown_event.is_set():
            return []
        if isinstance(item, str):
            item = {'url': item}
        resolved_url = self._resolve_url(item.get('url', ''), {})
        context = {**item, 'url': resolved_url}
        results = []
        for nested_step in step_config['steps']:
            result = self._execute_step(nested_step, context)
            if result:
                results.append(result)
                with self.data_lock:
                    self.collected_data.append(result)
        return results

    def _resolve_url(self, template: str, context: Dict) -> str:
        """
        Разрешает URL с учетом контекста и базового URL.

        Args:
            template: Шаблон URL
            context: Контекст для подстановки

        Returns:
            Полный URL
        """
        url = template.format(**context)
        if not url.startswith(('http://', 'https://')) and self.base_url:
            return urljoin(self.base_url, url)
        return url

    def _fetch_url(self, url: str) -> Optional[str]:
        """
        Получает содержимое страницы.

        Args:
            url: URL страницы

        Returns:
            HTML содержимое или None в случае ошибки
        """
        if self.render_js:
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
        else:
            for attempt in range(self.retries):
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    return response.text
                except requests.RequestException:
                    if attempt < self.retries - 1:
                        time.sleep(0.5 * (attempt + 1))
            return None

    def _handle_next_steps(self, step_config: Dict, context: Dict, result: Dict):
        """
        Обрабатывает следующие шаги.

        Args:
            step_config: Конфигурация текущего шага
            context: Текущий контекст
            result: Результат текущего шага
        """
        combined_context = {**context, **result}
        for next_step in step_config.get('next_steps', []):
            if self.shutdown_event.is_set():
                return
            step = self._get_step_by_name(next_step['step'])
            mapped_context = {k: combined_context.get(v) for k, v in next_step.get('context_map', {}).items()}
            self._execute_step(step, mapped_context)

    def _get_step_by_name(self, name: str) -> Dict:
        """
        Находит шаг по имени.

        Args:
            name: Имя шага

        Returns:
            Конфигурация шага

        Raises:
            ValueError: Если шаг не найден
        """
        for step in self.config['steps']:
            if step['name'] == name:
                return step
        raise ValueError(f"Step {name} not found")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()