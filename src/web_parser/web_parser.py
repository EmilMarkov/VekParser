import logging
import yaml
import json
from urllib.parse import urljoin
from selectorlib import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Lock
from typing import Optional, Dict, List
from http_client import HttpClient

class WebParser:
    """Парсит данные с веб-страниц используя конфигурацию YAML."""

    def __init__(self, config_path: str, base_url: Optional[str] = None,
                 headers: Optional[Dict] = None, request_interval: float = 0.5,
                 max_workers: int = 20, retries: int = 5, render_js: bool = False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)

        with open(config_path, encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.base_url = base_url.rstrip('/') if base_url else None
        self.max_workers = max_workers
        self.extractor_cache = {}
        self.http_client = HttpClient(
            headers=headers,
            retries=retries,
            request_interval=request_interval,
            render_js=render_js
        )
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.collected_data = []
        self.data_lock = Lock()
        self.shutdown_event = threading.Event()
        self._validate_config()

    def run(self, initial_context: Optional[Dict] = None):
        """Запускает процесс парсинга."""
        try:
            context = initial_context or {}
            self._execute_step(self.config['steps'][0], context)
        finally:
            self.wait_completion()

    def save_data(self, filename: str):
        """Сохраняет данные в JSON."""
        with self.data_lock:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.collected_data, f, ensure_ascii=False, indent=4)

    def wait_completion(self):
        """Ожидает завершения задач."""
        self.executor.shutdown(wait=True)
        self.shutdown_event.set()

    def close(self):
        """Закрывает ресурсы."""
        self.wait_completion()
        self.http_client.close()

    def _validate_config(self):
        """Проверяет конфигурацию."""
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
        """Выполняет шаг парсинга."""
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
        """Обрабатывает статический шаг."""
        return step_config.get('values', {}).copy()

    def _process_extract(self, step_config: Dict, context: Dict) -> Dict:
        """Извлекает данные со страницы."""
        url = self._resolve_url(step_config.get('url', ''), context)
        html = self.http_client.fetch(url)
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
        """Обрабатывает список элементов."""
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
        """Обрабатывает элемент списка."""
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
        """Генерирует полный URL."""
        url = template.format(**context)
        if not url.startswith(('http://', 'https://')) and self.base_url:
            return urljoin(self.base_url, url)
        return url

    def _handle_next_steps(self, step_config: Dict, context: Dict, result: Dict):
        """Обрабатывает следующие шаги."""
        combined_context = {**context, **result}
        for next_step in step_config.get('next_steps', []):
            if self.shutdown_event.is_set():
                return
            step = self._get_step_by_name(next_step['step'])
            mapped_context = {k: combined_context.get(v) for k, v in next_step.get('context_map', {}).items()}
            self._execute_step(step, mapped_context)

    def _get_step_by_name(self, name: str) -> Dict:
        """Находит шаг по имени."""
        for step in self.config['steps']:
            if step['name'] == name:
                return step
        raise ValueError(f"Step {name} not found")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()