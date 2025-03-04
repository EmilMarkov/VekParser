import logging
import yaml
import json
from urllib.parse import urljoin
from selectorlib import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Lock
from typing import Optional, Dict, List, Callable, Any
from .http_client import HttpClient


logging.getLogger("urllib3.connectionpool").disabled = True


class WebParser:
    """Парсит данные с веб-страниц используя конфигурацию YAML."""

    def __init__(self, config_path: str, base_url: Optional[str] = None,
                 headers: Optional[Dict] = None, request_interval: float = 0.5,
                 max_workers: int = 20, retries: int = 5, render_js: bool = False):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.logger.info(f"Инициализация парсера с конфигурацией из {config_path}")
        with open(config_path, encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.base_url = base_url.rstrip('/') if base_url else None
        self.max_workers = max_workers
        self.extractor_cache = {}
        self.http_client = HttpClient(
            headers=headers,
            retries=retries,
            request_interval=request_interval,
            render_js=render_js,
            disable_logging=True
        )
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.collected_data = []
        self.data_lock = Lock()
        self.shutdown_event = threading.Event()

        self.step_handlers = {
            'static': self._process_static,
            'extract': self._process_extract,
            'list': self._process_list,
            'save': self._process_save
        }

        self.item_handlers = {
            'string': self._handle_string_item,
            'link': self._handle_link_item
        }

        self._validate_config()
        self.logger.info("Парсер успешно инициализирован")

    def register_step_handler(self, step_type: str, handler: Callable[[Dict, Dict], Dict]):
        """Регистрирует пользовательский обработчик для типа шага."""
        self.step_handlers[step_type] = handler

    def register_item_handler(self, item_type: str, handler: Callable[[Any, Dict], Dict]):
        """Регистрирует обработчик для определенного типа элементов списка"""
        self.item_handlers[item_type] = handler

    def run(self, initial_context: Optional[Dict] = None):
        """Запускает процесс парсинга."""
        self.logger.info("Запуск процесса парсинга")
        try:
            context = initial_context or {}
            self._execute_step(self.config['steps'][0], context)
        finally:
            self.wait_completion()
            self.logger.info("Процесс парсинга завершен")

    def save_data(self, filename: str):
        """Сохраняет данные в JSON."""
        self.logger.info(f"Сохранение данных в файл: {filename}")
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
                raise ValueError("Пропущен тип шага")
            for field in required_fields.get(step_type, []):
                if field not in step:
                    raise ValueError(f"Отсутствует поле {field} в шаге {step.get('name')}")

    def _execute_step(self, step_config: Dict, context: Dict) -> Optional[Dict]:
        """Выполняет шаг парсинга."""
        if self.shutdown_event.is_set():
            return None

        step_type = step_config.get('type')
        step_name = step_config.get('name', 'unnamed')
        self.logger.debug(f"Выполнение шага '{step_name}' типа '{step_type}'")

        if not step_type:
            raise ValueError("Не указан тип шага")

        handler = self.step_handlers.get(step_type)
        if not handler:
            raise ValueError(f"Неподдерживаемый тип шага: {step_type}")

        try:
            result = handler(step_config, context)
            self._handle_next_steps(step_config, context, result)
            return result
        except Exception as e:
            self.logger.error(f"Ошибка при выполнении шага '{step_type}': {str(e)}")
            return None

    def _process_static(self, step_config: Dict, context: Dict) -> Dict:
        """Обрабатывает статический шаг."""
        return step_config.get('values', {}).copy()

    def _process_extract(self, step_config: Dict, context: Dict) -> Dict:
        """Извлекает данные со страницы."""
        url = self._resolve_url(step_config.get('url', ''), context)
        self.logger.debug(f"Извлечение данных с URL: {url}")
        
        html = self.http_client.fetch(url)
        if not html:
            self.logger.warning(f"Не удалось получить HTML с {url}")
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
        source = step_config.get('source')
        items = context.get(source, [])
        self.logger.debug(f"Обработка списка из {len(items)} элементов из источника '{source}'")
        
        futures = [self.executor.submit(self._process_item, step_config, item) for item in items]
        results = []
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                self.logger.error(f"Ошибка при обработке элемента: {str(e)}")
        
        output_key = step_config['output']
        return {output_key: results}

    def _process_item(self, step_config: Dict, item: Any) -> List[Dict]:
        """Обрабатывает элемент списка."""
        if self.shutdown_event.is_set():
            return []

        handler_name = self._detect_handler_name(step_config, item)

        handler = self.item_handlers.get(handler_name)
        if not handler:
            raise ValueError(f"Не найден обработчик списка: {type(item).__name__}")

        context = handler(item, step_config)
        results = []

        for nested_step in step_config['steps']:
            result = self._execute_step(nested_step, context)
            if result:
                results.append(result)

        return results

    def _detect_handler_name(self, step_config: Dict, item: Any) -> str:
        """Определяет имя обработчика для списка"""
        if isinstance(item, str):
            return 'link' if self._is_url(item) else 'string'

        if 'handler_name' in step_config:
            return step_config.get('handler_name')

        raise ValueError(f"Неизвестное имя обработчика списка: {type(item).__name__}")

    def _is_url(self, value: str) -> bool:
        """Проверяет является ли строка URL"""
        return value.startswith(('http://', 'https://', '/'))

    def _handle_string_item(self, item: str, config: Dict) -> Dict:
        """Обработчик строковых элементов"""
        return {'text': item}

    def _handle_link_item(self, item: str, config: Dict) -> Dict:
        """Обработчик URL-ссылок"""
        base = config.get('base_url') or self.base_url
        full_url = urljoin(base, item)
        return {'url': full_url}

    def _process_save(self, step_config: Dict, context: Dict) -> Dict:
        """Сохраняет указанные поля из контекста в результат"""
        fields = step_config.get('fields', [])
        data_to_save = {key: context.get(key) for key in fields}
        with self.data_lock:
            self.collected_data.append(data_to_save)
        return data_to_save

    def _resolve_url(self, template: str, context: Dict) -> str:
        """Генерирует полный URL."""
        url = template.format(**context)
        if not url.startswith(('http://', 'https://')) and self.base_url:
            url = urljoin(self.base_url, url)
        return url

    def _handle_next_steps(self, step_config: Dict, context: Dict, result: Dict):
        """Обрабатывает следующие шаги."""
        next_steps = step_config.get('next_steps', [])
        combined_context = {**context, **result}
        for next_step in next_steps:
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
        raise ValueError(f"Шаг {name} не найден")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()