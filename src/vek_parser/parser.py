import requests
import logging
import yaml
import time
import json
from urllib.parse import urljoin
from selectorlib import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Lock, Semaphore
from typing import Optional, Dict, List


class VekParser:
    """
    Парсер для извлечения данных с веб-сайтов на основе конфигурационного файла.
    Поддерживает многопоточную обработку с контролем частоты запросов.
    """

    def __init__(self, config_path: str,
                 base_url: Optional[str] = None,
                 headers: Optional[Dict] = None,
                 request_interval: float = 1.0,
                 max_workers: int = 10,
                 retries: int = 3):
        """
        Инициализация парсера.

        Args:
            config_path: Путь к файлу конфигурации YAML
            base_url: Базовый URL для относительных ссылок
            headers: HTTP заголовки для запросов
            request_interval: Интервал между запросами в секундах
            max_workers: Максимальное количество параллельных потоков
            retries: Количество повторных попыток для запросов
        """
        self._setup_logging()
        self.config = self._load_config(config_path)
        self.session = self._create_session(headers)
        self.base_url = base_url.rstrip('/') if base_url else None
        self.request_interval = request_interval
        self.retries = retries
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.collected_data = []
        self.extractor_cache = {}

        # Контроль параллелизма и состояния
        self.pending_futures = []
        self.shutdown_event = threading.Event()
        self.data_lock = Lock()
        self.extractor_lock = Lock()
        self.concurrent_requests = Semaphore(max_workers)

        self._processors = {
            'static': self._process_static,
            'extract': self._process_extract,
            'list': self._process_list
        }

        self._validate_config()

    def run(self, initial_context: Optional[Dict] = None):
        """Запускает процесс парсинга."""
        try:
            context = initial_context or {}
            self._execute_step(self.config['steps'][0], context)
        finally:
            self.wait_completion()

    def save_data(self, filename: str):
        """Сохраняет собранные данные в JSON файл."""
        with self.data_lock:
            data_copy = self.collected_data.copy()

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data_copy, f, ensure_ascii=False, indent=4)

    def wait_completion(self):
        """Ожидает завершения всех задач и закрывает executor"""
        for future in self.pending_futures:
            future.result()

        self.executor.shutdown(wait=True)
        self.shutdown_event.set()

    def close(self):
        """Явное закрытие ресурсов"""
        self.wait_completion()

    def _setup_logging(self):
        """Настраивает логирование."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _validate_config(self):
        """Проверяет обязательные поля в конфигурации."""
        required_fields = {
            'extract': ['url'],
            'list': ['source', 'steps']
        }

        for step in self.config.get('steps', []):
            step_type = step.get('type')
            if not step_type:
                raise ValueError("Шаг должен содержать поле 'type'")

            for field in required_fields.get(step_type, []):
                if field not in step:
                    raise ValueError(f"Шаг '{step.get('name')}' типа '{step_type}' должен содержать поле '{field}'")

    def _load_config(self, path: str) -> Dict:
        """Загружает YAML конфигурацию."""
        with open(path, encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _create_session(self, headers: Optional[Dict]) -> requests.Session:
        """Создает HTTP сессию."""
        session = requests.Session()
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        session.headers.update(headers or default_headers)
        return session

    def _execute_step(self, step_config: Dict, context: Dict) -> Optional[Dict]:
        """Выполняет шаг парсинга."""
        if self.shutdown_event.is_set():
            return None

        processor = self._processors.get(step_config['type'])
        if not processor:
            raise ValueError(f"Неподдерживаемый тип шага: {step_config['type']}")

        try:
            result = processor(step_config, context)
            self._handle_next_steps(step_config, context, result)
            return result
        except Exception as e:
            self.logger.error(f"Ошибка обработки шага {step_config.get('name')}: {str(e)}", exc_info=True)
            return None

    def _process_static(self, step_config: Dict, context: Dict) -> Dict:
        """Обрабатывает статический шаг."""
        return step_config.get('values', {}).copy()

    def _process_extract(self, step_config: Dict, context: Dict) -> Dict:
        """Извлекает данные с веб-страницы."""
        with self.concurrent_requests:
            time.sleep(self.request_interval)
            url = self._resolve_url(step_config.get('url', ''), context)
            response = self._fetch_url(url)

            if not response:
                return {}

            result = {}
            if 'data' in step_config:
                cache_key = json.dumps(step_config['data'], sort_keys=True)

                with self.extractor_lock:
                    if cache_key not in self.extractor_cache:
                        self.extractor_cache[cache_key] = Extractor.from_yaml_string(
                            yaml.dump(step_config['data'])
                        )
                    extractor = self.extractor_cache[cache_key]

                extracted = extractor.extract(response.text) or {}
                result.update(extracted)

            return result

    def _process_list(self, step_config: Dict, context: Dict) -> Dict:
        """Параллельно обрабатывает список элементов."""
        items = context.get(step_config.get('source'), [])
        chunk_size = max(1, len(items) // (self.max_workers * 2))
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

        futures = [
            self.executor.submit(self._process_chunk, step_config, chunk)
            for chunk in chunks
        ]
        self.pending_futures.extend(futures)

        results = []
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                self.logger.error(f"Ошибка обработки блока: {str(e)}")
            finally:
                self.pending_futures.remove(future)

        return {step_config['output']: results}

    def _process_chunk(self, step_config: Dict, chunk: List) -> List[Dict]:
        """Обрабатывает блок элементов."""
        chunk_results = []
        for item in chunk:
            try:
                if self.shutdown_event.is_set():
                    break

                result = self._process_item(step_config, item)
                if result:
                    chunk_results.extend(result)
            except Exception as e:
                self.logger.error(f"Ошибка элемента: {str(e)}")
        return chunk_results

    def _process_item(self, step_config: Dict, item: Dict) -> List[Dict]:
        """Обрабатывает отдельный элемент."""
        try:
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
        except Exception as e:
            self.logger.error(f"Ошибка обработки элемента: {str(e)}", exc_info=True)
            return []

    def _resolve_url(self, template: str, context: Dict) -> str:
        """Формирует корректный URL."""
        url = template.format(**context)

        if not url.startswith(('http://', 'https://')):
            if not self.base_url:
                raise ValueError("Базовый URL обязателен для относительных ссылок")
            return urljoin(self.base_url, url)

        return url

    def _fetch_url(self, url: str) -> Optional[requests.Response]:
        """Выполняет HTTP запрос с повторами."""
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=15)

                if response.status_code == 404:
                    self.logger.warning(f"Страница не найдена: {url}")
                    return None

                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < self.retries:
                    delay = 2 ** attempt
                    self.logger.warning(f"Повтор {url} (попытка {attempt}/{self.retries}), задержка {delay} сек")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Не удалось получить {url} после {self.retries} попыток")
                    return None

    def _handle_next_steps(self, step_config: Dict, context: Dict, result: Dict):
        """Обрабатывает следующие шаги."""
        combined_context = {**context, **result}
        for next_step in step_config.get('next_steps', []):
            if self.shutdown_event.is_set():
                return

            step = self._get_step_by_name(next_step['step'])
            mapped_context = {
                k: combined_context.get(v)
                for k, v in next_step.get('context_map', {}).items()
            }
            self._execute_step(step, mapped_context)

    def _get_step_by_name(self, name: str) -> Dict:
        """Находит шаг по имени."""
        for step in self.config['steps']:
            if step['name'] == name:
                return step
        raise ValueError(f"Шаг '{name}' не найден")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()