import requests
import logging
import yaml
import time
import json
from urllib.parse import urljoin
from selectorlib import Extractor
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class VekParser:
    """
    Парсер для извлечения данных с веб-сайтов на основе конфигурационного файла.
    
    Поддерживает многопоточную обработку, кэширование и гибкую настройку через YAML конфиг.
    """

    def __init__(self, config_path, base_url=None, headers=None, delay=1, max_workers=5):
        """
        Инициализация парсера.

        Args:
            config_path (str): Путь к файлу конфигурации YAML
            base_url (str, optional): Базовый URL для относительных ссылок
            headers (dict, optional): HTTP заголовки для запросов
            delay (int, optional): Задержка между запросами в секундах
            max_workers (int, optional): Максимальное количество потоков
        """
        self._setup_logging()
        self.config = self._load_config(config_path)
        self.session = self._create_session(headers)
        self.base_url = base_url.rstrip('/') if base_url else None
        self.delay = delay
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.collected_data = []
        self.collected_data_lock = threading.Lock()
        self.extractor_cache = {}
        
        self._processors = {
            'static': self._process_static,
            'extract': self._process_extract,
            'list': self._process_list
        }

    def run(self, initial_context=None):
        """
        Запускает процесс парсинга.

        Args:
            initial_context (dict, optional): Начальный контекст для первого шага
        """
        context = initial_context or {}
        self._execute_step(self.config['steps'][0], context)

    def save_data(self, filename):
        """
        Сохраняет собранные данные в JSON файл.

        Args:
            filename (str): Путь к файлу для сохранения
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, ensure_ascii=False, indent=4)

    def _setup_logging(self):
        """Настраивает логирование для парсера."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def _load_config(self, path):
        """
        Загружает конфигурацию из YAML файла.

        Args:
            path (str): Путь к файлу конфигурации
        Returns:
            dict: Загруженная конфигурация
        """
        with open(path) as f:
            return yaml.safe_load(f)

    def _create_session(self, headers):
        """
        Создает сессию requests с настроенными заголовками.

        Args:
            headers (dict, optional): Пользовательские HTTP заголовки
        Returns:
            requests.Session: Настроенная сессия
        """
        session = requests.Session()
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        session.headers.update(headers or default_headers)
        return session

    def _execute_step(self, step_config, context):
        """
        Выполняет шаг парсинга согласно конфигурации.

        Args:
            step_config (dict): Конфигурация шага
            context (dict): Контекст выполнения
        Returns:
            dict: Результат выполнения шага
        """
        processor = self._processors.get(step_config['type'])
        if not processor:
            raise ValueError(f"Unsupported step type: {step_config['type']}")
        
        try:
            result = processor(step_config, context)
            self._handle_next_steps(step_config, context, result)
            return result
        except Exception as e:
            self.logger.error(f"Error processing step {step_config['name']}: {str(e)}", exc_info=True)
            return None

    def _process_static(self, step_config, context):
        """
        Обрабатывает статический шаг конфигурации.

        Args:
            step_config (dict): Конфигурация шага
            context (dict): Контекст выполнения
        Returns:
            dict: Статические значения из конфигурации
        """
        return step_config.get('values', {})

    def _process_extract(self, step_config, context):
        """
        Извлекает данные из веб-страницы согласно конфигурации.

        Args:
            step_config (dict): Конфигурация шага
            context (dict): Контекст выполнения
        Returns:
            dict: Извлеченные данные
        """
        time.sleep(self.delay)
        url = self._resolve_url(step_config.get('url', ''), context)
        response = self._fetch_url(url)
        if not response:
            return {}

        result = {}
        if 'data' in step_config:
            config_yaml = yaml.dump(step_config['data'])
            if config_yaml not in self.extractor_cache:
                self.extractor_cache[config_yaml] = Extractor.from_yaml_string(config_yaml)
            extractor = self.extractor_cache[config_yaml]
            result.update(extractor.extract(response.text) or {})

        return result

    def _process_list(self, step_config, context):
        """
        Обрабатывает список элементов параллельно.

        Args:
            step_config (dict): Конфигурация шага
            context (dict): Контекст выполнения
        Returns:
            dict: Результаты обработки списка
        """
        items = context.get(step_config['source'], [])
        futures = [self.executor.submit(self._process_item, step_config, item) for item in items]
        results = []
        
        for future in as_completed(futures):
            results.extend(future.result() or [])
        
        return {step_config['output']: results}

    def _process_item(self, step_config, item):
        """
        Обрабатывает отдельный элемент списка.

        Args:
            step_config (dict): Конфигурация шага
            item (dict): Элемент для обработки
        Returns:
            list: Результаты обработки элемента
        """
        try:
            context = {'url': self._resolve_url(item.get('url', ''), {})}
            results = []
            
            for nested_step in step_config['steps']:
                result = self._execute_step(nested_step, context)
                if result:
                    results.append(result)
                    with self.collected_data_lock:
                        self.collected_data.append(result)
            
            return results
        except Exception as e:
            self.logger.error(f"Item processing error: {str(e)}", exc_info=True)
            return []

    def _resolve_url(self, template, context):
        """
        Разрешает URL относительно базового URL.

        Args:
            template (str): Шаблон URL
            context (dict): Контекст для форматирования
        Returns:
            str: Полный URL
        """
        return urljoin(self.base_url or '', template.format(**context))

    def _fetch_url(self, url):
        """
        Выполняет HTTP запрос.

        Args:
            url (str): URL для запроса
        Returns:
            Response|None: Ответ сервера или None при ошибке
        """
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            return None

    def _handle_next_steps(self, step_config, context, result):
        """
        Обрабатывает следующие шаги в конфигурации.

        Args:
            step_config (dict): Текущая конфигурация
            context (dict): Текущий контекст
            result (dict): Результат текущего шага
        """
        combined_context = {**context, **result}
        for next_step in step_config.get('next_steps', []):
            step = self._get_step_by_name(next_step['step'])
            mapped_context = {k: combined_context.get(v) for k, v in next_step.get('context_map', {}).items()}
            self._execute_step(step, mapped_context)

    def _get_step_by_name(self, name):
        """
        Находит шаг по имени в конфигурации.

        Args:
            name (str): Имя шага
        Returns:
            dict: Конфигурация шага
        Raises:
            ValueError: Если шаг не найден
        """
        for step in self.config['steps']:
            if step['name'] == name:
                return step
        raise ValueError(f"Step '{name}' not found in config")