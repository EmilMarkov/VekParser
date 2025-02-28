import requests
import logging
import yaml
import time
import json
from urllib.parse import urljoin
from selectorlib import Extractor
from parsel import Selector
from concurrent.futures import ThreadPoolExecutor, as_completed


class VekParser:
    """Универсальный парсер для сбора структурированных данных с веб-сайтов.
    
    Attributes:
        config (dict): Загруженная конфигурация парсера
        session (requests.Session): Сессия для HTTP-запросов
        base_url (str): Базовый URL для относительных ссылок
        logger (logging.Logger): Логгер для записи событий
        collected_data (list): Собранные данные в процессе работы
    """

    def __init__(self, config_path, base_url=None, headers=None, delay=1, max_workers=5):
        """Инициализация парсера.
        
        Args:
            config_path (str): Путь к YAML-файлу конфигурации
            base_url (str, optional): Базовый URL сайта
            headers (dict, optional): Кастомные HTTP-заголовки
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
        
        self._processors = {
            'static': self._process_static,
            'extract': self._process_extract,
            'list': self._process_list
        }

    def run(self, initial_context=None):
        """Запуск процесса парсинга.
        
        Args:
            initial_context (dict, optional): Начальный контекст выполнения
        """
        context = initial_context or {}
        self._execute_step(self.config['steps'][0], context)

    def save_data(self, filename):
        """Сохранение собранных данных в файл.
        
        Args:
            data (list): Данные для сохранения
            filename (str): Имя файла для сохранения данных
        """
        self.logger.info("Сохранение собранных данных")
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, ensure_ascii=False, indent=4)

    def _setup_logging(self):
        """Настройка системы логирования."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def _load_config(self, path):
        """Загрузка YAML-конфигурации."""
        with open(path) as f:
            return yaml.safe_load(f)

    def _create_session(self, headers):
        """Создание HTTP-сессии с настройками."""
        session = requests.Session()
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        session.headers.update(headers or default_headers)
        return session

    def _execute_step(self, step_config, context):
        """Выполнение одного шага обработки."""
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
        """Обработка статических данных."""
        return step_config.get('values', {})

    def _process_extract(self, step_config, context):
        """Извлечение данных со страницы."""
        time.sleep(self.delay)
        url = self._resolve_url(step_config.get('url', ''), context)
        response = self._fetch_url(url)
        if not response:
            return {}

        result = {}
        if 'data' in step_config:
            extractor = Extractor.from_yaml_string(yaml.dump(step_config['data']))
            result.update(extractor.extract(response.text) or {})

        if 'extract' in step_config:
            selector = Selector(response.text)
            for key, config in step_config['extract'].items():
                handler = getattr(self, f"_extract_{config['target']}")
                result[key] = handler(selector, config)

        return result

    def _process_list(self, step_config, context):
        """Обработка списка элементов."""
        items = context.get(step_config['source'], [])
        futures = [self.executor.submit(self._process_item, step_config, item) for item in items]
        results = []
        
        for future in as_completed(futures):
            results.extend(future.result() or [])
        
        return {step_config['output']: results}

    def _process_item(self, step_config, item):
        """Обработка одного элемента списка."""
        try:
            context = {'url': self._resolve_url(item.get('url', ''), {})}
            results = []
            
            for nested_step in step_config['steps']:
                result = self._execute_step(nested_step, context)
                if result:
                    results.append(result)
                    self.collected_data.append(result)
            
            return results
        except Exception as e:
            self.logger.error(f"Item processing error: {str(e)}", exc_info=True)
            return []

    def _extract_links(self, selector, config):
        """Извлечение списка ссылок."""
        elements = selector.css(config['selector'])
        return [{
            'url': el.css('::attr(href)').get(''),
            'text': el.css('::text').get('').strip(),
            **({config['attr']: el.css(f"::attr({config['attr']})").get('')} if 'attr' in config else {})
        } for el in elements]

    def _extract_data(self, selector, config):
        """Извлечение структурированных данных."""
        return {
            field: selector.css(f"{css}::text").get('').strip()
            for field, css in config['selectors'].items()
        }

    def _extract_html(self, selector, config):
        """Извлечение HTML-фрагментов."""
        if config.get('multiple', False):
            return selector.css(config['selector']).getall()
        return selector.css(config['selector']).get()

    def _resolve_url(self, template, context):
        """Формирование полного URL."""
        return urljoin(self.base_url or '', template.format(**context))

    def _fetch_url(self, url):
        """Выполнение HTTP-запроса."""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            return None

    def _handle_next_steps(self, step_config, context, result):
        """Обработка следующих шагов."""
        combined_context = {**context, **result}
        for next_step in step_config.get('next_steps', []):
            step = self._get_step_by_name(next_step['step'])
            mapped_context = {k: combined_context.get(v) for k, v in next_step.get('context_map', {}).items()}
            self._execute_step(step, mapped_context)

    def _get_step_by_name(self, name):
        """Поиск шага по имени."""
        for step in self.config['steps']:
            if step['name'] == name:
                return step
        raise ValueError(f"Step '{name}' not found in config")
