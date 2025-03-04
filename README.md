# VekParser: Расширяемый парсер для веб-сайтов

## Основные возможности

- Многоступенчатая обработка данных через YAML-конфигурацию
- Поддержка параллельного выполнения задач
- Встроенные механизмы для работы с динамическим контентом (через Playwright)
- Кастомные обработчики данных
- Автоматическое сохранение результатов в JSON

## Ключевые концепции

### Типы шагов обработки

| Тип       | Назначение                          | Особенности                         |
|-----------|-------------------------------------|-------------------------------------|
| static    | Инициализация статических значений | Задает начальный контекст выполнения|
| extract   | Извлечение данных со страницы       | Использует CSS/XPath селекторы      |
| list      | Параллельная обработка коллекций    | Запускает вложенные цепочки шагов   |
| save      | Финализация и сохранение результата | Экспорт в JSON-файл                |

### Жизненный цикл данных
1. Инициализация контекста (static)
2. Извлечение данных (extract)
3. Обработка списка (list)
4. Сохранение (save)

## Полная структура конфигурации

```yaml
steps:
  - name: уникальное_имя_шага
    type: [static|extract|list|save|свой тип]
    
    # Параметры для static
    values: { ключ: значение }
    
    # Параметры для extract
    url: "шаблон_URL"
    data: { селекторы }
    
    # Параметры для list
    source: "ключ_в_контексте"
    output: "выходной_ключ"
    handler_name: [string|link|dict|свой обработчик]
    
    # Общие параметры
    next_steps:
      - step: "имя_следующего_шага"
        context_map: { ключ: "значение_из_контекста" }
```

## Расширенные возможности

### 1. Кастомные обработчики шагов

Регистрация пользовательской логики:
```python
class DataProcessor:
    @staticmethod
    def process_data(step_config: Dict, context: Dict) -> Dict:
        return {"processed": True}

parser.register_step_handler('transform', DataProcessor.process_data)
```

Пример использования в конфиге:
```yaml
- name: advanced_transform
  type: transform
  next_steps: [...]
```

### 2. Обработчики элементов списка

Поддерживаемые типы:
- `string` - текстовые элементы
- `link` - URL-адреса

Регистрация обработчика для словарей:
```python
parser.register_item_handler('dict', lambda item, _: item)
```

Конфигурация:
```yaml
- name: process_items
  type: list
  handler_name: dict
  steps: [...]
```

### 3. Механизм сохранения данных

Конфигурация шага сохранения:
```yaml
- name: save_results
  type: save
  fields:
    - field1
    - field2
```

Результат:
```json
[
  {
    "field1": "value1",
    "field2": "value2"
  },
  {
    "field1": "value3",
    "field2": "value4"
  }
]
```

## Полный пример парсера

### 1. Код обработчика

```python
class AreaCalculator:
    @staticmethod
    def calculate_dimensions(step_config: Dict, context: Dict) -> Dict:
        # Логика вычислений
        return {
            'area': width * height,
            'volume': width * height * depth
        }
```

### 2. Конфигурация

```yaml
steps:
  - name: product_processing
    type: list
    handler_name: dict
    steps:
      - name: extract_data
        type: extract
        url: "{link}"
        data:
          price: "#price::text"
          dimensions: ".specs::text"
        next_steps:
          - step: calculate_metrics
          
      - name: calculate_metrics
        type: transform
        next_steps:
          - step: save_product
          
  - name: save_product
    type: save
    fields:
      - title
      - price
      - area
      - volume
```

### 3. Инициализация парсера

```python
parser = WebParser(
    config_path='config.yml',
    base_url='https://example.com',
    render_js=True
)
parser.register_step_handler('transform', AreaCalculator.calculate_dimensions)
parser.register_item_handler('dict', lambda item, _: item)
```

## Отладка и мониторинг

### Логирование
Уровни логирования:
- DEBUG: детальная информация о выполнении шагов
- INFO: основные этапы работы
- WARNING: проблемы с доступностью ресурсов
- ERROR: критические ошибки выполнения

Настройка:
```python
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Советы по оптимизации
1. Настройка параллелизма:
```python
WebParser(max_workers=10)
```

2. Интервал запросов:
```python
HttpClient(request_interval=1.0)
```

3. JS-рендеринг:
```python
WebParser(render_js=True)
```
