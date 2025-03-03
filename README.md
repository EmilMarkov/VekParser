# Полное руководство по настройке VekParser

## Основные концепции

### Контекст выполнения
- Динамический словарь данных, передаваемый между шагами
- Формируется из:
  - Результатов выполнения предыдущих шагов
  - Начального контекста (initial_context)
  - Статических значений (через шаг `static`)
- Используется через шаблоны `{variable}` в URL и параметрах

### Жизненный цикл шага
1. Получение входного контекста
2. Исполнение логики шага
3. Сохранение результатов в новый контекст
4. Передача обогащенного контекста последующим шагам

---

## Структура конфигурационного файла

### Корневая структура
```yaml
steps:            # Обязательный элемент. Список всех шагов
  - name: "..."   # Уникальный идентификатор шага
    type: "..."   # Тип обработки (static/extract/list)
    # ... параметры конкретного типа
```

---

## Типы шагов обработки

### 1. static — статическое заполнение контекста
```yaml
- name: init_vars
  type: static
  values:         # Фиксированные значения
    items_per_page: 20
    language: "ru"
  next_steps:     # Опциональное продолжение цепочки
    - step: "load_page"     # Имя следующего шага
      context_map:         # Маппинг данных в контекст
        page_num: items_per_page  # page_num = context['items_per_page']
```

### 2. extract — извлечение данных со страницы
```yaml
- name: parse_products
  type: extract
  url: "/catalog?page={page_num}"  # Подстановка из контекста
  data:              # Конфигурация для selectorlib
    products:        # Ключ для сохранения в контекст
      css: "div.product-card"
      multiple: true  # Сбор всех совпадений
      children:       # Вложенные элементы
        title: "h2::text"
        price: ".price::attr(data-value)"
        details_url: "a.more::attr(href)"
  next_steps:
    - step: "process_products"
      context_map:
        product_links: details_url  # Передача ссылок
```

### 3. list — параллельная обработка коллекции элементов
```yaml
- name: process_products
  type: list
  source: product_links  # Источник данных из контекста
  output: parsed_items   # Ключ для сохранения результатов
  steps:                 # Цепочка обработки для каждого элемента
    - name: product_page
      type: extract
      url: "{details_url}"  # URL из элемента коллекции
      data:
        specifications:
          css: "div.specs-table"
          children:
            weight: "span.weight::text"
            dimensions: "span.size::text"
```

---

## Механизм работы контекста

### Пример передачи данных
1. Исходный контекст:
   ```python
   {'category_id': 42, 'region': 'eu'}
   ```
2. После шага static:
   ```yaml
   values: {page: 3, currency: 'USD'}
   ```
   Контекст → `{'category_id': 42, 'region': 'eu', 'page': 3, 'currency': 'USD'}`

3. В шаге extract:
   ```yaml
   url: "/v2/{category_id}?region={region}&p={page}"
   ```
   Результат URL → `/v2/42?region=eu&p=3`

4. Результат выполнения extract:
   ```python
   {'product_count': 15, 'items': [...]}
   ```
   Обновленный контекст → `{..., 'product_count': 15, 'items': [...]}`

---

## Полный пример конфигурации

`config.yml`:
```yaml
steps:
  - name: initialization
    type: static
    values:
      catalog_section: "electronics"
      max_threads: 8
    next_steps:
      - step: parse_main_category

  - name: parse_main_category
    type: extract
    url: "/categories/{catalog_section}"
    data:
      subcategories:
        css: "ul.subcategories li"
        multiple: true
        children:
          name: "a::text"
          url: "a::attr(href)"
      products:
        css: "div.product-tile"
        multiple: true
        children:
          title: "h3::text"
          product_url: "a::attr(href)"
    next_steps:
      - step: process_subcategories
        context_map:
          subcategory_links: subcategories.url
      - step: process_products
        context_map:
          product_links: products.product_url

  - name: process_subcategories
    type: list
    source: subcategory_links
    output: parsed_subcategories
    steps:
      - name: subcategory_page
        type: extract
        url: "{item}"
        data:
          product_count: "span.count::text"
          description: "div.category-description::text"

  - name: process_products
    type: list
    source: product_links
    output: parsed_products
    steps:
      - name: product_details
        type: extract
        url: "{item}"
        data:
          title: "h1.product-title::text"
          price: "meta[itemprop=price]::attr(content)"
          sku: "div.product-id::text"
```

---

## Правила построения конфигурации

1. **Порядок объявления шагов** не влияет на выполнение — последовательность определяется через `next_steps`
2. **Точка входа** — всегда первый шаг в списке `steps`
3. **Автоматическое обогащение контекста**:
   - Результаты каждого шага добавляются в контекст
   - `context_map` позволяет переносить данные между шагами
4. **Динамические URL** должны использовать шаблоны `{variable_name}`

---

## Обработка ошибок и отладка

- **Ошибки выполнения шага**: Логируются с указанием шага, выполнение продолжается
- **Рекомендации по запросам**:
  - Увеличивайте `delay` при частых ошибках соединения
  - Настраивайте `max_workers` в зависимости от нагрузки
- **Файл лога**: `parser.log` с детальной информацией (уровень DEBUG)