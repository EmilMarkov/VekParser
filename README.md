# Полный гайд по конфигурации VekParser

## Базовые концепции

### Контекст (Context)
- Словарь данных, передаваемый между шагами
- Формируется из:
  - Результатов предыдущих шагов
  - initial_context (передается при запуске)
  - Ручных значений (через шаг `static`)
- Используется через `{variable}` в URL и параметрах

### Жизненный цикл шага
1. Получение входного контекста
2. Выполнение логики шага
3. Сохранение результатов в новый контекст
4. Передача данных в следующие шаги

---

## Структура конфига (config.yml)

### Корневые элементы
```yaml
steps:           # Обязательно. Список всех шагов
  - name: "..."  # Уникальное имя шага
    type: "..."  # Тип шага (static/extract/list)
    # ... параметры типа
```

---

## Типы шагов (подробно)

### 1. static — статические данные
```yaml
- name: setup_vars
  type: static
  values:          # Фиксированные значения
    page: 1
    lang: "en"
  next_steps:      # Опционально. Что выполнить после
    - step: "parse_page"  # Имя следующего шага
      context_map:        # Маппинг данных в контекст
        current_page: page  # current_page = context['page']
```

### 2. extract — парсинг страницы
```yaml
- name: parse_page
  type: extract
  url: "/products?page={current_page}"  # Используем контекст
  data:              # Использует selectorlib синтаксис
    product_list:
      css: "div.product"
      multiple: true
      children:
        title: "h2::text"
        link: "a::attr(href)"
  extract:           # Использует parsel синтаксис
    pagination:
      target: links  # Тип извлечения (links/data/html)
      selector: "a.page-link"
      attr: "data-page"  # Опциональный атрибут
  next_steps:
    - step: "process_products"
      context_map:
        items: product_list  # Передаем список продуктов
```

**Поддерживаемые target:**
- `links`: список {url, text, [attr]}
- `data`: словарь {field: value}
- `html`: сырой HTML/текст

### 3. list — обработка списка элементов
```yaml
- name: process_products
  type: list
  source: items     # Откуда брать элементы (из контекста)
  output: products  # Куда сохранить результат
  steps:            # Вложенные шаги для каждого элемента
    - name: product_detail
      type: extract
      url: "{link}"  # URL из элемента списка
      extract:
        specs:
          target: data
          selectors:
            sku: "div#sku::text"
            price: "span.price::text"
```

---

## Механика работы контекста

### Пример потока данных
1. Начальный контекст:
   ```python
   {'category': 'electronics'}
   ```
2. После шага static:
   ```yaml
   values: {page: 1}
   ```
   Контекст → `{'category': 'electronics', 'page': 1}`

3. В extract шаге:
   ```yaml
   url: "/catalog/{category}?page={page}"
   ```
   URL → `/catalog/electronics?page=1`

4. Результат extract шага:
   ```python
   {'products': [...]}
   ```
   Новый контекст → `{..., 'products': [...]}`

---

## Полный пример конфига

`config.yml`:
```yaml
steps:
  - name: init
    type: static
    values:
      base_category: "smartphones"
    next_steps:
      - step: parse_category

  - name: parse_category
    type: extract
    url: "/shop/{base_category}"
    extract:
      subcategories:
        target: links
        selector: "nav.subcategories a"
        attr: "data-id"
      items:
        target: links
        selector: "div.product-card a.title"
    next_steps:
      - step: process_subcategories
        context_map:
          subcats: subcategories
      - step: process_items
        context_map:
          products: items

  - name: process_subcategories
    type: list
    source: subcats
    output: parsed_subcats
    steps:
      - name: parse_subcategory
        type: extract
        url: "{url}"
        extract:
          title: "h1::text"
          description: "div.content::text"

  - name: process_items
    type: list
    source: products
    output: parsed_products
    steps:
      - name: item_page
        type: extract
        url: "{url}"
        extract:
          details:
            target: data
            selectors:
              brand: "meta[itemprop='brand']::attr(content)"
              rating: "span.score::text"
```

---

## Правила составления конфига

1. Порядок шагов в YAML не важен — выполнение определяется next_steps
2. Всегда стартует первый шаг в списке `steps`
3. Контекст автоматически обогащается:
   - Результатами каждого шага
   - Данными из context_map
4. Для работы с динамическими URL всегда используйте `{variable}`

---

## Обработка ошибок

- При ошибке в шаге: запись в лог, переход к следующему элементу
- Для повтора запросов: увеличивайте `delay` в конструкторе
- Для отладки: смотрите `parser.log` с уровнем DEBUG
