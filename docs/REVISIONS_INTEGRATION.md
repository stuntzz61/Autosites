# 🔄 Система правок сайтов (Revisions)

Документация по интеграции системы правок между Telegram-ботом/мини-аппой, API, deploy-node и n8n.

## 📋 Обзор

Система правок позволяет клиентам отправлять запросы на изменение сайта через Telegram или мини-аппу. Правки обрабатываются через n8n и автоматически деплоятся.

### Цикл обработки правок

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────┐
│  Telegram/App   │────▶│  Autosites  │────▶│    n8n       │
│  (клиент)       │     │    API      │     │  (обработка) │
└─────────────────┘     └─────────────┘     └──────────────┘
                               │                    │
                             ▼                    ▼
                        ┌─────────────┐     ┌──────────────┐
                        │ Deploy-Node │◀────│ Новый архив  │
                        │ (stop/start)│     │    S3        │
                        └─────────────┘     └──────────────┘
```

## 🗄️ Структура данных

### Таблица `revisions`

Основная таблица для хранения итераций правок:

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | UUID | Уникальный ID |
| `site_id` | UUID | Ссылка на client_sites |
| `iteration` | INTEGER | Номер итерации (1, 2, 3...) |
| `status` | VARCHAR | pending, in_progress, processing, completed, failed, cancelled |
| `s3_folder` | VARCHAR | Путь в S3: `sites/{site_id}/revisions/{iteration}/` |
| `n8n_job_id` | VARCHAR | ID задачи для корреляции с n8n |
| `result_archive_s3_key` | VARCHAR | Путь к архиву после обработки |

### Таблица `revision_changes`

Отдельные правки внутри итерации:

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | UUID | Уникальный ID |
| `revision_id` | UUID | Ссылка на revisions |
| `change_type` | VARCHAR | text_change, visual_change, layout_change, etc. |
| `location_area` | VARCHAR | hero, header, footer, about, contacts, etc. |
| `client_description` | TEXT | Описание от клиента |
| `screenshot_s3_key` | VARCHAR | Путь к скриншоту в S3 |
| `priority` | VARCHAR | low, normal, high, critical |

## 🔌 API Endpoints

### Создание правок

**POST** `/api/revisions`

```json
{
  "site_id": "uuid-of-site",
  "changes": [
    {
      "type": "text_change",
      "location": {
        "area": "footer",
        "description": "Нижняя часть страницы"
      },
      "client_description": "Измените текст внизу страницы",
      "priority": "normal"
    },
    {
      "type": "visual_change",
      "location": {
        "area": "hero"
      },
      "client_description": "Сделайте кнопку более заметной",
      "screenshot": {
        "file_key": "sites/xxx/revisions/1/screenshot_1.png"
      }
    }
  ],
  "source": "telegram_bot",
  "auto_submit": false
}
```

### Загрузка скриншота

**POST** `/api/revisions/{revision_id}/upload-screenshot`

Form data:
- `file`: Файл изображения
- `comment`: Комментарий к скриншоту (опционально)

Ответ:
```json
{
  "s3_key": "sites/{site_id}/revisions/{iteration}/screenshot_1.png",
  "url": "https://storage.example.com/sites/...",
  "comment": "Обведена кнопка"
}
```

### Отправка в обработку

**POST** `/api/revisions/{revision_id}/submit`

```json
{
  "stop_preview": true
}
```

При `stop_preview: true`:
1. Останавливается preview-сайт в deploy-node
2. Статус меняется на `in_progress`
3. Правки отправляются в n8n

### Webhook от n8n

**POST** `/api/revisions/webhook/n8n-callback`

n8n вызывает после обработки:

```json
{
  "job_id": "revision-uuid",
  "status": "completed",
  "result_archive_s3_key": "sites/{site_id}/archives/v2.zip",
  "changes_applied": [
    {
      "id": "change-uuid",
      "status": "applied",
      "ai_interpretation": "Изменен текст в footer"
    }
  ],
  "ai_summary": "Успешно применены 2 правки"
}
```

## 🔄 Интеграция с n8n

### Входной payload для n8n

```json
{
  "action": "process_revision",
  "revision_id": "uuid",
  "job_id": "uuid-for-correlation",
  "site_id": "uuid",
  "iteration": 2,
  "s3_folder": "sites/{site_id}/revisions/2/",
  "archive_url": "https://presigned-url-to-download",
  "archive_s3_key": "sites/{site_id}/archives/v1.zip",
  "callback_url": "https://api.example.com/api/revisions/webhook/n8n-callback",
  "site_info": {
    "company_name": "Компания",
    "domain": "site.ru",
    "preview_url": "https://abc123.autosites.ru"
  },
  "changes": [
    {
      "id": "change-uuid",
      "type": "text_change",
      "location": {
        "area": "footer",
        "selector": null,
        "description": "Нижняя часть"
      },
      "client_description": "Измените текст",
      "old_value": null,
      "new_value_suggestion": null,
      "screenshot": null,
      "priority": "normal"
    }
  ],
  "meta": {
    "source": "telegram_bot",
    "client_id": "tg_123456",
    "manager_id": "uuid"
  }
}
```

### Workflow n8n

Рекомендуемый workflow:

1. **Trigger**: Webhook node (POST /webhook/site-revisions)
2. **Download archive**: HTTP node для скачивания архива по presigned URL
3. **Extract**: Распаковка архива
4. **AI Processing**: Обработка правок с помощью AI (GPT/Claude)
5. **Apply changes**: Применение изменений к коду
6. **Build**: Сборка проекта (npm run build)
7. **Upload**: Загрузка нового архива в S3
8. **Callback**: HTTP node для отправки результата в API

## 📡 Интеграция с Deploy-Node

### Остановка/Запуск по ID

Deploy-node поддерживает управление сайтами по ID:

```bash
# Остановить сайт по ID
POST /api/sites/by-id/{id}/stop

# Запустить сайт по ID
POST /api/sites/by-id/{id}/start

# Перезапустить сайт по ID
POST /api/sites/by-id/{id}/restart
```

### Callback от Deploy-Node

При изменении статуса деплоя, deploy-node отправляет callback:

```json
{
  "deploy_id": "uuid",
  "status": "completed",
  "request_id": "uuid",
  "client_site_id": "uuid",
  "preview_slug": "abc123",
  "preview_url": "https://abc123.autosites.ru",
  "server_id": "agent1",
  "server_host": "1.2.3.4",
  "port": 3001
}
```

## 🗂️ Структура S3

```
autosites-bucket/
├── sites/
│   └── {site_id}/
│       ├── archives/
│       │   ├── v1.zip      # Первая версия
│       │   └── v2.zip      # После правок
│       └── revisions/
│           ├── 1/
│           │   ├── screenshot_1.png
│           │   └── screenshot_2.png
│           └── 2/
│               └── screenshot_1.png
```

## 🔐 Безопасность

### Callback Secret

Для защиты webhook от несанкционированных вызовов:

1. Установите `DEPLOY_CALLBACK_SECRET` в env
2. Deploy-node отправляет secret в заголовке `X-Callback-Secret`
3. API проверяет заголовок перед обработкой

### Presigned URLs

Архивы скачиваются через presigned URL с ограниченным временем жизни (1 час).

## 📱 Уведомления

При изменении статуса правок отправляются уведомления менеджеру через бота:

```json
{
  "action": "revision_status",
  "tg_id": 123456789,
  "revision_id": "uuid",
  "site_id": "uuid",
  "company_name": "Компания",
  "iteration": 2,
  "status": "completed",
  "message": "Правки успешно применены",
  "preview_url": "https://abc123.autosites.ru"
}
```

## ⚙️ Конфигурация

Переменные окружения:

```bash
# n8n webhook для правок
N8N_REVISIONS_WEBHOOK_URL=http://n8n:5678/webhook/site-revisions

# Deploy Node URL
DEPLOY_NODE_URL=http://deploy-node:8080

# Секрет для callback
DEPLOY_CALLBACK_SECRET=your_secret

# URL бота для уведомлений
BOT_WEBHOOK_URL=http://bot:8081
```

## 📊 Статусы правок

| Статус | Описание |
|--------|----------|
| `pending` | Создана, ожидает отправки |
| `in_progress` | Отправлена на обработку |
| `processing` | Обрабатывается в n8n |
| `completed` | Успешно обработана и задеплоена |
| `failed` | Ошибка при обработке |
| `cancelled` | Отменена пользователем |

## 🔍 Типы правок

| Тип | Описание |
|-----|----------|
| `text_change` | Изменение текста |
| `visual_change` | Визуальные изменения (цвета, размеры) |
| `layout_change` | Изменение расположения элементов |
| `content_add` | Добавление нового контента |
| `content_remove` | Удаление контента |
| `style_change` | Изменение стилей |

## 🚀 Быстрый старт

1. Примените миграцию:
```bash
psql $DATABASE_URL -f migrations/012_revisions.sql
```

2. Настройте переменные окружения

3. Создайте workflow в n8n

4. Протестируйте через API:
```bash
# Создать ревизию
curl -X POST https://api.example.com/api/revisions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "site_id": "your-site-id",
    "changes": [{
      "type": "text_change",
      "client_description": "Тестовая правка"
    }]
  }'
```

