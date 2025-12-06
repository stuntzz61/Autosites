# Интеграция Autosites с Deploy Node

## Обзор архитектуры

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Telegram      │────▶│   Autosites     │────▶│   n8n Workflow  │
│   WebApp        │     │   API           │     │   (генерация)   │
└─────────────────┘     └────────┬────────┘     └────────┬────────┘
                                 │                       │
                                 │                       │
                                 ▼                       ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │   PostgreSQL    │     │   Callback      │
                        │   (client_sites)│◀────│   /generation   │
                        └────────┬────────┘     └─────────────────┘
                                 │
                                 │
                                 ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │   Deploy Node   │────▶│   Agent         │
                        │   (main-deploy) │     │   (сервер)      │
                        └────────┬────────┘     └─────────────────┘
                                 │
                                 │ callback
                                 ▼
                        ┌─────────────────┐
                        │   /deploy-      │
                        │   callback      │
                        └─────────────────┘
```

## Новые таблицы в БД

### client_sites
Связывает заявки (requests) с деплоями:
- `request_id` — связь с заявкой
- `deploy_id` — ID деплоя в deploy-node
- `preview_url` — URL для предпросмотра
- `domain` — кастомный домен клиента
- `deploy_status` — статус деплоя (pending, deploying, active, failed)
- `hosting_plan` — тарифный план
- `hosting_expires_at` — дата окончания хостинга

### hosting_plans
Тарифные планы:
- `trial` — 7 дней бесплатно
- `basic` — 500₽/мес
- `pro` — 1500₽/мес
- `enterprise` — 5000₽/мес

### deploy_history
История деплоев для аудита и rollback.

## Конфигурация

### Autosites API (.env)
```bash
# URL deploy-node
DEPLOY_NODE_URL=http://deploy-node:8080

# Автодеплой после генерации
AUTO_DEPLOY_ENABLED=false
```

### Deploy Node (.env)
```bash
# URL для callback уведомлений
CALLBACK_URL=http://api:8000

# Секрет для аутентификации (опционально)
CALLBACK_SECRET=your-secret
```

## API Endpoints

### Sites CRUD

```bash
# Список сайтов менеджера
GET /api/sites

# Статистика (админ)
GET /api/sites/stats

# Сайты с истекающим хостингом (админ)
GET /api/sites/expiring?days=7

# Тарифные планы
GET /api/sites/plans

# Получить сайт
GET /api/sites/{site_id}

# История деплоев
GET /api/sites/{site_id}/history

# Создать сайт из заявки
POST /api/sites
{
  "request_id": "uuid",
  "company_name": "Company",
  "hosting_plan": "trial"
}

# Обновить сайт
PATCH /api/sites/{site_id}
{
  "notes": "Комментарий"
}

# Запустить деплой
POST /api/sites/{site_id}/deploy

# Привязать домен
POST /api/sites/{site_id}/domain
{
  "domain": "example.com",
  "enable_ssl": true
}

# Продлить хостинг
POST /api/sites/{site_id}/extend
{
  "plan": "basic",
  "months": 1
}

# Остановить сайт
POST /api/sites/{site_id}/stop

# Удалить сайт (админ)
DELETE /api/sites/{site_id}
```

### Webhooks

```bash
# Callback от deploy-node при изменении статуса
POST /api/sites/webhook/deploy-callback
{
  "deploy_id": "uuid",
  "status": "completed",
  "preview_url": "https://abc123.autosites.ru",
  ...
}

# Callback от n8n после генерации
POST /api/sites/webhook/generation-callback
{
  "request_id": "uuid",
  "status": "completed",
  "archive_s3_key": "path/to/archive.zip"
}
```

## Обновление n8n Workflow

Добавьте в конец workflow (после "Send a document") новую ноду:

### 1. HTTP Request - Generation Callback

```json
{
  "method": "POST",
  "url": "{{ $env.AUTOSITES_API_URL }}/api/sites/webhook/generation-callback",
  "headers": {
    "Content-Type": "application/json"
  },
  "body": {
    "request_id": "={{ $json.request_id }}",
    "status": "completed",
    "archive_s3_key": "={{ $json.archive_s3_key }}",
    "archive_size_bytes": "={{ $json.archive_size_bytes }}"
  }
}
```

### 2. (Опционально) Auto-Deploy

Если AUTO_DEPLOY_ENABLED=true, можно добавить ноду для автоматического деплоя:

```json
{
  "method": "POST",
  "url": "{{ $env.DEPLOY_NODE_URL }}/api/deploy",
  "headers": {
    "Content-Type": "multipart/form-data"
  },
  "body": {
    "archive": "={{ $binary.archive }}",
    "auto_select": "true"
  }
}
```

## Применение миграций

```bash
# Autosites
cd Autosites
psql $DATABASE_URL < migrations/009_client_sites.sql
```

## Flow: Генерация → Деплой

1. **Пользователь создает заявку** → `requests`
2. **Нажимает "Сгенерировать"** → n8n workflow
3. **n8n генерирует сайт** → ZIP архив
4. **n8n вызывает callback** → `/api/sites/webhook/generation-callback`
5. **API создает client_site** → `generation_status = 'completed'`
6. **Если AUTO_DEPLOY_ENABLED**:
   - API запускает деплой → deploy-node
   - Deploy-node деплоит на сервер
   - Deploy-node вызывает callback → `/api/sites/webhook/deploy-callback`
   - API обновляет `deploy_status = 'active'`
7. **Бот уведомляет менеджера** с preview URL

## Мониторинг

```sql
-- Активные сайты
SELECT * FROM client_sites WHERE deploy_status = 'active';

-- Истекающий хостинг
SELECT * FROM expiring_sites;

-- Статистика
SELECT * FROM sites_stats;
```

## Troubleshooting

### Callback не работает
1. Проверьте `CALLBACK_URL` в deploy-node
2. Проверьте логи: `docker logs deploy-node`
3. Проверьте доступность API из контейнера

### Деплой не запускается
1. Проверьте `DEPLOY_NODE_URL` в Autosites API
2. Проверьте наличие архива в S3
3. Проверьте статус `generation_status`

### Статус не обновляется
1. Проверьте webhook endpoint доступен
2. Проверьте формат payload
3. Смотрите логи API и deploy-node

