# Интеграция Autosites с Deploy Node

> 📝 **См. также:**
> - [Система правок сайтов (Revisions)](./REVISIONS_INTEGRATION.md) — документация по циклу правок через n8n
> - [Синхронизация баз данных](./DB_SYNC.md) — как синхронизируются статусы между двумя БД

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

### Revisions (Правки)

```bash
# Создать правку
POST /api/revisions
{
  "site_id": "uuid",
  "changes": [
    { "type": "text_change", "client_description": "Изменить текст" }
  ]
}

# Отправить правки в обработку
POST /api/revisions/{id}/submit

# Callback от n8n после обработки правок
POST /api/revisions/webhook/n8n-callback
{
  "job_id": "uuid",
  "status": "completed",
  "result_archive_s3_key": "path/to/new-archive.zip"
}
```

См. полную документацию: [REVISIONS_INTEGRATION.md](./REVISIONS_INTEGRATION.md)

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
6. **Бот уведомляет менеджера** о завершении генерации
7. **Если AUTO_DEPLOY_ENABLED** или **менеджер нажимает "Деплой"**:
   - API запускает деплой → deploy-node
   - Deploy-node деплоит на сервер
   - Deploy-node вызывает callback → `/api/sites/webhook/deploy-callback`
   - API обновляет `deploy_status = 'active'`
   - API обновляет статус заявки → `success`
8. **Бот уведомляет менеджера** с preview URL

## Уведомления бота

Бот отправляет уведомления через webhook `/webhook`:

```json
// Статус деплоя
{
  "action": "deploy_status",
  "tg_id": 123456789,
  "site_id": "uuid",
  "company_name": "Company",
  "status": "active|deploying|failed|stopped",
  "preview_url": "https://abc12345.autosites.ru",
  "domain": "example.com",
  "error": "error message if failed"
}

// Завершение генерации
{
  "action": "generation_complete",
  "tg_id": 123456789,
  "request_id": "uuid",
  "company_name": "Company",
  "status": "completed|error",
  "error": "error message if failed"
}

// Предупреждение об истечении хостинга
{
  "action": "hosting_warning",
  "tg_id": 123456789,
  "site_id": "uuid",
  "company_name": "Company",
  "days_remaining": 7
}
```

## Управление через WebApp

В приложении доступны следующие действия:

### Управление деплоем
- **Запустить деплой** - POST `/api/sites/{id}/deploy`
- **Остановить сайт** - POST `/api/sites/{id}/stop`
- **Принудительный редеплой** (admin) - POST `/api/sites/admin/{id}/force-deploy`

### Управление доменом
- **Привязать домен** - POST `/api/sites/{id}/domain`
  ```json
  { "domain": "example.com", "enable_ssl": true }
  ```

### Оплата хостинга
- **Создать платёж** - POST `/api/payments`
  ```json
  { "site_id": "uuid", "plan": "basic", "months": 1 }
  ```
- **Получить QR код** - GET `/api/payments/{id}/qr`
- **Проверить оплату** - POST `/api/payments/{id}/verify`

## Мониторинг

```sql
-- Активные сайты
SELECT * FROM client_sites WHERE deploy_status = 'active';

-- Истекающий хостинг (в течение 14 дней)
SELECT * FROM expiring_sites;

-- Статистика сайтов
SELECT * FROM sites_stats;

-- Сайты с ошибками
SELECT company_name, last_error, last_error_at
FROM client_sites
WHERE last_error IS NOT NULL
ORDER BY last_error_at DESC;
```

## Синхронизация статусов

Система использует **две отдельные БД**:
- **Autosites DB** (`autosites`) - хранит `client_sites`
- **Deploy Node DB** (`deploy_node`) - хранит `deployments`

Для синхронизации статусов реализовано:

1. **Callback механизм** - deploy-node отправляет webhook при изменении статуса
2. **Периодическая синхронизация** - каждые 5 минут (cron job)
3. **Ручная синхронизация** - кнопка "🔄" в веб-интерфейсе или API endpoint

**См. подробности:** [DB_SYNC.md](./DB_SYNC.md)

## Troubleshooting

### Статусы не обновляются

1. **Используйте кнопку синхронизации** (🔄) в веб-интерфейсе
2. **Проверьте DEPLOY_NODE_URL** в настройках
3. **Проверьте логи синхронизации:**
   ```bash
   docker logs autosites-api | grep "Deploy status sync"
   ```
4. **Ручная синхронизация через API:**
   ```bash
   POST /api/sites/{site_id}/sync-status
   ```

### Callback не работает
1. Проверьте `CALLBACK_URL` в deploy-node
2. Проверьте логи: `docker logs deploy-node`
3. Проверьте доступность API из контейнера
4. Проверьте что endpoint `/api/sites/webhook/deploy-callback` отвечает

### Деплой не запускается
1. Проверьте `DEPLOY_NODE_URL` в Autosites API
2. Проверьте наличие архива в S3 (`archive_s3_key` в `client_sites`)
3. Проверьте статус `generation_status = 'completed'`

### Статус не обновляется в приложении
1. Проверьте webhook endpoint доступен
2. Проверьте формат payload (JSON)
3. Смотрите логи API и deploy-node
4. Проверьте что `request_id` или `client_site_id` передаются в деплой

### Бот не отправляет уведомления
1. Проверьте `BOT_WEBHOOK_URL` в API конфигурации
2. Проверьте что бот запущен: `docker logs bot`
3. Проверьте webhook порт `BOT_WEBHOOK_PORT`

### QR код не отображается
1. Проверьте что библиотека qrcode установлена: `pip install qrcode[pil] pillow`
2. Проверьте логи API при создании платежа
3. QR генерируется как base64 data URL в поле `qr_code_url`

