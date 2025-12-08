# 🔄 Синхронизация баз данных

## Проблема

В системе используются **две отдельные базы данных**:

1. **Autosites DB** (`autosites`) - хранит:
   - `client_sites` - сайты клиентов
   - `requests` - заявки
   - `users` - пользователи
   - `revisions` - правки сайтов

2. **Deploy Node DB** (`deploy_node`) - хранит:
   - `deployments` - деплои
   - `sites` - развернутые сайты
   - `port_allocations` - распределение портов

Эти БД **не связаны напрямую**, поэтому нужна синхронизация статусов.

## Решение

Реализована **многоуровневая синхронизация**:

### 1. Callback механизм (реал-тайм)

Deploy-node отправляет webhook при изменении статуса:

```
Deploy-Node → POST /api/sites/webhook/deploy-callback → Autosites API
```

**Когда срабатывает:**
- При изменении статуса деплоя (pending → deploying → completed/failed)
- При завершении деплоя
- При ошибке деплоя

**Payload:**
```json
{
  "deploy_id": "uuid",
  "status": "completed",
  "client_site_id": "uuid",
  "preview_slug": "abc123",
  "preview_url": "https://abc123.autosites.ru",
  "server_id": "agent1",
  "port": 3001
}
```

### 2. Периодическая синхронизация (каждые 5 минут)

Cron job в Autosites API опрашивает deploy-node:

```python
# Автоматически каждые 5 минут
async def sync_deploy_statuses():
    # Получает все client_sites с deploy_id
    # Запрашивает статус из deploy-node
    # Обновляет статусы в Autosites DB
```

**Что синхронизируется:**
- `deploy_status` (pending, deploying, active, failed, stopped)
- `preview_slug` и `preview_url`
- `server_id`, `server_name`, `server_host`
- `container_port`
- `last_error` (если failed)

### 3. Ручная синхронизация

**API Endpoint:**
```bash
POST /api/sites/{site_id}/sync-status
```

**В веб-интерфейсе:**
- Кнопка "🔄" рядом со статусом деплоя
- Автоматическая синхронизация при загрузке страницы (если статус не финальный)

**Admin endpoint:**
```bash
POST /api/sites/admin/sync-all-statuses
```
Синхронизирует все сайты в фоне.

## Маппинг статусов

| Deploy-Node Status | Autosites Status |
|-------------------|------------------|
| `pending` | `pending` |
| `uploading` | `deploying` |
| `building` | `deploying` |
| `deploying` | `deploying` |
| `completed` | `active` |
| `failed` | `failed` |
| `rollback` | `failed` |

## Конфигурация

### Autosites API (.env)

```bash
# URL deploy-node для синхронизации
DEPLOY_NODE_URL=http://deploy-node:8080

# Callback secret (опционально)
DEPLOY_CALLBACK_SECRET=your_secret
```

### Deploy Node (.env)

```bash
# URL для callback уведомлений
CALLBACK_URL=http://api:8000/api/sites/webhook/deploy-callback

# Секрет для аутентификации (опционально)
CALLBACK_SECRET=your_secret
```

## Логи синхронизации

Синхронизация логируется в Autosites API:

```
[INFO] Syncing deployment statuses from deploy-node...
[INFO] Status changed for site {id} (deploy {deploy_id}): pending -> active
[INFO] Deploy status sync completed: 5 synced, 0 errors, 10 total
```

## Troubleshooting

### Статусы не обновляются

1. **Проверьте DEPLOY_NODE_URL:**
   ```bash
   curl http://deploy-node:8080/api/health
   ```

2. **Проверьте логи cron jobs:**
   ```bash
   docker logs autosites-api | grep "Deploy status sync"
   ```

3. **Ручная синхронизация:**
   - В веб-интерфейсе нажмите кнопку "🔄" рядом со статусом
   - Или через API: `POST /api/sites/{site_id}/sync-status`

4. **Проверьте deploy_id:**
   ```sql
   SELECT id, deploy_id, deploy_status FROM client_sites WHERE deploy_id IS NOT NULL;
   ```

### Callback не работает

1. **Проверьте CALLBACK_URL в deploy-node:**
   ```bash
   # В deploy-node .env
   CALLBACK_URL=http://api:8000/api/sites/webhook/deploy-callback
   ```

2. **Проверьте доступность API из deploy-node:**
   ```bash
   # Из контейнера deploy-node
   curl http://api:8000/api/health
   ```

3. **Проверьте логи deploy-node:**
   ```bash
   docker logs deploy-node | grep "callback"
   ```

### Статус "pending" не меняется

1. **Проверьте, что деплой действительно запущен:**
   ```bash
   curl http://deploy-node:8080/api/deploy/{deploy_id}
   ```

2. **Проверьте, что callback работает:**
   - Смотрите логи deploy-node при изменении статуса

3. **Используйте ручную синхронизацию:**
   - Кнопка "🔄" в веб-интерфейсе
   - Или подождите 5 минут (автоматическая синхронизация)

## Архитектура

```
┌─────────────────┐                    ┌─────────────────┐
│  Autosites DB   │                    │ Deploy Node DB  │
│   (autosites)   │                    │  (deploy_node)  │
│                 │                    │                 │
│ client_sites    │                    │ deployments     │
│   deploy_id ─────┼────────────────────┼─> id            │
│   deploy_status │                    │   status        │
│   preview_slug  │                    │   preview_slug  │
└─────────────────┘                    └─────────────────┘
         │                                       │
         │                                       │
         │ 1. Callback (webhook)                │
         │<──────────────────────────────────────│
         │                                       │
         │ 2. Periodic sync (every 5 min)       │
         │──────────────────────────────────────>│
         │                                       │
         │ 3. Manual sync (on demand)           │
         │──────────────────────────────────────>│
```

## Рекомендации

1. **Всегда используйте deploy_id** для связи между БД
2. **Проверяйте статусы** после деплоя через кнопку синхронизации
3. **Мониторьте логи** синхронизации для выявления проблем
4. **Настройте алерты** если синхронизация не работает более 10 минут

## API Endpoints

### Синхронизация статуса сайта

```bash
POST /api/sites/{site_id}/sync-status
Authorization: Bearer {token}
```

**Response:**
```json
{
  "success": true,
  "message": "Status synced",
  "status": "active",
  "deployment": {
    "id": "uuid",
    "status": "completed",
    "preview_slug": "abc123",
    "preview_url": "https://abc123.autosites.ru"
  }
}
```

### Синхронизация всех сайтов (admin)

```bash
POST /api/sites/admin/sync-all-statuses
Authorization: Bearer {admin_token}
```

**Response:**
```json
{
  "success": true,
  "message": "Status sync started in background"
}
```

