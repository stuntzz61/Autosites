# 🚀 Принцип работы авто-деплоя

## Обзор

Система поддерживает **два способа деплоя**:

1. **Прямой деплой** - загрузка архива напрямую в deploy-node (как сейчас через curl)
2. **Автоматический деплой через S3** - n8n генерирует архив → загружает в S3 → Autosites API скачивает и деплоит

---

## 📋 Схема работы

### Вариант 1: Прямой деплой (текущий)

```
Пользователь → curl → deploy-node → Agent → Сайт
```

**Как работает:**
```bash
curl -X POST https://autosites.ru/deploy-api/api/deploy \
  -F "archive=@Sborka.zip" \
  -F "auto_select=true" \
  -F "enable_ssl=true" \
  -F "wait=true" \
  -F "wait_timeout=600"
```

1. Пользователь загружает архив напрямую в deploy-node
2. Deploy-node сохраняет архив локально в `UPLOAD_DIR`
3. Deploy-node отправляет архив на Agent
4. Agent деплоит сайт
5. Deploy-node отправляет callback в Autosites API
6. Autosites API создаёт/обновляет `client_site`

**Преимущества:**
- ✅ Простота - один запрос
- ✅ Быстро - нет промежуточных шагов
- ✅ Подходит для ручного деплоя

**Недостатки:**
- ❌ Нет связи с заявкой (request_id)
- ❌ Нет истории в Autosites
- ❌ Нет управления через веб-интерфейс

---

### Вариант 2: Автоматический деплой через S3 (новый)

```
n8n → Генерирует архив → Загружает в S3 →
  Autosites API → Скачивает из S3 → deploy-node → Agent → Сайт
```

**Как работает:**

#### Шаг 1: n8n генерирует и загружает в S3

n8n workflow:
1. Генерирует сайт из заявки
2. Создаёт ZIP архив
3. Загружает в S3 (Yandex Cloud Object Storage)
4. Получает `archive_s3_key` (например: `requests/{request_id}/archive.zip`)
5. Вызывает Autosites API callback:

```json
POST /api/sites/webhook/generation-callback
{
  "request_id": "uuid",
  "status": "completed",
  "archive_s3_key": "requests/uuid/archive.zip",
  "archive_size_bytes": 1234567
}
```

#### Шаг 2: Autosites API создаёт client_site

Callback handler:
- Создаёт запись `client_sites` с `archive_s3_key`
- Устанавливает `generation_status = 'completed'`
- Сохраняет `archive_s3_key` для последующего деплоя

#### Шаг 3: Деплой через веб-интерфейс или API

**Через веб-интерфейс:**
- Менеджер/админ открывает заявку
- Нажимает кнопку "Запустить деплой"
- Autosites API:
  1. Скачивает архив из S3 по `archive_s3_key`
  2. Передаёт в deploy-node через multipart form
  3. Deploy-node деплоит как обычно

**Через API:**
```bash
POST /api/sites/{site_id}/deploy
Authorization: Bearer {token}
```

#### Шаг 4: Deploy-node отправляет callback

После деплоя deploy-node отправляет:
```json
POST /api/sites/webhook/deploy-callback
{
  "deploy_id": "uuid",
  "status": "completed",
  "request_id": "uuid",
  "client_site_id": "uuid",
  "preview_slug": "abc123",
  "preview_url": "https://abc123.autosites.ru",
  "server_id": "agent1",
  "port": 3001
}
```

#### Шаг 5: Autosites API обновляет статус

Callback handler:
- Если `client_site` не существует → **создаёт автоматически**
- Обновляет `deploy_status = 'active'`
- Сохраняет `deploy_id`, `preview_url`, `server_id`
- Обновляет статус заявки на `success`

**Преимущества:**
- ✅ Полная интеграция с заявками
- ✅ История деплоев
- ✅ Управление через веб-интерфейс
- ✅ Автоматическое создание `client_site`
- ✅ Связь с хостингом, правками, платежами

---

## 🔄 Полный цикл (n8n → S3 → Deploy)

```
┌─────────┐
│   n8n   │ Генерирует сайт
└────┬────┘
     │
     │ 1. Создаёт архив
     │ 2. Загружает в S3
     │ 3. Получает archive_s3_key
     │
     ▼
┌─────────────┐
│     S3      │ Хранит архив
│ (Yandex)    │ archive_s3_key: requests/{id}/archive.zip
└──────┬──────┘
       │
       │ 4. Callback: generation-callback
       │
       ▼
┌─────────────────┐
│ Autosites API   │ Создаёт client_site
│                 │ archive_s3_key сохранён
└────────┬────────┘
         │
         │ 5. Пользователь нажимает "Деплой"
         │
         │ 6. Скачивает из S3
         │ 7. Передаёт в deploy-node
         │
         ▼
┌─────────────────┐
│  deploy-node    │ Деплоит
└────────┬────────┘
         │
         │ 8. Callback: deploy-callback
         │
         ▼
┌─────────────────┐
│ Autosites API   │ Обновляет статус
│                 │ deploy_status = 'active'
└─────────────────┘
```

---

## 📝 Конфигурация

### Autosites API (.env)

```bash
# S3 Configuration
S3_ENDPOINT=https://storage.yandexcloud.net
S3_BUCKET=autosites-archives
S3_ACCESS_KEY=your_key
S3_SECRET_KEY=your_secret
S3_REGION=ru-central1
S3_PUBLIC_URL=https://storage.yandexcloud.net/autosites-archives

# Deploy Node
DEPLOY_NODE_URL=https://autosites.ru/deploy-api
DEPLOY_CALLBACK_SECRET=your_secret
```

### Deploy Node (.env)

```bash
# Callback URL для уведомлений Autosites API
CALLBACK_URL=https://autosites.ru/api
CALLBACK_SECRET=your_secret

# Upload directory (для прямых деплоев)
UPLOAD_DIR=./uploads
```

---

## 🔧 API Endpoints

### 1. Generation Callback (от n8n)

```bash
POST /api/sites/webhook/generation-callback
Content-Type: application/json

{
  "request_id": "uuid",
  "status": "completed",
  "archive_s3_key": "requests/uuid/archive.zip",
  "archive_size_bytes": 1234567
}
```

**Что делает:**
- Создаёт `client_site` если не существует
- Сохраняет `archive_s3_key`
- Устанавливает `generation_status = 'completed'`

### 2. Deploy Site (из веб-интерфейса)

```bash
POST /api/sites/{site_id}/deploy
Authorization: Bearer {token}
```

**Что делает:**
1. Проверяет наличие `archive_s3_key`
2. Скачивает архив из S3
3. Передаёт в deploy-node
4. Обновляет `deploy_status = 'deploying'`

### 3. Deploy Callback (от deploy-node)

```bash
POST /api/sites/webhook/deploy-callback
Content-Type: application/json

{
  "deploy_id": "uuid",
  "status": "completed",
  "request_id": "uuid",
  "client_site_id": "uuid",
  "preview_slug": "abc123",
  "preview_url": "https://abc123.autosites.ru"
}
```

**Что делает:**
- Если `client_site` не существует → **создаёт автоматически**
- Обновляет статус деплоя
- Сохраняет `deploy_id`, `preview_url`, `server_id`

---

## 💡 Важные моменты

### 1. Автоматическое создание client_site

Если deploy-node отправляет callback, но `client_site` не существует:
- Autosites API **автоматически создаёт** `client_site`
- Использует данные из callback и request (если `request_id` указан)
- Если `request_id` нет → создаёт "orphan" site (можно связать позже)

### 2. S3 vs Прямая загрузка

**S3 используется когда:**
- n8n генерирует сайт автоматически
- Нужна история и связь с заявкой
- Деплой через веб-интерфейс

**Прямая загрузка используется когда:**
- Ручной деплой через curl
- Тестирование
- Быстрый деплой без заявки

### 3. Синхронизация статусов

- Deploy-node отправляет callback при каждом изменении статуса
- Autosites API синхронизирует статусы каждые 5 минут (cron)
- Можно синхронизировать вручную через кнопку "🔄" в UI

---

## 🐛 Troubleshooting

### Архив не найден в S3

**Проблема:** `archive_s3_key` есть, но файл не скачивается

**Решение:**
1. Проверьте S3 credentials в `.env`
2. Проверьте права доступа к bucket
3. Проверьте, что файл действительно загружен:
   ```bash
   # Через AWS CLI или Yandex Cloud Console
   aws s3 ls s3://autosites-archives/requests/{request_id}/
   ```

### Deploy не запускается

**Проблема:** Кнопка "Деплой" не работает

**Решение:**
1. Проверьте, что `archive_s3_key` сохранён в `client_sites`
2. Проверьте логи Autosites API:
   ```bash
   docker logs autosites-api | grep "trigger_deploy"
   ```
3. Проверьте доступность deploy-node:
   ```bash
   curl https://autosites.ru/deploy-api/api/health
   ```

### Callback не приходит

**Проблема:** Deploy завершился, но статус не обновился

**Решение:**
1. Проверьте `CALLBACK_URL` в deploy-node `.env`
2. Проверьте доступность Autosites API из deploy-node
3. Используйте кнопку синхронизации "🔄" в UI
4. Проверьте логи deploy-node:
   ```bash
   docker logs deploy-node | grep "callback"
   ```

---

## 📚 См. также

- [DB_SYNC.md](./DB_SYNC.md) - Синхронизация между БД
- [DEPLOY_INTEGRATION.md](./DEPLOY_INTEGRATION.md) - Интеграция с deploy-node
- [REVISIONS_INTEGRATION.md](./REVISIONS_INTEGRATION.md) - Система правок

