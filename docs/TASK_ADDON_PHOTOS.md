# Техническое задание: Загрузка фото к доп. услугам (addons)

## 📋 Описание проблемы

### Текущая ситуация

В системе есть два типа дополнительных услуг:

1. **`additional_services[]`** - отдельные услуги (logo_design, seo_promotion и т.д.)
   - Хранятся в таблице `request_additional_services`
   - Передаются в webhook как отдельный массив
   - ✅ Работает корректно

2. **`addons[]`** - доп. опции внутри услуг (например, "Обработка стоп" для услуги "Педикюр")
   - Хранятся в `payload.site.services[].addons[]`
   - Передаются в webhook внутри структуры услуги
   - ❌ **ПРОБЛЕМА**: Фото к addons не загружаются и не передаются в webhook

### Проблема

При попытке загрузить фото к доп. услуге (addon) через drag-and-drop в модалке редактирования заявки:

1. Фото загружается к **основной услуге**, а не к addon
2. В логах API видно: `addon_index=None, addon_name=None`
3. В JSON webhook отсутствует поле `photos[]` в объектах addons

**Пример текущего JSON:**
```json
{
  "services": [
    {
      "name": "Педикюр с покрытием гель-лак",
      "addons": [
        {
          "name": "Обработка стоп",
          "price": "400"
          // ❌ Нет поля "photos"
        }
      ]
    }
  ]
}
```

**Ожидаемый JSON:**
```json
{
  "services": [
    {
      "name": "Педикюр с покрытием гель-лак",
      "addons": [
        {
          "name": "Обработка стоп",
          "price": "400",
          "photos": [
            "https://storage.yandexcloud.net/.../photo.jpg"
          ]
        }
      ]
    }
  ]
}
```

## 🔍 Анализ проблемы

### Что уже реализовано

#### Backend (API)
- ✅ Добавлены параметры `addon_index` и `addon_name` в endpoint `/api/requests/{id}/photos`
- ✅ Реализована логика сохранения фото в `service.addons[addonIndex].photos[]`
- ✅ Добавлено логирование для отладки
- ✅ Фото сохраняются в базу данных при загрузке

#### Frontend (UI)
- ✅ Добавлен интерфейс `AddonItem` с полем `photos?: string[]`
- ✅ Добавлена функция `handleAddonPhotoSelect` для загрузки фото к addon
- ✅ Добавлены обработчики drag-and-drop для addons:
  - `handleAddonDragEnter`
  - `handleAddonDragLeave`
  - `handleAddonDragOver`
  - `handleAddonDrop`
- ✅ Добавлена UI зона drag-and-drop под каждым addon
- ✅ Добавлено отображение фото addons (миниатюры)
- ✅ Добавлена кнопка удаления фото у addons

### Проблема в реализации

**Основная проблема:** Событие `drop` обрабатывается зоной **услуги**, а не зоной **addon**.

**Причина:**
1. Зона услуги находится выше в DOM и перехватывает событие раньше
2. Событие всплывает (event bubbling) от зоны addon к зоне услуги
3. Обработчик услуги срабатывает первым, игнорируя зону addon

**Доказательства из логов:**
```
[UPLOAD] Received photo upload params:
  service_index=2,
  service_name=Наращивание, длинна 2,
  addon_index=None,  ← Проблема: параметры addon не передаются
  addon_name=None,   ← Проблема: параметры addon не передаются
  category=services
```

## 🎯 Задача

Исправить обработку событий drag-and-drop так, чтобы:

1. При перетаскивании фото на зону addon событие обрабатывалось обработчиком addon
2. Параметры `addon_index` и `addon_name` передавались в API
3. Фото сохранялись в `service.addons[addonIndex].photos[]`
4. Фото передавались в webhook вместе с остальными данными

## 🔧 Технические детали

### Структура DOM

```
<div> <!-- Услуга -->
  <div data-service-drop-zone> <!-- Зона услуги -->
    <!-- Фото услуги -->
  </div>

  <div> <!-- Addons -->
    <div> <!-- Addon 1 -->
      <div data-addon-drop-zone> <!-- Зона addon -->
        <!-- Фото addon -->
      </div>
    </div>
  </div>
</div>
```

### Проблема с event bubbling

Когда пользователь перетаскивает фото на зону addon:
1. Событие `drop` возникает на зоне addon
2. Событие всплывает к родительским элементам
3. Зона услуги перехватывает событие раньше
4. Обработчик услуги обрабатывает событие без параметров addon

### Попытки исправления

1. ✅ Добавлен `stopPropagation()` и `stopImmediatePropagation()` в обработчики addon
2. ✅ Добавлена проверка в `handleDrop` услуги - игнорирует drop на зоне addon
3. ✅ Добавлены атрибуты `data-addon-drop-zone` для идентификации
4. ✅ Добавлен `zIndex: 10` для зоны addon
5. ✅ Добавлено логирование для отладки

**Но проблема остается** - событие все еще обрабатывается зоной услуги.

## 💡 Предлагаемые решения

### Вариант 1: Изменить порядок обработчиков (рекомендуется)

Использовать фазу захвата (capture phase) вместо всплытия:

```typescript
// В обработчике услуги
onDropCapture={(e) => {
  const target = e.target as HTMLElement
  if (target.closest('[data-addon-drop-zone]')) {
    return // Игнорируем, если это addon
  }
  handleDrop(e, i)
}}
```

### Вариант 2: Использовать делегирование событий

Обрабатывать все события на уровне контейнера и определять целевую зону:

```typescript
const handleContainerDrop = (e: React.DragEvent, serviceIndex: number) => {
  const target = e.target as HTMLElement
  const addonZone = target.closest('[data-addon-drop-zone]')

  if (addonZone) {
    const addonIndex = parseInt(addonZone.getAttribute('data-addon-index') || '0')
    handleAddonDrop(e, serviceIndex, addonIndex)
  } else {
    handleDrop(e, serviceIndex)
  }
}
```

### Вариант 3: Разделить зоны визуально и структурно

Вынести зону addon из контейнера услуги или использовать `pointer-events: none` на зоне услуги, когда курсор над зоной addon.

## 📝 Требования к реализации

### Обязательные требования

1. ✅ Фото должно загружаться к addon при перетаскивании на зону addon
2. ✅ Параметры `addon_index` и `addon_name` должны передаваться в API
3. ✅ Фото должно сохраняться в `service.addons[].photos[]`
4. ✅ Фото должно отображаться в UI под addon
5. ✅ Фото должно передаваться в webhook при генерации сайта

### Дополнительные требования

1. Визуальная обратная связь при перетаскивании (подсветка зоны addon)
2. Индикатор загрузки во время загрузки фото
3. Возможность удаления фото addon
4. Логирование для отладки

## 🧪 Тестирование

### Сценарий тестирования

1. Открыть модалку редактирования заявки
2. Перейти на вкладку "Товары/Услуги"
3. Найти услугу с доп. услугами (addons)
4. Перетащить фото на зону под доп. услугой (текст "Перетащите фото сюда")
5. Проверить:
   - ✅ Фото загружается
   - ✅ Фото отображается под addon
   - ✅ В логах API: `addon_index=0, addon_name=...`
   - ✅ В JSON webhook: `addons[].photos[]` содержит URL фото

### Ожидаемые логи API

```
[UPLOAD] Received photo upload params:
  service_index=0,
  service_name=Педикюр с покрытием гель-лак,
  addon_index=0,  ← Должно быть число, не None
  addon_name=Обработка стоп,  ← Должно быть название, не None
  category=services

Attached 1 photos to addon 0 (Обработка стоп) in service 0 (Педикюр с покрытием гель-лак)
```

## 📊 Текущий статус

- ✅ Backend API готов (принимает параметры addon)
- ✅ Frontend UI готов (есть зона drag-and-drop)
- ❌ **Проблема**: События не доходят до обработчика addon
- ❌ **Требуется**: Исправить обработку событий drag-and-drop

## 🔗 Связанные файлы

### Backend
- `api/routes/requests.py` - endpoint загрузки фото (строки 359-522)
- `api/routes/requests.py` - функция генерации сайта (строки 220-356)

### Frontend
- `webapp/src/pages/RequestDetailPage.tsx` - модалка редактирования заявки
  - `EditRequestForm` компонент (строки 2103-3020)
  - Обработчики drag-and-drop для addons (строки 2312-2337)
  - UI зона addon (строки 2815-2830)

## 📌 Приоритет

**Высокий** - Функциональность частично реализована, но не работает из-за проблемы с обработкой событий.

## 🎯 Критерии приемки

1. При перетаскивании фото на зону addon фото загружается к addon, а не к услуге
2. В логах API появляется сообщение: `Attached X photos to addon Y (...) in service Z (...)`
3. В JSON webhook присутствует поле `photos[]` в объектах addons
4. Фото отображается в UI под соответствующим addon
5. Фото сохраняется в базу данных и передается в webhook при генерации

