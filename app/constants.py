from aiogram import types

# Команды (по ролям)
GUEST_CMDS = [
    types.BotCommand("start","Старт"),
    types.BotCommand("register","Регистрация"),
    types.BotCommand("admin_login","Войти в админку"),
]
DEBUG_CMDS = [
    types.BotCommand("test_webhook", "DEBUG: тест n8n вебхука"),
    types.BotCommand("testcb", "DEBUG: тест callback-кнопки"),
    types.BotCommand("ping", "DEBUG: пинг"),
    types.BotCommand("myid", "DEBUG: мой chat_id"),
]
MANAGER_CMDS = [
    types.BotCommand("start","Старт"),
    types.BotCommand("new_request","Новая заявка"),
    types.BotCommand("my_requests","Мои заявки"),
    types.BotCommand("reset","Сбросить анкету"),
    types.BotCommand("admin_login","Войти в админку"),
]
ADMIN_CMDS = [
    types.BotCommand("start","Старт"),
    types.BotCommand("admin_panel","Админ: панель"),
    types.BotCommand("admin_users","Админ: пользователи"),
    types.BotCommand("admin_requests","Админ: заявки"),
    types.BotCommand("export_request","Админ: экспорт заявки"),
    types.BotCommand("export_all","Админ: экспорт всех"),
    types.BotCommand("logout","Админ: выйти"),
]

# Тексты кнопок
BTN_REG="📝 Регистрация"; BTN_ADMIN_LOGIN="🔐 Войти в админку"
BTN_NEW="➕ Создать заявку"; BTN_MY="📋 Мои заявки"; BTN_RESET="❌ Сброс формы"
BTN_PANEL="📊 Панель"; BTN_USERS="👥 Пользователи"; BTN_REQS="📦 Заявки"; BTN_LOGOUT="🚪 Выйти из админки"
BTN_BACK="⬅️ Назад"; BTN_EXIT="🚪 Выйти из формы"

# Префиксы callback'ов
CB_OPEN="open_"; CB_EDIT="edit_"; CB_DELETE="del_"
CB_EDIT_FIELD="ef_"; CB_BACK_TO_LIST="back_list"; CB_LIST_PAGE="plist_"
CB_EXPORT_ONE="exp_"; CB_GEN="gen_"

# Доп. детали
CB_MORE="more_"
CB_MORE_PORTF="more_portf_"
CB_MORE_TESTI="more_testi_"
CB_MORE_FAQ="more_faq_"
CB_MORE_SEO="more_seo_"
CB_MORE_HERO="more_hero_"
CB_DONE="more_done_"  # теперь с req_id

# Фото категории
CB_PHOTO_CAT="photo_cat_"
CB_PHOTO_DONE="photo_done_"
CB_PHOTO_CATS="photo_cats_"

# Редактируемые поля
EDITABLE_FIELDS = {
    "company":"Название компании",
    "business_type":"Тип бизнеса",
    "color_palette":"Цветовая гамма",
    "site_contacts":"Контакты/адреса для сайта",
    "summary":"Описание компании (2–3 предложения)",
    "work_hours":"Рабочие часы",
    "structure":"Структура (через запятую)",
    "images":"Изображения (описание)",
    "services":"Услуги (Название — кратко — цена — буллеты через «;» — опц.CTA)",
    "portfolio":"Портфолио (Проект — клиент — год — кратко — теги «;» — опц.ссылка)",
    "testimonials":"Отзывы (Имя — Компания/Роль — цитата — опц.рейтинг 1-5)",
    "faq":"FAQ (Вопрос — Ответ)",
    "seo_description":"Поисковое описание (1–2 предложения)",
    "hero_title":"Заголовок на первом экране",
    "hero_subtitle":"Короткое описание под заголовком"
}

# Подсказки для редактирования полей
EDIT_HINTS = {
    "company": "Введите <b>название компании</b>:",
    "business_type": "Введите <b>чем вы занимаетесь</b> (например: производственная компания):",
    "color_palette": "Введите <b>пожелания по цветам</b> (например: синий, белый, серый):",
    "site_contacts": "Укажите <b>контакты/адреса для сайта</b> (телефон, email, адрес):",
    "summary": "Опишите <b>компанию в 2–3 предложениях</b>:",
    "work_hours": "Введите <b>режим работы</b> (например: Пн-Пт 9:00-18:00):",
    "structure": "Секции сайта через запятую (например: Hero, О нас, Услуги, Портфолио, Отзывы, Контакты):",
    "services": "Введите <b>услуги</b> — по одной в строке: <i>Название — кратко — от цена</i>.",
    "portfolio": "Портфолио — по одной строке: <i>Проект — клиент — год — кратко — теги «;» — опц.ссылка</i>.",
    "testimonials": "Отзывы — по одной строке: <i>Имя — Компания/Роль — цитата — опц.оценка 1-5</i>.",
    "faq": "FAQ — по одной строке: <i>Вопрос — Ответ</i>.",
    "seo_description": "Описание для поисковиков (1–2 предложения).",
    "hero_title": "Заголовок на первом экране:",
    "hero_subtitle": "Короткое описание под заголовком:",
}

# Статусы заявок
STATUS_DRAFT = "draft"
STATUS_COLLECTING_INFO = "collecting_info"
STATUS_COLLECTING_PHOTOS = "collecting_photos"
STATUS_READY_TO_GENERATE = "ready_to_generate"
STATUS_QUEUED = "queued"
STATUS_GENERATED_OK = "generated_ok"
STATUS_GENERATED_ERROR = "generated_error"

# Дефолтная структура сайта
DEFAULT_STRUCTURE = ["Hero", "О нас", "Услуги", "Портфолио", "Отзывы", "Контакты", "Карта"]

# Категории фото
PHOTO_CATEGORIES = {
    "hero": "🏠 Главное фото (Hero)",
    "services": "🛠 Фото услуг",
    "portfolio": "📁 Фото портфолио",
    "team": "👥 Фото команды",
    "office": "🏢 Фото офиса/производства",
    "other": "📷 Другие фото",
}

# Валидация
MIN_COMPANY_NAME_LEN = 2
MAX_COMPANY_NAME_LEN = 100
MIN_DESCRIPTION_LEN = 10
MAX_DESCRIPTION_LEN = 500
