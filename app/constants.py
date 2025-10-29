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
CB_DONE="more_done"

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
