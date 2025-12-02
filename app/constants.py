from aiogram import types

# ==================== КОМАНДЫ ====================

GUEST_CMDS = [
    types.BotCommand("start", "Начать работу"),
    types.BotCommand("register", "Регистрация"),
    types.BotCommand("admin_login", "Вход для администратора"),
]

DEBUG_CMDS = [
    types.BotCommand("ping", "Проверка связи"),
    types.BotCommand("myid", "Мой ID"),
]

MANAGER_CMDS = [
    types.BotCommand("start", "Главное меню"),
    types.BotCommand("new_request", "Создать заявку"),
    types.BotCommand("my_requests", "Мои заявки"),
    types.BotCommand("reset", "Отменить текущую форму"),
    types.BotCommand("admin_login", "Вход для администратора"),
]

ADMIN_CMDS = [
    types.BotCommand("start", "Главное меню"),
    types.BotCommand("admin_panel", "Панель управления"),
    types.BotCommand("admin_users", "Список пользователей"),
    types.BotCommand("admin_requests", "Список заявок"),
    types.BotCommand("export_request", "Экспорт заявки"),
    types.BotCommand("export_all", "Экспорт всех заявок"),
    types.BotCommand("logout", "Выход из админки"),
]

# ==================== КНОПКИ ====================

# Гость
BTN_REG = "📝 Регистрация"
BTN_ADMIN_LOGIN = "🔐 Вход для администратора"

# Менеджер
BTN_NEW = "➕ Новая заявка"
BTN_MY = "📋 Мои заявки"
BTN_RESET = "❌ Отмена"

# Админ
BTN_PANEL = "📊 Панель управления"
BTN_USERS = "👥 Пользователи"
BTN_REQS = "📦 Все заявки"
BTN_LOGOUT = "🚪 Выход"

# Форма
BTN_BACK = "⬅️ Назад"
BTN_EXIT = "🚪 Отменить заполнение"
BTN_SKIP = "⏭ Пропустить"

# ==================== CALLBACK ПРЕФИКСЫ ====================

CB_OPEN = "open_"
CB_EDIT = "edit_"
CB_DELETE = "del_"
CB_EDIT_FIELD = "ef_"
CB_BACK_TO_LIST = "back_list"
CB_LIST_PAGE = "plist_"
CB_EXPORT_ONE = "exp_"
CB_GEN = "gen_"

# Доп. детали
CB_MORE = "more_"
CB_MORE_PORTF = "more_portf_"
CB_MORE_TESTI = "more_testi_"
CB_MORE_FAQ = "more_faq_"
CB_MORE_SEO = "more_seo_"
CB_MORE_HERO = "more_hero_"
CB_DONE = "more_done_"

# Фото категории
CB_PHOTO_CAT = "photo_cat_"
CB_PHOTO_DONE = "photo_done_"
CB_PHOTO_CATS = "photo_cats_"

# ==================== РЕДАКТИРУЕМЫЕ ПОЛЯ ====================

EDITABLE_FIELDS = {
    "company": "Название компании",
    "business_type": "Сфера деятельности",
    "color_palette": "Цветовая палитра",
    "phone": "Телефон",
    "email": "Email",
    "address": "Адрес",
    "summary": "Описание компании",
    "work_hours": "Режим работы",
    "structure": "Структура сайта",
    "services": "Услуги",
    "portfolio": "Портфолио",
    "testimonials": "Отзывы клиентов",
    "faq": "Частые вопросы",
    "seo_description": "SEO-описание",
    "hero_title": "Заголовок главной",
    "hero_subtitle": "Подзаголовок главной",
}

# Подсказки для редактирования полей
EDIT_HINTS = {
    "company": "Введите <b>название компании</b>:",
    "business_type": "Опишите <b>сферу деятельности</b> (например: строительная компания, IT-услуги):",
    "color_palette": "Укажите <b>предпочтительные цвета</b> (например: синий, белый) или напишите «на усмотрение дизайнера»:",
    "phone": "Введите <b>контактный телефон</b> для сайта:",
    "email": "Введите <b>email</b> для сайта:",
    "address": "Введите <b>адрес</b> офиса/производства:",
    "summary": "Опишите <b>компанию в 2–3 предложениях</b> для посетителей сайта:",
    "work_hours": "Введите <b>график работы</b> (например: Пн–Пт 9:00–18:00):",
    "structure": "Перечислите <b>разделы сайта</b> через запятую:",
    "services": "Введите <b>услуги</b> — каждую с новой строки:\n<i>Название — описание — цена</i>",
    "portfolio": "Введите <b>проекты</b> — каждый с новой строки:\n<i>Название — клиент — год — описание</i>",
    "testimonials": "Введите <b>отзывы</b> — каждый с новой строки:\n<i>Имя — Компания — текст отзыва — оценка</i>",
    "faq": "Введите <b>вопросы и ответы</b> — каждый с новой строки:\n<i>Вопрос — Ответ</i>",
    "seo_description": "Введите <b>описание для поисковиков</b> (1–2 предложения):",
    "hero_title": "Введите <b>главный заголовок</b> сайта:",
    "hero_subtitle": "Введите <b>подзаголовок</b> под главным заголовком:",
}

# ==================== СТАТУСЫ ЗАЯВОК ====================

STATUS_DRAFT = "draft"
STATUS_COLLECTING_INFO = "collecting_info"
STATUS_COLLECTING_PHOTOS = "collecting_photos"
STATUS_READY_TO_GENERATE = "ready_to_generate"
STATUS_QUEUED = "queued"
STATUS_GENERATING = "generating"
STATUS_GENERATED_OK = "generated_ok"
STATUS_GENERATED_ERROR = "generated_error"

# Читаемые статусы
STATUS_LABELS = {
    STATUS_DRAFT: "Черновик",
    STATUS_COLLECTING_INFO: "Сбор информации",
    STATUS_COLLECTING_PHOTOS: "Загрузка фото",
    STATUS_READY_TO_GENERATE: "Готова к генерации",
    STATUS_QUEUED: "В очереди",
    STATUS_GENERATING: "Генерируется",
    STATUS_GENERATED_OK: "Сайт готов ✅",
    STATUS_GENERATED_ERROR: "Ошибка генерации ❌",
}

# ==================== ДЕФОЛТЫ ====================

DEFAULT_STRUCTURE = ["Hero", "О компании", "Услуги", "Портфолио", "Отзывы", "Контакты", "Карта"]

# ==================== КАТЕГОРИИ ФОТО ====================

PHOTO_CATEGORIES = {
    "hero": "🏠 Главный баннер",
    "services": "🛠 Услуги",
    "portfolio": "📁 Портфолио",
    "team": "👥 Команда",
    "office": "🏢 Офис / Производство",
    "other": "📷 Прочее",
}

# ==================== ВАЛИДАЦИЯ ====================

MIN_COMPANY_NAME_LEN = 2
MAX_COMPANY_NAME_LEN = 100
MIN_DESCRIPTION_LEN = 10
MAX_DESCRIPTION_LEN = 1000

# ==================== СООБЩЕНИЯ ====================

MSG_GENERATION_STARTED = (
    "⏳ <b>Заявка принята в обработку</b>\n\n"
    "Генерация сайта запущена. Это может занять несколько минут.\n"
    "Результат будет отправлен в этот чат."
)

MSG_GENERATION_ERROR = "❌ <b>Ошибка генерации</b>\n\nПожалуйста, попробуйте позже или обратитесь в поддержку."

MSG_NO_WEBHOOK = "⚠️ Сервис генерации временно недоступен. Пожалуйста, попробуйте позже."

MSG_WELCOME_GUEST = (
    "👋 <b>Добро пожаловать!</b>\n\n"
    "Этот бот поможет создать техническое задание для разработки сайта.\n\n"
    "Для начала работы пройдите регистрацию."
)

MSG_WELCOME_MANAGER = (
    "👋 <b>Добро пожаловать!</b>\n\n"
    "Вы можете создать новую заявку или просмотреть существующие."
)

MSG_WELCOME_ADMIN = (
    "👋 <b>Панель администратора</b>\n\n"
    "Доступно управление пользователями и заявками."
)

MSG_REG_COMPLETE = (
    "✅ <b>Регистрация завершена!</b>\n\n"
    "Теперь вы можете создавать заявки на разработку сайтов."
)

MSG_REQUEST_CREATED = (
    "✅ <b>Заявка создана</b>\n\n"
    "Заявка: <code>{req_id}</code>\n"
    "Компания: <b>{company}</b>\n\n"
    "Далее загрузите фотографии для сайта."
)

MSG_PHOTOS_INSTRUCTION = (
    "📷 <b>Загрузка изображений</b>\n\n"
    "Выберите категорию и отправьте фотографии.\n"
    "Рекомендуемые форматы: JPG, PNG\n"
    "Максимальный размер: 20 МБ"
)
