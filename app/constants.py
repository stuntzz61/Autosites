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
    types.BotCommand("archive", "Архив заявок"),
    types.BotCommand("reset", "Отменить текущую форму"),
]

ADMIN_CMDS = [
    types.BotCommand("start", "Главное меню"),
    types.BotCommand("admin_panel", "Панель управления"),
    types.BotCommand("stats", "Статистика"),
    types.BotCommand("managers", "Управление менеджерами"),
    types.BotCommand("all_requests", "Все заявки"),
    types.BotCommand("export_all", "Экспорт всех заявок"),
    types.BotCommand("broadcast", "Рассылка"),
    types.BotCommand("logout", "Выход из админки"),
]

# ==================== КНОПКИ ====================

# Гость
BTN_REG = "📝 Регистрация"
BTN_ADMIN_LOGIN = "🔐 Вход для администратора"

# Менеджер
BTN_NEW = "➕ Новая заявка"
BTN_MY = "📋 Мои заявки"
BTN_ARCHIVE = "🗄 Архив"
BTN_RESET = "❌ Отмена"

# Админ
BTN_PANEL = "📊 Панель управления"
BTN_STATS = "📈 Статистика"
BTN_MANAGERS = "👥 Менеджеры"
BTN_PENDING = "⏳ Ожидают одобрения"
BTN_USERS = "👤 Пользователи"
BTN_REQS = "📦 Все заявки"
BTN_BROADCAST = "📢 Рассылка"
BTN_SEARCH = "🔍 Поиск"
BTN_MASS_OPS = "⚡ Массовые операции"
BTN_EXPORT = "📊 Экспорт"
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

# Статусы
CB_SET_STATUS = "set_status_"
CB_ARCHIVE_REQ = "archive_"
CB_CLOSE_REQ = "close_"

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

# Админ
CB_ADMIN_MANAGER = "adm_mgr_"
CB_ADMIN_BLOCK = "adm_block_"
CB_ADMIN_UNBLOCK = "adm_unblock_"
CB_ADMIN_STATS = "adm_stats_"
CB_ADMIN_REQUESTS = "adm_reqs_"
CB_ADMIN_DELETE_USER = "adm_del_user_"
CB_ADMIN_EDIT_USER = "adm_edit_user_"

# Одобрение регистраций
CB_APPROVE_USER = "approve_"
CB_REJECT_USER = "reject_"
CB_PENDING_LIST = "pending_list"

# Рассылка
CB_BROADCAST = "broadcast"
CB_BROADCAST_ALL = "bc_all"
CB_BROADCAST_SELECT = "bc_select"
CB_BC_MANAGER = "bc_mgr_"
CB_BC_CONFIRM = "bc_confirm"
CB_BC_CANCEL = "bc_cancel"
CB_BC_ADD_PHOTO = "bc_add_photo"
CB_BC_SKIP_PHOTO = "bc_skip_photo"
CB_BC_DONE = "bc_done"

# Поиск
CB_SEARCH = "search"
CB_SEARCH_RESULT = "sr_"

# Массовые операции
CB_MASS_OPS = "mass_ops"
CB_MASS_SELECT = "mass_sel_"
CB_MASS_ARCHIVE = "mass_archive"
CB_MASS_DELETE = "mass_delete"
CB_MASS_CONFIRM = "mass_confirm"
CB_MASS_CANCEL = "mass_cancel"

# Экспорт
CB_EXPORT_EXCEL = "export_excel"
CB_EXPORT_PDF = "export_pdf"

# Смена статуса
CB_CHANGE_STATUS = "chg_status_"
CB_SET_STATUS = "set_status_"

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
STATUS_DELIVERED = "delivered"      # Сайт доставлен клиенту
STATUS_CLOSED = "closed"            # Заявка закрыта
STATUS_ARCHIVED = "archived"        # В архиве
STATUS_CANCELLED = "cancelled"      # Отменена

# Читаемые статусы с эмодзи
STATUS_LABELS = {
    STATUS_DRAFT: "📝 Черновик",
    STATUS_COLLECTING_INFO: "📋 Сбор информации",
    STATUS_COLLECTING_PHOTOS: "📷 Загрузка фото",
    STATUS_READY_TO_GENERATE: "✅ Готова к генерации",
    STATUS_QUEUED: "⏳ В очереди",
    STATUS_GENERATING: "⚙️ Генерируется...",
    STATUS_GENERATED_OK: "🎉 Сайт готов",
    STATUS_GENERATED_ERROR: "❌ Ошибка генерации",
    STATUS_DELIVERED: "📬 Доставлено",
    STATUS_CLOSED: "✔️ Закрыта",
    STATUS_ARCHIVED: "🗄 В архиве",
    STATUS_CANCELLED: "🚫 Отменена",
}

# Статусы для фильтрации
ACTIVE_STATUSES = [STATUS_DRAFT, STATUS_COLLECTING_INFO, STATUS_COLLECTING_PHOTOS,
                   STATUS_READY_TO_GENERATE, STATUS_QUEUED, STATUS_GENERATING]
COMPLETED_STATUSES = [STATUS_GENERATED_OK, STATUS_DELIVERED, STATUS_CLOSED]
TERMINAL_STATUSES = [STATUS_GENERATED_ERROR, STATUS_ARCHIVED, STATUS_CANCELLED]

# ==================== ЭМОДЗИ ДЛЯ КОМПАНИЙ ====================

# По первой букве названия
COMPANY_EMOJIS_BY_LETTER = {
    'А': '🏢', 'Б': '🏗', 'В': '🏭', 'Г': '🏬', 'Д': '🏪',
    'Е': '🏨', 'Ж': '🏦', 'З': '🏥', 'И': '🏫', 'К': '🏛',
    'Л': '🏟', 'М': '🎪', 'Н': '🏰', 'О': '🗼', 'П': '🏠',
    'Р': '🏡', 'С': '🏘', 'Т': '⛪', 'У': '🕌', 'Ф': '🕍',
    'Х': '⛩', 'Ц': '🗽', 'Ч': '🗿', 'Ш': '🎡', 'Э': '💎',
    'Ю': '🌟', 'Я': '⭐',
}

# По типу бизнеса (ключевые слова)
BUSINESS_TYPE_EMOJIS = {
    'строител': '🏗',
    'ремонт': '🔧',
    'it': '💻',
    'программ': '💻',
    'разработ': '👨‍💻',
    'дизайн': '🎨',
    'маркетинг': '📢',
    'реклам': '📣',
    'юрид': '⚖️',
    'бухгалт': '📊',
    'медиц': '🏥',
    'красот': '💅',
    'ресторан': '🍽',
    'кафе': '☕',
    'доставк': '🚚',
    'логист': '📦',
    'образован': '📚',
    'автомобил': '🚗',
    'недвижим': '🏠',
    'туризм': '✈️',
    'спорт': '⚽',
    'фитнес': '💪',
    'производ': '🏭',
    'оптов': '📦',
    'розн': '🛒',
    'консалт': '💼',
    'финанс': '💰',
    'страхов': '🛡',
    'безопас': '🔒',
    'клинин': '🧹',
    'event': '🎉',
    'фото': '📸',
    'видео': '🎬',
}

def get_company_emoji(company_name: str = "", business_type: str = "") -> str:
    """Получить эмодзи для компании на основе названия или типа бизнеса"""
    business_lower = (business_type or "").lower()

    # Сначала проверяем тип бизнеса
    for keyword, emoji in BUSINESS_TYPE_EMOJIS.items():
        if keyword in business_lower:
            return emoji

    # Затем по первой букве названия
    if company_name:
        first_letter = company_name[0].upper()
        if first_letter in COMPANY_EMOJIS_BY_LETTER:
            return COMPANY_EMOJIS_BY_LETTER[first_letter]

    # Дефолт
    return '🏢'

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

MSG_GENERATION_COMPLETE = (
    "🎉 <b>Сайт успешно сгенерирован!</b>\n\n"
    "Заявка: <code>{req_id}</code>\n"
    "Компания: {company}\n\n"
    "Архив с сайтом прикреплён ниже."
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
    "Доступно управление пользователями, заявками и статистика."
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

MSG_BLOCKED_USER = (
    "⛔ <b>Доступ ограничен</b>\n\n"
    "Ваш аккаунт заблокирован администратором.\n"
    "Для разблокировки обратитесь в поддержку."
)

MSG_PENDING_APPROVAL = (
    "⏳ <b>Заявка на регистрацию отправлена</b>\n\n"
    "Ваша заявка находится на рассмотрении администратора.\n"
    "Вы получите уведомление после одобрения."
)

MSG_REGISTRATION_APPROVED = (
    "✅ <b>Регистрация одобрена!</b>\n\n"
    "Добро пожаловать в систему!\n"
    "Теперь вы можете создавать заявки на разработку сайтов."
)

MSG_REGISTRATION_REJECTED = (
    "❌ <b>Регистрация отклонена</b>\n\n"
    "К сожалению, ваша заявка была отклонена.\n"
    "Причина: {reason}\n\n"
    "Вы можете подать заявку повторно."
)

MSG_NEW_REGISTRATION_ADMIN = (
    "🆕 <b>Новая заявка на регистрацию</b>\n\n"
    "👤 {name}\n"
    "📱 {contact}\n"
    "🆔 ID: <code>{tg_id}</code>\n\n"
    "Одобрить или отклонить?"
)

# ==================== ЛИМИТЫ ====================

MAX_REQUESTS_PER_DAY = 50
MAX_ACTIVE_REQUESTS = 20
MAX_PHOTOS_PER_REQUEST = 50
