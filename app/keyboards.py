from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from typing import List, Any
from app.constants import (
    CB_OPEN, CB_LIST_PAGE, CB_BACK_TO_LIST,
    CB_EDIT, CB_DELETE, CB_EXPORT_ONE, CB_GEN,
    CB_MORE_PORTF, CB_MORE_TESTI, CB_MORE_FAQ, CB_MORE_SEO, CB_MORE_HERO, CB_DONE,
    CB_EDIT_FIELD, CB_PHOTO_CAT, CB_ARCHIVE_REQ, CB_CLOSE_REQ,
    CB_ADMIN_MANAGER, CB_ADMIN_BLOCK, CB_ADMIN_UNBLOCK, CB_ADMIN_STATS,
    CB_ADMIN_DELETE_USER, CB_APPROVE_USER, CB_REJECT_USER,
    CB_BROADCAST, CB_BROADCAST_ALL, CB_BROADCAST_SELECT, CB_BC_MANAGER,
    CB_BC_CONFIRM, CB_BC_CANCEL, CB_BC_ADD_PHOTO, CB_BC_SKIP_PHOTO, CB_BC_DONE,
    CB_SEARCH, CB_SEARCH_RESULT,
    CB_MASS_OPS, CB_MASS_SELECT, CB_MASS_ARCHIVE, CB_MASS_DELETE, CB_MASS_CONFIRM, CB_MASS_CANCEL,
    CB_EXPORT_EXCEL, CB_EXPORT_PDF,
    PHOTO_CATEGORIES, STATUS_LABELS,
    BTN_BACK, BTN_EXIT, BTN_SKIP,
    get_company_emoji,
)


def _truncate(text: str, max_len: int = 25) -> str:
    """Обрезает текст до max_len символов"""
    if not text:
        return ""
    return text[:max_len] + "…" if len(text) > max_len else text


def requests_list_inline(reqs: List[dict], page: int, total: int, per_page: int = 10) -> InlineKeyboardMarkup:
    """Список заявок с эмодзи компании"""
    ikb = InlineKeyboardMarkup(row_width=1)

    for r in reqs:
        company = _truncate(r.get('company_name') or '', 18)
        client = _truncate(r.get('client_name') or '', 12)
        business_type = r.get('business_type') or ''
        status = r.get('status') or ''

        # Эмодзи по типу бизнеса
        emoji = get_company_emoji(company, business_type)

        # Индикатор статуса
        status_indicator = ""
        if status == "generated_ok":
            status_indicator = " ✅"
        elif status == "generating" or status == "queued":
            status_indicator = " ⏳"
        elif status == "generated_error":
            status_indicator = " ❌"

        if company and client:
            title = f"{emoji} {company} • {client}{status_indicator}"
        elif company:
            title = f"{emoji} {company}{status_indicator}"
        elif client:
            title = f"👤 {client}{status_indicator}"
        else:
            req_id = str(r['id'])[:8]
            title = f"📋 {req_id}...{status_indicator}"

        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))

    # Навигация
    pages = max(1, (total + per_page - 1) // per_page)
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"{CB_LIST_PAGE}{page-1}"))
    nav.append(InlineKeyboardButton(f"{page} из {pages}", callback_data=f"{CB_LIST_PAGE}{page}"))
    if page < pages:
        nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"{CB_LIST_PAGE}{page+1}"))
    if nav:
        ikb.row(*nav)

    return ikb


def request_card_inline(req_id: Any, is_owner: bool, is_admin: bool, status: str = None) -> InlineKeyboardMarkup:
    """Карточка заявки с действиями в зависимости от статуса"""
    ikb = InlineKeyboardMarkup(row_width=2)

    if is_owner or is_admin:
        # Основные действия
        ikb.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"{CB_EDIT}{req_id}"),
            InlineKeyboardButton("📤 Экспорт", callback_data=f"{CB_EXPORT_ONE}{req_id}"),
        )

        # Действия в зависимости от статуса
        if status in ("ready_to_generate", "collecting_photos", "draft"):
            ikb.add(InlineKeyboardButton("⚙️ Сгенерировать сайт", callback_data=f"{CB_GEN}{req_id}"))
        elif status == "generated_ok":
            ikb.add(
                InlineKeyboardButton("✔️ Закрыть заявку", callback_data=f"{CB_CLOSE_REQ}{req_id}"),
                InlineKeyboardButton("🗄 В архив", callback_data=f"{CB_ARCHIVE_REQ}{req_id}"),
            )
        elif status in ("closed", "delivered"):
            ikb.add(InlineKeyboardButton("🗄 В архив", callback_data=f"{CB_ARCHIVE_REQ}{req_id}"))

        # Удаление (не для завершённых)
        if status not in ("generated_ok", "closed", "delivered", "archived"):
            ikb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"{CB_DELETE}{req_id}"))

    ikb.add(InlineKeyboardButton("⬅️ К списку заявок", callback_data=CB_BACK_TO_LIST))

    return ikb


def more_details_inline(req_id: str) -> InlineKeyboardMarkup:
    """Клавиатура для добавления дополнительных данных"""
    ikb = InlineKeyboardMarkup(row_width=2)

    ikb.add(
        InlineKeyboardButton("📁 Портфолио", callback_data=f"{CB_MORE_PORTF}{req_id}"),
        InlineKeyboardButton("💬 Отзывы", callback_data=f"{CB_MORE_TESTI}{req_id}")
    )
    ikb.add(
        InlineKeyboardButton("❓ FAQ", callback_data=f"{CB_MORE_FAQ}{req_id}"),
        InlineKeyboardButton("🔍 SEO", callback_data=f"{CB_MORE_SEO}{req_id}")
    )
    ikb.add(InlineKeyboardButton("🏠 Заголовки главной", callback_data=f"{CB_MORE_HERO}{req_id}"))
    ikb.add(InlineKeyboardButton("✅ Завершить и открыть заявку", callback_data=f"{CB_DONE}{req_id}"))

    return ikb


def edit_fields_inline(req_id: str) -> InlineKeyboardMarkup:
    """Полная клавиатура редактирования всех полей"""
    ikb = InlineKeyboardMarkup(row_width=2)

    # Основная информация
    ikb.add(
        InlineKeyboardButton("🏢 Компания", callback_data=f"{CB_EDIT_FIELD}{req_id}_company"),
        InlineKeyboardButton("💼 Сфера", callback_data=f"{CB_EDIT_FIELD}{req_id}_business_type"),
    )

    # Контакты
    ikb.add(
        InlineKeyboardButton("📞 Телефон", callback_data=f"{CB_EDIT_FIELD}{req_id}_phone"),
        InlineKeyboardButton("📧 Email", callback_data=f"{CB_EDIT_FIELD}{req_id}_email"),
    )
    ikb.add(
        InlineKeyboardButton("📍 Адрес", callback_data=f"{CB_EDIT_FIELD}{req_id}_address"),
        InlineKeyboardButton("🕐 График", callback_data=f"{CB_EDIT_FIELD}{req_id}_work_hours"),
    )

    # Описание и стиль
    ikb.add(
        InlineKeyboardButton("📝 Описание", callback_data=f"{CB_EDIT_FIELD}{req_id}_summary"),
        InlineKeyboardButton("🎨 Цвета", callback_data=f"{CB_EDIT_FIELD}{req_id}_color_palette"),
    )

    # Контент
    ikb.add(
        InlineKeyboardButton("🛠 Услуги", callback_data=f"{CB_EDIT_FIELD}{req_id}_services"),
        InlineKeyboardButton("📁 Портфолио", callback_data=f"{CB_EDIT_FIELD}{req_id}_portfolio"),
    )
    ikb.add(
        InlineKeyboardButton("💬 Отзывы", callback_data=f"{CB_EDIT_FIELD}{req_id}_testimonials"),
        InlineKeyboardButton("❓ FAQ", callback_data=f"{CB_EDIT_FIELD}{req_id}_faq"),
    )

    # SEO и заголовки
    ikb.add(
        InlineKeyboardButton("🔍 SEO", callback_data=f"{CB_EDIT_FIELD}{req_id}_seo_description"),
        InlineKeyboardButton("🏠 Заголовок", callback_data=f"{CB_EDIT_FIELD}{req_id}_hero_title"),
    )

    ikb.add(InlineKeyboardButton("⬅️ Назад к заявке", callback_data=f"{CB_OPEN}{req_id}"))

    return ikb


def photo_categories_inline(req_id: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора категории фото"""
    ikb = InlineKeyboardMarkup(row_width=2)

    cats = list(PHOTO_CATEGORIES.items())
    for i in range(0, len(cats), 2):
        row = []
        for j in range(2):
            if i + j < len(cats):
                cat_key, cat_name = cats[i + j]
                row.append(InlineKeyboardButton(cat_name, callback_data=f"{CB_PHOTO_CAT}{req_id}_{cat_key}"))
        if row:
            ikb.row(*row)

    ikb.add(InlineKeyboardButton("✅ Завершить загрузку", callback_data=f"photo_done_{req_id}"))

    return ikb


def photo_upload_inline(req_id: str, category: str) -> InlineKeyboardMarkup:
    """Клавиатура во время загрузки фото"""
    ikb = InlineKeyboardMarkup(row_width=1)

    ikb.add(InlineKeyboardButton("📂 Другая категория", callback_data=f"photo_cats_{req_id}"))
    ikb.add(InlineKeyboardButton("✅ Завершить загрузку", callback_data=f"photo_done_{req_id}"))

    return ikb


def form_navigation_keyboard(can_skip: bool = False) -> ReplyKeyboardMarkup:
    """Клавиатура навигации в форме"""
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)

    if can_skip:
        kb.add(KeyboardButton(BTN_SKIP))

    kb.add(KeyboardButton(BTN_BACK), KeyboardButton(BTN_EXIT))

    return kb


# ==================== ADMIN KEYBOARDS ====================

def admin_main_inline() -> InlineKeyboardMarkup:
    """Главное меню админа"""
    ikb = InlineKeyboardMarkup(row_width=2)

    ikb.add(
        InlineKeyboardButton("📈 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton("👥 Менеджеры", callback_data="admin_managers"),
    )
    ikb.add(
        InlineKeyboardButton("📦 Все заявки", callback_data="admin_requests"),
        InlineKeyboardButton("📋 Лог действий", callback_data="admin_log"),
    )
    ikb.add(
        InlineKeyboardButton("📊 Отчёт за неделю", callback_data="admin_weekly"),
        InlineKeyboardButton("📤 Экспорт всего", callback_data="admin_export"),
    )

    return ikb


def admin_managers_list_inline(managers: List[dict], page: int = 1) -> InlineKeyboardMarkup:
    """Список менеджеров для админа"""
    ikb = InlineKeyboardMarkup(row_width=1)

    for m in managers:
        name = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip() or "Без имени"
        total = m.get('total_requests', 0)
        completed = m.get('completed_requests', 0)
        blocked = "🔒" if m.get('is_blocked') else ""

        title = f"{blocked}👤 {_truncate(name, 15)} | 📋{total} ✅{completed}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_ADMIN_MANAGER}{m['id']}"))

    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))

    return ikb


def admin_manager_card_inline(manager_id: str, is_blocked: bool) -> InlineKeyboardMarkup:
    """Карточка менеджера для админа"""
    ikb = InlineKeyboardMarkup(row_width=2)

    ikb.add(
        InlineKeyboardButton("📊 Статистика", callback_data=f"{CB_ADMIN_STATS}{manager_id}"),
        InlineKeyboardButton("📋 Заявки", callback_data=f"admin_mgr_reqs_{manager_id}"),
    )

    if is_blocked:
        ikb.add(InlineKeyboardButton("🔓 Разблокировать", callback_data=f"{CB_ADMIN_UNBLOCK}{manager_id}"))
    else:
        ikb.add(InlineKeyboardButton("🔒 Заблокировать", callback_data=f"{CB_ADMIN_BLOCK}{manager_id}"))

    ikb.add(InlineKeyboardButton("⬅️ К списку", callback_data="admin_managers"))

    return ikb


def confirm_action_inline(action: str, entity_id: str) -> InlineKeyboardMarkup:
    """Подтверждение действия"""
    ikb = InlineKeyboardMarkup(row_width=2)
    ikb.add(
        InlineKeyboardButton("✅ Да", callback_data=f"confirm_{action}_{entity_id}"),
        InlineKeyboardButton("❌ Нет", callback_data=f"cancel_{action}_{entity_id}"),
    )
    return ikb


def pending_approval_inline(user_id: str) -> InlineKeyboardMarkup:
    """Клавиатура одобрения/отклонения регистрации"""
    ikb = InlineKeyboardMarkup(row_width=2)
    ikb.add(
        InlineKeyboardButton("✅ Одобрить", callback_data=f"{CB_APPROVE_USER}{user_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"{CB_REJECT_USER}{user_id}"),
    )
    ikb.add(InlineKeyboardButton("⬅️ К списку", callback_data="pending_list"))
    return ikb


def pending_list_inline(users: list) -> InlineKeyboardMarkup:
    """Список ожидающих одобрения"""
    ikb = InlineKeyboardMarkup(row_width=1)

    for u in users:
        name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip() or "Без имени"
        created = u.get('created_at', '')
        if hasattr(created, 'strftime'):
            created = created.strftime('%d.%m %H:%M')

        title = f"⏳ {_truncate(name, 20)} | {created}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"pending_user_{u['id']}"))

    if not users:
        ikb.add(InlineKeyboardButton("Нет ожидающих заявок", callback_data="noop"))

    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    return ikb


def manager_full_card_inline(manager_id: str, is_blocked: bool) -> InlineKeyboardMarkup:
    """Полная карточка менеджера с CRUD"""
    ikb = InlineKeyboardMarkup(row_width=2)

    ikb.add(
        InlineKeyboardButton("📊 Статистика", callback_data=f"{CB_ADMIN_STATS}{manager_id}"),
        InlineKeyboardButton("📋 Заявки", callback_data=f"admin_mgr_reqs_{manager_id}"),
    )

    if is_blocked:
        ikb.add(InlineKeyboardButton("🔓 Разблокировать", callback_data=f"{CB_ADMIN_UNBLOCK}{manager_id}"))
    else:
        ikb.add(InlineKeyboardButton("🔒 Заблокировать", callback_data=f"{CB_ADMIN_BLOCK}{manager_id}"))

    ikb.add(
        InlineKeyboardButton("✏️ Редактировать", callback_data=f"admin_edit_mgr_{manager_id}"),
        InlineKeyboardButton("🗑 Удалить", callback_data=f"{CB_ADMIN_DELETE_USER}{manager_id}"),
    )

    ikb.add(InlineKeyboardButton("⬅️ К списку", callback_data="admin_managers"))

    return ikb


# ==================== РАССЫЛКА ====================

def broadcast_start_inline() -> InlineKeyboardMarkup:
    """Выбор типа рассылки"""
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.add(
        InlineKeyboardButton("📢 Всем менеджерам", callback_data=CB_BROADCAST_ALL),
        InlineKeyboardButton("👤 Выбрать получателей", callback_data=CB_BROADCAST_SELECT),
        InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"),
    )
    return ikb


def broadcast_managers_select_inline(managers: list, selected_ids: set = None) -> InlineKeyboardMarkup:
    """Выбор менеджеров для рассылки (с чекбоксами)"""
    selected_ids = selected_ids or set()
    ikb = InlineKeyboardMarkup(row_width=1)

    for m in managers:
        name = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip() or "—"
        mid = str(m['id'])
        check = "✅" if mid in selected_ids else "⬜"
        ikb.add(InlineKeyboardButton(f"{check} {_truncate(name, 25)}", callback_data=f"{CB_BC_MANAGER}{mid}"))

    if selected_ids:
        ikb.add(InlineKeyboardButton(f"✅ Готово ({len(selected_ids)} выбрано)", callback_data=CB_BC_DONE))

    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data=CB_BROADCAST))
    return ikb


def broadcast_confirm_inline(with_photo: bool = False) -> InlineKeyboardMarkup:
    """Подтверждение рассылки"""
    ikb = InlineKeyboardMarkup(row_width=2)

    if not with_photo:
        ikb.add(InlineKeyboardButton("📷 Добавить фото", callback_data=CB_BC_ADD_PHOTO))

    ikb.add(
        InlineKeyboardButton("✅ Отправить", callback_data=CB_BC_CONFIRM),
        InlineKeyboardButton("❌ Отмена", callback_data=CB_BC_CANCEL),
    )
    return ikb


def broadcast_photo_inline() -> InlineKeyboardMarkup:
    """Добавление фото к рассылке"""
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.add(
        InlineKeyboardButton("⏭ Без фото", callback_data=CB_BC_SKIP_PHOTO),
        InlineKeyboardButton("❌ Отмена", callback_data=CB_BC_CANCEL),
    )
    return ikb


# ==================== ПОИСК ====================

def search_results_inline(results: list) -> InlineKeyboardMarkup:
    """Результаты поиска"""
    ikb = InlineKeyboardMarkup(row_width=1)

    for r in results[:15]:
        company = r.get('company_name') or r.get('client_name') or '—'
        status = r.get('status', 'draft')
        emoji = get_company_emoji(r.get('business_type', ''))
        status_label = STATUS_LABELS.get(status, '')[:2]

        title = f"{emoji} {_truncate(company, 20)} {status_label}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))

    if not results:
        ikb.add(InlineKeyboardButton("Ничего не найдено", callback_data="noop"))

    ikb.add(InlineKeyboardButton("🔍 Новый поиск", callback_data=CB_SEARCH))
    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    return ikb


# ==================== МАССОВЫЕ ОПЕРАЦИИ ====================

def mass_ops_start_inline() -> InlineKeyboardMarkup:
    """Начало массовых операций"""
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.add(
        InlineKeyboardButton("🗄 Архивировать все готовые", callback_data=f"{CB_MASS_ARCHIVE}_completed"),
        InlineKeyboardButton("🗑 Удалить ошибочные", callback_data=f"{CB_MASS_DELETE}_errors"),
        InlineKeyboardButton("📦 Архивировать старые (30+ дней)", callback_data=f"{CB_MASS_ARCHIVE}_old"),
        InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"),
    )
    return ikb


def mass_ops_confirm_inline(action: str, count: int) -> InlineKeyboardMarkup:
    """Подтверждение массовой операции"""
    ikb = InlineKeyboardMarkup(row_width=2)
    ikb.add(
        InlineKeyboardButton(f"✅ Да, выполнить ({count})", callback_data=f"{CB_MASS_CONFIRM}_{action}"),
        InlineKeyboardButton("❌ Отмена", callback_data=CB_MASS_CANCEL),
    )
    return ikb


# ==================== ЭКСПОРТ ====================

def export_options_inline() -> InlineKeyboardMarkup:
    """Варианты экспорта"""
    ikb = InlineKeyboardMarkup(row_width=2)
    ikb.add(
        InlineKeyboardButton("📊 Excel", callback_data=CB_EXPORT_EXCEL),
        InlineKeyboardButton("📄 PDF", callback_data=CB_EXPORT_PDF),
    )
    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data="admin_back"))
    return ikb
