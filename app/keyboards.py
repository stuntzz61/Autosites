from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from typing import List, Any
from app.constants import (
    CB_OPEN, CB_LIST_PAGE, CB_BACK_TO_LIST,
    CB_EDIT, CB_DELETE, CB_EXPORT_ONE, CB_GEN,
    CB_MORE_PORTF, CB_MORE_TESTI, CB_MORE_FAQ, CB_MORE_SEO, CB_MORE_HERO, CB_DONE,
    CB_EDIT_FIELD, CB_PHOTO_CAT,
    PHOTO_CATEGORIES,
    BTN_BACK, BTN_EXIT, BTN_SKIP,
)


def _truncate(text: str, max_len: int = 25) -> str:
    """Обрезает текст до max_len символов"""
    if not text:
        return ""
    return text[:max_len] + "…" if len(text) > max_len else text


def requests_list_inline(reqs: List[dict], page: int, total: int, per_page: int = 10) -> InlineKeyboardMarkup:
    """Список заявок с компанией и именем клиента"""
    ikb = InlineKeyboardMarkup(row_width=1)

    for r in reqs:
        # Формируем удобное название: компания | клиент
        company = _truncate(r.get('company_name') or '', 20)
        client = _truncate(r.get('client_name') or '', 15)

        if company and client:
            title = f"🏢 {company} • {client}"
        elif company:
            title = f"🏢 {company}"
        elif client:
            title = f"👤 {client}"
        else:
            # Fallback: короткий ID
            req_id = str(r['id'])[:8]
            title = f"📋 Заявка {req_id}..."

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


def request_card_inline(req_id: Any, is_owner: bool, is_admin: bool) -> InlineKeyboardMarkup:
    """Карточка заявки с действиями"""
    ikb = InlineKeyboardMarkup(row_width=2)

    if is_owner or is_admin:
        ikb.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"{CB_EDIT}{req_id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"{CB_DELETE}{req_id}"),
        )
        ikb.add(
            InlineKeyboardButton("📤 Экспорт", callback_data=f"{CB_EXPORT_ONE}{req_id}"),
            InlineKeyboardButton("⚙️ Сгенерировать", callback_data=f"{CB_GEN}{req_id}"),
        )

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

    # Контакты (разделены)
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
    ikb.add(
        InlineKeyboardButton("📄 Подзаголовок", callback_data=f"{CB_EDIT_FIELD}{req_id}_hero_subtitle"),
        InlineKeyboardButton("📐 Структура", callback_data=f"{CB_EDIT_FIELD}{req_id}_structure"),
    )

    # Назад
    ikb.add(InlineKeyboardButton("⬅️ Назад к заявке", callback_data=f"{CB_OPEN}{req_id}"))

    return ikb


def photo_categories_inline(req_id: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора категории фото"""
    ikb = InlineKeyboardMarkup(row_width=2)

    cats = list(PHOTO_CATEGORIES.items())
    # Парами
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


def confirm_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения"""
    ikb = InlineKeyboardMarkup(row_width=2)
    ikb.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_yes"),
        InlineKeyboardButton("❌ Отмена", callback_data="confirm_no"),
    )
    return ikb
