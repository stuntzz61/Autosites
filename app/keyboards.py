from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Any
from app.constants import (
    CB_OPEN, CB_LIST_PAGE, CB_BACK_TO_LIST,
    CB_EDIT, CB_DELETE, CB_EXPORT_ONE, CB_GEN,
    CB_MORE_PORTF, CB_MORE_TESTI, CB_MORE_FAQ, CB_MORE_SEO, CB_MORE_HERO, CB_DONE,
    CB_EDIT_FIELD,
)

def requests_list_inline(reqs: List[dict], page: int, total: int, per_page: int = 10) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=1)
    for r in reqs:
        title = f"#{r['id']} — {r.get('client_name') or 'Без имени'}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))
    pages = max(1, (total + per_page - 1) // per_page)
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"{CB_LIST_PAGE}{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{pages}", callback_data=f"{CB_LIST_PAGE}{page}"))
    if page < pages:
        nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"{CB_LIST_PAGE}{page+1}"))
    if nav:
        ikb.row(*nav)
    return ikb

def request_card_inline(req_id: Any, is_owner: bool, is_admin: bool) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=2)
    if is_owner or is_admin:
        ikb.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"{CB_EDIT}{req_id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"{CB_DELETE}{req_id}"),
        )
        ikb.add(
            InlineKeyboardButton("⬇️ Экспорт JSON", callback_data=f"{CB_EXPORT_ONE}{req_id}"),
            InlineKeyboardButton("⚙️ Сгенерировать сайт", callback_data=f"{CB_GEN}{req_id}"),
        )
    ikb.add(InlineKeyboardButton("⬅️ К списку", callback_data=CB_BACK_TO_LIST))
    return ikb

def more_details_inline(req_id: str) -> InlineKeyboardMarkup:
    """Клавиатура для добавления дополнительных деталей после создания заявки"""
    ikb = InlineKeyboardMarkup(row_width=2)
    ikb.add(
        InlineKeyboardButton("➕ Портфолио", callback_data=f"{CB_MORE_PORTF}{req_id}"),
        InlineKeyboardButton("➕ Отзывы", callback_data=f"{CB_MORE_TESTI}{req_id}")
    )
    ikb.add(
        InlineKeyboardButton("➕ FAQ", callback_data=f"{CB_MORE_FAQ}{req_id}"),
        InlineKeyboardButton("➕ Поисковое описание", callback_data=f"{CB_MORE_SEO}{req_id}")
    )
    ikb.add(InlineKeyboardButton("➕ Первый экран", callback_data=f"{CB_MORE_HERO}{req_id}"))
    ikb.add(InlineKeyboardButton("✅ Готово — перейти к заявке", callback_data=f"{CB_DONE}{req_id}"))
    return ikb

def edit_fields_inline(req_id: str) -> InlineKeyboardMarkup:
    """Полная клавиатура редактирования ВСЕХ полей заявки"""
    ikb = InlineKeyboardMarkup(row_width=2)

    # Основные поля сайта
    ikb.add(
        InlineKeyboardButton("🏢 Название компании", callback_data=f"{CB_EDIT_FIELD}{req_id}_company"),
        InlineKeyboardButton("💼 Тип бизнеса", callback_data=f"{CB_EDIT_FIELD}{req_id}_business_type"),
    )
    ikb.add(
        InlineKeyboardButton("🎨 Цвета", callback_data=f"{CB_EDIT_FIELD}{req_id}_color_palette"),
        InlineKeyboardButton("📞 Контакты", callback_data=f"{CB_EDIT_FIELD}{req_id}_site_contacts"),
    )
    ikb.add(
        InlineKeyboardButton("📝 Описание", callback_data=f"{CB_EDIT_FIELD}{req_id}_summary"),
        InlineKeyboardButton("🕐 Режим работы", callback_data=f"{CB_EDIT_FIELD}{req_id}_work_hours"),
    )

    # Услуги и портфолио
    ikb.add(
        InlineKeyboardButton("🛠 Услуги", callback_data=f"{CB_EDIT_FIELD}{req_id}_services"),
        InlineKeyboardButton("📁 Портфолио", callback_data=f"{CB_EDIT_FIELD}{req_id}_portfolio"),
    )

    # Отзывы и FAQ
    ikb.add(
        InlineKeyboardButton("💬 Отзывы", callback_data=f"{CB_EDIT_FIELD}{req_id}_testimonials"),
        InlineKeyboardButton("❓ FAQ", callback_data=f"{CB_EDIT_FIELD}{req_id}_faq"),
    )

    # SEO и Hero
    ikb.add(
        InlineKeyboardButton("🔍 SEO описание", callback_data=f"{CB_EDIT_FIELD}{req_id}_seo_description"),
        InlineKeyboardButton("🏠 Заголовок", callback_data=f"{CB_EDIT_FIELD}{req_id}_hero_title"),
    )
    ikb.add(
        InlineKeyboardButton("📄 Подзаголовок", callback_data=f"{CB_EDIT_FIELD}{req_id}_hero_subtitle"),
        InlineKeyboardButton("📐 Структура", callback_data=f"{CB_EDIT_FIELD}{req_id}_structure"),
    )

    # Назад к карточке
    ikb.add(InlineKeyboardButton("⬅️ Назад к заявке", callback_data=f"{CB_OPEN}{req_id}"))

    return ikb


# Категории фото
CB_PHOTO_CAT = "photo_cat_"
PHOTO_CATEGORIES = {
    "hero": "🏠 Главное фото (Hero)",
    "services": "🛠 Фото услуг",
    "portfolio": "📁 Фото портфолио",
    "team": "👥 Фото команды",
    "office": "🏢 Фото офиса/производства",
    "other": "📷 Другие фото",
}

def photo_categories_inline(req_id: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора категории фото"""
    ikb = InlineKeyboardMarkup(row_width=1)
    for cat_key, cat_name in PHOTO_CATEGORIES.items():
        ikb.add(InlineKeyboardButton(cat_name, callback_data=f"{CB_PHOTO_CAT}{req_id}_{cat_key}"))
    ikb.add(InlineKeyboardButton("✅ Закончить загрузку фото", callback_data=f"photo_done_{req_id}"))
    return ikb

def photo_upload_inline(req_id: str, category: str) -> InlineKeyboardMarkup:
    """Клавиатура во время загрузки фото определённой категории"""
    ikb = InlineKeyboardMarkup(row_width=1)
    ikb.add(InlineKeyboardButton("📤 Загрузить ещё в эту категорию", callback_data=f"{CB_PHOTO_CAT}{req_id}_{category}"))
    ikb.add(InlineKeyboardButton("🔄 Выбрать другую категорию", callback_data=f"photo_cats_{req_id}"))
    ikb.add(InlineKeyboardButton("✅ Закончить загрузку фото", callback_data=f"photo_done_{req_id}"))
    return ikb
