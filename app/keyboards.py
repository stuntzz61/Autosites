from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List, Any
from app.constants import (
    CB_OPEN, CB_LIST_PAGE, CB_BACK_TO_LIST,
    CB_EDIT, CB_DELETE, CB_EXPORT_ONE, CB_GEN,
    CB_MORE_PORTF, CB_MORE_TESTI, CB_MORE_FAQ, CB_MORE_SEO, CB_MORE_HERO, CB_DONE
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
    ikb.add(InlineKeyboardButton("Готово", callback_data=CB_DONE))
    return ikb

edit_fields_inline = more_details_inline
