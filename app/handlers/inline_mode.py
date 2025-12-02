# app/handlers/inline_mode.py
"""Telegram Inline Mode для быстрого поиска заявок"""

import logging
from aiogram import types
from aiogram.types import (
    InlineQuery, InlineQueryResultArticle, InputTextMessageContent
)

from app.db import search_requests, get_mode, get_user_by_tgid
from app.constants import STATUS_LABELS, get_company_emoji
from app.utils import e

log = logging.getLogger("bot")


def register(dp, bot):
    """Регистрация inline mode обработчиков"""

    async def inline_search(query: InlineQuery):
        """Обработка inline запроса"""
        user_id = query.from_user.id

        # Проверка прав (только менеджеры и админы)
        mode = get_mode(user_id)
        user = get_user_by_tgid(user_id)

        if not user or mode not in ("manager", "admin"):
            return await query.answer(
                results=[],
                switch_pm_text="🔐 Требуется авторизация",
                switch_pm_parameter="start",
                cache_time=60
            )

        search_text = query.query.strip()

        if len(search_text) < 2:
            # Подсказка для пользователя
            return await query.answer(
                results=[
                    InlineQueryResultArticle(
                        id="hint",
                        title="🔍 Введите минимум 2 символа",
                        description="Поиск по названию компании, имени клиента или сфере деятельности",
                        input_message_content=InputTextMessageContent(
                            message_text="Для поиска введите минимум 2 символа после @botname"
                        )
                    )
                ],
                cache_time=30
            )

        # Поиск
        results = search_requests(search_text, limit=20)

        if not results:
            return await query.answer(
                results=[
                    InlineQueryResultArticle(
                        id="not_found",
                        title="❌ Ничего не найдено",
                        description=f"По запросу «{search_text}» заявок не найдено",
                        input_message_content=InputTextMessageContent(
                            message_text=f"По запросу «{search_text}» заявок не найдено"
                        )
                    )
                ],
                cache_time=30
            )

        # Формируем результаты
        inline_results = []

        for i, r in enumerate(results):
            company = r.get('company_name') or '—'
            client = r.get('client_name') or '—'
            business = r.get('business_type') or ''
            status = r.get('status', 'draft')
            manager = f"{r.get('manager_first_name', '')} {r.get('manager_last_name', '')}".strip() or '—'

            created = r.get('created_at')
            if hasattr(created, 'strftime'):
                created = created.strftime('%d.%m.%Y')
            else:
                created = '—'

            emoji = get_company_emoji(business)
            status_label = STATUS_LABELS.get(status, status)

            # Заголовок
            title = f"{emoji} {company}"

            # Описание
            description = f"👤 {client} | {status_label} | {created}"

            # Текст сообщения
            message_text = (
                f"📋 <b>Заявка</b>\n\n"
                f"🏢 <b>Компания:</b> {e(company)}\n"
                f"👤 <b>Клиент:</b> {e(client)}\n"
                f"💼 <b>Сфера:</b> {e(business) or '—'}\n"
                f"📊 <b>Статус:</b> {status_label}\n"
                f"👔 <b>Менеджер:</b> {e(manager)}\n"
                f"📅 <b>Создано:</b> {created}\n\n"
                f"🔗 ID: <code>{r['id']}</code>"
            )

            inline_results.append(
                InlineQueryResultArticle(
                    id=str(r['id']),
                    title=title,
                    description=description,
                    input_message_content=InputTextMessageContent(
                        message_text=message_text,
                        parse_mode="HTML"
                    ),
                    thumb_url=None  # Можно добавить иконку статуса
                )
            )

        await query.answer(
            results=inline_results,
            cache_time=60,
            is_personal=True
        )

    dp.register_inline_handler(inline_search)

    # Обработчик выбора результата (опционально для логирования)
    async def chosen_inline_result(chosen_result: types.ChosenInlineResult):
        """Логирование выбранного результата"""
        log.info(
            "Inline result chosen: user=%s, result_id=%s, query=%s",
            chosen_result.from_user.id,
            chosen_result.result_id,
            chosen_result.query
        )

    dp.register_chosen_inline_handler(chosen_inline_result)

