# app/handlers/manager_profile.py
"""Личный кабинет менеджера"""

import logging
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

from app.constants import (
    BTN_MY, BTN_ARCHIVE, BTN_NEW, BTN_RESET, BTN_ADMIN_LOGIN,
    CB_ARCHIVE_REQ, CB_OPEN, STATUS_LABELS,
)
from app.db import (
    get_mode, get_user_by_tgid,
    list_manager_requests, count_manager_requests,
    list_manager_archive, count_manager_archive,
    get_manager_stats, archive_request, log_activity,
)
from app.keyboards import requests_list_inline
from app.utils import e

log = logging.getLogger("bot")


class ProfileEdit(StatesGroup):
    """Редактирование профиля"""
    new_contact = State()


def register(dp, bot):
    """Регистрация обработчиков личного кабинета"""

    def get_manager_keyboard():
        """Клавиатура менеджера"""
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW)
        kb.add(BTN_MY, BTN_ARCHIVE)
        kb.add("👤 Мой профиль", BTN_RESET)
        kb.add(BTN_ADMIN_LOGIN)
        return kb

    # ==================== ПРОФИЛЬ МЕНЕДЖЕРА ====================

    async def cmd_profile(message: types.Message):
        """Личный кабинет менеджера"""
        user = get_user_by_tgid(message.from_user.id)
        if not user:
            return await message.answer("Вы не зарегистрированы в системе.")

        mode = get_mode(message.from_user.id)
        if mode == "admin":
            return await message.answer("Используйте /admin_panel для панели администратора.")

        # Получаем статистику
        stats = get_manager_stats(str(user['id']))

        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or "—"
        contact = user.get('contact', '—')
        username = user.get('username')
        username_text = f"@{username}" if username else "Не указан"
        registered = user.get('created_at', '—')
        if hasattr(registered, 'strftime'):
            registered = registered.strftime('%d.%m.%Y')

        total_requests = stats.get('total_requests', 0)
        completed = stats.get('completed_requests', 0)
        pending = stats.get('pending_requests', 0)
        this_week = stats.get('this_week', 0)
        today = stats.get('today', 0)

        # Рассчитываем эффективность
        efficiency = round((completed / total_requests * 100) if total_requests > 0 else 0)

        # Определяем ранг
        if total_requests >= 100:
            rank = "🏆 Эксперт"
        elif total_requests >= 50:
            rank = "⭐ Профи"
        elif total_requests >= 20:
            rank = "🌟 Опытный"
        elif total_requests >= 5:
            rank = "📈 Новичок"
        else:
            rank = "🌱 Стажёр"

        text = (
            f"👤 <b>Личный кабинет</b>\n\n"
            f"<b>Имя:</b> {e(name)}\n"
            f"<b>Telegram:</b> {username_text}\n"
            f"<b>Контакт:</b> {e(contact)}\n"
            f"<b>В системе с:</b> {registered}\n\n"
            f"<b>📊 Статистика</b>\n"
            f"├ Всего заявок: <b>{total_requests}</b>\n"
            f"├ Завершено: <b>{completed}</b> ✅\n"
            f"├ В работе: <b>{pending}</b> ⏳\n"
            f"├ За неделю: <b>{this_week}</b>\n"
            f"├ Сегодня: <b>{today}</b>\n"
            f"└ Эффективность: <b>{efficiency}%</b>\n\n"
            f"<b>🎖 Ранг:</b> {rank}"
        )

        ikb = types.InlineKeyboardMarkup(row_width=2)
        ikb.add(
            types.InlineKeyboardButton("📋 Мои заявки", callback_data="my_requests"),
            types.InlineKeyboardButton("🗄 Архив", callback_data="my_archive"),
        )
        ikb.add(
            types.InlineKeyboardButton("✏️ Изменить контакт", callback_data="edit_contact"),
        )

        await message.answer(text, reply_markup=ikb)

    dp.register_message_handler(cmd_profile, lambda m: m.text == "👤 Мой профиль", state="*")
    dp.register_message_handler(cmd_profile, commands=["profile", "me"], state="*")

    # ==================== РЕДАКТИРОВАНИЕ КОНТАКТА ====================

    async def cb_edit_contact(call: types.CallbackQuery, state: FSMContext):
        """Начало редактирования контакта"""
        await call.answer()
        await ProfileEdit.new_contact.set()
        await call.message.edit_text(
            "✏️ <b>Изменение контакта</b>\n\n"
            "Введите новый контакт (телефон или email):\n\n"
            "Для отмены напишите /cancel"
        )

    dp.register_callback_query_handler(cb_edit_contact, lambda c: c.data == "edit_contact", state="*")

    async def process_new_contact(message: types.Message, state: FSMContext):
        """Сохранение нового контакта"""
        if message.text and message.text.startswith('/'):
            if message.text == '/cancel':
                await state.finish()
                return await message.answer("Отменено.", reply_markup=get_manager_keyboard())

        text = message.text.strip()
        if len(text) < 5:
            return await message.answer("❌ Контакт слишком короткий. Введите телефон или email:")

        from app.db import update_user
        user = get_user_by_tgid(message.from_user.id)
        if user:
            update_user(str(user['id']), contact=text)
            log_activity(str(user['id']), "contact_updated", "user", str(user['id']))

        await state.finish()
        await message.answer(
            f"✅ Контакт обновлён: <b>{e(text)}</b>",
            reply_markup=get_manager_keyboard()
        )

    dp.register_message_handler(process_new_contact, state=ProfileEdit.new_contact)

    # ==================== АРХИВИРОВАНИЕ ЗАЯВКИ ====================

    async def cb_archive_request(call: types.CallbackQuery):
        """Архивирование заявки менеджером"""
        req_id = call.data[len(CB_ARCHIVE_REQ):]

        user = get_user_by_tgid(call.from_user.id)
        if not user:
            return await call.answer("Вы не авторизованы", show_alert=True)

        user_id = str(user['id'])

        if archive_request(req_id, user_id, "completed_by_manager"):
            log_activity(user_id, "request_archived", "request", req_id)
            await call.answer("✅ Заявка отправлена в архив", show_alert=True)

            # Обновляем сообщение
            await call.message.edit_text(
                call.message.text + "\n\n<i>🗄 Заявка в архиве</i>",
                reply_markup=None
            )
        else:
            await call.answer("❌ Ошибка архивирования", show_alert=True)

    dp.register_callback_query_handler(
        cb_archive_request,
        lambda c: c.data and c.data.startswith(CB_ARCHIVE_REQ),
        state="*"
    )

    # ==================== МОЙ АРХИВ ====================

    async def cmd_my_archive(message: types.Message):
        """Архив заявок менеджера"""
        user = get_user_by_tgid(message.from_user.id)
        if not user:
            return await message.answer("Вы не зарегистрированы.")

        total = count_manager_archive(message.from_user.id)

        if total == 0:
            return await message.answer(
                "🗄 <b>Архив пуст</b>\n\n"
                "Здесь будут отображаться завершённые заявки.",
                reply_markup=get_manager_keyboard()
            )

        rows = list_manager_archive(message.from_user.id, 0, 15)

        ikb = types.InlineKeyboardMarkup(row_width=1)
        for r in rows:
            company = r.get('company_name') or r.get('client_name') or '—'
            status = r.get('status', '')
            status_label = STATUS_LABELS.get(status, '')[:15]

            title = f"🗄 {company[:20]} | {status_label}"
            ikb.add(types.InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))

        # Пагинация
        if total > 15:
            ikb.add(types.InlineKeyboardButton(f"Показано 15 из {total}", callback_data="noop"))

        await message.answer(
            f"🗄 <b>Архив заявок</b> ({total})",
            reply_markup=ikb
        )

    dp.register_message_handler(cmd_my_archive, lambda m: m.text == BTN_ARCHIVE, state="*")
    dp.register_message_handler(cmd_my_archive, commands=["archive"], state="*")

    async def cb_my_archive(call: types.CallbackQuery):
        """Архив через callback"""
        await call.answer()

        total = count_manager_archive(call.from_user.id)

        if total == 0:
            return await call.message.edit_text(
                "🗄 <b>Архив пуст</b>\n\n"
                "Здесь будут отображаться завершённые заявки."
            )

        rows = list_manager_archive(call.from_user.id, 0, 15)

        ikb = types.InlineKeyboardMarkup(row_width=1)
        for r in rows:
            company = r.get('company_name') or r.get('client_name') or '—'
            status = r.get('status', '')
            status_label = STATUS_LABELS.get(status, '')[:15]

            title = f"🗄 {company[:20]} | {status_label}"
            ikb.add(types.InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))

        ikb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="my_requests"))

        await call.message.edit_text(
            f"🗄 <b>Архив заявок</b> ({total})",
            reply_markup=ikb
        )

    dp.register_callback_query_handler(cb_my_archive, lambda c: c.data == "my_archive", state="*")

    async def cb_my_requests(call: types.CallbackQuery):
        """Мои заявки через callback"""
        await call.answer()

        total = count_manager_requests(call.from_user.id)

        if total == 0:
            return await call.message.edit_text(
                "📋 <b>Заявок нет</b>\n\n"
                "Создайте первую заявку, нажав «➕ Новая заявка»"
            )

        rows = list_manager_requests(call.from_user.id, 0, 15)
        await call.message.edit_text(
            f"📋 <b>Мои заявки</b> ({total})",
            reply_markup=requests_list_inline(rows, 1, total, 15)
        )

    dp.register_callback_query_handler(cb_my_requests, lambda c: c.data == "my_requests", state="*")

