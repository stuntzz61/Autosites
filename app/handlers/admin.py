# app/handlers/admin.py
"""Административные функции: статистика, управление менеджерами, одобрение регистраций"""

import io
import json
import logging
from datetime import datetime
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from functools import wraps

from app.constants import (
    BTN_PANEL, BTN_STATS, BTN_MANAGERS, BTN_PENDING, BTN_REQS, BTN_LOGOUT,
    CB_ADMIN_MANAGER, CB_ADMIN_BLOCK, CB_ADMIN_UNBLOCK, CB_ADMIN_STATS,
    CB_ADMIN_DELETE_USER, CB_APPROVE_USER, CB_REJECT_USER,
    CB_OPEN, STATUS_LABELS,
    MSG_REGISTRATION_APPROVED, MSG_REGISTRATION_REJECTED,
)
from app.db import (
    get_mode, get_user_by_tgid, get_user_by_id,
    admin_counts, admin_users, admin_managers, admin_export_all,
    list_all_requests, count_all_requests,
    get_overall_stats, get_manager_stats, get_requests_by_status,
    get_manager_leaderboard, get_requests_this_week,
    block_manager, unblock_manager, get_manager_settings,
    get_activity_log, log_activity,
    list_pending_registrations, count_pending_registrations,
    approve_user, reject_user, delete_user, update_user,
    create_admin_notification,
)
from app.keyboards import (
    admin_main_inline, admin_managers_list_inline, manager_full_card_inline,
    requests_list_inline, confirm_action_inline, _truncate,
    pending_list_inline, pending_approval_inline,
)
from app.utils import e, chunks, convert_uuids_to_strings

log = logging.getLogger("bot")


class AdminStates(StatesGroup):
    """Состояния для админских действий"""
    reject_reason = State()
    edit_manager = State()


def require_admin(handler):
    """Декоратор для проверки прав администратора"""
    @wraps(handler)
    async def wrapper(obj, *args, **kwargs):
        if isinstance(obj, types.Message):
            user_id = obj.from_user.id
        else:
            user_id = obj.from_user.id

        if get_mode(user_id) != "admin":
            if isinstance(obj, types.CallbackQuery):
                return await obj.answer("⛔ Требуются права администратора", show_alert=True)
            return await obj.answer("⛔ Доступ запрещён.")

        return await handler(obj, *args, **kwargs)
    return wrapper


def register(dp, bot):

    def get_admin_keyboard():
        """Клавиатура администратора"""
        pending = count_pending_registrations()
        pending_text = f"⏳ Ожидают ({pending})" if pending > 0 else BTN_PENDING

        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_PANEL, BTN_STATS)
        kb.add(BTN_MANAGERS, pending_text)
        kb.add(BTN_REQS, BTN_LOGOUT)
        return kb

    # ==================== ГЛАВНАЯ ПАНЕЛЬ ====================

    @require_admin
    async def cmd_admin_panel(message: types.Message):
        """Главная панель администратора"""
        stats = get_overall_stats()
        pending = count_pending_registrations()

        pending_text = f"\n\n🔔 <b>Ожидают одобрения: {pending}</b>" if pending > 0 else ""

        text = (
            "📊 <b>Панель администратора</b>\n\n"
            f"👥 Пользователей: <b>{stats.get('total_users', 0)}</b>\n"
            f"👔 Менеджеров: <b>{stats.get('total_managers', 0)}</b>\n"
            f"📋 Заявок всего: <b>{stats.get('total_requests', 0)}</b>\n\n"
            f"📅 Сегодня: <b>{stats.get('requests_today', 0)}</b>\n"
            f"📆 За неделю: <b>{stats.get('requests_this_week', 0)}</b>\n"
            f"📊 За месяц: <b>{stats.get('requests_this_month', 0)}</b>\n\n"
            f"⏳ В очереди: <b>{stats.get('pending_generation', 0)}</b>\n"
            f"✅ Готово сегодня: <b>{stats.get('completed_today', 0)}</b>"
            f"{pending_text}"
        )

        await message.answer(text, reply_markup=admin_main_inline())

    dp.register_message_handler(cmd_admin_panel, commands=["admin_panel", "admin"], state="*")
    dp.register_message_handler(cmd_admin_panel, lambda m: m.text == BTN_PANEL, state="*")

    # ==================== ОДОБРЕНИЕ РЕГИСТРАЦИЙ ====================

    @require_admin
    async def cb_pending_list(call: types.CallbackQuery):
        """Список ожидающих одобрения"""
        await call.answer()

        pending = list_pending_registrations()
        count = len(pending)

        await call.message.edit_text(
            f"⏳ <b>Ожидают одобрения</b> ({count})\n\n"
            "Выберите заявку для рассмотрения:",
            reply_markup=pending_list_inline(pending)
        )

    dp.register_callback_query_handler(cb_pending_list, lambda c: c.data == "pending_list")

    @require_admin
    async def cb_pending_user(call: types.CallbackQuery):
        """Карточка ожидающего пользователя"""
        await call.answer()

        user_id = call.data.replace("pending_user_", "")
        user = get_user_by_id(user_id)

        if not user:
            return await call.message.edit_text("Пользователь не найден.", reply_markup=admin_main_inline())

        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or "—"
        contact = user.get('contact', '—')
        tg_id = user.get('tg_id', '—')
        created = user.get('created_at', '—')
        if hasattr(created, 'strftime'):
            created = created.strftime('%d.%m.%Y %H:%M')

        text = (
            f"👤 <b>Заявка на регистрацию</b>\n\n"
            f"Имя: <b>{e(name)}</b>\n"
            f"Контакт: {e(contact)}\n"
            f"Telegram ID: <code>{tg_id}</code>\n"
            f"Дата заявки: {created}\n\n"
            "Одобрить или отклонить?"
        )

        await call.message.edit_text(text, reply_markup=pending_approval_inline(user_id))

    dp.register_callback_query_handler(cb_pending_user, lambda c: c.data and c.data.startswith("pending_user_"))

    @require_admin
    async def cb_approve_user(call: types.CallbackQuery):
        """Одобрить регистрацию"""
        user_id = call.data[len(CB_APPROVE_USER):]

        admin = get_user_by_tgid(call.from_user.id)
        admin_id = str(admin['id']) if admin else None

        user = get_user_by_id(user_id)
        if not user:
            return await call.answer("Пользователь не найден", show_alert=True)

        if approve_user(user_id, admin_id):
            log_activity(admin_id, "user_approved", "user", user_id)

            # Уведомляем пользователя
            try:
                await bot.send_message(user['tg_id'], MSG_REGISTRATION_APPROVED)
            except Exception:
                pass

            await call.answer("✅ Пользователь одобрен!", show_alert=True)

            # Возвращаемся к списку
            pending = list_pending_registrations()
            await call.message.edit_text(
                f"⏳ <b>Ожидают одобрения</b> ({len(pending)})",
                reply_markup=pending_list_inline(pending)
            )
        else:
            await call.answer("❌ Ошибка одобрения", show_alert=True)

    dp.register_callback_query_handler(cb_approve_user, lambda c: c.data and c.data.startswith(CB_APPROVE_USER))

    @require_admin
    async def cb_reject_user(call: types.CallbackQuery, state: FSMContext):
        """Отклонить регистрацию - запрос причины"""
        user_id = call.data[len(CB_REJECT_USER):]

        await state.update_data(reject_user_id=user_id)
        await AdminStates.reject_reason.set()

        await call.message.edit_text(
            "❌ <b>Отклонение регистрации</b>\n\n"
            "Введите причину отклонения (будет отправлена пользователю):"
        )

    dp.register_callback_query_handler(cb_reject_user, lambda c: c.data and c.data.startswith(CB_REJECT_USER))

    async def process_reject_reason(message: types.Message, state: FSMContext):
        """Обработка причины отклонения"""
        data = await state.get_data()
        user_id = data.get("reject_user_id")
        reason = message.text.strip()

        admin = get_user_by_tgid(message.from_user.id)
        admin_id = str(admin['id']) if admin else None

        user = get_user_by_id(user_id)
        if not user:
            await state.finish()
            return await message.answer("Пользователь не найден.", reply_markup=get_admin_keyboard())

        if reject_user(user_id, admin_id, reason):
            log_activity(admin_id, "user_rejected", "user", user_id, {"reason": reason})

            # Уведомляем пользователя
            try:
                await bot.send_message(
                    user['tg_id'],
                    MSG_REGISTRATION_REJECTED.format(reason=reason)
                )
            except Exception:
                pass

            await state.finish()
            await message.answer("❌ Регистрация отклонена.", reply_markup=get_admin_keyboard())
        else:
            await state.finish()
            await message.answer("Ошибка отклонения.", reply_markup=get_admin_keyboard())

    dp.register_message_handler(process_reject_reason, state=AdminStates.reject_reason)

    # ==================== СТАТИСТИКА ====================

    @require_admin
    async def cmd_stats(message: types.Message):
        """Детальная статистика"""
        stats = get_overall_stats()
        by_status = get_requests_by_status()
        leaderboard = get_manager_leaderboard(5)

        status_lines = []
        for item in by_status:
            status_key = item.get('status', 'unknown')
            count = item.get('count', 0)
            label = STATUS_LABELS.get(status_key, status_key)
            status_lines.append(f"  • {label}: {count}")
        status_text = "\n".join(status_lines) if status_lines else "  Нет данных"

        leader_lines = []
        for i, m in enumerate(leaderboard, 1):
            name = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip() or "—"
            total = m.get('total_requests', 0)
            completed = m.get('completed', 0)
            leader_lines.append(f"  {i}. {_truncate(name, 15)} — {total} заявок ({completed} ✅)")
        leader_text = "\n".join(leader_lines) if leader_lines else "  Нет данных"

        text = (
            "📈 <b>Детальная статистика</b>\n\n"
            f"<b>По статусам:</b>\n{status_text}\n\n"
            f"<b>🏆 Топ менеджеров:</b>\n{leader_text}"
        )

        await message.answer(text, reply_markup=get_admin_keyboard())

    dp.register_message_handler(cmd_stats, commands=["stats"], state="*")
    dp.register_message_handler(cmd_stats, lambda m: m.text == BTN_STATS, state="*")

    @require_admin
    async def cb_admin_stats(call: types.CallbackQuery):
        await call.answer()

        stats = get_overall_stats()
        by_status = get_requests_by_status()

        status_lines = []
        for item in by_status:
            status_key = item.get('status', 'unknown')
            count = item.get('count', 0)
            label = STATUS_LABELS.get(status_key, status_key)
            status_lines.append(f"• {label}: {count}")
        status_text = "\n".join(status_lines) if status_lines else "Нет данных"

        text = (
            "📈 <b>Статистика по статусам</b>\n\n"
            f"{status_text}\n\n"
            f"📊 Всего заявок: {stats.get('total_requests', 0)}"
        )

        await call.message.edit_text(text, reply_markup=admin_main_inline())

    dp.register_callback_query_handler(cb_admin_stats, lambda c: c.data == "admin_stats")

    # ==================== УПРАВЛЕНИЕ МЕНЕДЖЕРАМИ ====================

    @require_admin
    async def cmd_managers(message: types.Message):
        """Список менеджеров"""
        managers = admin_managers()

        if not managers:
            return await message.answer("Менеджеров пока нет.", reply_markup=get_admin_keyboard())

        text = f"👥 <b>Менеджеры</b> ({len(managers)})\n\nВыберите менеджера:"
        await message.answer(text, reply_markup=admin_managers_list_inline(managers))

    dp.register_message_handler(cmd_managers, commands=["managers"], state="*")
    dp.register_message_handler(cmd_managers, lambda m: m.text == BTN_MANAGERS, state="*")

    @require_admin
    async def cb_admin_managers(call: types.CallbackQuery):
        await call.answer()
        managers = admin_managers()

        if not managers:
            return await call.message.edit_text("Менеджеров пока нет.", reply_markup=admin_main_inline())

        text = f"👥 <b>Менеджеры</b> ({len(managers)})\n\nВыберите:"
        await call.message.edit_text(text, reply_markup=admin_managers_list_inline(managers))

    dp.register_callback_query_handler(cb_admin_managers, lambda c: c.data == "admin_managers")

    # ==================== КАРТОЧКА МЕНЕДЖЕРА ====================

    @require_admin
    async def cb_manager_card(call: types.CallbackQuery):
        await call.answer()

        manager_id = call.data[len(CB_ADMIN_MANAGER):]
        user = get_user_by_id(manager_id)

        if not user:
            return await call.message.edit_text("Менеджер не найден.", reply_markup=admin_main_inline())

        settings = get_manager_settings(manager_id)
        stats = get_manager_stats(manager_id)

        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or "—"
        contact = user.get('contact', '—')
        tg_id = user.get('tg_id', '—')
        registered = user.get('created_at', '—')
        if hasattr(registered, 'strftime'):
            registered = registered.strftime('%d.%m.%Y')

        is_blocked = settings.get('is_blocked', False) if settings else False
        block_reason = settings.get('block_reason', '') if settings else ''

        status_text = "🔒 <b>Заблокирован</b>" if is_blocked else "✅ Активен"
        if block_reason:
            status_text += f"\nПричина: {e(block_reason)}"

        text = (
            f"👤 <b>Менеджер: {e(name)}</b>\n\n"
            f"📱 Контакт: {e(contact)}\n"
            f"🆔 Telegram ID: <code>{tg_id}</code>\n"
            f"📅 Регистрация: {registered}\n\n"
            f"<b>Статус:</b> {status_text}\n\n"
            f"<b>📊 Статистика:</b>\n"
            f"  📋 Всего заявок: {stats.get('total_requests', 0)}\n"
            f"  ⏳ В работе: {stats.get('pending_requests', 0)}\n"
            f"  ✅ Завершено: {stats.get('completed_requests', 0)}\n"
            f"  ❌ Ошибок: {stats.get('failed_requests', 0)}\n"
            f"  📷 Фото: {stats.get('total_photos', 0)}"
        )

        await call.message.edit_text(text, reply_markup=manager_full_card_inline(manager_id, is_blocked))

    dp.register_callback_query_handler(cb_manager_card, lambda c: c.data and c.data.startswith(CB_ADMIN_MANAGER))

    # ==================== БЛОКИРОВКА/РАЗБЛОКИРОВКА ====================

    @require_admin
    async def cb_block_manager(call: types.CallbackQuery):
        manager_id = call.data[len(CB_ADMIN_BLOCK):]

        admin_user = get_user_by_tgid(call.from_user.id)
        admin_id = str(admin_user['id']) if admin_user else None

        block_manager(manager_id, admin_id, "Заблокирован администратором")
        log_activity(admin_id, "manager_blocked", "user", manager_id)

        # Уведомляем менеджера
        user = get_user_by_id(manager_id)
        if user:
            try:
                await bot.send_message(
                    user['tg_id'],
                    "⛔ <b>Ваш аккаунт заблокирован</b>\n\n"
                    "Для разблокировки обратитесь к администратору."
                )
            except Exception:
                pass

        await call.answer("✅ Менеджер заблокирован", show_alert=True)

        # Обновляем карточку
        stats = get_manager_stats(manager_id)
        if user:
            name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            await call.message.edit_text(
                f"🔒 Менеджер <b>{e(name)}</b> заблокирован.",
                reply_markup=manager_full_card_inline(manager_id, True)
            )

    dp.register_callback_query_handler(cb_block_manager, lambda c: c.data and c.data.startswith(CB_ADMIN_BLOCK))

    @require_admin
    async def cb_unblock_manager(call: types.CallbackQuery):
        manager_id = call.data[len(CB_ADMIN_UNBLOCK):]

        admin_user = get_user_by_tgid(call.from_user.id)
        admin_id = str(admin_user['id']) if admin_user else None

        unblock_manager(manager_id)
        log_activity(admin_id, "manager_unblocked", "user", manager_id)

        # Уведомляем менеджера
        user = get_user_by_id(manager_id)
        if user:
            try:
                await bot.send_message(
                    user['tg_id'],
                    "✅ <b>Ваш аккаунт разблокирован</b>\n\n"
                    "Вы можете продолжить работу."
                )
            except Exception:
                pass

        await call.answer("✅ Менеджер разблокирован", show_alert=True)

        if user:
            name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            await call.message.edit_text(
                f"✅ Менеджер <b>{e(name)}</b> разблокирован.",
                reply_markup=manager_full_card_inline(manager_id, False)
            )

    dp.register_callback_query_handler(cb_unblock_manager, lambda c: c.data and c.data.startswith(CB_ADMIN_UNBLOCK))

    # ==================== УДАЛЕНИЕ МЕНЕДЖЕРА ====================

    @require_admin
    async def cb_delete_user(call: types.CallbackQuery):
        manager_id = call.data[len(CB_ADMIN_DELETE_USER):]

        user = get_user_by_id(manager_id)
        if not user:
            return await call.answer("Пользователь не найден", show_alert=True)

        name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()

        await call.message.edit_text(
            f"🗑 <b>Удаление менеджера</b>\n\n"
            f"Вы уверены, что хотите удалить менеджера <b>{e(name)}</b>?\n\n"
            "⚠️ Это действие нельзя отменить!",
            reply_markup=confirm_action_inline("delete_user", manager_id)
        )

    dp.register_callback_query_handler(cb_delete_user, lambda c: c.data and c.data.startswith(CB_ADMIN_DELETE_USER))

    @require_admin
    async def cb_confirm_delete_user(call: types.CallbackQuery):
        manager_id = call.data.replace("confirm_delete_user_", "")

        admin_user = get_user_by_tgid(call.from_user.id)
        admin_id = str(admin_user['id']) if admin_user else None

        if delete_user(manager_id):
            log_activity(admin_id, "user_deleted", "user", manager_id)
            await call.answer("✅ Менеджер удалён", show_alert=True)

            managers = admin_managers()
            await call.message.edit_text(
                f"👥 <b>Менеджеры</b> ({len(managers)})",
                reply_markup=admin_managers_list_inline(managers)
            )
        else:
            await call.answer("❌ Ошибка удаления", show_alert=True)

    dp.register_callback_query_handler(cb_confirm_delete_user, lambda c: c.data and c.data.startswith("confirm_delete_user_"))

    @require_admin
    async def cb_cancel_delete_user(call: types.CallbackQuery):
        manager_id = call.data.replace("cancel_delete_user_", "")

        user = get_user_by_id(manager_id)
        if user:
            settings = get_manager_settings(manager_id)
            is_blocked = settings.get('is_blocked', False) if settings else False
            await call.message.edit_text(
                "Удаление отменено.",
                reply_markup=manager_full_card_inline(manager_id, is_blocked)
            )
        else:
            await call.message.edit_text("Пользователь не найден.", reply_markup=admin_main_inline())

    dp.register_callback_query_handler(cb_cancel_delete_user, lambda c: c.data and c.data.startswith("cancel_delete_user_"))

    # ==================== ВСЕ ЗАЯВКИ ====================

    @require_admin
    async def cmd_all_requests(message: types.Message):
        """Все заявки системы"""
        total = count_all_requests()

        if total == 0:
            return await message.answer("Заявок пока нет.", reply_markup=get_admin_keyboard())

        rows = list_all_requests(0, 15)
        await message.answer(
            f"📦 <b>Все заявки</b> ({total})\n\nВыберите:",
            reply_markup=requests_list_inline(rows, 1, total, 15)
        )

    dp.register_message_handler(cmd_all_requests, commands=["all_requests", "admin_requests"], state="*")
    dp.register_message_handler(cmd_all_requests, lambda m: m.text == BTN_REQS, state="*")

    @require_admin
    async def cb_admin_requests(call: types.CallbackQuery):
        await call.answer()
        total = count_all_requests()

        if total == 0:
            return await call.message.edit_text("Заявок пока нет.", reply_markup=admin_main_inline())

        rows = list_all_requests(0, 15)
        await call.message.edit_text(
            f"📦 <b>Все заявки</b> ({total})",
            reply_markup=requests_list_inline(rows, 1, total, 15)
        )

    dp.register_callback_query_handler(cb_admin_requests, lambda c: c.data == "admin_requests")

    # ==================== ЛОГ / ОТЧЁТЫ / ЭКСПОРТ ====================

    @require_admin
    async def cb_admin_log(call: types.CallbackQuery):
        await call.answer()

        logs = get_activity_log(0, 20)

        if not logs:
            return await call.message.edit_text("Лог пуст.", reply_markup=admin_main_inline())

        lines = []
        for log_item in logs:
            action = log_item.get('action', '—')
            user_name = f"{log_item.get('first_name', '')} {log_item.get('last_name', '')}".strip() or "System"
            created = log_item.get('created_at', '')
            if hasattr(created, 'strftime'):
                created = created.strftime('%d.%m %H:%M')

            lines.append(f"• {created} | {_truncate(user_name, 10)} | {action}")

        text = "📋 <b>Последние действия</b>\n\n" + "\n".join(lines[:15])
        await call.message.edit_text(text, reply_markup=admin_main_inline())

    dp.register_callback_query_handler(cb_admin_log, lambda c: c.data == "admin_log")

    @require_admin
    async def cb_admin_weekly(call: types.CallbackQuery):
        await call.answer()

        weekly = get_requests_this_week()
        leaderboard = get_manager_leaderboard(10)

        days_lines = []
        for item in weekly:
            date = item.get('date')
            if hasattr(date, 'strftime'):
                date = date.strftime('%d.%m')
            count = item.get('count', 0)
            bar = "█" * min(count, 20)
            days_lines.append(f"{date} | {bar} {count}")
        days_text = "\n".join(days_lines) if days_lines else "Нет данных"

        leader_lines = []
        for m in leaderboard:
            name = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip() or "—"
            this_week = m.get('this_week', 0)
            if this_week > 0:
                leader_lines.append(f"  • {_truncate(name, 15)}: {this_week}")
        leader_text = "\n".join(leader_lines[:5]) if leader_lines else "  Нет активности"

        text = (
            "📊 <b>Отчёт за неделю</b>\n\n"
            f"<b>Заявки по дням:</b>\n<code>{days_text}</code>\n\n"
            f"<b>Активные менеджеры:</b>\n{leader_text}"
        )

        await call.message.edit_text(text, reply_markup=admin_main_inline())

    dp.register_callback_query_handler(cb_admin_weekly, lambda c: c.data == "admin_weekly")

    @require_admin
    async def cb_admin_export(call: types.CallbackQuery):
        await call.answer("Формирую экспорт...")

        data = admin_export_all()

        if not data:
            return await call.message.answer("Нет данных для экспорта.")

        export = {
            "exported_at": datetime.now().isoformat(),
            "total_requests": len(data),
            "requests": [convert_uuids_to_strings(dict(r)) for r in data]
        }

        json_str = json.dumps(export, ensure_ascii=False, indent=2, default=str)
        file_bytes = io.BytesIO(json_str.encode('utf-8'))
        file_bytes.name = f"export_{datetime.now().strftime('%Y%m%d_%H%M')}.json"

        await call.message.answer_document(
            types.InputFile(file_bytes, filename=file_bytes.name),
            caption=f"📤 Экспорт ({len(data)} заявок)"
        )

    dp.register_callback_query_handler(cb_admin_export, lambda c: c.data == "admin_export")

    # ==================== НАВИГАЦИЯ ====================

    @require_admin
    async def cb_admin_back(call: types.CallbackQuery):
        await call.answer()
        stats = get_overall_stats()
        pending = count_pending_registrations()

        pending_text = f"\n\n🔔 <b>Ожидают одобрения: {pending}</b>" if pending > 0 else ""

        text = (
            "📊 <b>Панель администратора</b>\n\n"
            f"👥 Пользователей: <b>{stats.get('total_users', 0)}</b>\n"
            f"👔 Менеджеров: <b>{stats.get('total_managers', 0)}</b>\n"
            f"📋 Заявок: <b>{stats.get('total_requests', 0)}</b>"
            f"{pending_text}"
        )

        await call.message.edit_text(text, reply_markup=admin_main_inline())

    dp.register_callback_query_handler(cb_admin_back, lambda c: c.data == "admin_back")

    # Обработчик для noop
    dp.register_callback_query_handler(lambda c: c.answer(), lambda c: c.data == "noop")
