from aiogram import types
from aiogram.dispatcher import FSMContext
from functools import wraps
import time

from app.config import ADMIN_PASSWORD
from app.constants import (
    GUEST_CMDS, MANAGER_CMDS, ADMIN_CMDS, DEBUG_CMDS,
    BTN_REG, BTN_ADMIN_LOGIN, BTN_NEW, BTN_MY, BTN_RESET,
    BTN_PANEL, BTN_USERS, BTN_REQS, BTN_LOGOUT,
    MSG_WELCOME_GUEST, MSG_WELCOME_MANAGER, MSG_WELCOME_ADMIN, MSG_REG_COMPLETE,
)
from app.db import (
    init_db, get_user_by_tgid, get_mode, set_mode,
    admin_counts, admin_users, list_all_requests, count_all_requests,
)
from app.states import RegForm, AdminLogin


def register(dp, bot):

    # ==================== HELPERS ====================

    async def set_scope_cmds(chat_id: int, mode: str, is_registered: bool, language_code: str = None):
        """Установка команд меню для пользователя"""
        if mode == "admin":
            cmds = ADMIN_CMDS[:]
        elif is_registered:
            cmds = MANAGER_CMDS[:]
        else:
            cmds = GUEST_CMDS[:]

        cmds += DEBUG_CMDS
        lang = (language_code or "").split("-")[0] or None

        try:
            await bot.set_my_commands(cmds, scope=types.BotCommandScopeChat(chat_id), language_code=lang)
        except Exception:
            pass  # Игнорируем ошибки установки команд

    def require_admin(handler):
        """Декоратор для проверки прав администратора"""
        @wraps(handler)
        async def wrapper(message: types.Message, *args, **kwargs):
            if get_mode(message.from_user.id) != "admin":
                return await message.answer("⛔ Доступ запрещён. Требуются права администратора.")
            kwargs.pop("state", None)
            kwargs.pop("raw_state", None)
            return await handler(message, *args, **kwargs)
        return wrapper

    # ==================== /start ====================

    async def cmd_start(message: types.Message):
        init_db()
        user = get_user_by_tgid(message.from_user.id)
        is_reg = bool(user)
        mode = get_mode(message.from_user.id)

        if is_reg and mode != "admin":
            set_mode(message.from_user.id, "manager")
            mode = "manager"

        await set_scope_cmds(message.chat.id, mode, is_reg, message.from_user.language_code)

        if mode == "admin":
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_PANEL)
            kb.add(BTN_USERS, BTN_REQS)
            kb.add(BTN_LOGOUT)
            await message.answer(MSG_WELCOME_ADMIN, reply_markup=kb)
        elif is_reg:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW)
            kb.add(BTN_MY)
            kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            await message.answer(MSG_WELCOME_MANAGER, reply_markup=kb)
        else:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_REG, BTN_ADMIN_LOGIN)
            await message.answer(MSG_WELCOME_GUEST, reply_markup=kb)

    dp.register_message_handler(cmd_start, commands=["start"], state="*")

    # ==================== /ping ====================

    async def cmd_ping(message: types.Message):
        """Проверка работоспособности бота"""
        start_time = time.time()
        msg = await message.answer("🔄 Проверка...")
        latency = round((time.time() - start_time) * 1000)

        await msg.edit_text(
            f"✅ <b>Бот работает</b>\n\n"
            f"Задержка: {latency} мс\n"
            f"Ваш ID: <code>{message.from_user.id}</code>"
        )

    dp.register_message_handler(cmd_ping, commands=["ping"], state="*")

    # ==================== /myid ====================

    async def cmd_myid(message: types.Message):
        """Показать ID пользователя"""
        user = message.from_user
        await message.answer(
            f"👤 <b>Информация о пользователе</b>\n\n"
            f"ID: <code>{user.id}</code>\n"
            f"Username: @{user.username or '—'}\n"
            f"Имя: {user.first_name or '—'} {user.last_name or ''}"
        )

    dp.register_message_handler(cmd_myid, commands=["myid"], state="*")

    # ==================== РЕГИСТРАЦИЯ ====================

    async def cmd_register(message: types.Message):
        if get_mode(message.from_user.id) == "admin":
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_PANEL)
            kb.add(BTN_USERS, BTN_REQS)
            kb.add(BTN_LOGOUT)
            return await message.answer(
                "Вы находитесь в режиме администратора.\n"
                "Для регистрации сначала выйдите из админки.",
                reply_markup=kb
            )

        user = get_user_by_tgid(message.from_user.id)
        if user:
            set_mode(message.from_user.id, "manager")
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW)
            kb.add(BTN_MY)
            kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            return await message.answer("Вы уже зарегистрированы в системе.", reply_markup=kb)

        await RegForm.first_name.set()
        await message.answer(
            "📋 <b>Регистрация</b>\n\n"
            "Для работы с системой необходимо пройти регистрацию.\n\n"
            "Введите ваше <b>имя</b>:",
            reply_markup=types.ReplyKeyboardRemove()
        )

    dp.register_message_handler(cmd_register, commands=["register"], state="*")
    dp.register_message_handler(cmd_register, lambda m: m.text == BTN_REG, state="*")

    from app.db import create_user

    async def reg_first_name(message: types.Message, state: FSMContext):
        text = message.text.strip()
        if len(text) < 2:
            return await message.answer("❌ Имя слишком короткое. Введите ещё раз:")
        await state.update_data(first_name=text)
        await RegForm.next()
        await message.answer("Введите вашу <b>фамилию</b>:")

    async def reg_last_name(message: types.Message, state: FSMContext):
        text = message.text.strip()
        if len(text) < 2:
            return await message.answer("❌ Фамилия слишком короткая. Введите ещё раз:")
        await state.update_data(last_name=text)
        await RegForm.next()
        await message.answer("Введите ваш <b>возраст</b>:")

    async def reg_age(message: types.Message, state: FSMContext):
        txt = message.text.strip()
        if not txt.isdigit() or not (18 <= int(txt) <= 100):
            return await message.answer("❌ Укажите корректный возраст (18–100):")
        await state.update_data(age=int(txt))
        await RegForm.next()
        await message.answer("Введите ваш <b>контакт</b> (телефон или email):")

    async def reg_contact(message: types.Message, state: FSMContext):
        text = message.text.strip()
        if len(text) < 5:
            return await message.answer("❌ Контакт слишком короткий. Введите телефон или email:")

        data = await state.get_data()
        try:
            create_user(
                tg_id=message.from_user.id,
                first_name=data.get("first_name"),
                last_name=data.get("last_name"),
                contact=text,
            )
            set_mode(message.from_user.id, "manager")
            await state.finish()
            await set_scope_cmds(message.chat.id, "manager", True, message.from_user.language_code)

            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW)
            kb.add(BTN_MY)
            kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            await message.answer(MSG_REG_COMPLETE, reply_markup=kb)

        except Exception:
            await state.finish()
            await message.answer("⚠️ Ошибка сохранения данных. Попробуйте ещё раз: /register")

    dp.register_message_handler(reg_first_name, state=RegForm.first_name)
    dp.register_message_handler(reg_last_name, state=RegForm.last_name)
    dp.register_message_handler(reg_age, state=RegForm.age)
    dp.register_message_handler(reg_contact, state=RegForm.contact)

    # ==================== АВТОРИЗАЦИЯ АДМИНА ====================

    async def cmd_admin_login(message: types.Message, state: FSMContext):
        if get_mode(message.from_user.id) == "admin":
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_PANEL)
            kb.add(BTN_USERS, BTN_REQS)
            kb.add(BTN_LOGOUT)
            return await message.answer("Вы уже авторизованы как администратор.", reply_markup=kb)

        await AdminLogin.password.set()
        await message.answer(
            "🔐 <b>Авторизация администратора</b>\n\n"
            "Введите пароль:",
            reply_markup=types.ReplyKeyboardRemove()
        )

    dp.register_message_handler(cmd_admin_login, commands=["admin_login"], state="*")
    dp.register_message_handler(cmd_admin_login, lambda m: m.text == BTN_ADMIN_LOGIN, state="*")

    async def admin_check_pass(message: types.Message, state: FSMContext):
        # Удаляем сообщение с паролем для безопасности
        try:
            await message.delete()
        except Exception:
            pass

        if message.text.strip() != ADMIN_PASSWORD:
            await state.finish()
            return await message.answer("❌ Неверный пароль.")

        set_mode(message.from_user.id, "admin")
        await state.finish()
        await set_scope_cmds(message.chat.id, "admin", True, message.from_user.language_code)

        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_PANEL)
        kb.add(BTN_USERS, BTN_REQS)
        kb.add(BTN_LOGOUT)
        await message.answer("✅ Авторизация успешна. Режим администратора включён.", reply_markup=kb)

    dp.register_message_handler(admin_check_pass, state=AdminLogin.password)

    async def cmd_logout(message: types.Message):
        if get_mode(message.from_user.id) != "admin":
            return await message.answer("Вы не авторизованы как администратор.")

        set_mode(message.from_user.id, "manager")
        await set_scope_cmds(message.chat.id, "manager", True, message.from_user.language_code)

        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW)
        kb.add(BTN_MY)
        kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("✅ Вы вышли из режима администратора.", reply_markup=kb)

    dp.register_message_handler(cmd_logout, commands=["logout"], state="*")
    dp.register_message_handler(cmd_logout, lambda m: m.text == BTN_LOGOUT, state="*")

    # ==================== АДМИН-ПАНЕЛЬ ====================

    @require_admin
    async def cmd_admin_panel(message: types.Message):
        users_count, reqs_count = admin_counts()
        await message.answer(
            "📊 <b>Панель администратора</b>\n\n"
            f"👥 Пользователей: <b>{users_count}</b>\n"
            f"📋 Заявок: <b>{reqs_count}</b>\n\n"
            "<b>Команды:</b>\n"
            "• 👥 Пользователи — список всех пользователей\n"
            "• 📦 Все заявки — список всех заявок\n"
            "• /export_request <id> — экспорт заявки\n"
            "• /export_all — экспорт всех заявок\n"
            "• 🚪 Выход — выйти из режима админа"
        )

    dp.register_message_handler(cmd_admin_panel, commands=["admin_panel"], state="*")
    dp.register_message_handler(cmd_admin_panel, lambda m: m.text == BTN_PANEL, state="*")

    @require_admin
    async def cmd_admin_users(message: types.Message):
        rows = admin_users()
        if not rows:
            return await message.answer("Пользователей пока нет.")

        from app.utils import e, chunks
        lines = []
        for u in rows:
            name = f"{(u.get('first_name') or '')} {(u.get('last_name') or '')}".strip() or "—"
            role = u.get('role', 'guest')
            role_emoji = "👑" if role == "admin" else "👤"
            lines.append(
                f"{role_emoji} <b>{e(name)}</b>\n"
                f"   📱 {e(u.get('contact') or '—')} | ID: <code>{u.get('tg_id')}</code>"
            )

        for part in chunks("\n\n".join(lines)):
            await message.answer(part)

    dp.register_message_handler(cmd_admin_users, commands=["admin_users"], state="*")
    dp.register_message_handler(cmd_admin_users, lambda m: m.text == BTN_USERS, state="*")

    @require_admin
    async def cmd_admin_requests(message: types.Message):
        total = count_all_requests()
        if total == 0:
            return await message.answer("Заявок пока нет.")

        rows = list_all_requests(0, 20)
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        from app.constants import CB_OPEN
        from app.keyboards import _truncate

        ikb = InlineKeyboardMarkup(row_width=1)
        for r in rows:
            company = _truncate(r.get('company_name') or '', 20)
            client = _truncate(r.get('client_name') or '', 15)

            if company and client:
                title = f"🏢 {company} • {client}"
            elif company:
                title = f"🏢 {company}"
            elif client:
                title = f"👤 {client}"
            else:
                req_id = str(r['id'])[:8]
                title = f"📋 Заявка {req_id}..."

            ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))

        await message.answer(f"📦 <b>Все заявки</b> ({total})\n\nВыберите заявку:", reply_markup=ikb)

    dp.register_message_handler(cmd_admin_requests, commands=["admin_requests"], state="*")
    dp.register_message_handler(cmd_admin_requests, lambda m: m.text == BTN_REQS, state="*")
