from aiogram import types
from aiogram.dispatcher import FSMContext
from functools import wraps
import time

from app.config import ADMIN_PASSWORD
from app.constants import (
    GUEST_CMDS, MANAGER_CMDS, ADMIN_CMDS, DEBUG_CMDS,
    BTN_REG, BTN_ADMIN_LOGIN, BTN_NEW, BTN_MY, BTN_ARCHIVE, BTN_RESET,
    BTN_PANEL, BTN_STATS, BTN_MANAGERS, BTN_USERS, BTN_REQS, BTN_LOGOUT,
    MSG_WELCOME_GUEST, MSG_WELCOME_MANAGER, MSG_WELCOME_ADMIN, MSG_REG_COMPLETE,
    MSG_BLOCKED_USER,
)
from app.db import (
    init_db, get_user_by_tgid, get_mode, set_mode,
    admin_counts, admin_users, list_all_requests, count_all_requests,
    is_manager_blocked, log_activity,
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
            pass

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

    def get_manager_keyboard():
        """Клавиатура менеджера"""
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW)
        kb.add(BTN_MY, BTN_ARCHIVE)
        kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        return kb

    def get_admin_keyboard():
        """Клавиатура администратора"""
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_PANEL, BTN_STATS)
        kb.add(BTN_MANAGERS, BTN_REQS)
        kb.add(BTN_LOGOUT)
        return kb

    # ==================== /start ====================

    async def cmd_start(message: types.Message):
        init_db()
        user = get_user_by_tgid(message.from_user.id)
        is_reg = bool(user)
        mode = get_mode(message.from_user.id)

        # Проверка блокировки
        if is_reg and mode == "manager" and is_manager_blocked(message.from_user.id):
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_ADMIN_LOGIN)
            return await message.answer(MSG_BLOCKED_USER, reply_markup=kb)

        if is_reg and mode != "admin":
            set_mode(message.from_user.id, "manager")
            mode = "manager"

        await set_scope_cmds(message.chat.id, mode, is_reg, message.from_user.language_code)

        if mode == "admin":
            await message.answer(MSG_WELCOME_ADMIN, reply_markup=get_admin_keyboard())
        elif is_reg:
            await message.answer(MSG_WELCOME_MANAGER, reply_markup=get_manager_keyboard())
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
        db_user = get_user_by_tgid(user.id)
        mode = get_mode(user.id)

        status = "Администратор" if mode == "admin" else "Менеджер" if db_user else "Гость"
        blocked = ""
        if db_user and is_manager_blocked(user.id):
            blocked = "\n⚠️ Статус: Заблокирован"

        await message.answer(
            f"👤 <b>Информация о пользователе</b>\n\n"
            f"🆔 ID: <code>{user.id}</code>\n"
            f"👤 Username: @{user.username or '—'}\n"
            f"📝 Имя: {user.first_name or '—'} {user.last_name or ''}\n"
            f"🔑 Роль: {status}{blocked}"
        )

    dp.register_message_handler(cmd_myid, commands=["myid"], state="*")

    # ==================== РЕГИСТРАЦИЯ ====================

    async def cmd_register(message: types.Message):
        if get_mode(message.from_user.id) == "admin":
            return await message.answer(
                "Вы находитесь в режиме администратора.\n"
                "Для регистрации сначала выйдите из админки.",
                reply_markup=get_admin_keyboard()
            )

        user = get_user_by_tgid(message.from_user.id)
        if user:
            # Проверка блокировки
            if is_manager_blocked(message.from_user.id):
                kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
                kb.add(BTN_ADMIN_LOGIN)
                return await message.answer(MSG_BLOCKED_USER, reply_markup=kb)

            set_mode(message.from_user.id, "manager")
            return await message.answer("Вы уже зарегистрированы в системе.", reply_markup=get_manager_keyboard())

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

            # Логируем регистрацию
            user = get_user_by_tgid(message.from_user.id)
            if user:
                log_activity(str(user["id"]), "user_registered", "user", str(user["id"]))

            await message.answer(MSG_REG_COMPLETE, reply_markup=get_manager_keyboard())

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
            return await message.answer("Вы уже авторизованы как администратор.", reply_markup=get_admin_keyboard())

        await AdminLogin.password.set()
        await message.answer(
            "🔐 <b>Авторизация администратора</b>\n\n"
            "Введите пароль:",
            reply_markup=types.ReplyKeyboardRemove()
        )

    dp.register_message_handler(cmd_admin_login, commands=["admin_login"], state="*")
    dp.register_message_handler(cmd_admin_login, lambda m: m.text == BTN_ADMIN_LOGIN, state="*")

    async def admin_check_pass(message: types.Message, state: FSMContext):
        # Удаляем сообщение с паролем
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

        # Логируем вход
        user = get_user_by_tgid(message.from_user.id)
        if user:
            log_activity(str(user["id"]), "admin_login", "user", str(user["id"]))

        await message.answer("✅ Авторизация успешна. Режим администратора включён.", reply_markup=get_admin_keyboard())

    dp.register_message_handler(admin_check_pass, state=AdminLogin.password)

    async def cmd_logout(message: types.Message):
        if get_mode(message.from_user.id) != "admin":
            return await message.answer("Вы не авторизованы как администратор.")

        set_mode(message.from_user.id, "manager")
        await set_scope_cmds(message.chat.id, "manager", True, message.from_user.language_code)

        # Логируем выход
        user = get_user_by_tgid(message.from_user.id)
        if user:
            log_activity(str(user["id"]), "admin_logout", "user", str(user["id"]))

        await message.answer("✅ Вы вышли из режима администратора.", reply_markup=get_manager_keyboard())

    dp.register_message_handler(cmd_logout, commands=["logout"], state="*")
    dp.register_message_handler(cmd_logout, lambda m: m.text == BTN_LOGOUT, state="*")
