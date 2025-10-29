from aiogram import types
from aiogram.dispatcher import FSMContext
from functools import wraps

from app.config import ADMIN_PASSWORD
from app.constants import (
    GUEST_CMDS, MANAGER_CMDS, ADMIN_CMDS, DEBUG_CMDS,
    BTN_REG, BTN_ADMIN_LOGIN, BTN_NEW, BTN_MY, BTN_RESET,
    BTN_PANEL, BTN_USERS, BTN_REQS, BTN_LOGOUT,
)
from app.db import (
    init_db, get_user_by_tgid, get_mode, set_mode,
    admin_counts, admin_users, list_all_requests, count_all_requests,
)
from app.states import RegForm, AdminLogin

def register(dp, bot):
    # ===== helpers =====
    async def set_scope_cmds(chat_id: int, mode: str, is_registered: bool, language_code: str = None):
        if mode == "admin":
            cmds = ADMIN_CMDS[:]
        elif is_registered:
            cmds = MANAGER_CMDS[:]
        else:
            cmds = GUEST_CMDS[:]
        cmds += DEBUG_CMDS
        lang = (language_code or "").split("-")[0] or None
        await bot.set_my_commands(cmds, scope=types.BotCommandScopeChat(chat_id), language_code=lang)

    def require_admin(handler):
        @wraps(handler)
        async def wrapper(message: types.Message, *args, **kwargs):
            if get_mode(message.from_user.id) != "admin":
                return await message.answer("Доступ запрещён. Войдите как админ.")
            kwargs.pop("state", None)
            kwargs.pop("raw_state", None)
            return await handler(message, *args, **kwargs)
        return wrapper

    # ===== /start =====
    async def cmd_start(message: types.Message):
        init_db()
        user = get_user_by_tgid(message.from_user.id)
        is_reg = bool(user)
        mode = get_mode(message.from_user.id)
        if is_reg and mode != "admin":
            set_mode(message.from_user.id, "manager"); mode = "manager"
        await set_scope_cmds(message.chat.id, mode, is_reg, message.from_user.language_code)

        if mode == "admin":
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
            await message.answer("Здравствуйте! Режим: <b>Админ</b>", reply_markup=kb)
        elif is_reg:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            await message.answer("Здравствуйте! Режим: <b>Менеджер</b>", reply_markup=kb)
        else:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_REG, BTN_ADMIN_LOGIN)
            await message.answer("Здравствуйте! Вы ещё не зарегистрированы.", reply_markup=kb)

    dp.register_message_handler(cmd_start, commands=["start"], state="*")

    # ===== register flow =====
    async def cmd_register(message: types.Message):
        if get_mode(message.from_user.id) == "admin":
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
            return await message.answer("Сейчас включён режим админа. Нажмите «🚪 Выйти из админки».", reply_markup=kb)

        user = get_user_by_tgid(message.from_user.id)
        if user:
            set_mode(message.from_user.id, "manager")
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            return await message.answer("Вы уже зарегистрированы.", reply_markup=kb)

        await RegForm.first_name.set()
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_REG, BTN_ADMIN_LOGIN)
        await message.answer("Введите ваше <b>имя</b>:", reply_markup=kb)

    dp.register_message_handler(cmd_register, commands=["register"], state="*")
    dp.register_message_handler(cmd_register, lambda m: m.text == BTN_REG, state="*")

    from app.db import create_user

    async def reg_first_name(message: types.Message, state: FSMContext):
        await state.update_data(first_name=message.text.strip()); await RegForm.next()
        await message.answer("Введите вашу <b>фамилию</b>:")
    async def reg_last_name(message: types.Message, state: FSMContext):
        await state.update_data(last_name=message.text.strip()); await RegForm.next()
        await message.answer("Введите ваш <b>возраст</b> (числом):")
    async def reg_age(message: types.Message, state: FSMContext):
        txt = message.text.strip()
        if not txt.isdigit() or not (0 < int(txt) < 120):
            return await message.answer("Возраст должен быть числом 1–119. Попробуйте снова:")
        await state.update_data(age=int(txt)); await RegForm.next()
        await message.answer("Укажите ваш <b>контакт</b> (телефон/email/@username):")
    async def reg_contact(message: types.Message, state: FSMContext):
        data = await state.get_data()
        try:
            create_user(
                tg_id=message.from_user.id,
                first_name=data.get("first_name"),
                last_name=data.get("last_name"),
                contact=message.text.strip(),
            )
            set_mode(message.from_user.id, "manager")
            await state.finish()
            await set_scope_cmds(message.chat.id, "manager", True, message.from_user.language_code)
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            await message.answer("✅ Регистрация завершена!", reply_markup=kb)
        except Exception:
            await state.finish()
            await message.answer("⚠️ Ошибка сохранения. Попробуйте ещё раз /register.")

    dp.register_message_handler(reg_first_name, state=RegForm.first_name)
    dp.register_message_handler(reg_last_name, state=RegForm.last_name)
    dp.register_message_handler(reg_age, state=RegForm.age)
    dp.register_message_handler(reg_contact, state=RegForm.contact)

    # ===== admin login/logout =====
    async def cmd_admin_login(message: types.Message, state: FSMContext):
        if get_mode(message.from_user.id) == "admin":
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
            return await message.answer("Вы уже в админке.", reply_markup=kb)
        await AdminLogin.password.set()
        await message.answer("Введите пароль администратора:")
    dp.register_message_handler(cmd_admin_login, commands=["admin_login"], state="*")
    dp.register_message_handler(cmd_admin_login, lambda m: m.text == BTN_ADMIN_LOGIN, state="*")

    async def admin_check_pass(message: types.Message, state: FSMContext):
        if message.text.strip() != ADMIN_PASSWORD:
            await state.finish(); return await message.answer("Пароль неверный.")
        set_mode(message.from_user.id, "admin")
        await state.finish(); await set_scope_cmds(message.chat.id, "admin", True, message.from_user.language_code)
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
        await message.answer("Готово. Режим админа включён.", reply_markup=kb)
    dp.register_message_handler(admin_check_pass, state=AdminLogin.password)

    async def cmd_logout(message: types.Message):
        if get_mode(message.from_user.id) != "admin":
            return await message.answer("Сейчас не режим админа.")
        set_mode(message.from_user.id, "manager"); await set_scope_cmds(message.chat.id, "manager", True, message.from_user.language_code)
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("Вы вышли из админки. Вернулся режим менеджера.", reply_markup=kb)
    dp.register_message_handler(cmd_logout, commands=["logout"], state="*")
    dp.register_message_handler(cmd_logout, lambda m: m.text == BTN_LOGOUT, state="*")

    # ===== admin panel =====
    @require_admin
    async def cmd_admin_panel(message: types.Message):
        users_count, reqs_count = admin_counts()
        await message.answer(
            "<b>Админ-панель</b>\n\n"
            f"Пользователей: <b>{users_count}</b>\n"
            f"Заявок: <b>{reqs_count}</b>\n\n"
            "Команды:\n"
            "• 👥 Пользователи — список\n"
            "• 📦 Заявки — список\n"
            "• /export_request <id> — экспорт заявки в JSON\n"
            "• /export_all — экспорт всех заявок (ZIP)\n"
            "• 🚪 Выйти из админки"
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
            lines.append(
                f"#{u['id']}: <b>{e(name)}</b> | {e(u.get('contact'))} | роль: {e(u.get('role'))} | "
                f"tg_id: {e(u.get('tg_id'))} | {e(u.get('created_at'))}"
            )
        for part in chunks("\n".join(lines)):
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
        ikb = InlineKeyboardMarkup(row_width=1)
        for r in rows:
            title = f"#{r['id']} — {r.get('client_name') or 'Без имени'}"
            ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))
        await message.answer("Админ: заявки — выберите:", reply_markup=ikb)
    dp.register_message_handler(cmd_admin_requests, commands=["admin_requests"], state="*")
    dp.register_message_handler(cmd_admin_requests, lambda m: m.text == BTN_REQS, state="*")
