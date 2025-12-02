from aiogram import types
from aiogram.dispatcher import FSMContext
from functools import wraps
import time

from app.config import ADMIN_PASSWORD, get_admin_chat_ids
from app.constants import (
    GUEST_CMDS, MANAGER_CMDS, ADMIN_CMDS, DEBUG_CMDS,
    BTN_REG, BTN_ADMIN_LOGIN, BTN_NEW, BTN_MY, BTN_ARCHIVE, BTN_RESET,
    BTN_PANEL, BTN_STATS, BTN_MANAGERS, BTN_PENDING, BTN_REQS, BTN_LOGOUT,
    MSG_WELCOME_GUEST, MSG_WELCOME_MANAGER, MSG_WELCOME_ADMIN,
    MSG_BLOCKED_USER, MSG_PENDING_APPROVAL, MSG_NEW_REGISTRATION_ADMIN,
)
from app.db import (
    init_db, get_user_by_tgid, get_mode, set_mode,
    is_manager_blocked, is_user_approved, get_user_approval_status,
    create_admin_notification, count_pending_registrations,
    log_activity, create_user,
)
from app.states import RegForm, AdminLogin
from app.keyboards import pending_approval_inline


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
        pending = count_pending_registrations()
        pending_text = f"⏳ Ожидают ({pending})" if pending > 0 else BTN_PENDING

        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_PANEL, BTN_STATS)
        kb.add(BTN_MANAGERS, pending_text)
        kb.add(BTN_REQS, BTN_LOGOUT)
        return kb

    def get_guest_keyboard():
        """Клавиатура гостя"""
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_REG, BTN_ADMIN_LOGIN)
        return kb

    async def notify_admins_new_registration(user_data: dict, tg_id: int):
        """Уведомить админов о новой регистрации"""
        name = f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        contact = user_data.get('contact', '—')

        text = MSG_NEW_REGISTRATION_ADMIN.format(
            name=name,
            contact=contact,
            tg_id=tg_id
        )

        # Получаем ID админов из конфига
        admin_ids = get_admin_chat_ids()

        for admin_id in admin_ids:
            try:
                user = get_user_by_tgid(tg_id)
                if user:
                    await bot.send_message(
                        admin_id,
                        text,
                        reply_markup=pending_approval_inline(str(user['id']))
                    )
            except Exception:
                pass

    # ==================== /start ====================

    async def cmd_start(message: types.Message):
        init_db()
        user = get_user_by_tgid(message.from_user.id)
        is_reg = bool(user)
        mode = get_mode(message.from_user.id)

        # Проверка статуса одобрения
        if is_reg and mode != "admin":
            approval_status = get_user_approval_status(message.from_user.id)

            if approval_status == "pending":
                return await message.answer(MSG_PENDING_APPROVAL, reply_markup=get_guest_keyboard())

            if approval_status == "rejected":
                return await message.answer(
                    "❌ <b>Ваша заявка была отклонена</b>\n\n"
                    "Вы можете подать новую заявку на регистрацию.",
                    reply_markup=get_guest_keyboard()
                )

            # Проверка блокировки (только для одобренных)
            if is_manager_blocked(message.from_user.id):
                kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
                kb.add(BTN_ADMIN_LOGIN)
                return await message.answer(MSG_BLOCKED_USER, reply_markup=kb)

            set_mode(message.from_user.id, "manager")
            mode = "manager"

        await set_scope_cmds(message.chat.id, mode, is_reg and is_user_approved(message.from_user.id), message.from_user.language_code)

        if mode == "admin":
            await message.answer(MSG_WELCOME_ADMIN, reply_markup=get_admin_keyboard())
        elif is_reg and is_user_approved(message.from_user.id):
            await message.answer(MSG_WELCOME_MANAGER, reply_markup=get_manager_keyboard())
        else:
            await message.answer(MSG_WELCOME_GUEST, reply_markup=get_guest_keyboard())

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

        extra_info = ""
        if db_user:
            approval = get_user_approval_status(user.id)
            if approval == "pending":
                extra_info = "\n⏳ Статус: Ожидает одобрения"
            elif approval == "rejected":
                extra_info = "\n❌ Статус: Отклонён"
            elif is_manager_blocked(user.id):
                extra_info = "\n⛔ Статус: Заблокирован"
            else:
                extra_info = "\n✅ Статус: Активен"

        await message.answer(
            f"👤 <b>Информация о пользователе</b>\n\n"
            f"🆔 ID: <code>{user.id}</code>\n"
            f"👤 Username: @{user.username or '—'}\n"
            f"📝 Имя: {user.first_name or '—'} {user.last_name or ''}\n"
            f"🔑 Роль: {status}{extra_info}"
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
            approval = get_user_approval_status(message.from_user.id)

            if approval == "pending":
                return await message.answer(MSG_PENDING_APPROVAL, reply_markup=get_guest_keyboard())

            if approval == "rejected":
                # Разрешаем повторную регистрацию
                pass
            elif is_manager_blocked(message.from_user.id):
                kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
                kb.add(BTN_ADMIN_LOGIN)
                return await message.answer(MSG_BLOCKED_USER, reply_markup=kb)
            else:
                set_mode(message.from_user.id, "manager")
                return await message.answer("Вы уже зарегистрированы в системе.", reply_markup=get_manager_keyboard())

        await RegForm.first_name.set()
        await message.answer(
            "📋 <b>Регистрация менеджера</b>\n\n"
            "Для работы с системой необходимо пройти регистрацию.\n"
            "После заполнения формы ваша заявка будет отправлена на рассмотрение администратору.\n\n"
            "Введите ваше <b>имя</b>:",
            reply_markup=types.ReplyKeyboardRemove()
        )

    dp.register_message_handler(cmd_register, commands=["register"], state="*")
    dp.register_message_handler(cmd_register, lambda m: m.text == BTN_REG, state="*")

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
            user_id = create_user(
                tg_id=message.from_user.id,
                first_name=data.get("first_name"),
                last_name=data.get("last_name"),
                contact=text,
            )

            await state.finish()

            if user_id:
                # Логируем регистрацию
                log_activity(user_id, "registration_submitted", "user", user_id)

                # Создаём уведомление для админа
                name = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()
                create_admin_notification(
                    "new_registration",
                    f"Новая заявка: {name}",
                    f"Контакт: {text}, TG ID: {message.from_user.id}",
                    "user",
                    user_id
                )

                # Уведомляем админов
                await notify_admins_new_registration(
                    {"first_name": data.get("first_name"), "last_name": data.get("last_name"), "contact": text},
                    message.from_user.id
                )

            await message.answer(MSG_PENDING_APPROVAL, reply_markup=get_guest_keyboard())

        except Exception as e:
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

    # ==================== ОЖИДАЮЩИЕ ОДОБРЕНИЯ (по кнопке) ====================

    async def cmd_pending(message: types.Message):
        if get_mode(message.from_user.id) != "admin":
            return await message.answer("⛔ Требуются права администратора.")

        from app.db import list_pending_registrations
        from app.keyboards import pending_list_inline

        pending = list_pending_registrations()
        count = len(pending)

        await message.answer(
            f"⏳ <b>Ожидают одобрения</b> ({count})\n\n"
            "Выберите заявку для рассмотрения:",
            reply_markup=pending_list_inline(pending)
        )

    dp.register_message_handler(cmd_pending, lambda m: m.text and m.text.startswith("⏳"), state="*")
    dp.register_message_handler(cmd_pending, lambda m: m.text == BTN_PENDING, state="*")
