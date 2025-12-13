"""
Telegram Bot for AutoSites
Handles:
- Registration flow
- Opening Mini App
- Admin login by password
- Notifications
- Additional services offer after request creation
- Manager feedback notifications
"""
import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

import db

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Environment
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://your-webapp-url.com")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")
BOT_WEBHOOK_PORT = int(os.getenv("BOT_WEBHOOK_PORT", "8081"))

# Bot setup
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Additional services definitions
ADDITIONAL_SERVICES = [
    {
        "code": "logo_design",
        "name": "Разработка логотипа",
        "icon": "🎨",
        "description": "Профессиональный дизайн логотипа"
    },
    {
        "code": "seo_promotion",
        "name": "SEO продвижение",
        "icon": "📈",
        "description": "Комплексное продвижение в поисковиках"
    },
    {
        "code": "business_automation",
        "name": "Автоматизация бизнеса",
        "icon": "⚙️",
        "description": "CRM, чат-боты, интеграции"
    }
]


# States
class Registration(StatesGroup):
    waiting_contact = State()


class AdminLogin(StatesGroup):
    waiting_password = State()


# Keyboards
def get_main_keyboard(is_approved: bool = False, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Main keyboard with WebApp button."""
    buttons = []

    if is_approved or is_admin:
        # Добавляем параметр admin=1 если админ
        url = f"{WEBAPP_URL}{'?admin=1' if is_admin else ''}"
        buttons.append([
            InlineKeyboardButton(
                text="🚀 Открыть приложение",
                web_app=WebAppInfo(url=url)
            )
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_registration_keyboard() -> InlineKeyboardMarkup:
    """Registration keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Зарегистрироваться", callback_data="register")]
    ])


def get_admin_pending_keyboard(user_tg_id: int) -> InlineKeyboardMarkup:
    """Admin keyboard for pending registration."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"approve_{user_tg_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_tg_id}")
        ]
    ])


def get_additional_services_keyboard(request_id: str) -> InlineKeyboardMarkup:
    """Keyboard for additional services offer."""
    buttons = []
    for service in ADDITIONAL_SERVICES:
        buttons.append([
            InlineKeyboardButton(
                text=f"{service['icon']} {service['name']}",
                callback_data=f"add_service_{request_id}_{service['code']}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="⏭ Пропустить", callback_data=f"skip_services_{request_id}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_more_services_keyboard(request_id: str, selected_codes: list) -> InlineKeyboardMarkup:
    """Keyboard to add more services after selecting one."""
    buttons = []
    for service in ADDITIONAL_SERVICES:
        if service['code'] not in selected_codes:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{service['icon']} {service['name']}",
                    callback_data=f"add_service_{request_id}_{service['code']}"
                )
            ])

    if buttons:
        buttons.append([
            InlineKeyboardButton(text="✅ Готово", callback_data=f"finish_services_{request_id}")
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None


# Handlers
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    """Handle /start command with optional invite code deep link."""
    await state.clear()

    tg_id = message.from_user.id
    user = await db.get_user_by_tg_id(tg_id)

    # Parse deep link parameter (format: /start invite_XXXXX)
    args = message.text.split(maxsplit=1)
    invite_code = None
    if len(args) > 1 and args[1].startswith("invite_"):
        invite_code = args[1].replace("invite_", "").strip().upper()

    if not user:
        # New user - check for invite code
        if invite_code:
            # Validate invite code
            is_valid, result = await db.validate_invite_code(invite_code)
            if is_valid:
                invite = result
                # Store invite code in state for later use
                await state.update_data(invite_code=invite_code)

                group_info = f"\n📁 Группа: {invite['group_name']}" if invite.get('group_name') else ""
                auto_info = "\n✅ Автоматическое одобрение" if invite.get('auto_approve') else ""

                await message.answer(
                    "👋 Добро пожаловать в Wenlyx!\n\n"
                    f"🔗 Вы регистрируетесь по приглашению:{group_info}{auto_info}\n\n"
                    "Нажмите кнопку для регистрации.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="📝 Зарегистрироваться", callback_data=f"register_invite_{invite_code}")]
                    ])
                )
                return
            else:
                # Invalid invite code
                await message.answer(
                    f"❌ {result}\n\n"
                    "Попросите новую ссылку у администратора или зарегистрируйтесь обычным способом.",
                    reply_markup=get_registration_keyboard()
                )
                return

        # No invite code - standard registration
        await message.answer(
            "👋 Добро пожаловать в Wenlyx!\n\n"
            "Чтобы начать работу, необходимо зарегистрироваться.",
            reply_markup=get_registration_keyboard()
        )
        return

    # Check if admin
    if user.get("role") == "admin":
        await message.answer(
            "👋 Привет, администратор!\n\n"
            "Откройте приложение для управления системой.",
            reply_markup=get_main_keyboard(is_admin=True)
        )
        return

    # Check approval status
    approval_status = user.get("approval_status", "pending")

    if approval_status == "pending":
        await message.answer(
            "⏳ Ваша заявка на регистрацию находится на рассмотрении.\n\n"
            "Мы уведомим вас, когда администратор примет решение."
        )
        return

    if approval_status == "rejected":
        await message.answer(
            "❌ К сожалению, ваша заявка была отклонена.\n\n"
            "Если вы считаете это ошибкой, свяжитесь с администратором."
        )
        return

    if user.get("is_blocked"):
        await message.answer(
            "🚫 Ваш аккаунт заблокирован.\n\n"
            "Свяжитесь с администратором для разблокировки."
        )
        return

    # Approved user
    await message.answer(
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        "Откройте приложение для создания и управления заявками.",
        reply_markup=get_main_keyboard(is_approved=True)
    )


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message, state: FSMContext):
    """Handle /admin command - login by password."""
    tg_id = message.from_user.id
    user = await db.get_user_by_tg_id(tg_id)

    # Already admin
    if user and user.get("role") == "admin":
        await message.answer(
            "✅ Вы уже авторизованы как администратор.\n\n"
            "Откройте приложение для управления.",
            reply_markup=get_main_keyboard(is_admin=True)
        )
        return

    # Ask for password
    await state.set_state(AdminLogin.waiting_password)
    await message.answer(
        "🔐 Введите пароль администратора:",
        reply_markup=types.ReplyKeyboardRemove()
    )


@dp.message(AdminLogin.waiting_password)
async def process_admin_password(message: types.Message, state: FSMContext):
    """Process admin password."""
    password = message.text.strip()

    # Delete message with password for security
    try:
        await message.delete()
    except:
        pass

    if password != ADMIN_PASSWORD:
        await state.clear()
        await message.answer("❌ Неверный пароль.")
        return

    # Password correct - make user admin
    tg_id = message.from_user.id
    user = await db.get_user_by_tg_id(tg_id)

    if not user:
        # Create admin user
        await db.create_user(
            tg_id=tg_id,
            username=message.from_user.username or "",
            first_name=message.from_user.first_name or "",
            last_name=message.from_user.last_name or "",
            role="admin",
            approval_status="approved"
        )
    else:
        # Update existing user to admin
        await db.set_user_role(tg_id, "admin")

    await state.clear()
    await message.answer(
        "✅ Вы авторизованы как администратор!\n\n"
        "Откройте приложение для управления системой.",
        reply_markup=get_main_keyboard(is_admin=True)
    )
    logger.info(f"Admin login: {message.from_user.id} (@{message.from_user.username})")


@dp.callback_query(F.data == "register")
async def cb_register(callback: types.CallbackQuery, state: FSMContext):
    """Handle registration button."""
    tg_id = callback.from_user.id

    # Check if already registered
    user = await db.get_user_by_tg_id(tg_id)
    if user:
        await callback.answer("Вы уже зарегистрированы", show_alert=True)
        return

    # Create user with pending status
    await db.create_user(
        tg_id=tg_id,
        username=callback.from_user.username or "",
        first_name=callback.from_user.first_name or "",
        last_name=callback.from_user.last_name or "",
        role="manager",
        approval_status="pending"
    )

    await callback.message.edit_text(
        "✅ Заявка на регистрацию отправлена!\n\n"
        "Администратор рассмотрит её в ближайшее время.\n"
        "Мы уведомим вас о решении."
    )

    # Notify all admins
    admins = await db.get_all_admins()
    for admin in admins:
        try:
            await bot.send_message(
                admin["tg_id"],
                f"🆕 Новая заявка на регистрацию!\n\n"
                f"👤 {callback.from_user.first_name} {callback.from_user.last_name or ''}\n"
                f"📱 @{callback.from_user.username or 'нет username'}\n"
                f"🆔 {tg_id}",
                reply_markup=get_admin_pending_keyboard(tg_id)
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin['tg_id']}: {e}")

    await callback.answer()


@dp.callback_query(F.data.startswith("register_invite_"))
async def cb_register_with_invite(callback: types.CallbackQuery, state: FSMContext):
    """Handle registration with invite code."""
    tg_id = callback.from_user.id
    invite_code = callback.data.replace("register_invite_", "")

    # Check if already registered
    user = await db.get_user_by_tg_id(tg_id)
    if user:
        await callback.answer("Вы уже зарегистрированы", show_alert=True)
        return

    # Validate invite code again
    is_valid, result = await db.validate_invite_code(invite_code)
    if not is_valid:
        await callback.message.edit_text(
            f"❌ {result}\n\n"
            "Попросите новую ссылку у администратора."
        )
        await callback.answer()
        return

    invite = result

    # Create user with invite code
    new_user = await db.create_user_with_invite(
        tg_id=tg_id,
        username=callback.from_user.username or "",
        first_name=callback.from_user.first_name or "",
        last_name=callback.from_user.last_name or "",
        invite_code=invite_code
    )

    if invite.get('auto_approve'):
        # Auto-approved - show success
        group_info = f"\n📁 Группа: {invite.get('group_name')}" if invite.get('group_name') else ""

        await callback.message.edit_text(
            f"🎉 Регистрация успешна!{group_info}\n\n"
            "Вы можете сразу начать работу.",
            reply_markup=get_main_keyboard(is_approved=True)
        )

        # Notify admins about new user (info only)
        admins = await db.get_all_admins()
        for admin in admins:
            try:
                await bot.send_message(
                    admin["tg_id"],
                    f"👤 <b>Новый менеджер зарегистрирован</b>\n\n"
                    f"👤 {callback.from_user.first_name} {callback.from_user.last_name or ''}\n"
                    f"📱 @{callback.from_user.username or 'нет username'}\n"
                    f"🔗 Инвайт-код: <code>{invite_code}</code>\n"
                    f"📁 Группа: {invite.get('group_name', 'Без группы')}\n"
                    f"✅ Автоматически одобрен",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {admin['tg_id']}: {e}")
    else:
        # Need manual approval
        await callback.message.edit_text(
            "✅ Заявка на регистрацию отправлена!\n\n"
            f"🔗 Инвайт-код: {invite_code}\n"
            f"📁 Группа: {invite.get('group_name', 'Без группы')}\n\n"
            "Администратор рассмотрит её в ближайшее время."
        )

        # Notify admin who created the invite code
        if invite.get('created_by'):
            try:
                creator = await db.get_user_by_id(invite['created_by'])
                if creator:
                    await bot.send_message(
                        creator["tg_id"],
                        f"🆕 <b>Новая регистрация по вашему инвайту</b>\n\n"
                        f"👤 {callback.from_user.first_name} {callback.from_user.last_name or ''}\n"
                        f"📱 @{callback.from_user.username or 'нет username'}\n"
                        f"🔗 Код: <code>{invite_code}</code>\n"
                        f"📁 Группа: {invite.get('group_name', 'Без группы')}",
                        parse_mode="HTML",
                        reply_markup=get_admin_pending_keyboard(tg_id)
                    )
            except Exception as e:
                logger.error(f"Failed to notify invite creator: {e}")

        # Also notify other admins
        admins = await db.get_all_admins()
        for admin in admins:
            if str(admin.get('id')) == str(invite.get('created_by')):
                continue  # Skip invite creator
            try:
                await bot.send_message(
                    admin["tg_id"],
                    f"🆕 Новая заявка на регистрацию!\n\n"
                    f"👤 {callback.from_user.first_name} {callback.from_user.last_name or ''}\n"
                    f"📱 @{callback.from_user.username or 'нет username'}\n"
                    f"🔗 Инвайт: <code>{invite_code}</code>\n"
                    f"📁 Группа: {invite.get('group_name', 'Без группы')}",
                    parse_mode="HTML",
                    reply_markup=get_admin_pending_keyboard(tg_id)
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {admin['tg_id']}: {e}")

    await callback.answer()


@dp.callback_query(F.data.startswith("approve_"))
async def cb_approve(callback: types.CallbackQuery):
    """Handle approval of registration."""
    # Check if user is admin
    admin_user = await db.get_user_by_tg_id(callback.from_user.id)
    if not admin_user or admin_user.get("role") != "admin":
        await callback.answer("Нет доступа", show_alert=True)
        return

    tg_id = int(callback.data.split("_")[1])

    # Approve user
    await db.approve_user_by_tg_id(tg_id)

    await callback.message.edit_text(
        callback.message.text + "\n\n✅ Одобрено"
    )

    # Notify user
    try:
        await bot.send_message(
            tg_id,
            "🎉 Ваша заявка одобрена!\n\n"
            "Теперь вы можете использовать приложение.",
            reply_markup=get_main_keyboard(is_approved=True)
        )
    except Exception as e:
        logger.error(f"Failed to notify user {tg_id}: {e}")

    await callback.answer("Пользователь одобрен")


@dp.callback_query(F.data.startswith("reject_"))
async def cb_reject(callback: types.CallbackQuery):
    """Handle rejection of registration."""
    # Check if user is admin
    admin_user = await db.get_user_by_tg_id(callback.from_user.id)
    if not admin_user or admin_user.get("role") != "admin":
        await callback.answer("Нет доступа", show_alert=True)
        return

    tg_id = int(callback.data.split("_")[1])

    # Reject user
    await db.reject_user_by_tg_id(tg_id, "Отклонено администратором")

    await callback.message.edit_text(
        callback.message.text + "\n\n❌ Отклонено"
    )

    # Notify user
    try:
        await bot.send_message(
            tg_id,
            "❌ К сожалению, ваша заявка на регистрацию была отклонена.\n\n"
            "Если у вас есть вопросы, свяжитесь с администратором."
        )
    except Exception as e:
        logger.error(f"Failed to notify user {tg_id}: {e}")

    await callback.answer("Пользователь отклонён")


# ==================== Additional Services Handlers ====================

# Store selected services in memory (simple approach for demo)
# In production, consider using FSM or database
selected_services_cache: dict = {}


@dp.callback_query(F.data.startswith("add_service_"))
async def cb_add_service(callback: types.CallbackQuery):
    """Handle adding additional service to request."""
    parts = callback.data.split("_")
    if len(parts) < 4:
        await callback.answer("Ошибка", show_alert=True)
        return

    request_id = parts[2]
    service_code = "_".join(parts[3:])  # Handle codes with underscores

    # Find service info
    service = next((s for s in ADDITIONAL_SERVICES if s['code'] == service_code), None)
    if not service:
        await callback.answer("Услуга не найдена", show_alert=True)
        return

    # Track selected services
    cache_key = f"{callback.from_user.id}_{request_id}"
    if cache_key not in selected_services_cache:
        selected_services_cache[cache_key] = []

    if service_code not in selected_services_cache[cache_key]:
        selected_services_cache[cache_key].append(service_code)

    selected = selected_services_cache[cache_key]

    # Get names of selected services
    selected_names = [
        f"{s['icon']} {s['name']}"
        for s in ADDITIONAL_SERVICES
        if s['code'] in selected
    ]

    # Check if there are more services to offer
    more_keyboard = get_more_services_keyboard(request_id, selected)

    if more_keyboard:
        await callback.message.edit_text(
            f"✅ Добавлено: {service['icon']} {service['name']}\n\n"
            f"📋 Выбранные услуги:\n" + "\n".join(selected_names) + "\n\n"
            "Выберите ещё услугу или нажмите «Готово»:",
            reply_markup=more_keyboard
        )
    else:
        # All services selected
        await callback.message.edit_text(
            f"✅ Все дополнительные услуги выбраны!\n\n"
            f"📋 Ваши услуги:\n" + "\n".join(selected_names) + "\n\n"
            "Менеджер свяжется с вами для уточнения деталей."
        )
        # Clear cache
        selected_services_cache.pop(cache_key, None)

    await callback.answer(f"✅ {service['name']} добавлена")


@dp.callback_query(F.data.startswith("skip_services_"))
async def cb_skip_services(callback: types.CallbackQuery):
    """Handle skipping additional services."""
    request_id = callback.data.replace("skip_services_", "")

    await callback.message.edit_text(
        "👍 Хорошо! Вы можете добавить дополнительные услуги позже в приложении.\n\n"
        "Откройте приложение для просмотра и редактирования заявки.",
        reply_markup=get_main_keyboard(is_approved=True)
    )

    # Clear any cached selections
    cache_key = f"{callback.from_user.id}_{request_id}"
    selected_services_cache.pop(cache_key, None)

    await callback.answer()


@dp.callback_query(F.data.startswith("finish_services_"))
async def cb_finish_services(callback: types.CallbackQuery):
    """Handle finishing service selection."""
    request_id = callback.data.replace("finish_services_", "")
    cache_key = f"{callback.from_user.id}_{request_id}"

    selected = selected_services_cache.get(cache_key, [])

    if selected:
        selected_names = [
            f"{s['icon']} {s['name']}"
            for s in ADDITIONAL_SERVICES
            if s['code'] in selected
        ]

        await callback.message.edit_text(
            f"✅ Отлично! Выбранные услуги:\n\n" +
            "\n".join(selected_names) + "\n\n"
            "Менеджер свяжется с вами для уточнения деталей.\n"
            "Откройте приложение для просмотра заявки.",
            reply_markup=get_main_keyboard(is_approved=True)
        )
    else:
        await callback.message.edit_text(
            "👍 Готово! Откройте приложение для просмотра заявки.",
            reply_markup=get_main_keyboard(is_approved=True)
        )

    # Clear cache
    selected_services_cache.pop(cache_key, None)

    await callback.answer("✅ Сохранено")


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Handle /help command."""
    await message.answer(
        "📖 AutoSites - Помощь\n\n"
        "Этот бот позволяет автоматически генерировать сайты.\n\n"
        "Команды:\n"
        "/start - Начать работу\n"
        "/admin - Войти как администратор\n"
        "/help - Показать эту справку\n\n"
        "Для работы с заявками откройте приложение."
    )


# Notification functions (called from API)
async def send_notification(tg_id: int, text: str, keyboard: InlineKeyboardMarkup = None):
    """Send notification to user."""
    try:
        await bot.send_message(tg_id, text, reply_markup=keyboard)
        return True
    except Exception as e:
        logger.error(f"Failed to send notification to {tg_id}: {e}")
        return False


async def send_file(tg_id: int, file_path: str, caption: str = None):
    """Send file to user."""
    try:
        await bot.send_document(tg_id, types.FSInputFile(file_path), caption=caption)
        return True
    except Exception as e:
        logger.error(f"Failed to send file to {tg_id}: {e}")
        return False


async def send_additional_services_offer(tg_id: int, request_id: str, company_name: str = ""):
    """Send additional services offer to user after request creation."""
    try:
        company_text = f" для «{company_name}»" if company_name else ""
        await bot.send_message(
            tg_id,
            f"🎉 Заявка{company_text} успешно создана!\n\n"
            "💼 Хотите добавить дополнительные услуги?\n\n"
            "🎨 <b>Разработка логотипа</b> — профессиональный дизайн\n"
            "📈 <b>SEO продвижение</b> — раскрутка в поисковиках\n"
            "⚙️ <b>Автоматизация</b> — CRM, боты, интеграции\n\n"
            "Выберите нужные услуги или пропустите:",
            parse_mode="HTML",
            reply_markup=get_additional_services_keyboard(request_id)
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send services offer to {tg_id}: {e}")
        return False


async def send_feedback_response_notification(tg_id: int, subject: str, response: str):
    """Notify manager about admin response to their feedback."""
    try:
        await bot.send_message(
            tg_id,
            f"📬 <b>Ответ на ваше обращение</b>\n\n"
            f"<b>Тема:</b> {subject}\n\n"
            f"<b>Ответ администратора:</b>\n{response}",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_approved=True)
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send feedback notification to {tg_id}: {e}")
        return False


async def notify_admins_new_feedback(manager_name: str, subject: str, priority: str):
    """Notify admins about new feedback from manager."""
    try:
        admins = await db.get_all_admins()
        priority_emoji = {
            'urgent': '🔴',
            'high': '🟠',
            'normal': '🟡',
            'low': '🟢'
        }.get(priority, '🟡')

        for admin in admins:
            try:
                await bot.send_message(
                    admin["tg_id"],
                    f"📨 <b>Новое обращение от менеджера</b>\n\n"
                    f"👤 {manager_name}\n"
                    f"📋 <b>Тема:</b> {subject}\n"
                    f"{priority_emoji} <b>Приоритет:</b> {priority}\n\n"
                    f"Откройте админ-панель для просмотра.",
                    parse_mode="HTML",
                    reply_markup=get_main_keyboard(is_admin=True)
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {admin['tg_id']}: {e}")
        return True
    except Exception as e:
        logger.error(f"Failed to notify admins: {e}")
        return False


# ==================== Webhook Server for API notifications ====================

async def send_deploy_status_notification(tg_id: int, site_id: str, company_name: str, status: str, preview_url: str = None, domain: str = None, error: str = None):
    """Notify manager about deployment status change."""
    try:
        if status == 'active':
            # Успешный деплой
            urls = []
            if preview_url:
                urls.append(f"🔗 Preview: {preview_url}")
            if domain:
                urls.append(f"🌐 Домен: https://{domain}")

            message = f"✅ <b>Сайт «{company_name}» успешно задеплоен!</b>\n\n"
            if urls:
                message += "\n".join(urls) + "\n\n"
            message += "Сайт доступен для просмотра."

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🚀 Открыть приложение",
                    web_app=WebAppInfo(url=WEBAPP_URL)
                )]
            ])

            if preview_url:
                keyboard.inline_keyboard.insert(0, [
                    InlineKeyboardButton(text="🌐 Открыть сайт", url=preview_url)
                ])

            await bot.send_message(tg_id, message, parse_mode="HTML", reply_markup=keyboard)

        elif status == 'deploying':
            # Деплой в процессе
            await bot.send_message(
                tg_id,
                f"🔄 <b>Деплой сайта «{company_name}» начат</b>\n\n"
                f"Обычно это занимает 1-3 минуты. Мы уведомим вас о результате.",
                parse_mode="HTML"
            )

        elif status == 'failed':
            # Ошибка деплоя
            error_text = f"\n\n<b>Ошибка:</b> {error}" if error else ""
            await bot.send_message(
                tg_id,
                f"❌ <b>Ошибка деплоя сайта «{company_name}»</b>{error_text}\n\n"
                f"Попробуйте запустить деплой повторно или обратитесь к администратору.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(is_approved=True)
            )

        elif status == 'stopped':
            # Сайт остановлен
            await bot.send_message(
                tg_id,
                f"⏸ <b>Сайт «{company_name}» остановлен</b>\n\n"
                f"Вы можете запустить его снова в приложении.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(is_approved=True)
            )

        return True
    except Exception as e:
        logger.error(f"Failed to send deploy status notification to {tg_id}: {e}")
        return False


async def send_generation_complete_notification(tg_id: int, request_id: str, company_name: str, status: str, error: str = None):
    """Notify manager about generation completion."""
    try:
        if status == 'completed':
            await bot.send_message(
                tg_id,
                f"🎉 <b>Генерация сайта «{company_name}» завершена!</b>\n\n"
                f"Теперь вы можете запустить деплой сайта в приложении.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(is_approved=True)
            )
        elif status == 'error':
            error_text = f"\n\n<b>Ошибка:</b> {error}" if error else ""
            await bot.send_message(
                tg_id,
                f"❌ <b>Ошибка генерации сайта «{company_name}»</b>{error_text}\n\n"
                f"Попробуйте снова или обратитесь к администратору.",
                parse_mode="HTML",
                reply_markup=get_main_keyboard(is_approved=True)
            )
        return True
    except Exception as e:
        logger.error(f"Failed to send generation notification to {tg_id}: {e}")
        return False


async def send_hosting_warning_notification(tg_id: int, site_id: str, company_name: str, days_remaining: int):
    """Notify manager about expiring hosting."""
    try:
        days_text = f"{days_remaining} " + (
            "день" if days_remaining == 1 else
            "дня" if 2 <= days_remaining <= 4 else
            "дней"
        )

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💳 Продлить хостинг",
                web_app=WebAppInfo(url=f"{WEBAPP_URL}/sites/{site_id}/payment")
            )]
        ])

        await bot.send_message(
            tg_id,
            f"⚠️ <b>Внимание! Хостинг сайта «{company_name}» истекает через {days_text}</b>\n\n"
            f"Продлите хостинг, чтобы сайт продолжал работать.",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send hosting warning to {tg_id}: {e}")
        return False


async def handle_webhook(request):
    """Handle webhook requests from API."""
    try:
        data = await request.json()
        action = data.get("action")

        if action == "request_created":
            # Send additional services offer
            tg_id = data.get("tg_id")
            request_id = data.get("request_id")
            company_name = data.get("company_name", "")

            if tg_id and request_id:
                await send_additional_services_offer(tg_id, request_id, company_name)
                return web.json_response({"success": True})

        elif action == "deploy_status":
            # Notify about deployment status change
            tg_id = data.get("tg_id")
            site_id = data.get("site_id")
            company_name = data.get("company_name", "")
            status = data.get("status")
            preview_url = data.get("preview_url")
            domain = data.get("domain")
            error = data.get("error")

            if tg_id and status:
                await send_deploy_status_notification(
                    tg_id=tg_id,
                    site_id=site_id,
                    company_name=company_name,
                    status=status,
                    preview_url=preview_url,
                    domain=domain,
                    error=error
                )
                return web.json_response({"success": True})

        elif action == "generation_complete":
            # Notify about generation completion
            tg_id = data.get("tg_id")
            request_id = data.get("request_id")
            company_name = data.get("company_name", "")
            status = data.get("status")
            error = data.get("error")

            if tg_id and status:
                await send_generation_complete_notification(
                    tg_id=tg_id,
                    request_id=request_id,
                    company_name=company_name,
                    status=status,
                    error=error
                )
                return web.json_response({"success": True})

        elif action == "hosting_warning":
            # Notify about expiring hosting
            tg_id = data.get("tg_id")
            site_id = data.get("site_id")
            company_name = data.get("company_name", "")
            days_remaining = data.get("days_remaining", 7)

            if tg_id and site_id:
                await send_hosting_warning_notification(
                    tg_id=tg_id,
                    site_id=site_id,
                    company_name=company_name,
                    days_remaining=days_remaining
                )
                return web.json_response({"success": True})

        elif action == "feedback_response":
            # Notify manager about feedback response
            tg_id = data.get("tg_id")
            subject = data.get("subject", "")
            response = data.get("response", "")

            if tg_id and response:
                await send_feedback_response_notification(tg_id, subject, response)
                return web.json_response({"success": True})

        elif action == "new_feedback":
            # Notify admins about new feedback
            manager_name = data.get("manager_name", "")
            subject = data.get("subject", "")
            priority = data.get("priority", "normal")

            await notify_admins_new_feedback(manager_name, subject, priority)
            return web.json_response({"success": True})

        elif action == "send_message":
            # Generic message sending
            tg_id = data.get("tg_id")
            text = data.get("text")

            if tg_id and text:
                await send_notification(tg_id, text)
                return web.json_response({"success": True})

        return web.json_response({"success": False, "error": "Unknown action"})

    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.json_response({"success": False, "error": str(e)}, status=500)


async def start_webhook_server():
    """Start webhook server for API notifications."""
    app = web.Application()
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok"}))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", BOT_WEBHOOK_PORT)
    await site.start()
    logger.info(f"Webhook server started on port {BOT_WEBHOOK_PORT}")
    return runner


async def main():
    """Main function."""
    logger.info("Initializing database...")
    await db.init_pool()

    logger.info(f"Bot starting... WebApp URL: {WEBAPP_URL}")

    webhook_runner = None
    try:
        # Get bot info
        me = await bot.get_me()
        logger.info(f"Bot: {me.full_name} [@{me.username}]")

        # Start webhook server for API notifications
        webhook_runner = await start_webhook_server()

        # Start polling
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await db.close_pool()
        await bot.session.close()
        if webhook_runner:
            await webhook_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
