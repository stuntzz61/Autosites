"""
Simplified Telegram Bot for AutoSites
Only handles:
- Registration flow
- Opening Mini App
- Admin login by password
- Notifications
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

# Bot setup
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


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


# Handlers
@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    """Handle /start command."""
    await state.clear()

    tg_id = message.from_user.id
    user = await db.get_user_by_tg_id(tg_id)

    if not user:
        # New user - show registration
        await message.answer(
            "👋 Добро пожаловать в AutoSites!\n\n"
            "Это сервис для автоматической генерации сайтов.\n\n"
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


async def main():
    """Main function."""
    logger.info("Initializing database...")
    await db.init_pool()

    logger.info(f"Bot starting... WebApp URL: {WEBAPP_URL}")

    try:
        # Get bot info
        me = await bot.get_me()
        logger.info(f"Bot: {me.full_name} [@{me.username}]")

        # Start polling
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await db.close_pool()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
