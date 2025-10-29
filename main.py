# -*- coding: utf-8 -*-
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils import executor

from app.config import BOT_TOKEN, init_logging
from app.handlers import register_all_handlers

def on_startup_factory(bot: Bot):
    async def on_startup(dp: Dispatcher):
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            logging.exception("delete_webhook failed")
        try:
            # дефолтные команды (перечень по чатам ставим в /start)
            await bot.set_my_commands([
                types.BotCommand("start", "Старт"),
                types.BotCommand("register", "Регистрация"),
                types.BotCommand("admin_login", "Войти в админку"),
                types.BotCommand("test_webhook", "DEBUG: тест n8n вебхука"),
                types.BotCommand("photos", "Загрузить фото"),
                types.BotCommand("done", "Закончить загрузку фото"),
            ])
        except Exception:
            logging.exception("set_my_commands (default) failed")
        logging.getLogger("bot").info("Bot is running…")
    return on_startup

def main():
    init_logging()
    bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
    dp = Dispatcher(bot, storage=MemoryStorage())
    register_all_handlers(dp, bot)

    executor.start_polling(
        dp,
        skip_updates=True,
        on_startup=on_startup_factory(bot),
        allowed_updates=['message', 'callback_query'],
    )

if __name__ == "__main__":
    main()
