import json
import logging
import aiohttp
from aiogram import types
from app.config import N8N_GEN_WEBHOOK
from app.constants import MSG_GENERATION_STARTED, MSG_GENERATION_ERROR, MSG_NO_WEBHOOK

log = logging.getLogger("bot")


async def post_generate_site(chat_id: int, payload: dict, message: types.Message):
    """Отправка заявки на генерацию сайта"""

    if not N8N_GEN_WEBHOOK:
        await message.answer(MSG_NO_WEBHOOK)
        return False

    body = {
        "chat_id": int(chat_id),
        "request": payload
    }

    await message.reply(MSG_GENERATION_STARTED)

    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                N8N_GEN_WEBHOOK,
                data=json.dumps(body, ensure_ascii=False, default=str),
                headers={"Content-Type": "application/json"},
            ) as resp:
                text = await resp.text()
                log.info("n8n POST %s -> %s %s", N8N_GEN_WEBHOOK, resp.status, text[:400])

                if resp.status >= 400:
                    log.error("n8n error response: %s", text[:1000])
                    await message.reply(
                        f"⚠️ <b>Ошибка сервиса генерации</b>\n\n"
                        f"Код: {resp.status}\n"
                        f"Пожалуйста, попробуйте позже."
                    )
                    return False

                return True

    except aiohttp.ClientTimeout:
        log.exception("n8n request timeout")
        await message.reply(
            "⚠️ <b>Превышено время ожидания</b>\n\n"
            "Сервер генерации не ответил вовремя. Попробуйте позже."
        )
        return False

    except Exception as e:
        log.exception("POST to n8n failed")
        await message.reply(MSG_GENERATION_ERROR)
        return False


async def test_webhook(chat_id: int, message: types.Message):
    """Тестовый запрос к вебхуку"""

    if not N8N_GEN_WEBHOOK:
        return await message.answer(
            "⚠️ <b>Вебхук не настроен</b>\n\n"
            "Переменная окружения N8N_GEN_WEBHOOK не задана."
        )

    body = {
        "chat_id": chat_id,
        "request": {"test": True, "ping": "pong"}
    }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                N8N_GEN_WEBHOOK,
                data=json.dumps(body, ensure_ascii=False, default=str),
                headers={"Content-Type": "application/json"},
            ) as resp:
                text = await resp.text()

                status_emoji = "✅" if resp.status < 400 else "❌"

                await message.answer(
                    f"{status_emoji} <b>Результат проверки</b>\n\n"
                    f"Статус: {resp.status}\n"
                    f"Ответ: <code>{text[:500]}</code>"
                )

    except aiohttp.ClientTimeout:
        await message.answer(
            "❌ <b>Таймаут</b>\n\n"
            "Сервер не ответил в течение 30 секунд."
        )

    except Exception as e:
        await message.answer(
            f"❌ <b>Ошибка подключения</b>\n\n"
            f"<code>{str(e)[:300]}</code>"
        )
