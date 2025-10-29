import json, logging, aiohttp
from aiogram import types
from app.config import N8N_GEN_WEBHOOK

log = logging.getLogger("bot")

async def post_generate_site(chat_id: int, payload: dict, message: types.Message):
    if not N8N_GEN_WEBHOOK:
        await message.answer("N8N_GEN_WEBHOOK не задан.")
        return
    body = {"chat_id": int(chat_id), "request": payload}
    await message.reply("🚀 Запускаю генерацию… Файл придёт отдельным сообщением.")
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
                    await message.reply(f"⚠️ n8n вернул {resp.status}: {text[:1000]}")
    except Exception as e:
        log.exception("POST to n8n failed")
        await message.reply(f"⚠️ Не удалось отправить в n8n: {e}")

async def test_webhook(chat_id: int, message: types.Message):
    if not N8N_GEN_WEBHOOK:
        return await message.answer("N8N_GEN_WEBHOOK не задан.")
    body = {"chat_id": chat_id, "request": {"ping": "pong"}}
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(
                N8N_GEN_WEBHOOK,
                data=json.dumps(body, ensure_ascii=False, default=str),
                headers={"Content-Type": "application/json"},
            )
            txt = await r.text()
            await message.answer(f"POST {r.status}\n{txt[:800]}")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")
