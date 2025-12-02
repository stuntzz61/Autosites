# app/services/webhook.py
"""Webhook для получения уведомлений от n8n о статусе генерации"""

import logging
from aiohttp import web
from app.db import (
    mark_generation_complete, mark_generation_error,
    get_request, log_activity,
)
from app.constants import MSG_GENERATION_COMPLETE

log = logging.getLogger("bot")


async def handle_generation_callback(request: web.Request) -> web.Response:
    """
    POST /webhook/generation

    Body:
    {
        "request_id": "uuid",
        "status": "success" | "error",
        "result_url": "https://...",  # optional
        "error_message": "...",        # optional
        "chat_id": 123456              # для отправки уведомления
    }
    """
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    request_id = data.get("request_id")
    status = data.get("status")
    result_url = data.get("result_url")
    error_message = data.get("error_message")
    chat_id = data.get("chat_id")

    if not request_id or not status:
        return web.json_response({"error": "Missing request_id or status"}, status=400)

    # Проверяем что заявка существует
    rec = get_request(request_id)
    if not rec:
        return web.json_response({"error": "Request not found"}, status=404)

    # Обновляем статус
    if status == "success":
        mark_generation_complete(request_id, result_url)
        log_activity(None, "generation_completed", "request", request_id, {"result_url": result_url})
        log.info(f"Generation completed for request {request_id}")
    else:
        mark_generation_error(request_id, error_message)
        log_activity(None, "generation_failed", "request", request_id, {"error": error_message})
        log.error(f"Generation failed for request {request_id}: {error_message}")

    # Если есть chat_id, нужно отправить уведомление
    # Это можно сделать через бота, но для этого нужен экземпляр бота
    # Возвращаем данные для n8n чтобы он сам отправил

    return web.json_response({
        "success": True,
        "request_id": request_id,
        "status": status,
        "notify_chat_id": chat_id,
    })


def create_webhook_app():
    """Создать aiohttp приложение для вебхуков"""
    app = web.Application()
    app.router.add_post("/webhook/generation", handle_generation_callback)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok"}))
    return app


# Для запуска как отдельного сервиса:
# from aiohttp import web
# app = create_webhook_app()
# web.run_app(app, port=8080)

