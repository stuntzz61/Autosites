from io import BytesIO
from uuid import uuid4
import re
import asyncio
from aiogram import types, Dispatcher, Bot
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from app.states import RequestForm
from app.s3 import put_bytes, guess_mime
from app.utils import slugify
from app.db import (
    get_current_request_id_by_tgid,
    get_request_payload,
    append_images_to_request,
)

class Photos(StatesGroup):
    collecting = State()

def _sanitize_filename(name: str) -> str:
    name = name or "image"
    m = re.match(r"^(.*?)(\.[A-Za-z0-9]{1,8})?$", name)
    base = (m.group(1) or "image")
    ext  = (m.group(2) or "")
    base = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:80] or "image"
    return base + ext

def register(dp: Dispatcher, bot: Bot):
    # /photos
    async def start_collecting_photos(message: types.Message, state: FSMContext):
        await Photos.collecting.set()
        await message.answer(
            "Пришли одно или несколько фото (можно по одному). "
            "Когда закончишь — отправь /done."
        )

    # /done
    async def finish_collecting(message: types.Message, state: FSMContext):
        req_id = get_current_request_id_by_tgid(message.chat.id)
        payload = get_request_payload(req_id) if req_id else {}
        images = (payload.get("site") or {}).get("assets", {}).get("images", []) if payload else []

        if not images:
            return await message.answer(
                "Пока нет ни одной картинки в заявке. Пришли хотя бы одно фото и снова нажми /done."
            )

        # Переходим к следующему вопросу анкеты
        await RequestForm.services.set()
        await message.answer(
            "Ок! Фото сохранены в заявку.\n\n"
            "Теперь — <b>услуги</b>.\n"
            "Введите по одной в строке в формате: <i>Название — кратко — от цена</i>."
        )

    # Если прислали текст на этапе сбора фото
    async def on_text_during_collecting(message: types.Message, state: FSMContext):
        await message.answer("Это не похоже на изображение.\nПришли фото (или файл-изображение). Завершить — /done.")

    # Фото/картинка во время сбора
    async def on_photo(message: types.Message, state: FSMContext):
        # пробуем взять req_id из FSM (установлен в q_structure)
        st = await state.get_data()
        req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(message.chat.id)
        if not req_id:
            return await message.reply("Не нашёл активную заявку. Сначала создай заявку /new.")

        payload = get_request_payload(req_id)
        if not payload:
            return await message.reply("Эта заявка уже удалена. Создай новую /new и повтори загрузку.")

        # 1) определить источник
        if message.photo:
            p = message.photo[-1]
            file_id = p.file_id
            filename = f"photo_{p.file_unique_id}.jpg"
            mime = "image/jpeg"
            width, height = p.width, p.height
        elif message.document and (message.document.mime_type or "").startswith("image/"):
            d = message.document
            file_id = d.file_id
            filename = _sanitize_filename(d.file_name or f"img_{d.file_unique_id}")
            mime = d.mime_type or guess_mime(filename, "image/*")
            width = height = None
        else:
            return await message.reply("Это не похоже на изображение. Пришли фото или файл-картинку.")

        # 2) скачать из Telegram
        try:
            tg_file = await bot.get_file(file_id)
            bio = BytesIO()
            await bot.download_file(tg_file.file_path, destination=bio)
            data = bio.getvalue()
        except Exception as e:
            return await message.reply(f"Не удалось скачать файл из Telegram: {e}")

        if len(data) > 20 * 1024 * 1024:
            return await message.reply("Слишком большой файл (>20 МБ). Пришли картинку поменьше.")

        # 3) ключ в S3
        site = payload.get("site") or {}
        company = site.get("company") or ""
        company_slug = slugify(company, fallback=str(message.chat.id))
        request_slug = slugify(str(req_id), fallback="req")
        key = f"uploads/{company_slug}/{request_slug}/original/{uuid4().hex}_{_sanitize_filename(filename)}"

        # 4) загрузить в S3
        loop = asyncio.get_running_loop()
        try:
            url = await loop.run_in_executor(
                None,
                lambda: put_bytes(
                    key=key,
                    data=data,
                    content_type=mime,
                    metadata={"source": "telegram-bot", "chat_id": str(message.chat.id), "request_id": str(req_id)},
                ),
            )
        except Exception as e:
            return await message.reply(f"Ошибка загрузки в S3: {e}")

        # 5) записать в payload_json->site.assets.images[]
        image_rec = {
            "url": url,
            "key": key,
            "name": filename,
            "mime": mime,
            "width": width,
            "height": height,
            "alt": (message.caption or "").strip() or "Изображение",
        }
        try:
            append_images_to_request(req_id, [image_rec])
        except Exception as e:
            return await message.reply(f"Ошибка записи в заявку: {e}")

        await message.reply(f"✅ Загружено: {filename}")

    # Регистрация хэндлеров
    dp.register_message_handler(start_collecting_photos, commands=["photos", "add_photos"], state="*")
    dp.register_message_handler(finish_collecting, commands=["done"], state=Photos.collecting)
    dp.register_message_handler(on_text_during_collecting, content_types=[types.ContentType.TEXT], state=Photos.collecting)
    dp.register_message_handler(
        on_photo,
        content_types=[types.ContentType.PHOTO, types.ContentType.DOCUMENT],
        state=Photos.collecting
    )
