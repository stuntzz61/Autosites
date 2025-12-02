from io import BytesIO
from uuid import uuid4
import re
import asyncio
from aiogram import types, Dispatcher, Bot
from aiogram.dispatcher import FSMContext
from app.states import RequestForm, PhotoUpload
from app.s3 import put_bytes, guess_mime
from app.utils import slugify
from app.db import (
    get_current_request_id_by_tgid,
    get_request_payload,
    get_request,
    append_images_to_request,
    get_mode,
    get_user_by_tgid,
)
from app.keyboards import (
    photo_categories_inline,
    photo_upload_inline,
    request_card_inline,
    more_details_inline,
)
from app.constants import (
    PHOTO_CATEGORIES, CB_PHOTO_CAT, STATUS_READY_TO_GENERATE,
    BTN_NEW, BTN_MY, BTN_RESET, BTN_ADMIN_LOGIN,
    MSG_PHOTOS_INSTRUCTION,
)


def _sanitize_filename(name: str) -> str:
    """Очистка имени файла"""
    name = name or "image"
    m = re.match(r"^(.*?)(\.[A-Za-z0-9]{1,8})?$", name)
    base = (m.group(1) or "image")
    ext = (m.group(2) or "")
    base = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:80] or "image"
    return base + ext


def register(dp: Dispatcher, bot: Bot):

    # ==================== ВЫБОР КАТЕГОРИИ ====================

    async def cb_photo_category(call: types.CallbackQuery, state: FSMContext):
        """Обработка выбора категории фото"""
        await call.answer()

        data = call.data[len(CB_PHOTO_CAT):]
        parts = data.rsplit("_", 1)

        if len(parts) != 2:
            return await call.message.answer("⚠️ Ошибка данных.")

        req_id, category = parts

        if category not in PHOTO_CATEGORIES:
            return await call.message.answer("⚠️ Неизвестная категория.")

        await PhotoUpload.uploading.set()
        await state.update_data(edit_req_id=req_id, photo_category=category)

        cat_name = PHOTO_CATEGORIES[category]

        await call.message.answer(
            f"📷 <b>{cat_name}</b>\n\n"
            f"Отправьте фотографии для этой категории.\n"
            f"Можно отправить несколько фото подряд.\n\n"
            f"<i>Рекомендуемый размер: от 1200px по ширине</i>",
            reply_markup=photo_upload_inline(req_id, category)
        )

    dp.register_callback_query_handler(
        cb_photo_category,
        lambda c: c.data and c.data.startswith(CB_PHOTO_CAT),
        state="*"
    )

    # ==================== ВОЗВРАТ К КАТЕГОРИЯМ ====================

    async def cb_photo_cats(call: types.CallbackQuery, state: FSMContext):
        """Возврат к выбору категории"""
        await call.answer()

        req_id = call.data.replace("photo_cats_", "")

        await PhotoUpload.choosing_category.set()
        await state.update_data(edit_req_id=req_id, photo_category=None)

        await call.message.answer(
            MSG_PHOTOS_INSTRUCTION,
            reply_markup=photo_categories_inline(req_id)
        )

    dp.register_callback_query_handler(
        cb_photo_cats,
        lambda c: c.data and c.data.startswith("photo_cats_"),
        state="*"
    )

    # ==================== ЗАВЕРШЕНИЕ ЗАГРУЗКИ ====================

    async def cb_photo_done(call: types.CallbackQuery, state: FSMContext):
        """Завершение загрузки фото"""
        await call.answer("Загрузка завершена")

        req_id = call.data.replace("photo_done_", "")

        payload = get_request_payload(req_id) if req_id else {}
        images = (payload.get("site") or {}).get("assets", {}).get("images", []) if payload else []

        if not images:
            await call.message.answer(
                "⚠️ <b>Фото не загружены</b>\n\n"
                "Для генерации сайта необходимо загрузить хотя бы одно изображение.",
                reply_markup=photo_categories_inline(req_id)
            )
            return

        # Статистика по категориям
        categories_count = {}
        for img in images:
            cat = img.get("category", "other")
            cat_name = PHOTO_CATEGORIES.get(cat, cat)
            categories_count[cat_name] = categories_count.get(cat_name, 0) + 1

        report = "\n".join([f"• {name}: {count}" for name, count in categories_count.items()])

        # Переход к услугам
        await RequestForm.services.set()
        await state.update_data(edit_req_id=req_id)

        await call.message.answer(
            f"✅ <b>Фото загружены</b>\n\n"
            f"Всего изображений: {len(images)}\n\n"
            f"<b>По категориям:</b>\n{report}\n\n"
            f"Теперь введите <b>услуги компании</b>.\n"
            f"Каждую услугу с новой строки:\n"
            f"<i>Название — описание — цена</i>\n\n"
            f"<b>Пример:</b>\n"
            f"<code>Разработка сайта — под ключ — от 50000\n"
            f"SEO продвижение — комплексное — от 30000</code>"
        )

    dp.register_callback_query_handler(
        cb_photo_done,
        lambda c: c.data and c.data.startswith("photo_done_"),
        state="*"
    )

    # ==================== КОМАНДА /photos ====================

    async def start_collecting_photos(message: types.Message, state: FSMContext):
        """Начало загрузки фото"""
        req_id = get_current_request_id_by_tgid(message.chat.id)

        if not req_id:
            return await message.answer(
                "❌ Не найдена активная заявка.\n\n"
                "Сначала создайте заявку: /new_request"
            )

        await PhotoUpload.choosing_category.set()
        await state.update_data(edit_req_id=req_id, photo_category=None)

        await message.answer(
            MSG_PHOTOS_INSTRUCTION,
            reply_markup=photo_categories_inline(req_id)
        )

    dp.register_message_handler(start_collecting_photos, commands=["photos", "add_photos"], state="*")

    # ==================== КОМАНДА /done ====================

    async def finish_collecting(message: types.Message, state: FSMContext):
        """Завершение загрузки по команде"""
        st = await state.get_data()
        req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(message.chat.id)

        if not req_id:
            return await message.answer("❌ Не найдена активная заявка.")

        payload = get_request_payload(req_id) if req_id else {}
        images = (payload.get("site") or {}).get("assets", {}).get("images", []) if payload else []

        if not images:
            return await message.answer(
                "⚠️ <b>Фото не загружены</b>\n\n"
                "Загрузите хотя бы одно изображение.",
                reply_markup=photo_categories_inline(req_id)
            )

        # Статистика
        categories_count = {}
        for img in images:
            cat = img.get("category", "other")
            cat_name = PHOTO_CATEGORIES.get(cat, cat)
            categories_count[cat_name] = categories_count.get(cat_name, 0) + 1

        report = "\n".join([f"• {name}: {count}" for name, count in categories_count.items()])

        await RequestForm.services.set()
        await state.update_data(edit_req_id=req_id)

        await message.answer(
            f"✅ <b>Фото загружены</b>\n\n"
            f"Всего: {len(images)}\n{report}\n\n"
            f"Теперь введите <b>услуги</b>:\n"
            f"<i>Название — описание — цена</i>"
        )

    dp.register_message_handler(
        finish_collecting,
        commands=["done"],
        state=[PhotoUpload.choosing_category, PhotoUpload.uploading]
    )

    # ==================== ТЕКСТ ПРИ ВЫБОРЕ КАТЕГОРИИ ====================

    async def on_text_choosing_category(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        await message.answer(
            "👆 Выберите категорию из меню выше.",
            reply_markup=photo_categories_inline(req_id) if req_id else None
        )

    dp.register_message_handler(
        on_text_choosing_category,
        content_types=[types.ContentType.TEXT],
        state=PhotoUpload.choosing_category
    )

    # ==================== ТЕКСТ ПРИ ЗАГРУЗКЕ ====================

    async def on_text_during_uploading(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        category = st.get("photo_category", "other")
        await message.answer(
            "📷 Отправьте изображение или выберите действие ниже.",
            reply_markup=photo_upload_inline(req_id, category) if req_id else None
        )

    dp.register_message_handler(
        on_text_during_uploading,
        content_types=[types.ContentType.TEXT],
        state=PhotoUpload.uploading
    )

    # ==================== ФОТО БЕЗ КАТЕГОРИИ ====================

    async def on_photo_choosing_category(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        await message.answer(
            "👆 Сначала выберите категорию для фото.",
            reply_markup=photo_categories_inline(req_id) if req_id else None
        )

    dp.register_message_handler(
        on_photo_choosing_category,
        content_types=[types.ContentType.PHOTO, types.ContentType.DOCUMENT],
        state=PhotoUpload.choosing_category
    )

    # ==================== ЗАГРУЗКА ФОТО ====================

    async def on_photo_uploading(message: types.Message, state: FSMContext):
        """Обработка загрузки фото"""
        st = await state.get_data()
        req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(message.chat.id)
        category = st.get("photo_category", "other")

        if not req_id:
            return await message.reply("❌ Не найдена активная заявка.")

        payload = get_request_payload(req_id)
        if not payload:
            return await message.reply("❌ Заявка удалена. Создайте новую: /new_request")

        # Определение источника
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
            return await message.reply("❌ Отправьте изображение (JPG, PNG).")

        # Скачивание
        try:
            tg_file = await bot.get_file(file_id)
            bio = BytesIO()
            await bot.download_file(tg_file.file_path, destination=bio)
            data = bio.getvalue()
        except Exception as e:
            return await message.reply(f"⚠️ Ошибка загрузки: {e}")

        if len(data) > 20 * 1024 * 1024:
            return await message.reply("❌ Файл слишком большой (максимум 20 МБ).")

        # Путь в S3
        site = payload.get("site") or {}
        company = site.get("company") or ""
        company_slug = slugify(company, fallback=str(message.chat.id))
        request_slug = slugify(str(req_id), fallback="req")
        key = f"uploads/{company_slug}/{request_slug}/{category}/{uuid4().hex}_{_sanitize_filename(filename)}"

        # Загрузка в S3
        loop = asyncio.get_running_loop()
        try:
            url = await loop.run_in_executor(
                None,
                lambda: put_bytes(
                    key=key,
                    data=data,
                    content_type=mime,
                    metadata={
                        "source": "telegram-bot",
                        "chat_id": str(message.chat.id),
                        "request_id": str(req_id),
                        "category": category,
                    },
                ),
            )
        except Exception as e:
            return await message.reply(f"⚠️ Ошибка сохранения: {e}")

        # Запись в БД
        image_rec = {
            "url": url,
            "key": key,
            "name": filename,
            "mime": mime,
            "width": width,
            "height": height,
            "category": category,
            "alt": (message.caption or "").strip() or PHOTO_CATEGORIES.get(category, "Изображение"),
        }

        try:
            append_images_to_request(req_id, [image_rec])
        except Exception as e:
            return await message.reply(f"⚠️ Ошибка сохранения: {e}")

        cat_name = PHOTO_CATEGORIES.get(category, category)
        await message.reply(
            f"✅ <b>Загружено</b>\n\n"
            f"Категория: {cat_name}\n"
            f"Файл: {filename}\n\n"
            f"Отправьте ещё фото или выберите действие:",
            reply_markup=photo_upload_inline(req_id, category)
        )

    dp.register_message_handler(
        on_photo_uploading,
        content_types=[types.ContentType.PHOTO, types.ContentType.DOCUMENT],
        state=PhotoUpload.uploading
    )

    # ==================== LEGACY: Photos.collecting ====================

    from aiogram.dispatcher.filters.state import State, StatesGroup

    class Photos(StatesGroup):
        collecting = State()

    async def legacy_photos_redirect(message: types.Message, state: FSMContext):
        """Редирект со старого состояния"""
        st = await state.get_data()
        req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(message.chat.id)

        if req_id:
            await PhotoUpload.choosing_category.set()
            await state.update_data(edit_req_id=req_id, photo_category=None)
            await message.answer(
                MSG_PHOTOS_INSTRUCTION,
                reply_markup=photo_categories_inline(req_id)
            )
        else:
            await message.answer("❌ Не найдена активная заявка.")

    dp.register_message_handler(
        legacy_photos_redirect,
        content_types=[types.ContentType.PHOTO, types.ContentType.DOCUMENT],
        state=Photos.collecting
    )

    dp.register_message_handler(
        lambda msg, state: legacy_photos_redirect(msg, state),
        content_types=[types.ContentType.TEXT],
        state=Photos.collecting
    )
