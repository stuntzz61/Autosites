# app/handlers/requests.py
import io
import json
import logging
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.utils.exceptions import MessageNotModified

from app.constants import (
    BTN_NEW, BTN_MY, BTN_RESET, BTN_ADMIN_LOGIN, BTN_ARCHIVE,
    CB_OPEN, CB_LIST_PAGE, CB_BACK_TO_LIST, CB_DELETE,
    CB_EDIT, CB_EDIT_FIELD, CB_EXPORT_ONE, CB_GEN,
    CB_ARCHIVE_REQ, CB_CLOSE_REQ,
    EDITABLE_FIELDS, EDIT_HINTS, STATUS_LABELS, PHOTO_CATEGORIES,
    STATUS_COLLECTING_INFO, STATUS_COLLECTING_PHOTOS, STATUS_READY_TO_GENERATE,
    STATUS_QUEUED, STATUS_CLOSED, STATUS_ARCHIVED,
    get_company_emoji,
)
from app.db import (
    get_mode, get_user_by_tgid, list_manager_requests, count_manager_requests,
    list_manager_archive, count_manager_archive,
    get_request, delete_request, update_request_site_json,
    get_request_payload, set_request_status, archive_request,
    mark_generation_started, log_activity,
)
from app.keyboards import requests_list_inline, request_card_inline, edit_fields_inline
from app.utils import (
    e, parse_services, parse_portfolio, parse_testimonials, parse_faq,
    default_seo_title, chunks, convert_uuids_to_strings, has_min_requirements,
)
from app.states import EditField
from app.services.n8n import post_generate_site, test_webhook

log = logging.getLogger("bot")


def format_request_card(rec: dict, show_private: bool = True) -> str:
    """Форматирование карточки заявки"""
    payload = get_request_payload(str(rec["id"])) or {}
    site = payload.get("site") or {}
    client_json = payload.get("client") or {}

    services = site.get("services") or []
    portfolio = site.get("portfolio") or []
    testimonials = site.get("testimonials") or []
    faq = site.get("faq") or []
    structure = site.get("structure") or []

    assets = site.get("assets") or {}
    images = assets.get("images") or []
    images_count = len(images)

    # Группировка фото по категориям
    photo_cats = {}
    for img in images:
        cat = img.get("category", "other")
        cat_name = PHOTO_CATEGORIES.get(cat, cat)
        photo_cats[cat_name] = photo_cats.get(cat_name, 0) + 1
    photo_summary = ", ".join([f"{name}: {cnt}" for name, cnt in photo_cats.items()]) if photo_cats else "не загружены"

    # Услуги
    services_txt = ""
    if services:
        services_list = []
        for s in services[:5]:
            name = e(s.get('name', ''))
            price = f" — {e(s.get('priceFrom', ''))}" if s.get("priceFrom") else ""
            services_list.append(f"• {name}{price}")
        services_txt = "\n".join(services_list)
        if len(services) > 5:
            services_txt += f"\n<i>...и ещё {len(services) - 5}</i>"
    else:
        services_txt = "—"

    # Статус
    meta = site.get("meta") or {}
    status_key = meta.get("status") or "draft"
    status = STATUS_LABELS.get(status_key, status_key)

    # Эмодзи компании
    company_name = site.get("company") or ""
    business_type = site.get("business_type") or ""
    company_emoji = get_company_emoji(company_name, business_type)

    # Контактные данные
    phone = site.get("phone") or "—"
    email = site.get("email") or "—"
    address = site.get("address") or "—"

    # Результат генерации
    result_block = ""
    if rec.get("result_url"):
        result_block = f"\n\n🌐 <b>Результат:</b> {rec.get('result_url')}"

    # Блок клиента
    client_block = ""
    if show_private:
        client_name = rec.get("client_name") or client_json.get("name") or "—"
        client_company = rec.get("client_company") or client_json.get("company") or "—"
        client_contact = rec.get("client_contact") or client_json.get("contact") or "—"
        client_block = (
            f"<b>👤 Данные клиента</b>\n"
            f"Имя: {e(client_name)}\n"
            f"Компания: {e(client_company)}\n"
            f"Контакт: {e(client_contact)}\n\n"
        )

    return (
        f"{company_emoji} <b>Заявка #{str(rec['id'])[:8]}...</b>\n"
        f"Статус: {status}\n\n"
        f"{client_block}"
        f"<b>🏢 Информация для сайта</b>\n"
        f"Компания: <b>{e(company_name)}</b>\n"
        f"Сфера: {e(business_type)}\n"
        f"Цвета: {e(site.get('color_palette'))}\n\n"
        f"<b>📞 Контакты</b>\n"
        f"Телефон: {e(phone)}\n"
        f"Email: {e(email)}\n"
        f"Адрес: {e(address)}\n\n"
        f"<b>📝 Описание</b>\n"
        f"{e(site.get('summary') or '—')}\n\n"
        f"<b>🕐 Режим работы:</b> {e(site.get('work_hours'))}\n\n"
        f"<b>📷 Фото:</b> {images_count} ({photo_summary})\n\n"
        f"<b>🛠 Услуги ({len(services)}):</b>\n{services_txt}\n\n"
        f"<b>📊 Дополнительно:</b>\n"
        f"• Портфолио: {len(portfolio)}\n"
        f"• Отзывы: {len(testimonials)}\n"
        f"• FAQ: {len(faq)}"
        f"{result_block}"
    )


def register(dp, bot):

    # ==================== МОИ ЗАЯВКИ ====================

    async def cmd_my_requests(message: types.Message, state: FSMContext):
        mode = get_mode(message.from_user.id)
        if mode not in ("manager", "admin"):
            return await message.answer("Эта функция доступна только для зарегистрированных пользователей.")

        if not get_user_by_tgid(message.from_user.id):
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW, BTN_ADMIN_LOGIN)
            return await message.answer("Сначала необходимо пройти регистрацию.", reply_markup=kb)

        page, per_page = 1, 10
        total = count_manager_requests(message.from_user.id)

        if total == 0:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW)
            kb.add(BTN_MY, BTN_ARCHIVE)
            kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            return await message.answer(
                "📋 <b>Мои заявки</b>\n\n"
                "У вас пока нет активных заявок.\n"
                "Нажмите «➕ Новая заявка» для создания.",
                reply_markup=kb
            )

        rows = list_manager_requests(message.from_user.id, offset=(page - 1) * per_page, limit=per_page)

        await message.answer(
            f"📋 <b>Мои заявки</b> ({total})\n\n"
            "Выберите заявку для просмотра:",
            reply_markup=requests_list_inline(rows, page, total, per_page)
        )

    dp.register_message_handler(cmd_my_requests, commands=["my_requests"], state="*")
    dp.register_message_handler(cmd_my_requests, lambda m: m.text == BTN_MY, state="*")

    # ==================== АРХИВ ====================

    async def cmd_archive(message: types.Message, state: FSMContext):
        """Архив заявок"""
        total = count_manager_archive(message.from_user.id)

        if total == 0:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW)
            kb.add(BTN_MY, BTN_ARCHIVE)
            return await message.answer("🗄 <b>Архив пуст</b>", reply_markup=kb)

        rows = list_manager_archive(message.from_user.id, 0, 10)

        await message.answer(
            f"🗄 <b>Архив заявок</b> ({total})",
            reply_markup=requests_list_inline(rows, 1, total, 10)
        )

    dp.register_message_handler(cmd_archive, commands=["archive"], state="*")
    dp.register_message_handler(cmd_archive, lambda m: m.text == BTN_ARCHIVE, state="*")

    # ==================== ПАГИНАЦИЯ ====================

    async def cb_list_page(call: types.CallbackQuery):
        try:
            page = int(call.data[len(CB_LIST_PAGE):])
        except ValueError:
            page = 1

        per_page = 10
        total = count_manager_requests(call.from_user.id)
        pages = max(1, (total + per_page - 1) // per_page)
        page = min(max(1, page), pages)
        rows = list_manager_requests(call.from_user.id, offset=(page - 1) * per_page, limit=per_page)

        try:
            await call.message.edit_reply_markup(requests_list_inline(rows, page, total, per_page))
        except MessageNotModified:
            pass

    dp.register_callback_query_handler(cb_list_page, lambda c: c.data and c.data.startswith(CB_LIST_PAGE))

    # ==================== ОТКРЫТЬ ЗАЯВКУ ====================

    async def cb_open_request(call: types.CallbackQuery):
        await call.answer()
        req_id = call.data[len(CB_OPEN):]

        try:
            rec = get_request(req_id)
            if not rec:
                return await call.message.answer("❌ Заявка не найдена.")

            user = get_user_by_tgid(call.from_user.id)
            is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
            is_admin = (get_mode(call.from_user.id) == "admin")

            # Получаем статус
            payload = get_request_payload(req_id) or {}
            site = payload.get("site") or {}
            meta = site.get("meta") or {}
            status = meta.get("status") or "draft"

            txt = format_request_card(rec, show_private=(is_owner or is_admin))

            try:
                await call.message.edit_text(
                    txt,
                    reply_markup=request_card_inline(rec["id"], is_owner, is_admin, status)
                )
            except MessageNotModified:
                pass

        except Exception as ex:
            log.exception("cb_open_request failed")
            await call.message.answer("⚠️ Ошибка при открытии заявки.")

    dp.register_callback_query_handler(cb_open_request, lambda c: c.data and c.data.startswith(CB_OPEN), state="*")

    async def cb_back_list(call: types.CallbackQuery):
        page, per_page = 1, 10
        total = count_manager_requests(call.from_user.id)

        if total == 0:
            return await call.message.edit_text("У вас пока нет заявок.")

        rows = list_manager_requests(call.from_user.id, offset=0, limit=per_page)
        await call.message.edit_text(
            f"📋 <b>Мои заявки</b> ({total})\n\nВыберите заявку:",
            reply_markup=requests_list_inline(rows, page, total, per_page)
        )

    dp.register_callback_query_handler(cb_back_list, lambda c: c.data and c.data == CB_BACK_TO_LIST)

    # ==================== ЗАКРЫТЬ ЗАЯВКУ ====================

    async def cb_close_request(call: types.CallbackQuery):
        req_id = call.data[len(CB_CLOSE_REQ):]

        user = get_user_by_tgid(call.from_user.id)
        if not user:
            return await call.answer("⛔ Нет прав", show_alert=True)

        set_request_status(req_id, STATUS_CLOSED)
        log_activity(str(user["id"]), "request_closed", "request", req_id)

        await call.answer("✅ Заявка закрыта", show_alert=True)

        # Обновляем карточку
        rec = get_request(req_id)
        if rec:
            is_owner = str(rec.get("manager_id")) == str(user["id"])
            is_admin = get_mode(call.from_user.id) == "admin"
            txt = format_request_card(rec, show_private=True)
            await call.message.edit_text(txt, reply_markup=request_card_inline(req_id, is_owner, is_admin, STATUS_CLOSED))

    dp.register_callback_query_handler(cb_close_request, lambda c: c.data and c.data.startswith(CB_CLOSE_REQ))

    # ==================== В АРХИВ ====================

    async def cb_archive_request(call: types.CallbackQuery):
        req_id = call.data[len(CB_ARCHIVE_REQ):]

        user = get_user_by_tgid(call.from_user.id)
        if not user:
            return await call.answer("⛔ Нет прав", show_alert=True)

        ok = archive_request(req_id, str(user["id"]), "completed")

        if ok:
            await call.answer("✅ Заявка перемещена в архив", show_alert=True)
            await call.message.edit_text(
                "🗄 <b>Заявка в архиве</b>\n\n"
                "Заявка успешно архивирована. Вы можете найти её в разделе «Архив»."
            )
        else:
            await call.answer("❌ Не удалось архивировать", show_alert=True)

    dp.register_callback_query_handler(cb_archive_request, lambda c: c.data and c.data.startswith(CB_ARCHIVE_REQ))

    # ==================== УДАЛЕНИЕ ====================

    async def cb_delete_request(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_DELETE):]
        user = get_user_by_tgid(call.from_user.id)

        if not user:
            return await call.answer("⛔ Нет прав для удаления.", show_alert=True)

        ok = delete_request(req_id, manager_id=str(user["id"])) or (
            get_mode(call.from_user.id) == "admin" and delete_request(req_id)
        )

        if ok:
            log_activity(str(user["id"]), "request_deleted", "request", req_id)
            await call.message.edit_text("✅ Заявка удалена.")
        else:
            await call.answer("❌ Не удалось удалить заявку.", show_alert=True)

    dp.register_callback_query_handler(cb_delete_request, lambda c: c.data and c.data.startswith(CB_DELETE))

    # ==================== РЕДАКТИРОВАНИЕ ====================

    async def cb_edit_request(call: types.CallbackQuery):
        req_id = call.data[len(CB_EDIT):]
        rec = get_request(req_id)

        if not rec:
            return await call.message.edit_text("❌ Заявка не найдена.")

        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))

        if not (is_admin or is_owner):
            return await call.answer("⛔ Нет прав на редактирование.", show_alert=True)

        await call.message.edit_text(
            f"✏️ <b>Редактирование заявки</b>\n\n"
            "Выберите поле для изменения:",
            reply_markup=edit_fields_inline(rec["id"])
        )

    dp.register_callback_query_handler(
        cb_edit_request,
        lambda c: c.data and c.data.startswith(CB_EDIT) and not c.data.startswith(CB_EDIT_FIELD)
    )

    async def cb_edit_field(call: types.CallbackQuery, state: FSMContext):
        payload = call.data[len(CB_EDIT_FIELD):]
        parts = payload.split("_", 1)

        if len(parts) != 2:
            return await call.answer("Ошибка данных", show_alert=True)

        req_id, field = parts
        rec = get_request(req_id)

        if not rec:
            return await call.message.edit_text("❌ Заявка не найдена.")

        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))

        if not (is_admin or is_owner):
            return await call.answer("⛔ Нет прав на редактирование.", show_alert=True)

        title = EDITABLE_FIELDS.get(field, field)
        hint = EDIT_HINTS.get(field, f"Введите новое значение для: <b>{title}</b>")

        # Текущее значение
        payload_data = get_request_payload(req_id) or {}
        site = payload_data.get("site") or {}

        current_value = ""
        if field == "seo_description":
            current_value = (site.get("seo") or {}).get("description", "")
        elif field == "hero_title":
            current_value = (site.get("hero") or {}).get("title", "")
        elif field == "hero_subtitle":
            current_value = (site.get("hero") or {}).get("subtitle", "")
        elif field in ("services", "portfolio", "testimonials", "faq"):
            items = site.get(field) or []
            current_value = f"({len(items)} записей)"
        else:
            current_value = site.get(field, "")

        await state.update_data(edit_req_id=req_id, edit_field=field)
        await EditField.waiting_value.set()

        current_txt = f"\n\n📋 <b>Текущее значение:</b>\n<i>{e(current_value)}</i>" if current_value else ""

        await call.message.edit_text(f"✏️ <b>{title}</b>{current_txt}\n\n{hint}")

    dp.register_callback_query_handler(cb_edit_field, lambda c: c.data and c.data.startswith(CB_EDIT_FIELD))

    async def on_edit_value(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req_id, field = data.get("edit_req_id"), data.get("edit_field")
        rec = get_request(req_id)

        if not rec:
            await state.finish()
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW, BTN_MY, BTN_RESET, BTN_ADMIN_LOGIN)
            return await message.answer("❌ Заявка не найдена.", reply_markup=kb)

        user = get_user_by_tgid(message.from_user.id)
        is_admin = get_mode(message.from_user.id) == "admin"
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))

        if not (is_admin or is_owner):
            await state.finish()
            return await message.answer("⛔ Нет прав на редактирование.")

        payload = get_request_payload(req_id) or {}
        site_json = payload.get("site") or {}
        val = (message.text or "").strip()

        if not val:
            return await message.answer("❌ Значение не может быть пустым.")

        # Обработка полей
        if field == "structure":
            site_json["structure"] = [s.strip() for s in val.replace(";", ",").split(",") if s.strip()]
        elif field == "services":
            services = parse_services(val)
            if not services:
                return await message.answer("❌ Не удалось распознать услуги.")
            site_json["services"] = services
        elif field == "portfolio":
            portfolio = parse_portfolio(val)
            if not portfolio:
                return await message.answer("❌ Не удалось распознать портфолио.")
            site_json["portfolio"] = portfolio
        elif field == "testimonials":
            testimonials = parse_testimonials(val)
            if not testimonials:
                return await message.answer("❌ Не удалось распознать отзывы.")
            site_json["testimonials"] = testimonials
        elif field == "faq":
            faq = parse_faq(val)
            if not faq:
                return await message.answer("❌ Не удалось распознать FAQ.")
            site_json["faq"] = faq
        elif field == "seo_description":
            if len(val) < 10:
                return await message.answer("❌ Описание слишком короткое.")
            site_json.setdefault("seo", {})["description"] = val
            site_json["seo"]["title"] = default_seo_title(site_json.get("company", ""), site_json.get("business_type", ""))
        elif field == "hero_title":
            if len(val) < 3:
                return await message.answer("❌ Заголовок слишком короткий.")
            site_json.setdefault("hero", {})["title"] = val
        elif field == "hero_subtitle":
            if len(val) < 5:
                return await message.answer("❌ Подзаголовок слишком короткий.")
            site_json.setdefault("hero", {})["subtitle"] = val
        elif field == "summary":
            if len(val) < 10:
                return await message.answer("❌ Описание слишком короткое.")
            site_json["summary"] = val
        elif field == "company":
            if len(val) < 2:
                return await message.answer("❌ Название слишком короткое.")
            site_json["company"] = val
        elif field == "phone":
            import re
            clean = re.sub(r'[^\d+]', '', val)
            if len(clean) < 10:
                return await message.answer("❌ Введите корректный номер телефона.")
            site_json["phone"] = val
        elif field == "email":
            if '@' not in val or '.' not in val:
                return await message.answer("❌ Введите корректный email.")
            site_json["email"] = val
        else:
            site_json[field] = val

        try:
            update_request_site_json(req_id, site_json)

            payload_after = get_request_payload(req_id) or {}
            if has_min_requirements(payload_after):
                set_request_status(req_id, STATUS_READY_TO_GENERATE)

            await state.finish()

            # Получаем статус для карточки
            meta = site_json.get("meta") or {}
            status = meta.get("status") or "draft"

            txt = format_request_card(get_request(req_id), show_private=True)
            await message.answer(f"✅ <b>{EDITABLE_FIELDS.get(field, field)}</b> обновлено.")
            await message.answer(txt, reply_markup=request_card_inline(req_id, is_owner, is_admin, status))

        except Exception:
            await state.finish()
            await message.answer("⚠️ Не удалось сохранить изменения.")

    dp.register_message_handler(on_edit_value, state=EditField.waiting_value)

    # ==================== ЭКСПОРТ ====================

    async def cb_export_one(call: types.CallbackQuery):
        req_id = call.data[len(CB_EXPORT_ONE):]
        rec = get_request(req_id)

        if not rec:
            return await call.answer("❌ Заявка не найдена.", show_alert=True)

        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))

        if not (is_admin or is_owner):
            return await call.answer("⛔ Нет прав на экспорт.", show_alert=True)

        payload = get_request_payload(req_id) or {}
        payload = {
            "request_id": str(rec["id"]),
            "manager_id": str(rec.get("manager_id") or ""),
            **payload,
        }
        payload = convert_uuids_to_strings(payload)

        json_str = json.dumps(payload, ensure_ascii=False, indent=2)
        file_bytes = io.BytesIO(json_str.encode('utf-8'))
        file_bytes.name = f"request_{str(rec['id'])[:8]}.json"

        await call.message.answer_document(
            types.InputFile(file_bytes, filename=file_bytes.name),
            caption="📤 Экспорт заявки"
        )

    dp.register_callback_query_handler(cb_export_one, lambda c: c.data and c.data.startswith(CB_EXPORT_ONE))

    # ==================== ГЕНЕРАЦИЯ ====================

    async def cb_generate_site(call: types.CallbackQuery):
        req_id = call.data[len(CB_GEN):]
        rec = get_request(req_id)

        if not rec:
            return await call.answer("❌ Заявка не найдена.", show_alert=True)

        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))

        if not (is_admin or is_owner):
            return await call.answer("⛔ Нет прав на генерацию.", show_alert=True)

        payload = get_request_payload(req_id) or {}

        if not has_min_requirements(payload):
            site = payload.get("site") or {}
            has_company = bool((site.get("company") or "").strip())

            if not has_company:
                set_request_status(req_id, STATUS_COLLECTING_INFO)
                return await call.answer("❌ Укажите название компании.", show_alert=True)
            else:
                set_request_status(req_id, STATUS_COLLECTING_PHOTOS)
                return await call.answer("❌ Добавьте хотя бы одно фото.", show_alert=True)

        # Отмечаем начало генерации
        mark_generation_started(req_id)
        log_activity(str(user["id"]), "generation_started", "request", req_id)

        safe_payload = {
            "request_id": str(rec["id"]),
            "manager_id": str(rec.get("manager_id") or ""),
            **payload,
        }
        safe_payload = convert_uuids_to_strings(safe_payload)

        await call.answer()
        await post_generate_site(call.message.chat.id, safe_payload, call.message)

    dp.register_callback_query_handler(cb_generate_site, lambda c: c.data and c.data.startswith(CB_GEN))

    # ==================== ТЕСТ ВЕБХУКА ====================

    async def cmd_test_webhook(message: types.Message):
        await test_webhook(message.chat.id, message)

    dp.register_message_handler(cmd_test_webhook, commands=["test_webhook"], state="*")
