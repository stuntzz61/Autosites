# app/handlers/requests.py
import io
import json
import zipfile
import logging
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.utils.exceptions import MessageNotModified

from app.constants import (
    BTN_NEW, BTN_MY, BTN_RESET, BTN_ADMIN_LOGIN,
    CB_OPEN, CB_LIST_PAGE, CB_BACK_TO_LIST, CB_DELETE,
    CB_EDIT, CB_EDIT_FIELD, CB_EXPORT_ONE, CB_GEN,
    EDITABLE_FIELDS,
    # статусы
    STATUS_COLLECTING_INFO, STATUS_COLLECTING_PHOTOS, STATUS_READY_TO_GENERATE, STATUS_QUEUED,
)
from app.db import (
    get_mode, get_user_by_tgid, list_manager_requests, count_manager_requests,
    get_request, delete_request, update_request_site_json,
    get_request_payload,  # читаем payload из БД везде
    set_request_status,   # меняем статус заявки
)
from app.keyboards import requests_list_inline, request_card_inline
from app.utils import (
    e, parse_services, parse_portfolio, parse_testimonials, parse_faq,
    default_seo_title, chunks, convert_uuids_to_strings, has_min_requirements,
)
from app.states import EditField
from app.services.n8n import post_generate_site, test_webhook

log = logging.getLogger("bot")


def register(dp, bot):

    # ------- формат карточки -------
    def format_request_card(rec: dict, show_private: bool = True) -> str:
        """Рендер карточки заявки из актуального payload_json."""
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

        services_txt = "\n".join(
            [
                f"• {e(s.get('name', ''))}"
                + (f" — {e(s.get('summary', ''))}" if s.get("summary") else "")
                + (f" — {e(s.get('priceFrom', ''))}" if s.get("priceFrom") else "")
                for s in services
            ]
        ) or "—"

        structure_txt = ", ".join([e(s) for s in structure]) or "—"

        seo = site.get("seo") or {}
        hero = site.get("hero") or {}
        has_seo = bool((seo.get("description") or "").strip())
        has_hero = bool((hero.get("title") or "").strip())

        # приватные поля клиента — только владельцу/админу
        client_name = rec.get("client_name") or client_json.get("name")
        client_company = rec.get("client_company") or client_json.get("company")
        client_contact = rec.get("client_contact") or client_json.get("contact")
        client_block = (
            f"Клиент: <b>{e(client_name)}</b>\n"
            f"Компания клиента: {e(client_company)}\n"
            f"Контакты клиента: {e(client_contact)}\n\n"
        ) if show_private else ""

        # статус из payload_json.site.meta.status
        meta = site.get("meta") or {}
        status = meta.get("status") or "draft"

        return (
            f"<b>Заявка #{rec['id']}</b>\n"
            f"Статус: <i>{e(status)}</i>\n"
            f"{client_block}"
            f"<b>Для сайта</b>\n"
            f"Название компании: {e(site.get('company'))}\n"
            f"Чем занимаетесь: {e(site.get('business_type'))}\n"
            f"Цвета: {e(site.get('color_palette'))}\n"
            f"Контакты: {e(site.get('site_contacts'))}\n"
            f"Описание компании: {e(site.get('summary'))}\n"
            f"Режим работы: {e(site.get('work_hours'))}\n"
            f"Структура: {structure_txt}\n"
            f"Картинки: {images_count}\n"
            f"Услуги ({len(services)}):\n{services_txt}\n\n"
            f"Портфолио: {len(portfolio)} | Отзывы: {len(testimonials)} | FAQ: {len(faq)}\n"
            f"Поисковое описание: {'добавлено' if has_seo else '—'} | Первый экран: {'настроен' if has_hero else '—'}"
        )

    # ------- /my_requests -------
    async def cmd_my_requests(message: types.Message, state: FSMContext):
        mode = get_mode(message.from_user.id)
        if mode not in ("manager", "admin"):
            return await message.answer("Доступно только для менеджера.")
        if not get_user_by_tgid(message.from_user.id):
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW, BTN_ADMIN_LOGIN)
            return await message.answer("Сначала регистрация: «📝 Регистрация».", reply_markup=kb)

        page, per_page = 1, 10
        total = count_manager_requests(message.from_user.id)
        if total == 0:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW)
            kb.add(BTN_MY)
            kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            return await message.answer("У вас пока нет заявок. Нажмите «➕ Создать заявку».", reply_markup=kb)

        rows = list_manager_requests(message.from_user.id, offset=(page - 1) * per_page, limit=per_page)
        await message.answer("Список ваших заявок:", reply_markup=types.ReplyKeyboardRemove())
        await message.answer("Выберите заявку:", reply_markup=requests_list_inline(rows, page, total, per_page))

    dp.register_message_handler(cmd_my_requests, commands=["my_requests"], state="*")
    dp.register_message_handler(cmd_my_requests, lambda m: m.text == BTN_MY, state="*")

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

    async def cb_open_request(call: types.CallbackQuery):
        await call.answer()
        req_id = call.data[len(CB_OPEN):]
        try:
            rec = get_request(req_id)
            if not rec:
                return await call.message.answer("Заявка не найдена.")
            user = get_user_by_tgid(call.from_user.id)
            is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
            is_admin = (get_mode(call.from_user.id) == "admin")

            txt = format_request_card(rec, show_private=(is_owner or is_admin))
            await call.message.edit_text(txt, reply_markup=request_card_inline(rec["id"], is_owner, is_admin))

            payload = get_request_payload(req_id) or {}
            site = payload.get("site") or {}
            need_more = not site.get("portfolio") or not site.get("testimonials") or not site.get("faq")
            if need_more:
                from app.keyboards import more_details_inline
                await call.message.answer("Можно добавить детали:", reply_markup=more_details_inline(rec["id"]))
        except Exception as ex:
            log.exception("cb_open_request failed")
            await call.message.answer(f"⚠️ Ошибка открытия: {ex}")

    dp.register_callback_query_handler(cb_open_request, lambda c: c.data and c.data.startswith(CB_OPEN), state="*")

    async def cb_back_list(call: types.CallbackQuery):
        page, per_page = 1, 10
        total = count_manager_requests(call.from_user.id)
        if total == 0:
            return await call.message.edit_text("У вас пока нет заявок.")
        rows = list_manager_requests(call.from_user.id, offset=0, limit=per_page)
        await call.message.edit_text("Список ваших заявок:", reply_markup=requests_list_inline(rows, page, total, per_page))

    dp.register_callback_query_handler(cb_back_list, lambda c: c.data and c.data == CB_BACK_TO_LIST)

    # ------- delete -------
    async def cb_delete_request(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_DELETE):]
        user = get_user_by_tgid(call.from_user.id)
        if not user:
            return await call.answer("Нет прав.", show_alert=True)
        ok = delete_request(req_id, manager_id=str(user["id"])) or (
            get_mode(call.from_user.id) == "admin" and delete_request(req_id)
        )
        if ok:
            await call.message.edit_text(f"Заявка #{req_id} удалена.")
        else:
            await call.answer("Не удалось удалить (возможно, нет прав).", show_alert=True)

    dp.register_callback_query_handler(cb_delete_request, lambda c: c.data and c.data.startswith(CB_DELETE))

    # ------- edit -------
    async def cb_edit_request(call: types.CallbackQuery):
        req_id = call.data[len(CB_EDIT):]
        rec = get_request(req_id)
        if not rec:
            return await call.message.edit_text("Заявка не найдена.")
        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        if not (is_admin or is_owner):
            return await call.answer("Нет прав на редактирование.", show_alert=True)
        from app.keyboards import edit_fields_inline
        await call.message.edit_text(
            f"Редактирование заявки #{rec['id']}: выберите поле ниже.",
            reply_markup=edit_fields_inline(rec["id"])
        )

    dp.register_callback_query_handler(cb_edit_request, lambda c: c.data and c.data.startswith(CB_EDIT))

    async def cb_edit_field(call: types.CallbackQuery, state: FSMContext):
        payload = call.data[len(CB_EDIT_FIELD):]
        req_id, field = payload.split("_", 1)
        rec = get_request(req_id)
        if not rec:
            return await call.message.edit_text("Заявка не найдена.")
        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        if not (is_admin or is_owner):
            return await call.answer("Нет прав на редактирование.", show_alert=True)

        title = EDITABLE_FIELDS.get(field, field)
        await state.update_data(edit_req_id=req_id, edit_field=field)
        await EditField.waiting_value.set()
        await call.message.edit_text(
            f"Введите новое значение для: <b>{title}</b>\n\n"
            f"Если «Структура» — перечислите секции через запятую.\n"
            f"Если «Услуги» — по одной строке: Название — кратко — от цена."
        )

    dp.register_callback_query_handler(cb_edit_field, lambda c: c.data and c.data.startswith(CB_EDIT_FIELD))

    async def on_edit_value(message: types.Message, state: FSMContext):
        data = await state.get_data()
        req_id, field = data.get("edit_req_id"), data.get("edit_field")
        rec = get_request(req_id)
        if not rec:
            await state.finish()
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW, BTN_MY, BTN_RESET, BTN_ADMIN_LOGIN)
            return await message.answer("Заявка не найдена.", reply_markup=kb)

        user = get_user_by_tgid(message.from_user.id)
        is_admin = get_mode(message.from_user.id) == "admin"
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        if not (is_admin or is_owner):
            await state.finish()
            return await message.answer("Нет прав на редактирование.", reply_markup=types.ReplyKeyboardRemove())

        payload = get_request_payload(req_id) or {}
        site_json = payload.get("site") or {}
        val = (message.text or "").strip()

        if field == "structure":
            site_json["structure"] = [s.strip() for s in val.replace(";", ",").split(",") if s.strip()]
        elif field == "services":
            site_json["services"] = parse_services(val)
        elif field == "portfolio":
            site_json["portfolio"] = parse_portfolio(val)
        elif field == "testimonials":
            site_json["testimonials"] = parse_testimonials(val)
        elif field == "faq":
            site_json["faq"] = parse_faq(val)
        elif field == "seo_description":
            site_json.setdefault("seo", {})["description"] = val
            site_json["seo"]["title"] = default_seo_title(site_json.get("company", ""), site_json.get("business_type", ""))
        elif field == "hero_title":
            site_json.setdefault("hero", {})["title"] = val
            site_json["hero"].setdefault("primaryCta", {"label": "Оставить заявку", "href": "#contact"})
            site_json["hero"].setdefault("secondaryCta", {"label": "Портфолио", "href": "#portfolio"})
            site_json["hero"].setdefault("image", "/public/illustrations/hero.svg")
        elif field == "hero_subtitle":
            site_json.setdefault("hero", {})["subtitle"] = val
        elif field in {"summary", "short_desc"}:
            site_json["summary"] = val
        else:
            # универсально для простых полей site.*
            site_json[field] = val

        try:
            update_request_site_json(req_id, site_json)

            # авто-статус, если выполнились минимальные требования
            payload_after = get_request_payload(req_id) or {}
            if has_min_requirements(payload_after):
                set_request_status(req_id, STATUS_READY_TO_GENERATE)

            await state.finish()
            txt = format_request_card(get_request(req_id), show_private=True)
            await message.answer(f"✅ Поле <b>{EDITABLE_FIELDS.get(field, field)}</b> обновлено.", parse_mode="HTML")
            await message.answer(txt, reply_markup=request_card_inline(req_id, is_owner, is_admin))
        except Exception:
            await state.finish()
            await message.answer("⚠️ Не удалось сохранить изменения. Попробуйте ещё раз.", reply_markup=types.ReplyKeyboardRemove())

    dp.register_message_handler(on_edit_value, state=EditField.waiting_value)

    # ------- export -------
    async def cb_export_one(call: types.CallbackQuery):
        req_id = call.data[len(CB_EXPORT_ONE):]
        rec = get_request(req_id)
        if not rec:
            return await call.answer("Заявка не найдена.", show_alert=True)

        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        if not (is_admin or is_owner):
            return await call.answer("Нет прав на экспорт этой заявки.", show_alert=True)

        payload = get_request_payload(req_id) or {}
        payload = {
            "request_id": str(rec["id"]),
            "manager_id": str(rec.get("manager_id") or ""),
            **payload,
        }
        payload = convert_uuids_to_strings(payload)

        json_str = json.dumps(payload, ensure_ascii=False, indent=2)
        fname = f"request_{rec['id']}.json"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(json_str)
        await call.message.answer_document(types.InputFile(fname), caption=f"Экспорт заявки #{rec['id']} (JSON)")

    dp.register_callback_query_handler(cb_export_one, lambda c: c.data and c.data.startswith(CB_EXPORT_ONE))

    # ------- generate (n8n) -------
    async def cb_generate_site(call: types.CallbackQuery):
        req_id = call.data[len(CB_GEN):]
        rec = get_request(req_id)
        if not rec:
            return await call.answer("Заявка не найдена.", show_alert=True)

        user = get_user_by_tgid(call.from_user.id)
        is_admin = (get_mode(call.from_user.id) == "admin")
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        if not (is_admin or is_owner):
            return await call.answer("Нет прав на генерацию по этой заявке.", show_alert=True)

        payload = get_request_payload(req_id) or {}
        if not has_min_requirements(payload):
            # Подсказка и перевод статуса
            site = payload.get("site") or {}
            has_company = bool((site.get("company") or "").strip())
            if not has_company:
                set_request_status(req_id, STATUS_COLLECTING_INFO)
                return await call.answer("Сначала заполните анкету (нет названия компании).", show_alert=True)
            else:
                set_request_status(req_id, STATUS_COLLECTING_PHOTOS)
                return await call.answer("Добавьте хотя бы одно фото перед генерацией.", show_alert=True)

        # Готово → queued и отправляем
        set_request_status(req_id, STATUS_QUEUED)

        safe_payload = {
            "request_id": str(rec["id"]),
            "manager_id": str(rec.get("manager_id") or ""),
            **payload,
        }
        safe_payload = convert_uuids_to_strings(safe_payload)

        await call.answer()
        await post_generate_site(call.message.chat.id, safe_payload, call.message)

    dp.register_callback_query_handler(cb_generate_site, lambda c: c.data and c.data.startswith(CB_GEN))

    # ------- test webhook -------
    async def cmd_test_webhook(message: types.Message):
        await test_webhook(message.chat.id, message)

    dp.register_message_handler(cmd_test_webhook, commands=["test_webhook"], state="*")

    # === DEBUG: ловим любые callback_query (кроме ping) ===
    async def _cb_debug_all(call: types.CallbackQuery):
        if call.data in {"ping"}:
            return
        log.warning(f"[CB_DEBUG] data={call.data!r} from user={call.from_user.id}")
        await call.answer()
        try:
            await call.message.answer(f"DEBUG: callback = <code>{call.data or ''}</code>")
        except Exception:
            pass

    dp.register_callback_query_handler(_cb_debug_all, lambda c: c.data not in {"ping"}, state="*")
