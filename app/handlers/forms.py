from aiogram import types
from aiogram.dispatcher import FSMContext

from app.constants import (
    BTN_BACK, BTN_EXIT, BTN_NEW, BTN_MY, BTN_RESET, BTN_ADMIN_LOGIN,
    DEFAULT_STRUCTURE, MIN_COMPANY_NAME_LEN, MAX_COMPANY_NAME_LEN,
    MIN_DESCRIPTION_LEN, MAX_DESCRIPTION_LEN,
    STATUS_COLLECTING_PHOTOS, STATUS_READY_TO_GENERATE,
    CB_DONE, CB_MORE_PORTF, CB_MORE_TESTI, CB_MORE_FAQ, CB_MORE_SEO, CB_MORE_HERO,
    CB_OPEN,
)
from app.states import RequestForm, PhotoUpload
from app.db import (
    get_mode, get_user_by_tgid, create_request_by_tgid,
    update_request_site_json, get_request, get_request_payload, get_current_request_id_by_tgid,
    set_request_status,
)
from app.utils import (
    default_seo_title, parse_services, parse_portfolio,
    parse_testimonials, parse_faq, slugify, has_min_requirements,
)
from app.keyboards import more_details_inline, photo_categories_inline, request_card_inline


def validate_text(text: str, min_len: int = 1, max_len: int = 500) -> tuple[bool, str]:
    """Валидация текстового поля"""
    if not text or not text.strip():
        return False, "❌ Поле не может быть пустым. Введите значение:"
    if len(text.strip()) < min_len:
        return False, f"❌ Слишком короткое значение (минимум {min_len} символов). Попробуйте ещё раз:"
    if len(text.strip()) > max_len:
        return False, f"❌ Слишком длинное значение (максимум {max_len} символов). Сократите и попробуйте ещё раз:"
    return True, ""


def validate_contact(text: str) -> tuple[bool, str]:
    """Валидация контактных данных"""
    if not text or not text.strip():
        return False, "❌ Контактные данные не могут быть пустыми. Введите телефон, email или другой контакт:"
    # Проверяем наличие хотя бы цифр или @ (email)
    has_digits = any(c.isdigit() for c in text)
    has_email = '@' in text
    if not has_digits and not has_email and len(text.strip()) < 5:
        return False, "❌ Введите корректные контактные данные (телефон, email и т.д.):"
    return True, ""


def register(dp, bot):
    # ------- prompts -------
    PREV_STATE = {
        RequestForm.client_company: RequestForm.client_name,
        RequestForm.client_contact: RequestForm.client_company,
        RequestForm.site_company:   RequestForm.client_contact,
        RequestForm.business_type:  RequestForm.site_company,
        RequestForm.color_palette:  RequestForm.business_type,
        RequestForm.site_contacts:  RequestForm.color_palette,
        RequestForm.short_desc:     RequestForm.site_contacts,
        RequestForm.work_hours:     RequestForm.short_desc,
        # structure убран - после work_hours сразу создаём заявку
    }

    async def prompt_for_state(state_name: RequestForm, message: types.Message):
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        kb.add(BTN_BACK, BTN_EXIT)
        prompts = {
            RequestForm.client_name:   "Введите <b>имя клиента</b>:",
            RequestForm.client_company:"Введите <b>название компании клиента</b>:",
            RequestForm.client_contact:"Введите <b>контактные данные клиента</b> (телефон, email):",
            RequestForm.site_company:  "Введите <b>название компании для сайта</b>:",
            RequestForm.business_type: "Введите <b>чем вы занимаетесь</b> (например: производственная компания):",
            RequestForm.color_palette: "Введите <b>пожелания по цветам</b> (или «-» если нет предпочтений):",
            RequestForm.site_contacts: "Укажите <b>контакты/адреса для сайта</b>:",
            RequestForm.short_desc:    "Опишите <b>компанию в 2–3 предложениях</b>:",
            RequestForm.work_hours:    "Введите <b>режим работы</b> (например: Пн-Пт 9:00-18:00):",
            RequestForm.services:      "Введите <b>услуги</b> — по одной в строке: <i>Название — кратко — от цена</i>.",
            RequestForm.portfolio:     "Портфолио — по одной строке: <i>Проект — клиент — год — кратко — теги «;» — опц.ссылка</i>.",
            RequestForm.testimonials:  "Отзывы — по одной строке: <i>Имя — Компания/Роль — цитата — опц.оценка 1-5</i>.",
            RequestForm.faq:           "FAQ — по одной строке: <i>Вопрос — Ответ</i>.",
            RequestForm.seo_description:"Описание для поисковиков (1–2 предложения).",
            RequestForm.hero_title:    "Заголовок на первом экране:",
            RequestForm.hero_subtitle: "Короткое описание под заголовком:",
        }
        await message.answer(prompts[state_name], reply_markup=kb)

    # ------- exit/back during forms -------
    async def cmd_exit_form(message: types.Message, state: FSMContext):
        cur_state = await state.get_state()
        from app.constants import (
            BTN_REG, BTN_ADMIN_LOGIN, BTN_NEW, BTN_MY, BTN_RESET,
            BTN_PANEL, BTN_USERS, BTN_REQS, BTN_LOGOUT
        )
        from app.db import set_mode
        if cur_state is None:
            mode = get_mode(message.from_user.id)
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            if mode == "admin":
                kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
            elif mode == "manager":
                kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            else:
                kb.add(BTN_REG, BTN_ADMIN_LOGIN)
            return await message.answer("Нет активной анкеты.", reply_markup=kb)
        await state.finish()
        set_mode(message.from_user.id, "manager")
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("Анкета закрыта. Вы в режиме менеджера.", reply_markup=kb)
    dp.register_message_handler(cmd_exit_form, lambda m: m.text in {BTN_EXIT, "/reset", "/cancel", "выйти", "отмена"}, state="*")

    async def go_back(message: types.Message, state: FSMContext):
        cur = await state.get_state()
        if not cur or not cur.startswith(RequestForm.__name__):
            return await message.answer("Сейчас не идёт заполнение анкеты.")
        cur_state_obj = None
        for s in RequestForm.states:
            if cur.endswith(s.state):
                cur_state_obj = s; break
        if cur_state_obj and cur_state_obj in PREV_STATE:
            prev = PREV_STATE[cur_state_obj]; await prev.set(); await prompt_for_state(prev, message)
        else:
            await message.answer("Назад идти больше некуда.")
    dp.register_message_handler(go_back, lambda m: m.text in {BTN_BACK, "назад", "/back"}, state="*")

    # ------- new_request (минимум) -------
    async def cmd_new_request(message: types.Message):
        if get_mode(message.from_user.id) not in ("manager","admin"):
            return await message.answer("Эта функция доступна только менеджеру.")
        if not get_user_by_tgid(message.from_user.id):
            from app.constants import BTN_REG, BTN_ADMIN_LOGIN
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_REG, BTN_ADMIN_LOGIN)
            return await message.answer("Сначала регистрация: «📝 Регистрация».", reply_markup=kb)
        await RequestForm.client_name.set()
        await prompt_for_state(RequestForm.client_name, message)
    dp.register_message_handler(cmd_new_request, commands=["new_request"], state="*")
    from app.constants import BTN_NEW
    dp.register_message_handler(cmd_new_request, lambda m: m.text == BTN_NEW, state="*")

    # ------- вопросы анкеты по порядку с валидацией -------
    async def q_client_name(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=2, max_len=100)
        if not valid:
            return await message.answer(error)
        await state.update_data(client_name=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.client_company, message)

    async def q_client_company(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=2, max_len=150)
        if not valid:
            return await message.answer(error)
        await state.update_data(client_company=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.client_contact, message)

    async def q_client_contact(message: types.Message, state: FSMContext):
        valid, error = validate_contact(message.text)
        if not valid:
            return await message.answer(error)
        await state.update_data(client_contact=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.site_company, message)

    async def q_site_company(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=MIN_COMPANY_NAME_LEN, max_len=MAX_COMPANY_NAME_LEN)
        if not valid:
            return await message.answer(error)
        await state.update_data(site_company=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.business_type, message)

    async def q_business_type(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=3, max_len=200)
        if not valid:
            return await message.answer(error)
        await state.update_data(business_type=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.color_palette, message)

    async def q_color_palette(message: types.Message, state: FSMContext):
        # Цвета опциональны, можно ввести "-"
        text = message.text.strip() if message.text else "-"
        if text == "-":
            text = "На усмотрение дизайнера"
        await state.update_data(color_palette=text)
        await RequestForm.next()
        await prompt_for_state(RequestForm.site_contacts, message)

    async def q_site_contacts(message: types.Message, state: FSMContext):
        valid, error = validate_contact(message.text)
        if not valid:
            return await message.answer(error)
        await state.update_data(site_contacts=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.short_desc, message)

    async def q_short_desc(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=MIN_DESCRIPTION_LEN, max_len=MAX_DESCRIPTION_LEN)
        if not valid:
            return await message.answer(error)
        await state.update_data(short_desc=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.work_hours, message)

    # После work_hours сразу создаём заявку (без вопроса про структуру)
    async def q_work_hours(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=3, max_len=200)
        if not valid:
            return await message.answer(error)
        await state.update_data(work_hours=message.text.strip())

        # Собираем данные и создаём заявку с дефолтной структурой
        data = await state.get_data()
        payload = _build_payload_from_state(data)
        req_id = create_request_by_tgid(message.from_user.id, payload)
        await state.update_data(edit_req_id=req_id)

        # Сразу переводим заявку в сбор фото
        set_request_status(req_id, STATUS_COLLECTING_PHOTOS)

        # Переходим к пошаговой загрузке фото
        await PhotoUpload.choosing_category.set()
        await state.update_data(edit_req_id=req_id, photo_category=None)

        await message.answer(
            f"✅ Создал черновик заявки <code>{req_id}</code> для <b>{payload['site'].get('company') or '—'}</b>.\n\n"
            "Теперь загрузим <b>фото</b>. Выберите категорию:",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await message.answer(
            "📷 <b>Выберите категорию фото для загрузки:</b>\n\n"
            "• 🏠 <b>Главное фото</b> — большой баннер на первом экране\n"
            "• 🛠 <b>Услуги</b> — иллюстрации для услуг\n"
            "• 📁 <b>Портфолио</b> — примеры работ\n"
            "• 👥 <b>Команда</b> — фото сотрудников\n"
            "• 🏢 <b>Офис</b> — фото офиса/производства",
            reply_markup=photo_categories_inline(req_id)
        )

    # helper — собрать payload из FSM (с дефолтной структурой)
    def _build_payload_from_state(data: dict) -> dict:
        company = data.get("site_company","")
        business = data.get("business_type","")
        short = (data.get("short_desc") or "")
        return {
            "client": {
                "name": data.get("client_name"),
                "company": data.get("client_company"),
                "contact": data.get("client_contact"),
            },
            "site": {
                "company": company,
                "business_type": data.get("business_type"),
                "color_palette": data.get("color_palette"),
                "site_contacts": data.get("site_contacts"),
                "summary": short,
                "work_hours": data.get("work_hours"),
                "structure": DEFAULT_STRUCTURE,  # Дефолтная структура
                "assets": {"images": []},
                "hero": {
                    "title": default_seo_title(company, business),
                    "subtitle": short[:120],
                    "primaryCta": {"label":"Оставить заявку","href":"#contact"},
                    "secondaryCta": {"label":"Портфолио","href":"#portfolio"},
                    "image": "/public/illustrations/hero.svg"
                },
                "seo": {
                    "title": default_seo_title(company, business),
                    "description": short[:160]
                }
            }
        }

    # helper — если заявка уже соответствует минимальным требованиям, помечаем как готовую к генерации
    def _reeval_status(req_id: str) -> None:
        try:
            payload = get_request_payload(req_id) or {}
            if has_min_requirements(payload):
                set_request_status(req_id, STATUS_READY_TO_GENERATE)
        except Exception:
            pass

    dp.register_message_handler(q_client_name, state=RequestForm.client_name)
    dp.register_message_handler(q_client_company, state=RequestForm.client_company)
    dp.register_message_handler(q_client_contact, state=RequestForm.client_contact)
    dp.register_message_handler(q_site_company, state=RequestForm.site_company)
    dp.register_message_handler(q_business_type, state=RequestForm.business_type)
    dp.register_message_handler(q_color_palette, state=RequestForm.color_palette)
    dp.register_message_handler(q_site_contacts, state=RequestForm.site_contacts)
    dp.register_message_handler(q_short_desc, state=RequestForm.short_desc)
    dp.register_message_handler(q_work_hours, state=RequestForm.work_hours)

    async def q_services(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(message.from_user.id)
        services = parse_services(message.text or "")

        if not services:
            return await message.answer("❌ Не удалось распознать услуги. Введите по одной в строке: <i>Название — кратко — от цена</i>")

        if not req_id:
            # крайний случай — не нашли черновик: создадим
            payload = _build_payload_from_state(st)
            payload["site"]["services"] = services
            req_id = create_request_by_tgid(message.from_user.id, payload)
            _reeval_status(req_id)
        else:
            rec = get_request(req_id)
            if not rec:
                await state.finish()
                return await message.answer("Заявка не найдена.", reply_markup=types.ReplyKeyboardRemove())
            payload = get_request_payload(req_id)
            site = (payload.get("site") or {})
            site["services"] = services
            # подстраховка hero/seo
            company = site.get("company",""); business = site.get("business_type",""); short = site.get("summary","")
            site.setdefault("hero", {
                "title": default_seo_title(company, business),
                "subtitle": short[:120],
                "primaryCta": {"label":"Оставить заявку","href":"#contact"},
                "secondaryCta": {"label":"Портфолио","href":"#portfolio"},
                "image": "/public/illustrations/hero.svg"
            })
            site.setdefault("seo", {
                "title": default_seo_title(company, business),
                "description": short[:160]
            })
            update_request_site_json(req_id, site)
            _reeval_status(req_id)

        await state.finish()
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        extras = "⚠️ Услуг меньше 3 — желательно добавить." if len(services) < 3 else "✅ Услуг достаточно."
        await message.answer(f"✅ Заявка обновлена! {extras}\n\nПри желании можно добавить детали (необязательно):", reply_markup=kb)
        await message.answer("Выберите, что добавить:", reply_markup=more_details_inline(req_id))
    dp.register_message_handler(q_services, state=RequestForm.services)

    # ------- extra fields callbacks -------
    async def _load_site(req_id: str) -> dict | None:
        rec = get_request(req_id)
        if not rec:
            return None
        data = get_request_payload(req_id)
        return data.get("site") or {}

    # ИСПРАВЛЕНО: кнопка "Готово" теперь перенаправляет к заявке
    async def cb_done(call: types.CallbackQuery, state: FSMContext):
        await call.answer("Готово!")

        # Извлекаем req_id из callback_data
        req_id = call.data[len(CB_DONE):] if call.data.startswith(CB_DONE) else None

        if not req_id:
            # Пробуем взять из FSM
            st = await state.get_data()
            req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(call.from_user.id)

        if not req_id:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            return await call.message.answer("Заявка не найдена. Перейдите в «📋 Мои заявки».", reply_markup=kb)

        rec = get_request(req_id)
        if not rec:
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            return await call.message.answer("Заявка удалена или не найдена.", reply_markup=kb)

        # Показываем карточку заявки
        from app.handlers.requests import format_request_card
        user = get_user_by_tgid(call.from_user.id)
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        is_admin = (get_mode(call.from_user.id) == "admin")

        txt = format_request_card(rec, show_private=(is_owner or is_admin))
        await call.message.answer(txt, reply_markup=request_card_inline(rec["id"], is_owner, is_admin))

        # Возвращаем клавиатуру менеджера
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await call.message.answer("Вы можете отредактировать заявку или запустить генерацию сайта.", reply_markup=kb)

    dp.register_callback_query_handler(cb_done, lambda c: c.data and c.data.startswith(CB_DONE), state="*")

    async def cb_more_portf(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_PORTF):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.portfolio.set()
        await call.message.answer("Добавьте портфолио.\nПример: «Интеграция 1С — Ритейл X — 2025 — realtime остатки — 1С; e-commerce — https://…»")
    dp.register_callback_query_handler(cb_more_portf, lambda c: c.data and c.data.startswith(CB_MORE_PORTF), state="*")

    async def cb_more_testi(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_TESTI):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.testimonials.set()
        await call.message.answer("Добавьте отзывы.\nПример: «Ирина Кузнецова — ООО «Альфа» — Всё быстро и по делу — 5»")
    dp.register_callback_query_handler(cb_more_testi, lambda c: c.data and c.data.startswith(CB_MORE_TESTI), state="*")

    async def cb_more_faq(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_FAQ):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.faq.set()
        await call.message.answer("Добавьте FAQ. Пример: «Сроки? — Пилот 2 недели, проект 4–6 недель.»")
    dp.register_callback_query_handler(cb_more_faq, lambda c: c.data and c.data.startswith(CB_MORE_FAQ), state="*")

    async def cb_more_seo(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_SEO):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.seo_description.set()
        await call.message.answer("Поисковое описание (1–2 предложения). Можно вставить/исправить готовое.")
    dp.register_callback_query_handler(cb_more_seo, lambda c: c.data and c.data.startswith(CB_MORE_SEO), state="*")

    async def cb_more_hero(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_HERO):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.hero_title.set()
        await call.message.answer("Заголовок на первом экране (крупная надпись):")
    dp.register_callback_query_handler(cb_more_hero, lambda c: c.data and c.data.startswith(CB_MORE_HERO), state="*")

    async def save_portfolio(message: types.Message, state: FSMContext):
        st = await state.get_data(); req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Эта заявка уже удалена.")
        portfolio = parse_portfolio(message.text or "")
        if not portfolio:
            return await message.answer("❌ Не удалось распознать портфолио. Введите по формату: <i>Проект — клиент — год — кратко</i>")
        site["portfolio"] = portfolio
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ Портфолио сохранено.", reply_markup=more_details_inline(req_id))

    async def save_testimonials(message: types.Message, state: FSMContext):
        st = await state.get_data(); req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Эта заявка уже удалена.")
        testimonials = parse_testimonials(message.text or "")
        if not testimonials:
            return await message.answer("❌ Не удалось распознать отзывы. Введите по формату: <i>Имя — Компания — цитата</i>")
        site["testimonials"] = testimonials
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ Отзывы сохранены.", reply_markup=more_details_inline(req_id))

    async def save_faq(message: types.Message, state: FSMContext):
        st = await state.get_data(); req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Эта заявка уже удалена.")
        faq = parse_faq(message.text or "")
        if not faq:
            return await message.answer("❌ Не удалось распознать FAQ. Введите по формату: <i>Вопрос — Ответ</i>")
        site["faq"] = faq
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ FAQ сохранён.", reply_markup=more_details_inline(req_id))

    async def save_seo(message: types.Message, state: FSMContext):
        st = await state.get_data(); req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Эта заявка уже удалена.")
        text = (message.text or "").strip()
        if len(text) < 10:
            return await message.answer("❌ Описание слишком короткое (минимум 10 символов).")
        company = (site.get("company") or "")
        business = (site.get("business_type") or "")
        site.setdefault("seo", {})
        site["seo"]["title"] = default_seo_title(company, business)
        site["seo"]["description"] = text
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ Поисковое описание сохранено.", reply_markup=more_details_inline(req_id))

    async def save_hero_title(message: types.Message, state: FSMContext):
        text = (message.text or "").strip()
        if len(text) < 3:
            return await message.answer("❌ Заголовок слишком короткий (минимум 3 символа).")
        await state.update_data(hero_t=text)
        await RequestForm.hero_subtitle.set()
        await message.answer("Короткое описание под заголовком:")

    async def save_hero_subtitle(message: types.Message, state: FSMContext):
        st = await state.get_data(); req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Эта заявка уже удалена.")
        title = st.get("hero_t") or site.get("hero", {}).get("title", "")
        subtitle = (message.text or "").strip()
        if len(subtitle) < 5:
            return await message.answer("❌ Подзаголовок слишком короткий (минимум 5 символов).")
        site.setdefault("hero", {})
        site["hero"]["title"] = title
        site["hero"]["subtitle"] = subtitle
        site["hero"]["primaryCta"] = {"label":"Оставить заявку","href":"#contact"}
        site["hero"]["secondaryCta"] = {"label":"Портфолио","href":"#portfolio"}
        site["hero"].setdefault("image", "/public/illustrations/hero.svg")
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ Первый экран сохранён.", reply_markup=more_details_inline(req_id))

    dp.register_message_handler(save_portfolio, state=RequestForm.portfolio)
    dp.register_message_handler(save_testimonials, state=RequestForm.testimonials)
    dp.register_message_handler(save_faq, state=RequestForm.faq)
    dp.register_message_handler(save_seo, state=RequestForm.seo_description)
    dp.register_message_handler(save_hero_title, state=RequestForm.hero_title)
    dp.register_message_handler(save_hero_subtitle, state=RequestForm.hero_subtitle)
