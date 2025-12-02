from aiogram import types
from aiogram.dispatcher import FSMContext

from app.constants import (
    BTN_BACK, BTN_EXIT, BTN_SKIP, BTN_NEW, BTN_MY, BTN_ARCHIVE, BTN_RESET, BTN_ADMIN_LOGIN,
    DEFAULT_STRUCTURE, MIN_COMPANY_NAME_LEN, MAX_COMPANY_NAME_LEN,
    MIN_DESCRIPTION_LEN, MAX_DESCRIPTION_LEN,
    STATUS_COLLECTING_PHOTOS, STATUS_READY_TO_GENERATE,
    CB_DONE, CB_MORE_PORTF, CB_MORE_TESTI, CB_MORE_FAQ, CB_MORE_SEO, CB_MORE_HERO,
    CB_OPEN, MSG_REQUEST_CREATED, MSG_PHOTOS_INSTRUCTION, MSG_BLOCKED_USER,
)
from app.states import RequestForm, PhotoUpload
from app.db import (
    get_mode, get_user_by_tgid, create_request_by_tgid,
    update_request_site_json, get_request, get_request_payload, get_current_request_id_by_tgid,
    set_request_status, is_manager_blocked,
)
from app.utils import (
    default_seo_title, parse_services, parse_portfolio,
    parse_testimonials, parse_faq, slugify, has_min_requirements,
)
from app.keyboards import more_details_inline, photo_categories_inline, request_card_inline, form_navigation_keyboard
import re


# ==================== ВАЛИДАЦИЯ ====================

def validate_text(text: str, min_len: int = 1, max_len: int = 500, field_name: str = "Поле") -> tuple[bool, str]:
    """Валидация текстового поля"""
    if not text or not text.strip():
        return False, f"❌ {field_name} не может быть пустым. Введите значение:"
    text = text.strip()
    if len(text) < min_len:
        return False, f"❌ {field_name} слишком короткое (минимум {min_len} симв.). Попробуйте ещё раз:"
    if len(text) > max_len:
        return False, f"❌ {field_name} слишком длинное (максимум {max_len} симв.). Сократите:"
    return True, ""


def validate_phone(text: str) -> tuple[bool, str]:
    """Валидация телефона"""
    if not text or not text.strip():
        return False, "❌ Введите номер телефона:"
    # Убираем всё кроме цифр и +
    clean = re.sub(r'[^\d+]', '', text.strip())
    if len(clean) < 10:
        return False, "❌ Номер телефона слишком короткий. Введите полный номер:"
    return True, ""


def validate_email(text: str) -> tuple[bool, str]:
    """Валидация email"""
    if not text or not text.strip():
        return False, "❌ Введите email:"
    text = text.strip()
    if '@' not in text or '.' not in text:
        return False, "❌ Некорректный email. Введите в формате: example@domain.com"
    return True, ""


def validate_contact(text: str) -> tuple[bool, str]:
    """Валидация контактных данных клиента"""
    if not text or not text.strip():
        return False, "❌ Контактные данные не могут быть пустыми:"
    text = text.strip()
    has_digits = any(c.isdigit() for c in text)
    has_email = '@' in text
    if not has_digits and not has_email and len(text) < 5:
        return False, "❌ Введите корректные контактные данные (телефон, email или Telegram):"
    return True, ""


# ==================== РЕГИСТРАЦИЯ ХЭНДЛЕРОВ ====================

def register(dp, bot):

    # ------- Карта предыдущих состояний -------
    PREV_STATE = {
        RequestForm.client_company: RequestForm.client_name,
        RequestForm.client_contact: RequestForm.client_company,
        RequestForm.site_company:   RequestForm.client_contact,
        RequestForm.business_type:  RequestForm.site_company,
        RequestForm.color_palette:  RequestForm.business_type,
        RequestForm.phone:          RequestForm.color_palette,
        RequestForm.email:          RequestForm.phone,
        RequestForm.address:        RequestForm.email,
        RequestForm.short_desc:     RequestForm.address,
        RequestForm.work_hours:     RequestForm.short_desc,
    }

    # ------- Промпты для каждого состояния -------
    PROMPTS = {
        RequestForm.client_name:    ("👤 <b>Имя клиента</b>\n\nВведите ФИО или имя заказчика:", False),
        RequestForm.client_company: ("🏢 <b>Компания клиента</b>\n\nВведите название компании заказчика:", False),
        RequestForm.client_contact: ("📱 <b>Контакты клиента</b>\n\nВведите телефон, email или Telegram заказчика:", False),
        RequestForm.site_company:   ("🏢 <b>Название для сайта</b>\n\nВведите название компании, которое будет на сайте:", False),
        RequestForm.business_type:  ("💼 <b>Сфера деятельности</b>\n\nОпишите, чем занимается компания:\n<i>Например: Производство мебели, IT-услуги, Строительство</i>", False),
        RequestForm.color_palette:  ("🎨 <b>Цветовая палитра</b>\n\nУкажите предпочтительные цвета для сайта:\n<i>Например: Синий, белый, серый</i>\n\nИли нажмите «Пропустить» для выбора дизайнером.", True),
        RequestForm.phone:          ("📞 <b>Телефон для сайта</b>\n\nВведите контактный номер телефона:", False),
        RequestForm.email:          ("📧 <b>Email для сайта</b>\n\nВведите контактный email:", False),
        RequestForm.address:        ("📍 <b>Адрес</b>\n\nВведите адрес офиса или производства:\n\nИли нажмите «Пропустить», если адрес не нужен.", True),
        RequestForm.short_desc:     ("📝 <b>Описание компании</b>\n\nНапишите 2–3 предложения о компании для посетителей сайта:", False),
        RequestForm.work_hours:     ("🕐 <b>График работы</b>\n\nВведите режим работы:\n<i>Например: Пн–Пт 9:00–18:00, Сб 10:00–15:00</i>", False),
        RequestForm.services:       ("🛠 <b>Услуги компании</b>\n\nВведите услуги, каждую с новой строки:\n<i>Название — описание — цена</i>\n\n<b>Пример:</b>\n<code>Разработка сайта — под ключ — от 50000\nSEO продвижение — комплексное — от 30000</code>", False),
    }

    async def prompt_for_state(state: RequestForm, message: types.Message):
        """Отправить промпт для текущего состояния"""
        prompt_text, can_skip = PROMPTS.get(state, (f"Введите значение:", False))
        kb = form_navigation_keyboard(can_skip=can_skip)
        await message.answer(prompt_text, reply_markup=kb)

    # ------- Выход из формы -------
    async def cmd_exit_form(message: types.Message, state: FSMContext):
        cur_state = await state.get_state()
        from app.db import set_mode

        if cur_state is None:
            mode = get_mode(message.from_user.id)
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            if mode == "admin":
                from app.constants import BTN_PANEL, BTN_USERS, BTN_REQS, BTN_LOGOUT
                kb.add(BTN_PANEL)
                kb.add(BTN_USERS, BTN_REQS)
                kb.add(BTN_LOGOUT)
            elif mode == "manager":
                kb.add(BTN_NEW)
                kb.add(BTN_MY)
                kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
            else:
                from app.constants import BTN_REG
                kb.add(BTN_REG, BTN_ADMIN_LOGIN)
            return await message.answer("Нет активной формы.", reply_markup=kb)

        await state.finish()
        set_mode(message.from_user.id, "manager")
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW)
        kb.add(BTN_MY)
        kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("✅ Форма отменена.\n\nВы можете создать новую заявку или просмотреть существующие.", reply_markup=kb)

    dp.register_message_handler(cmd_exit_form, lambda m: m.text in {BTN_EXIT, "/reset", "/cancel", "выйти", "отмена"}, state="*")

    # ------- Назад -------
    async def go_back(message: types.Message, state: FSMContext):
        cur = await state.get_state()
        if not cur or not cur.startswith(RequestForm.__name__):
            return await message.answer("Сейчас не идёт заполнение формы.")

        cur_state_obj = None
        for s in RequestForm.states:
            if cur.endswith(s.state):
                cur_state_obj = s
                break

        if cur_state_obj and cur_state_obj in PREV_STATE:
            prev = PREV_STATE[cur_state_obj]
            await prev.set()
            await prompt_for_state(prev, message)
        else:
            await message.answer("Это первый шаг формы.")

    dp.register_message_handler(go_back, lambda m: m.text in {BTN_BACK, "назад", "/back"}, state="*")

    # ------- Пропустить -------
    async def handle_skip(message: types.Message, state: FSMContext):
        cur = await state.get_state()
        if not cur:
            return

        # Определяем, какое поле можно пропустить
        if cur.endswith("color_palette"):
            await state.update_data(color_palette="На усмотрение дизайнера")
            await RequestForm.phone.set()
            await prompt_for_state(RequestForm.phone, message)
        elif cur.endswith("address"):
            await state.update_data(address="")
            # После address идёт создание заявки
            await process_work_hours_done(message, state)
        else:
            await message.answer("Это поле нельзя пропустить.")

    dp.register_message_handler(handle_skip, lambda m: m.text == BTN_SKIP, state="*")

    def get_manager_keyboard():
        """Клавиатура менеджера"""
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW)
        kb.add(BTN_MY, BTN_ARCHIVE)
        kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        return kb

    # ------- Начало новой заявки -------
    async def cmd_new_request(message: types.Message):
        if get_mode(message.from_user.id) not in ("manager", "admin"):
            return await message.answer("Эта функция доступна только для зарегистрированных пользователей.")

        if not get_user_by_tgid(message.from_user.id):
            from app.constants import BTN_REG
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_REG, BTN_ADMIN_LOGIN)
            return await message.answer("Сначала необходимо пройти регистрацию.", reply_markup=kb)

        # Проверка блокировки
        if is_manager_blocked(message.from_user.id):
            kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
            kb.add(BTN_ADMIN_LOGIN)
            return await message.answer(MSG_BLOCKED_USER, reply_markup=kb)

        await RequestForm.client_name.set()
        await message.answer(
            "📋 <b>Создание новой заявки</b>\n\n"
            "Сейчас мы соберём информацию для разработки сайта.\n"
            "Вы можете вернуться назад или отменить заполнение в любой момент.\n\n"
            "Начнём!",
            reply_markup=types.ReplyKeyboardRemove()
        )
        await prompt_for_state(RequestForm.client_name, message)

    dp.register_message_handler(cmd_new_request, commands=["new_request"], state="*")
    dp.register_message_handler(cmd_new_request, lambda m: m.text == BTN_NEW, state="*")

    # ------- Обработчики полей формы -------

    async def q_client_name(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=2, max_len=100, field_name="Имя")
        if not valid:
            return await message.answer(error)
        await state.update_data(client_name=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.client_company, message)

    async def q_client_company(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=2, max_len=150, field_name="Название компании")
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
        valid, error = validate_text(message.text, min_len=MIN_COMPANY_NAME_LEN, max_len=MAX_COMPANY_NAME_LEN, field_name="Название")
        if not valid:
            return await message.answer(error)
        await state.update_data(site_company=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.business_type, message)

    async def q_business_type(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=3, max_len=200, field_name="Сфера деятельности")
        if not valid:
            return await message.answer(error)
        await state.update_data(business_type=message.text.strip())
        await RequestForm.next()
        await prompt_for_state(RequestForm.color_palette, message)

    async def q_color_palette(message: types.Message, state: FSMContext):
        text = message.text.strip() if message.text else "На усмотрение дизайнера"
        await state.update_data(color_palette=text)
        await RequestForm.phone.set()
        await prompt_for_state(RequestForm.phone, message)

    async def q_phone(message: types.Message, state: FSMContext):
        valid, error = validate_phone(message.text)
        if not valid:
            return await message.answer(error)
        await state.update_data(phone=message.text.strip())
        await RequestForm.email.set()
        await prompt_for_state(RequestForm.email, message)

    async def q_email(message: types.Message, state: FSMContext):
        valid, error = validate_email(message.text)
        if not valid:
            return await message.answer(error)
        await state.update_data(email=message.text.strip())
        await RequestForm.address.set()
        await prompt_for_state(RequestForm.address, message)

    async def q_address(message: types.Message, state: FSMContext):
        # Адрес может быть пустым
        await state.update_data(address=message.text.strip() if message.text else "")
        await RequestForm.short_desc.set()
        await prompt_for_state(RequestForm.short_desc, message)

    async def q_short_desc(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=MIN_DESCRIPTION_LEN, max_len=MAX_DESCRIPTION_LEN, field_name="Описание")
        if not valid:
            return await message.answer(error)
        await state.update_data(short_desc=message.text.strip())
        await RequestForm.work_hours.set()
        await prompt_for_state(RequestForm.work_hours, message)

    async def q_work_hours(message: types.Message, state: FSMContext):
        valid, error = validate_text(message.text, min_len=3, max_len=200, field_name="График работы")
        if not valid:
            return await message.answer(error)
        await state.update_data(work_hours=message.text.strip())
        await process_work_hours_done(message, state)

    async def process_work_hours_done(message: types.Message, state: FSMContext):
        """Создание заявки после заполнения основных полей"""
        data = await state.get_data()
        payload = _build_payload_from_state(data)
        req_id = create_request_by_tgid(message.from_user.id, payload)
        await state.update_data(edit_req_id=req_id)

        set_request_status(req_id, STATUS_COLLECTING_PHOTOS)

        # Переход к загрузке фото
        await PhotoUpload.choosing_category.set()
        await state.update_data(edit_req_id=req_id, photo_category=None)

        company = payload['site'].get('company') or '—'

        await message.answer(
            MSG_REQUEST_CREATED.format(req_id=str(req_id)[:8], company=company),
            reply_markup=types.ReplyKeyboardRemove()
        )
        await message.answer(
            MSG_PHOTOS_INSTRUCTION,
            reply_markup=photo_categories_inline(req_id)
        )

    # ------- Построение payload -------

    def _build_payload_from_state(data: dict) -> dict:
        """Сборка payload из данных формы"""
        company = data.get("site_company", "")
        business = data.get("business_type", "")
        short = data.get("short_desc", "")

        return {
            "client": {
                "name": data.get("client_name"),
                "company": data.get("client_company"),
                "contact": data.get("client_contact"),
            },
            "site": {
                "company": company,
                "business_type": business,
                "color_palette": data.get("color_palette"),
                "phone": data.get("phone"),
                "email": data.get("email"),
                "address": data.get("address"),
                "summary": short,
                "work_hours": data.get("work_hours"),
                "structure": DEFAULT_STRUCTURE,
                "assets": {"images": []},
                "hero": {
                    "title": default_seo_title(company, business),
                    "subtitle": short[:120] if short else "",
                    "primaryCta": {"label": "Оставить заявку", "href": "#contact"},
                    "secondaryCta": {"label": "Портфолио", "href": "#portfolio"},
                    "image": "/public/illustrations/hero.svg"
                },
                "seo": {
                    "title": default_seo_title(company, business),
                    "description": short[:160] if short else ""
                }
            }
        }

    def _reeval_status(req_id: str) -> None:
        """Переоценка статуса заявки"""
        try:
            payload = get_request_payload(req_id) or {}
            if has_min_requirements(payload):
                set_request_status(req_id, STATUS_READY_TO_GENERATE)
        except Exception:
            pass

    # ------- Регистрация обработчиков -------

    dp.register_message_handler(q_client_name, state=RequestForm.client_name)
    dp.register_message_handler(q_client_company, state=RequestForm.client_company)
    dp.register_message_handler(q_client_contact, state=RequestForm.client_contact)
    dp.register_message_handler(q_site_company, state=RequestForm.site_company)
    dp.register_message_handler(q_business_type, state=RequestForm.business_type)
    dp.register_message_handler(q_color_palette, state=RequestForm.color_palette)
    dp.register_message_handler(q_phone, state=RequestForm.phone)
    dp.register_message_handler(q_email, state=RequestForm.email)
    dp.register_message_handler(q_address, state=RequestForm.address)
    dp.register_message_handler(q_short_desc, state=RequestForm.short_desc)
    dp.register_message_handler(q_work_hours, state=RequestForm.work_hours)

    # ------- Обработчик услуг -------

    async def q_services(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(message.from_user.id)
        services = parse_services(message.text or "")

        if not services:
            return await message.answer(
                "❌ Не удалось распознать услуги.\n\n"
                "Введите каждую услугу с новой строки:\n"
                "<i>Название — описание — цена</i>"
            )

        if not req_id:
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
            site = payload.get("site") or {}
            site["services"] = services

            company = site.get("company", "")
            business = site.get("business_type", "")
            short = site.get("summary", "")

            site.setdefault("hero", {
                "title": default_seo_title(company, business),
                "subtitle": short[:120],
                "primaryCta": {"label": "Оставить заявку", "href": "#contact"},
                "secondaryCta": {"label": "Портфолио", "href": "#portfolio"},
                "image": "/public/illustrations/hero.svg"
            })
            site.setdefault("seo", {
                "title": default_seo_title(company, business),
                "description": short[:160]
            })

            update_request_site_json(req_id, site)
            _reeval_status(req_id)

        await state.finish()

        kb = get_manager_keyboard()

        count_msg = f"Добавлено услуг: {len(services)}"
        if len(services) < 3:
            count_msg += " (рекомендуется минимум 3)"

        await message.answer(
            f"✅ <b>Услуги сохранены</b>\n\n{count_msg}\n\n"
            "Вы можете добавить дополнительную информацию или завершить заполнение:",
            reply_markup=kb
        )
        await message.answer("Выберите раздел для дополнения:", reply_markup=more_details_inline(req_id))

    dp.register_message_handler(q_services, state=RequestForm.services)

    # ------- Callback: Готово -------

    async def cb_done(call: types.CallbackQuery, state: FSMContext):
        await call.answer("Открываю заявку...")

        req_id = call.data[len(CB_DONE):] if call.data.startswith(CB_DONE) else None

        if not req_id:
            st = await state.get_data()
            req_id = st.get("edit_req_id") or get_current_request_id_by_tgid(call.from_user.id)

        if not req_id:
            return await call.message.answer("Заявка не найдена.", reply_markup=get_manager_keyboard())

        rec = get_request(req_id)
        if not rec:
            return await call.message.answer("Заявка удалена или не найдена.", reply_markup=get_manager_keyboard())

        from app.handlers.requests import format_request_card

        user = get_user_by_tgid(call.from_user.id)
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        is_admin = (get_mode(call.from_user.id) == "admin")

        # Получаем статус для карточки
        payload = get_request_payload(req_id) or {}
        site = payload.get("site") or {}
        meta = site.get("meta") or {}
        status = meta.get("status") or "draft"

        txt = format_request_card(rec, show_private=(is_owner or is_admin))
        await call.message.answer(txt, reply_markup=request_card_inline(rec["id"], is_owner, is_admin, status))

        await call.message.answer(
            "Вы можете отредактировать заявку, добавить данные или запустить генерацию сайта.",
            reply_markup=get_manager_keyboard()
        )

    dp.register_callback_query_handler(cb_done, lambda c: c.data and c.data.startswith(CB_DONE), state="*")

    # ------- Callback: Доп. поля -------

    async def _load_site(req_id: str) -> dict | None:
        rec = get_request(req_id)
        if not rec:
            return None
        data = get_request_payload(req_id)
        return data.get("site") or {}

    async def cb_more_portf(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_PORTF):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.portfolio.set()
        await call.message.answer(
            "📁 <b>Портфолио</b>\n\n"
            "Введите проекты, каждый с новой строки:\n"
            "<i>Название — клиент — год — описание</i>\n\n"
            "<b>Пример:</b>\n"
            "<code>Интернет-магазин — ООО Ритейл — 2024 — Каталог 5000 товаров</code>"
        )

    dp.register_callback_query_handler(cb_more_portf, lambda c: c.data and c.data.startswith(CB_MORE_PORTF), state="*")

    async def cb_more_testi(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_TESTI):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.testimonials.set()
        await call.message.answer(
            "💬 <b>Отзывы клиентов</b>\n\n"
            "Введите отзывы, каждый с новой строки:\n"
            "<i>Имя — Компания — текст отзыва — оценка</i>\n\n"
            "<b>Пример:</b>\n"
            "<code>Иван Петров — ООО Альфа — Отличная работа, рекомендую — 5</code>"
        )

    dp.register_callback_query_handler(cb_more_testi, lambda c: c.data and c.data.startswith(CB_MORE_TESTI), state="*")

    async def cb_more_faq(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_FAQ):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.faq.set()
        await call.message.answer(
            "❓ <b>Частые вопросы (FAQ)</b>\n\n"
            "Введите вопросы и ответы, каждый с новой строки:\n"
            "<i>Вопрос — Ответ</i>\n\n"
            "<b>Пример:</b>\n"
            "<code>Какие сроки? — Стандартный проект 2–4 недели</code>"
        )

    dp.register_callback_query_handler(cb_more_faq, lambda c: c.data and c.data.startswith(CB_MORE_FAQ), state="*")

    async def cb_more_seo(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_SEO):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.seo_description.set()
        await call.message.answer(
            "🔍 <b>SEO-описание</b>\n\n"
            "Введите описание для поисковых систем (1–2 предложения).\n"
            "Это описание будет показываться в результатах поиска Google и Яндекс."
        )

    dp.register_callback_query_handler(cb_more_seo, lambda c: c.data and c.data.startswith(CB_MORE_SEO), state="*")

    async def cb_more_hero(call: types.CallbackQuery, state: FSMContext):
        req_id = call.data[len(CB_MORE_HERO):]
        await state.update_data(edit_req_id=req_id)
        await RequestForm.hero_title.set()
        await call.message.answer(
            "🏠 <b>Заголовок главной страницы</b>\n\n"
            "Введите главный заголовок сайта — это первое, что увидят посетители:"
        )

    dp.register_callback_query_handler(cb_more_hero, lambda c: c.data and c.data.startswith(CB_MORE_HERO), state="*")

    # ------- Сохранение доп. полей -------

    async def save_portfolio(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Заявка не найдена.")

        portfolio = parse_portfolio(message.text or "")
        if not portfolio:
            return await message.answer(
                "❌ Не удалось распознать портфолио.\n"
                "Введите каждый проект с новой строки:\n"
                "<i>Название — клиент — год — описание</i>"
            )

        site["portfolio"] = portfolio
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer(f"✅ Добавлено проектов: {len(portfolio)}", reply_markup=more_details_inline(req_id))

    async def save_testimonials(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Заявка не найдена.")

        testimonials = parse_testimonials(message.text or "")
        if not testimonials:
            return await message.answer(
                "❌ Не удалось распознать отзывы.\n"
                "Введите каждый отзыв с новой строки:\n"
                "<i>Имя — Компания — текст — оценка</i>"
            )

        site["testimonials"] = testimonials
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer(f"✅ Добавлено отзывов: {len(testimonials)}", reply_markup=more_details_inline(req_id))

    async def save_faq(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Заявка не найдена.")

        faq = parse_faq(message.text or "")
        if not faq:
            return await message.answer(
                "❌ Не удалось распознать FAQ.\n"
                "Введите каждый пункт с новой строки:\n"
                "<i>Вопрос — Ответ</i>"
            )

        site["faq"] = faq
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer(f"✅ Добавлено вопросов: {len(faq)}", reply_markup=more_details_inline(req_id))

    async def save_seo(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Заявка не найдена.")

        text = (message.text or "").strip()
        if len(text) < 10:
            return await message.answer("❌ Описание слишком короткое (минимум 10 символов).")

        company = site.get("company", "")
        business = site.get("business_type", "")
        site.setdefault("seo", {})
        site["seo"]["title"] = default_seo_title(company, business)
        site["seo"]["description"] = text
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ SEO-описание сохранено", reply_markup=more_details_inline(req_id))

    async def save_hero_title(message: types.Message, state: FSMContext):
        text = (message.text or "").strip()
        if len(text) < 3:
            return await message.answer("❌ Заголовок слишком короткий.")
        await state.update_data(hero_t=text)
        await RequestForm.hero_subtitle.set()
        await message.answer(
            "📝 <b>Подзаголовок</b>\n\n"
            "Введите подзаголовок — краткое описание под главным заголовком:"
        )

    async def save_hero_subtitle(message: types.Message, state: FSMContext):
        st = await state.get_data()
        req_id = st.get("edit_req_id")
        site = await _load_site(req_id)
        if site is None:
            await state.finish()
            return await message.answer("Заявка не найдена.")

        title = st.get("hero_t") or site.get("hero", {}).get("title", "")
        subtitle = (message.text or "").strip()
        if len(subtitle) < 5:
            return await message.answer("❌ Подзаголовок слишком короткий (минимум 5 символов).")

        site.setdefault("hero", {})
        site["hero"]["title"] = title
        site["hero"]["subtitle"] = subtitle
        site["hero"]["primaryCta"] = {"label": "Оставить заявку", "href": "#contact"}
        site["hero"]["secondaryCta"] = {"label": "Портфолио", "href": "#portfolio"}
        site["hero"].setdefault("image", "/public/illustrations/hero.svg")
        update_request_site_json(req_id, site)
        _reeval_status(req_id)
        await state.finish()
        await message.answer("✅ Заголовки сохранены", reply_markup=more_details_inline(req_id))

    dp.register_message_handler(save_portfolio, state=RequestForm.portfolio)
    dp.register_message_handler(save_testimonials, state=RequestForm.testimonials)
    dp.register_message_handler(save_faq, state=RequestForm.faq)
    dp.register_message_handler(save_seo, state=RequestForm.seo_description)
    dp.register_message_handler(save_hero_title, state=RequestForm.hero_title)
    dp.register_message_handler(save_hero_subtitle, state=RequestForm.hero_subtitle)
