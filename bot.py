#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram-бот (aiogram 2.25.1) + PostgreSQL (твоя схема app_db)
- Роли берутся из users.role (guest/customer/manager/admin)
- «Мои заявки» берутся из requests JOIN projects по manager_id
- Кнопка «⚙️ Сгенерировать сайт» шлёт payload в n8n webhook
Зависимости:
  pip install aiogram==2.25.1 python-dotenv==1.0.1 "psycopg[binary]==3.2.1"
ENV:
  TG_BOT_TOKEN=xxxxx
  ADMIN_PASSWORD=changeme
  N8N_GEN_WEBHOOK=https://.../n8n/webhook/...
  # либо DB_URL, либо PG_* / POSTGRES_*:
  # DB_URL=postgresql://user:pass@postgres:5432/app_db
  # PG_USER, PG_PASSWORD, PG_HOST=postgres, PG_PORT=5432, PG_DB=app_db
"""

import os, io, json, zipfile, logging, html
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import aiohttp
import psycopg
from psycopg.rows import dict_row

# ----------------- logging & env -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")
load_dotenv()

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("TG_BOT_TOKEN is required")

N8N_GEN_WEBHOOK = os.getenv("N8N_GEN_WEBHOOK", "").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")

def _build_db_url_from_env() -> str:
    user = os.getenv("DB_USER") or os.getenv("PG_USER") or os.getenv("POSTGRES_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("DB_HOST") or os.getenv("PG_HOST", "postgres")
    port = os.getenv("DB_PORT") or os.getenv("PG_PORT", "5432")
    db   = os.getenv("DB_NAME") or os.getenv("PG_DB") or os.getenv("POSTGRES_DB") or "app_db"
    return f"postgresql://{user}:{password}@{host}:{port}/{db}" if user and password else ""

DB_URL = os.getenv("DB_URL", "").strip() or _build_db_url_from_env()
if not DB_URL:
    raise SystemExit("No Postgres connection info. Set DB_URL or PG_USER/PG_PASSWORD/etc.")

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

# ----------------- helpers -----------------
def e(s: Any) -> str:
    return "—" if s is None else html.escape(str(s), quote=False)

def now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def chunks(text: str, size: int = 3800) -> List[str]:
    if len(text) <= size: return [text]
    out, cur = [], 0
    while cur < len(text):
        out.append(text[cur:cur+size]); cur += size
    return out

def get_db():
    return psycopg.connect(DB_URL, row_factory=dict_row, autocommit=True)

def init_db():
    # Ты схему держишь в БД, тут только лог.
    safe_url = DB_URL.replace(os.getenv("PG_PASSWORD","***"), "****")
    log.info(f"Postgres mode; DB_URL={safe_url}")

# ----------------- FSM -----------------
class RegForm(StatesGroup):
    first_name = State()
    last_name  = State()
    age        = State()      # не пишем в PG, просто оставим шаг
    contact    = State()

class RequestForm(StatesGroup):
    client_name    = State()
    client_company = State()
    client_contact = State()
    site_company   = State()
    business_type  = State()
    color_palette  = State()
    site_contacts  = State()
    short_desc     = State()
    work_hours     = State()
    structure      = State()
    images         = State()
    services       = State()

class AdminLogin(StatesGroup):
    password = State()

class EditField(StatesGroup):
    waiting_value = State()

# ----------------- commands & labels -----------------
GUEST_CMDS   = [types.BotCommand("start","Старт"), types.BotCommand("register","Регистрация"), types.BotCommand("admin_login","Войти в админку")]
MANAGER_CMDS = [types.BotCommand("start","Старт"), types.BotCommand("new_request","Новая заявка"), types.BotCommand("my_requests","Мои заявки"), types.BotCommand("reset","Сбросить анкету"), types.BotCommand("admin_login","Войти в админку")]
ADMIN_CMDS   = [types.BotCommand("start","Старт"), types.BotCommand("admin_panel","Админ: панель"), types.BotCommand("admin_users","Админ: пользователи"), types.BotCommand("admin_requests","Админ: заявки"), types.BotCommand("export_request","Админ: экспорт заявки"), types.BotCommand("export_all","Админ: экспорт всех заявок"), types.BotCommand("logout","Админ: выйти")]

BTN_REG="📝 Регистрация"; BTN_ADMIN_LOGIN="🔐 Войти в админку"
BTN_NEW="➕ Создать заявку"; BTN_MY="📋 Мои заявки"; BTN_RESET="❌ Сброс формы"
BTN_PANEL="📊 Панель"; BTN_USERS="👥 Пользователи"; BTN_REQS="📦 Заявки"; BTN_LOGOUT="🚪 Выйти из админки"
BTN_BACK="⬅️ Назад"; BTN_EXIT="🚪 Выйти из формы"

CB_OPEN="open_"; CB_EDIT="edit_"; CB_DELETE="del_"; CB_EDIT_FIELD="ef_"; CB_BACK_TO_LIST="back_list"; CB_LIST_PAGE="plist_"; CB_EXPORT_ONE="exp_"; CB_GEN="gen_"

EDITABLE_FIELDS = {
    "company":"Название компании",
    "business_type":"Тип бизнеса",
    "color_palette":"Цветовая гамма",
    "site_contacts":"Контакты/адреса для сайта",
    "short_desc":"Краткое описание",
    "work_hours":"Рабочие часы",
    "structure":"Структура (через запятую)",
    "images":"Изображения (описание)",
    "services":"Услуги (Название — описание — цена на каждой строке)"
}

# ----------------- DB access (PG only) -----------------
def get_user_by_tgid(tg_id: int):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE tg_id=%s", (tg_id,))
        return cur.fetchone()

def get_user_by_id(uid: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id=%s::uuid", (uid,))
        return cur.fetchone()

def create_user(tg_id: int, first_name: str, last_name: str, contact: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (tg_id, role, first_name, last_name, contact)
            VALUES (%s, 'manager', %s, %s, %s)
            ON CONFLICT (tg_id) DO UPDATE
            SET first_name=EXCLUDED.first_name,
                last_name=EXCLUDED.last_name,
                contact=EXCLUDED.contact
        """, (tg_id, first_name, last_name, contact))

def get_mode(tg_id: int) -> str:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT role FROM users WHERE tg_id=%s", (tg_id,))
        row = cur.fetchone()
        return row["role"] if row and row["role"] else "guest"

def set_mode(tg_id: int, mode: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (tg_id, role)
            VALUES (%s, %s)
            ON CONFLICT (tg_id) DO UPDATE SET role=EXCLUDED.role
        """, (tg_id, mode))

def list_manager_requests(tg_id: int, offset=0, limit=10):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT r.id,
                   COALESCE(r.payload_json->'client'->>'name','Без имени') AS client_name,
                   r.status,
                   r.created_at
            FROM requests r
            JOIN projects p ON p.id = r.project_id
            JOIN users u ON u.id = p.manager_id
            WHERE u.tg_id = %s
            ORDER BY r.created_at DESC
            LIMIT %s OFFSET %s
        """, (tg_id, limit, offset))
        return cur.fetchall()

def count_manager_requests(tg_id: int) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS n
            FROM requests r
            JOIN projects p ON p.id = r.project_id
            JOIN users u ON u.id = p.manager_id
            WHERE u.tg_id = %s
        """, (tg_id,))
        row = cur.fetchone()
        return row["n"] if row else 0

def list_all_requests(offset=0, limit=20):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, p.manager_id,
                   COALESCE(r.payload_json->'client'->>'name','Без имени') AS client_name,
                   r.status, r.created_at
            FROM requests r
            JOIN projects p ON p.id = r.project_id
            ORDER BY r.created_at DESC
            LIMIT %s OFFSET %s
        """, (limit, offset))
        return cur.fetchall()

def count_all_requests() -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM requests")
        row = cur.fetchone()
        return row["n"] if row else 0

def get_request(req_id: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT r.*, p.manager_id,
                   (r.payload_json->'client'->>'name')    AS client_name,
                   (r.payload_json->'client'->>'company') AS client_company,
                   (r.payload_json->'client'->>'contact') AS client_contact,
                   (r.payload_json->'site')               AS site_json
            FROM requests r
            JOIN projects p ON p.id = r.project_id
            WHERE r.id = %s::uuid
        """, (req_id,))
        row = cur.fetchone()
        if not row: return None
        return {
            "id": row["id"],
            "manager_id": row["manager_id"],
            "client_name": row["client_name"],
            "client_company": row["client_company"],
            "client_contact": row["client_contact"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
            "site_params_json": json.dumps(row["site_json"] or {}, ensure_ascii=False)
        }

def create_request_by_tgid(tg_id: int, payload: Dict[str, Any]):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE tg_id=%s", (tg_id,))
        u = cur.fetchone()
        if not u: raise RuntimeError("User not found in users")
        uid = u["id"]
        title = payload.get("site",{}).get("company") or payload.get("client",{}).get("company") or "Project"
        cur.execute("INSERT INTO projects (manager_id, title, status) VALUES (%s, %s, 'draft') RETURNING id", (uid, title))
        pid = cur.fetchone()["id"]
        cur.execute("INSERT INTO requests (project_id, payload_json, status) VALUES (%s, %s::jsonb, 'new')",
                    (pid, json.dumps(payload, ensure_ascii=False)))

def update_request_site_json(req_id: str, site_json: Dict[str, Any]):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE requests
            SET payload_json = jsonb_set(payload_json, '{site}', %s::jsonb, true)
            WHERE id = %s::uuid
        """, (json.dumps(site_json, ensure_ascii=False), req_id))

def delete_request(req_id: str, manager_id: Optional[str] = None) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        if manager_id is None:
            cur.execute("DELETE FROM requests WHERE id=%s::uuid", (req_id,))
            return cur.rowcount > 0
        cur.execute("""
            DELETE FROM requests r
            USING projects p
            WHERE r.id=%s::uuid AND r.project_id=p.id AND p.manager_id=%s::uuid
        """, (req_id, manager_id))
        return cur.rowcount > 0

# ----------------- keyboards -----------------
def requests_list_inline(reqs: List[dict], page: int, total: int, per_page: int = 10) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=1)
    for r in reqs:
        title = f"#{r['id']} — {r.get('client_name') or 'Без имени'}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))
    pages = max(1, (total + per_page - 1) // per_page)
    nav = []
    if page > 1: nav.append(InlineKeyboardButton("« Назад", callback_data=f"{CB_LIST_PAGE}{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{pages}", callback_data=f"{CB_LIST_PAGE}{page}"))
    if page < pages: nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"{CB_LIST_PAGE}{page+1}"))
    if nav: ikb.row(*nav)
    return ikb

def request_card_inline(req_id: Any, is_owner: bool, is_admin: bool) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=2)
    if is_owner or is_admin:
        ikb.add(InlineKeyboardButton("✏️ Редактировать", callback_data=f"{CB_EDIT}{req_id}"),
                InlineKeyboardButton("🗑 Удалить", callback_data=f"{CB_DELETE}{req_id}"))
    ikb.add(InlineKeyboardButton("⬇️ Экспорт JSON", callback_data=f"{CB_EXPORT_ONE}{req_id}"),
            InlineKeyboardButton("⚙️ Сгенерировать сайт", callback_data=f"{CB_GEN}{req_id}"))
    ikb.add(InlineKeyboardButton("⬅️ К списку", callback_data=CB_BACK_TO_LIST))
    return ikb

def edit_fields_inline(req_id: Any) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=2)
    btns = [InlineKeyboardButton(title, callback_data=f"{CB_EDIT_FIELD}{req_id}_{field}") for field, title in EDITABLE_FIELDS.items()]
    for i in range(0, len(btns), 2): ikb.row(*btns[i:i+2])
    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"{CB_OPEN}{req_id}"))
    return ikb

# ----------------- prompts -----------------
PREV_STATE = {
    RequestForm.client_company: RequestForm.client_name,
    RequestForm.client_contact: RequestForm.client_company,
    RequestForm.site_company:   RequestForm.client_contact,
    RequestForm.business_type:  RequestForm.site_company,
    RequestForm.color_palette:  RequestForm.business_type,
    RequestForm.site_contacts:  RequestForm.color_palette,
    RequestForm.short_desc:     RequestForm.site_contacts,
    RequestForm.work_hours:     RequestForm.short_desc,
    RequestForm.structure:      RequestForm.work_hours,
    RequestForm.images:         RequestForm.structure,
    RequestForm.services:       RequestForm.images,
}

async def prompt_for_state(state_name: State, message: types.Message):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    kb.add(BTN_BACK, BTN_EXIT)
    prompts = {
        RequestForm.client_name:   "Введите <b>имя клиента</b>:",
        RequestForm.client_company:"Введите <b>название компании клиента</b>:",
        RequestForm.client_contact:"Введите <b>контактные данные клиента</b>:",
        RequestForm.site_company:  "Введите <b>название компании для сайта</b>:",
        RequestForm.business_type: "Введите <b>тип бизнеса</b> (например: студия маникюра):",
        RequestForm.color_palette: "Введите <b>пожелания по цветовой гамме</b>:",
        RequestForm.site_contacts: "Укажите <b>контакты/адреса для сайта</b>:",
        RequestForm.short_desc:    "Введите <b>краткое описание</b> (1–2 предложения):",
        RequestForm.work_hours:    "Введите <b>рабочие часы</b> (формат «Пн–Пт 10:00–19:00»):",
        RequestForm.structure:     "Укажите <b>структуру сайта</b> (Hero, О нас, Услуги, Портфолио, Отзывы, FAQ, Контакты, Карта):",
        RequestForm.images:        "Опишите <b>изображения</b> (куда какие фото):",
        RequestForm.services:      "Введите <b>услуги</b> (каждая с новой строки: Название — описание — цена):",
    }
    await message.answer(prompts[state_name], reply_markup=kb)

# ----------------- global exit/back -----------------
@dp.message_handler(lambda m: m.text in {BTN_EXIT, "/reset", "/cancel", "выйти", "отмена"}, state="*")
async def cmd_exit_form(message: types.Message, state: FSMContext):
    cur_state = await state.get_state()
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

@dp.message_handler(lambda m: m.text in {BTN_BACK, "назад", "/back"}, state="*")
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

# ----------------- n8n generation -----------------
@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_GEN))
async def cb_generate_site(call: types.CallbackQuery):
    if not N8N_GEN_WEBHOOK:
        return await call.answer("N8N_GEN_WEBHOOK не задан.", show_alert=True)
    req_id = call.data[len(CB_GEN):]
    rec = get_request(req_id)
    if not rec: return await call.answer("Заявка не найдена.", show_alert=True)
    user = get_user_by_tgid(call.from_user.id)
    is_admin = (get_mode(call.from_user.id) == "admin")
    is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
    if not (is_admin or is_owner):
        return await call.answer("Нет прав на генерацию по этой заявке.", show_alert=True)
    payload = build_request_payload(rec)
    await call.answer()
    await call.message.reply("🚀 Запускаю генерацию… Файл придёт отдельным сообщением.")
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(N8N_GEN_WEBHOOK, json={"chat_id": call.message.chat.id, "request": payload}) as resp:
                _ = await resp.text()
    except Exception as e:
        await call.message.reply(f"⚠️ Не удалось отправить в n8n: {e}")

# ----------------- /start & role menus -----------------
async def set_scope_cmds(chat_id: int, mode: str, is_registered: bool):
    if mode == "admin":
        await bot.set_my_commands(ADMIN_CMDS, scope=types.BotCommandScopeChat(chat_id))
    elif is_registered:
        await bot.set_my_commands(MANAGER_CMDS, scope=types.BotCommandScopeChat(chat_id))
    else:
        await bot.set_my_commands(GUEST_CMDS, scope=types.BotCommandScopeChat(chat_id))

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    init_db()
    user = get_user_by_tgid(message.from_user.id)
    is_reg = bool(user)
    mode = get_mode(message.from_user.id)
    if is_reg and mode != "admin":
        set_mode(message.from_user.id, "manager"); mode = "manager"
    await set_scope_cmds(message.chat.id, mode, is_reg)
    if mode == "admin":
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
        await message.answer("Здравствуйте! Режим: <b>Админ</b>", reply_markup=kb)
    elif is_reg:
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("Здравствуйте! Режим: <b>Менеджер</b>", reply_markup=kb)
    else:
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_REG, BTN_ADMIN_LOGIN)
        await message.answer("Здравствуйте! Вы ещё не зарегистрированы.", reply_markup=kb)

# ----------------- register -----------------
@dp.message_handler(commands=["register"])
@dp.message_handler(lambda m: m.text == BTN_REG)
async def cmd_register(message: types.Message):
    if get_mode(message.from_user.id) == "admin":
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
        return await message.answer("Сейчас включён режим админа. Нажмите «🚪 Выйти из админки».", reply_markup=kb)
    user = get_user_by_tgid(message.from_user.id)
    if user:
        set_mode(message.from_user.id, "manager")
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        return await message.answer("Вы уже зарегистрированы.", reply_markup=kb)
    await RegForm.first_name.set()
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_REG, BTN_ADMIN_LOGIN)
    await message.answer("Введите ваше <b>имя</b>:", reply_markup=kb)

@dp.message_handler(state=RegForm.first_name)
async def reg_first_name(message: types.Message, state: FSMContext):
    await state.update_data(first_name=message.text.strip()); await RegForm.next()
    await message.answer("Введите вашу <b>фамилию</b>:")

@dp.message_handler(state=RegForm.last_name)
async def reg_last_name(message: types.Message, state: FSMContext):
    await state.update_data(last_name=message.text.strip()); await RegForm.next()
    await message.answer("Введите ваш <b>возраст</b> (числом):")

@dp.message_handler(state=RegForm.age)
async def reg_age(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    if not txt.isdigit() or not (0 < int(txt) < 120):
        return await message.answer("Возраст должен быть числом 1–119. Попробуйте снова:")
    await state.update_data(age=int(txt)); await RegForm.next()
    await message.answer("Укажите ваш <b>контакт</b> (телефон/email/@username):")

@dp.message_handler(state=RegForm.contact)
async def reg_contact(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        create_user(
            tg_id=message.from_user.id,
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            contact=message.text.strip(),
        )
        set_mode(message.from_user.id, "manager")
        await state.finish()
        await set_scope_cmds(message.chat.id, "manager", True)
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("✅ Регистрация завершена!", reply_markup=kb)
    except Exception:
        log.exception("Регистрация: ошибка сохранения")
        await state.finish()
        await message.answer("⚠️ Ошибка сохранения. Попробуйте ещё раз /register.")

# ----------------- new request -----------------
@dp.message_handler(commands=["new_request"])
@dp.message_handler(lambda m: m.text == BTN_NEW)
async def cmd_new_request(message: types.Message):
    if get_mode(message.from_user.id) not in ("manager","admin"):
        return await message.answer("Эта функция доступна только менеджеру.")
    if not get_user_by_tgid(message.from_user.id):
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_REG, BTN_ADMIN_LOGIN)
        return await message.answer("Сначала регистрация: «📝 Регистрация».", reply_markup=kb)
    await RequestForm.client_name.set()
    await prompt_for_state(RequestForm.client_name, message)

@dp.message_handler(state=RequestForm.client_name)
async def q_client_name(message: types.Message, state: FSMContext):
    await state.update_data(client_name=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.client_company, message)

@dp.message_handler(state=RequestForm.client_company)
async def q_client_company(message: types.Message, state: FSMContext):
    await state.update_data(client_company=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.client_contact, message)

@dp.message_handler(state=RequestForm.client_contact)
async def q_client_contact(message: types.Message, state: FSMContext):
    await state.update_data(client_contact=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.site_company, message)

@dp.message_handler(state=RequestForm.site_company)
async def q_site_company(message: types.Message, state: FSMContext):
    await state.update_data(site_company=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.business_type, message)

@dp.message_handler(state=RequestForm.business_type)
async def q_business_type(message: types.Message, state: FSMContext):
    await state.update_data(business_type=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.color_palette, message)

@dp.message_handler(state=RequestForm.color_palette)
async def q_color_palette(message: types.Message, state: FSMContext):
    await state.update_data(color_palette=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.site_contacts, message)

@dp.message_handler(state=RequestForm.site_contacts)
async def q_site_contacts(message: types.Message, state: FSMContext):
    await state.update_data(site_contacts=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.short_desc, message)

@dp.message_handler(state=RequestForm.short_desc)
async def q_short_desc(message: types.Message, state: FSMContext):
    await state.update_data(short_desc=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.work_hours, message)

@dp.message_handler(state=RequestForm.work_hours)
async def q_work_hours(message: types.Message, state: FSMContext):
    await state.update_data(work_hours=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.structure, message)

@dp.message_handler(state=RequestForm.structure)
async def q_structure(message: types.Message, state: FSMContext):
    await state.update_data(structure=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.images, message)

@dp.message_handler(state=RequestForm.images)
async def q_images(message: types.Message, state: FSMContext):
    await state.update_data(images=message.text.strip()); await RequestForm.next()
    await prompt_for_state(RequestForm.services, message)

def parse_services(text: str) -> List[Dict[str, str]]:
    services = []
    for line in (text or "").splitlines():
        norm = line.replace("|","—").replace(" - "," — ").replace("-", "—")
        parts = [p.strip() for p in norm.split("—") if p.strip()]
        if not parts: continue
        item = {"name": parts[0]}
        if len(parts)>1: item["desc"]=parts[1]
        if len(parts)>2: item["price"]=parts[2]
        services.append(item)
    return services

@dp.message_handler(state=RequestForm.services)
async def q_services(message: types.Message, state: FSMContext):
    data = await state.get_data()
    payload = {
        "client": {
            "name": data.get("client_name"),
            "company": data.get("client_company"),
            "contact": data.get("client_contact"),
        },
        "site": {
            "company": data.get("site_company"),
            "business_type": data.get("business_type"),
            "color_palette": data.get("color_palette"),
            "site_contacts": data.get("site_contacts"),
            "short_desc": data.get("short_desc"),
            "work_hours": data.get("work_hours"),
            "structure": [s.strip() for s in (data.get("structure") or "").replace(";", ",").split(",") if s.strip()],
            "images": data.get("images"),
            "services": parse_services(message.text or ""),
        }
    }
    try:
        create_request_by_tgid(message.from_user.id, payload)
        await state.finish()
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("✅ Заявка сохранена!\nОткройте «📋 Мои заявки», чтобы посмотреть или отредактировать.", reply_markup=kb)
    except Exception:
        log.exception("Ошибка сохранения заявки")
        await state.finish()
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        await message.answer("⚠️ Не удалось сохранить заявку. Попробуйте ещё раз.", reply_markup=kb)

# ----------------- my requests -----------------
def format_request_card(rec: Dict[str, Any], show_private: bool = True) -> str:
    site: Dict[str, Any] = json.loads(rec["site_params_json"] or "{}")
    services = site.get("services") or []
    services_txt = "\n".join(
        [f"• {e(s.get('name',''))}" + (f" — {e(s.get('desc',''))}" if s.get('desc') else "") + (f" — {e(s.get('price',''))}" if s.get('price') else "")
         for s in services]
    ) or "—"
    structure_txt = ", ".join([e(s) for s in (site.get("structure") or [])]) or "—"
    client_block = (
        f"Клиент: <b>{e(rec.get('client_name'))}</b>\n"
        f"Компания клиента: {e(rec.get('client_company'))}\n"
        f"Контакты клиента: {e(rec.get('client_contact'))}\n\n"
    ) if show_private else ""
    return (
        f"<b>Заявка #{rec['id']}</b>\n"
        f"Статус: <i>{e(rec['status'])}</i>\n"
        f"{client_block}"
        f"<b>Для сайта</b>\n"
        f"Название компании: {e(site.get('company'))}\n"
        f"Тип бизнеса: {e(site.get('business_type'))}\n"
        f"Цветовая гамма: {e(site.get('color_palette'))}\n"
        f"Контакты/адреса для сайта: {e(site.get('site_contacts'))}\n"
        f"Краткое описание: {e(site.get('short_desc'))}\n"
        f"Часы работы: {e(site.get('work_hours'))}\n"
        f"Структура: {structure_txt}\n"
        f"Изображения: {e(site.get('images'))}\n"
        f"Услуги:\n{services_txt}"
    )

@dp.message_handler(commands=["my_requests"])
@dp.message_handler(lambda m: m.text == BTN_MY)
async def cmd_my_requests(message: types.Message, state: FSMContext):
    mode = get_mode(message.from_user.id)
    if mode not in ("manager","admin"):
        return await message.answer("Доступно только для менеджера.")
    if not get_user_by_tgid(message.from_user.id):
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_REG, BTN_ADMIN_LOGIN)
        return await message.answer("Сначала регистрация: «📝 Регистрация».", reply_markup=kb)
    page, per_page = 1, 10
    total = count_manager_requests(message.from_user.id)
    if total == 0:
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
        kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        return await message.answer("У вас пока нет заявок. Нажмите «➕ Создать заявку».", reply_markup=kb)
    rows = list_manager_requests(message.from_user.id, offset=(page-1)*per_page, limit=per_page)
    await message.answer("Список ваших заявок:", reply_markup=types.ReplyKeyboardRemove())
    await message.answer("Выберите заявку:", reply_markup=requests_list_inline(rows, page, total, per_page))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_LIST_PAGE))
async def cb_list_page(call: types.CallbackQuery):
    try: page = int(call.data[len(CB_LIST_PAGE):])
    except ValueError: page = 1
    per_page = 10
    total = count_manager_requests(call.from_user.id)
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(max(1, page), pages)
    rows = list_manager_requests(call.from_user.id, offset=(page-1)*per_page, limit=per_page)
    await call.message.edit_reply_markup(requests_list_inline(rows, page, total, per_page))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_OPEN), state="*")
async def cb_open_request(call: types.CallbackQuery):
    await call.answer()  # закрыть индикатор
    try:
        req_id = call.data[len(CB_OPEN):]
        rec = get_request(req_id)
        if not rec:
            return await call.message.answer("Заявка не найдена.")
        user = get_user_by_tgid(call.from_user.id)
        is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
        is_admin = (get_mode(call.from_user.id) == "admin")
        txt = format_request_card(rec, show_private=(is_owner or is_admin))
        await call.message.edit_text(txt, reply_markup=request_card_inline(rec["id"], is_owner, is_admin))
    except Exception as e:
        log.exception("cb_open_request failed")
        await call.message.answer(f"⚠️ Ошибка открытия: {e}")


@dp.callback_query_handler(lambda c: c.data and c.data == CB_BACK_TO_LIST)
async def cb_back_list(call: types.CallbackQuery):
    page, per_page = 1, 10
    total = count_manager_requests(call.from_user.id)
    if total == 0: return await call.message.edit_text("У вас пока нет заявок.")
    rows = list_manager_requests(call.from_user.id, offset=0, limit=per_page)
    await call.message.edit_text("Список ваших заявок:", reply_markup=requests_list_inline(rows, page, total, per_page))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_DELETE))
async def cb_delete_request(call: types.CallbackQuery, state: FSMContext):
    req_id = call.data[len(CB_DELETE):]
    user = get_user_by_tgid(call.from_user.id)
    if not user: return await call.answer("Нет прав.", show_alert=True)
    ok = delete_request(req_id, manager_id=str(user["id"])) or (get_mode(call.from_user.id) == "admin" and delete_request(req_id))
    if ok: await call.message.edit_text(f"Заявка #{req_id} удалена.")
    else:  await call.answer("Не удалось удалить (возможно, нет прав).", show_alert=True)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_EDIT))
async def cb_edit_request(call: types.CallbackQuery):
    req_id = call.data[len(CB_EDIT):]
    rec = get_request(req_id)
    if not rec: return await call.message.edit_text("Заявка не найдена.")
    user = get_user_by_tgid(call.from_user.id)
    is_admin = (get_mode(call.from_user.id) == "admin")
    is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
    if not (is_admin or is_owner):
        return await call.answer("Нет прав на редактирование.", show_alert=True)
    await call.message.edit_text(f"Редактирование заявки #{rec['id']}: выберите поле ниже.", reply_markup=edit_fields_inline(rec["id"]))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_EDIT_FIELD))
async def cb_edit_field(call: types.CallbackQuery, state: FSMContext):
    payload = call.data[len(CB_EDIT_FIELD):]
    req_id, field = payload.split("_", 1)
    rec = get_request(req_id)
    if not rec: return await call.message.edit_text("Заявка не найдена.")
    user = get_user_by_tgid(call.from_user.id)
    is_admin = (get_mode(call.from_user.id) == "admin")
    is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
    if not (is_admin or is_owner):
        return await call.answer("Нет прав на редактирование.", show_alert=True)
    title = EDITABLE_FIELDS.get(field, field)
    await state.update_data(edit_req_id=req_id, edit_field=field)
    await EditField.waiting_value.set()
    await call.message.edit_text(
        f"Введите новое значение для: <b>{e(title)}</b>\n\n"
        f"Если «Структура» — перечислите секции через запятую.\n"
        f"Если «Услуги» — каждая услуга с новой строки: Название — описание — цена."
    )

@dp.message_handler(state=EditField.waiting_value)
async def on_edit_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    req_id, field = data.get("edit_req_id"), data.get("edit_field")
    rec = get_request(req_id)
    if not rec:
        await state.finish()
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        return await message.answer("Заявка не найдена.", reply_markup=kb)
    user = get_user_by_tgid(message.from_user.id)
    is_admin = (get_mode(message.from_user.id) == "admin")
    is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
    if not (is_admin or is_owner):
        await state.finish()
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
        return await message.answer("Нет прав на редактирование.", reply_markup=kb)

    site = json.loads(rec["site_params_json"] or "{}")
    value = (message.text or "").strip()
    if field == "structure":
        site["structure"] = [s.strip() for s in value.replace(";", ",").split(",") if s.strip()]
    elif field == "services":
        site["services"] = parse_services(value)
    else:
        site[field] = value

    update_request_site_json(rec["id"], site)
    await state.finish()
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
    await message.answer("✅ Сохранено.", reply_markup=kb)

# ----------------- export -----------------
def build_request_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "request_id": rec["id"],
        "manager_id": rec["manager_id"],
        "client": {
            "name": rec.get("client_name"),
            "company": rec.get("client_company"),
            "contact": rec.get("client_contact"),
        },
        "site": json.loads(rec["site_params_json"] or "{}"),
        "status": rec["status"],
        "created_at": rec["created_at"],
    }

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_EXPORT_ONE))
async def cb_export_one(call: types.CallbackQuery):
    req_id = call.data[len(CB_EXPORT_ONE):]
    rec = get_request(req_id)
    if not rec: return await call.answer("Заявка не найдена.", show_alert=True)
    user = get_user_by_tgid(call.from_user.id)
    is_admin = (get_mode(call.from_user.id) == "admin")
    is_owner = bool(user and rec.get("manager_id") and str(rec["manager_id"]) == str(user["id"]))
    if not (is_admin or is_owner):
        return await call.answer("Нет прав на экспорт этой заявки.", show_alert=True)
    json_str = json.dumps(build_request_payload(rec), ensure_ascii=False, indent=2)
    fname = f"request_{rec['id']}.json"
    with open(fname, "w", encoding="utf-8") as f: f.write(json_str)
    await call.message.answer_document(types.InputFile(fname), caption=f"Экспорт заявки #{rec['id']} (JSON)")

# ----------------- admin -----------------
def require_admin(handler):
    async def wrapper(message: types.Message, *args, **kwargs):
        if get_mode(message.from_user.id) != "admin":
            return await message.answer("Доступ запрещён. Войдите как админ.")
        return await handler(message, *args, **kwargs)
    return wrapper

@dp.message_handler(commands=["admin_login"])
@dp.message_handler(lambda m: m.text == BTN_ADMIN_LOGIN)
async def cmd_admin_login(message: types.Message, state: FSMContext):
    if get_mode(message.from_user.id) == "admin":
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
        return await message.answer("Вы уже в админке.", reply_markup=kb)
    await AdminLogin.password.set()
    await message.answer("Введите пароль администратора:")

@dp.message_handler(state=AdminLogin.password)
async def admin_check_pass(message: types.Message, state: FSMContext):
    if message.text.strip() != ADMIN_PASSWORD:
        await state.finish(); return await message.answer("Пароль неверный.")
    set_mode(message.from_user.id, "admin")
    await state.finish(); await set_scope_cmds(message.chat.id, "admin", True)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_PANEL); kb.add(BTN_USERS, BTN_REQS); kb.add(BTN_LOGOUT)
    await message.answer("Готово. Режим админа включён.", reply_markup=kb)

@dp.message_handler(commands=["logout"])
@dp.message_handler(lambda m: m.text == BTN_LOGOUT)
async def cmd_logout(message: types.Message):
    if get_mode(message.from_user.id) != "admin":
        return await message.answer("Сейчас не режим админа.")
    set_mode(message.from_user.id, "manager"); await set_scope_cmds(message.chat.id, "manager", True)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True); kb.add(BTN_NEW); kb.add(BTN_MY); kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
    await message.answer("Вы вышли из админки. Вернулся режим менеджера.", reply_markup=kb)

@dp.message_handler(commands=["admin_panel"])
@dp.message_handler(lambda m: m.text == BTN_PANEL)
@require_admin
async def cmd_admin_panel(message: types.Message):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM users"); users_count = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) AS n FROM requests"); reqs_count = cur.fetchone()["n"]
    await message.answer(
        "<b>Админ-панель</b>\n\n"
        f"Пользователей: <b>{users_count}</b>\n"
        f"Заявок: <b>{reqs_count}</b>\n\n"
        "Команды:\n"
        "• 👥 Пользователи — список\n"
        "• 📦 Заявки — список\n"
        "• /export_request <id> — экспорт заявки в JSON\n"
        "• /export_all — экспорт всех заявок (ZIP)\n"
        "• 🚪 Выйти из админки"
    )

@dp.message_handler(commands=["admin_users"])
@dp.message_handler(lambda m: m.text == BTN_USERS)
@require_admin
async def cmd_admin_users(message: types.Message):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, first_name, last_name, contact, created_at, tg_id, role FROM users ORDER BY created_at DESC")
        rows = cur.fetchall()
    if not rows: return await message.answer("Пользователей пока нет.")
    lines = []
    for u in rows:
        name = f"{(u.get('first_name') or '')} {(u.get('last_name') or '')}".strip() or "—"
        lines.append(f"#{u['id']}: <b>{e(name)}</b> | {e(u.get('contact'))} | роль: {e(u.get('role'))} | tg_id: {e(u.get('tg_id'))} | {e(u.get('created_at'))}")
    for part in chunks("\n".join(lines)): await message.answer(part)

@dp.message_handler(commands=["admin_requests"])
@dp.message_handler(lambda m: m.text == BTN_REQS)
@require_admin
async def cmd_admin_requests(message: types.Message):
    total = count_all_requests()
    if total == 0: return await message.answer("Заявок пока нет.")
    rows = list_all_requests(0, 20)
    ikb = InlineKeyboardMarkup(row_width=1)
    for r in rows:
        title = f"#{r['id']} — {r.get('client_name') or 'Без имени'}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))
    await message.answer("Админ: заявки — выберите:", reply_markup=ikb)

@dp.message_handler(commands=["export_request"])
@require_admin
async def cmd_export_request(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2: return await message.answer("Использование: /export_request <id>")
    req_id = parts[1]
    rec = get_request(req_id)
    if not rec: return await message.answer("Заявка не найдена.")
    fname = f"request_{rec['id']}.json"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(json.dumps(build_request_payload(rec), ensure_ascii=False, indent=2))
    await message.answer_document(types.InputFile(fname), caption=f"Экспорт заявки #{rec['id']}")

@dp.message_handler(commands=["export_all"])
@require_admin
async def cmd_export_all(message: types.Message):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, p.manager_id,
                   r.payload_json->'client' AS client,
                   r.payload_json->'site'   AS site,
                   r.status, r.created_at
            FROM requests r
            JOIN projects p ON p.id = r.project_id
            ORDER BY r.created_at DESC
        """)
        rows = cur.fetchall()
    if not rows: return await message.answer("Заявок нет для экспорта.")
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for r in rows:
            payload = {
                "request_id": r["id"], "manager_id": r["manager_id"],
                "client": r.get("client"), "site": r.get("site"),
                "status": r["status"], "created_at": str(r["created_at"]),
            }
            zf.writestr(f"request_{r['id']}.json", json.dumps(payload, ensure_ascii=False, indent=2))
    bio.seek(0)
    await message.answer_document(types.InputFile(bio, filename="requests_export.zip"), caption="Экспорт всех заявок (ZIP)")

# ----------------- on_startup -----------------
async def on_startup(dp):
    init_db()
    await bot.set_my_commands(GUEST_CMDS)
    log.info("Bot is running…")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
