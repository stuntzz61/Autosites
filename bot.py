#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram-бот (aiogram 2.25.1 + SQLite)
Назначение: сбор структурированных заявок для автогенерации сайтов.

Что внутри:
- Роли: guest → manager → admin (в sessions)
- Регистрация менеджера
- Анкета заявки (жёсткий порядок, во время анкеты только «⬅️ Назад» и «🚪 Выйти из формы»)
- «Мои заявки» с пагинацией, карточкой (Открыть/Редактировать/Удалить/Экспорт)
- Редактирование конкретных полей заявки (безопасно, только свои заявки)
- Админ-панель: сводка, список пользователей, список заявок, экспорт одной/всех
- JSON-экспорт на диск (и ZIP для массового экспорта)
- Безопасность: токен строго из ENV/.env, пароль админа из ENV/.env, HTML escape, проверки прав

Установка:
  pip install aiogram==2.25.1 python-dotenv==1.0.1

Запуск:
  # .env:
  # TG_BOT_TOKEN=XXXXX
  # ADMIN_PASSWORD=changeme
  python bot.py

Проверено на Python 3.11.
"""

import os
import io
import json
import zipfile
import sqlite3
import logging
import html
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# =====================
# ЛОГИРОВАНИЕ
# =====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

# =====================
# НАСТРОЙКИ/ENV
# =====================
load_dotenv()  # читаем .env, если есть

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("[!] TG_BOT_TOKEN обязателен. Укажите его в .env или переменных окружения.")

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")
DB_PATH = os.path.join(os.path.dirname(__file__), "bot.db")

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

# =====================
# ВСПОМОГАТЕЛЬНОЕ
# =====================
def e(s: Any) -> str:
    """HTML-escape для отображения в parse_mode=HTML."""
    if s is None:
        return "—"
    return html.escape(str(s), quote=False)

def now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def chunks(text: str, size: int = 3800) -> List[str]:
    """Безопасно режем длинные сообщения, оставляя запас под разметку/кнопки."""
    if len(text) <= size:
        return [text]
    out, buf = [], []
    cur = 0
    while cur < len(text):
        out.append(text[cur:cur+size])
        cur += size
    return out

# =====================
# БАЗА ДАННЫХ
# =====================
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            contact TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            tg_id INTEGER PRIMARY KEY,
            mode TEXT NOT NULL DEFAULT 'guest', -- guest | manager | admin
            updated_at TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            client_name TEXT,
            client_company TEXT,
            client_contact TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            site_params_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(manager_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_requests_manager_id ON requests(manager_id)")
    conn.commit()
    conn.close()

# =====================
# USERS / SESSIONS
# =====================
def get_user_by_tgid(tg_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row

def get_user_by_id(uid: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (uid,))
    row = cur.fetchone()
    conn.close()
    return row

def create_user(tg_id: int, first_name: str, last_name: str, age: int, contact: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (tg_id, first_name, last_name, age, contact, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tg_id, first_name, last_name, age, contact, now_str())
    )
    conn.commit()
    conn.close()

def get_mode(tg_id: int) -> str:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT mode FROM sessions WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return (row[0] if row else "guest")

def set_mode(tg_id: int, mode: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sessions (tg_id, mode, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET mode=excluded.mode, updated_at=excluded.updated_at
        """,
        (tg_id, mode, now_str()),
    )
    conn.commit()
    conn.close()

# =====================
# REQUESTS
# =====================
def list_manager_requests(manager_id: int, offset: int = 0, limit: int = 10) -> List[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, client_name, status, created_at FROM requests WHERE manager_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
        (manager_id, limit, offset)
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def count_manager_requests(manager_id: int) -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM requests WHERE manager_id = ?", (manager_id,))
    n = cur.fetchone()[0]
    conn.close()
    return n

def list_all_requests(offset: int = 0, limit: int = 20) -> List[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, manager_id, client_name, status, created_at FROM requests ORDER BY id DESC LIMIT ? OFFSET ?",
        (limit, offset)
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def count_all_requests() -> int:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM requests")
    n = cur.fetchone()[0]
    conn.close()
    return n

def get_request(req_id: int) -> Optional[sqlite3.Row]:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests WHERE id = ?", (req_id,))
    row = cur.fetchone()
    conn.close()
    return row

def create_request(manager_id: int, payload: Dict[str, Any]):
    client = payload.get("client", {})
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO requests (manager_id, client_name, client_company, client_contact, status, site_params_json, created_at)
        VALUES (?, ?, ?, ?, 'new', ?, ?)
        """,
        (
            manager_id,
            client.get("name"),
            client.get("company"),
            client.get("contact"),
            json.dumps(payload.get("site", {}), ensure_ascii=False),
            now_str(),
        )
    )
    conn.commit()
    conn.close()

def update_request_site_json(req_id: int, site_json: Dict[str, Any]):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE requests SET site_params_json=? WHERE id=?", (json.dumps(site_json, ensure_ascii=False), req_id))
    conn.commit()
    conn.close()

def delete_request(req_id: int, manager_id: Optional[int] = None) -> bool:
    conn = get_db()
    cur = conn.cursor()
    if manager_id is None:
        cur.execute("DELETE FROM requests WHERE id=?", (req_id,))
    else:
        cur.execute("DELETE FROM requests WHERE id=? AND manager_id=?", (req_id, manager_id))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted > 0

# =====================
# FSM
# =====================
class RegForm(StatesGroup):
    first_name = State()
    last_name = State()
    age = State()
    contact = State()

class RequestForm(StatesGroup):
    client_name = State()
    client_company = State()
    client_contact = State()
    site_company = State()
    business_type = State()
    color_palette = State()
    site_contacts = State()
    short_desc = State()
    work_hours = State()
    structure = State()
    images = State()
    services = State()

class AdminLogin(StatesGroup):
    password = State()

class EditField(StatesGroup):
    waiting_value = State()

# =====================
# КОМАНДЫ/ТЕКСТЫ
# =====================
GUEST_CMDS = [
    types.BotCommand("start", "Старт"),
    types.BotCommand("register", "Регистрация"),
    types.BotCommand("admin_login", "Войти в админку"),
]

MANAGER_CMDS = [
    types.BotCommand("start", "Старт"),
    types.BotCommand("new_request", "Новая заявка"),
    types.BotCommand("my_requests", "Мои заявки"),
    types.BotCommand("reset", "Сбросить анкету"),
    types.BotCommand("admin_login", "Войти в админку"),
]

ADMIN_CMDS = [
    types.BotCommand("start", "Старт"),
    types.BotCommand("admin_panel", "Админ: панель"),
    types.BotCommand("admin_users", "Админ: пользователи"),
    types.BotCommand("admin_requests", "Админ: заявки"),
    types.BotCommand("export_request", "Админ: экспорт заявки"),
    types.BotCommand("export_all", "Админ: экспорт всех заявок"),
    types.BotCommand("logout", "Админ: выйти"),
]

# Главные кнопки
BTN_REG = "📝 Регистрация"
BTN_ADMIN_LOGIN = "🔐 Войти в админку"

BTN_NEW = "➕ Создать заявку"
BTN_MY = "📋 Мои заявки"
BTN_RESET = "❌ Сброс формы"

BTN_PANEL = "📊 Панель"
BTN_USERS = "👥 Пользователи"
BTN_REQS = "📦 Заявки"
BTN_LOGOUT = "🚪 Выйти из админки"

# Кнопки анкеты
BTN_BACK = "⬅️ Назад"
BTN_EXIT = "🚪 Выйти из формы"

# Callback префиксы
CB_OPEN = "open_"            # open_<id>
CB_EDIT = "edit_"            # edit_<id>
CB_DELETE = "del_"           # del_<id>
CB_EDIT_FIELD = "ef_"        # ef_<id>_<field>
CB_BACK_TO_LIST = "back_list"
CB_LIST_PAGE = "plist_"      # plist_<page>
CB_EXPORT_ONE = "exp_"       # exp_<id>

EDITABLE_FIELDS = {
    "company": "Название компании",
    "business_type": "Тип бизнеса",
    "color_palette": "Цветовая гамма",
    "site_contacts": "Контакты/адреса для сайта",
    "short_desc": "Краткое описание",
    "work_hours": "Рабочие часы",
    "structure": "Структура (через запятую)",
    "images": "Изображения (описание)",
    "services": "Услуги (Название — описание — цена на каждой строке)",
}

# =====================
# КЛАВИАТУРЫ
# =====================
def guest_kb() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(BTN_REG, BTN_ADMIN_LOGIN)
    return kb

def manager_kb() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(BTN_NEW)
    kb.add(BTN_MY)
    kb.add(BTN_RESET, BTN_ADMIN_LOGIN)
    return kb

def admin_kb() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(BTN_PANEL)
    kb.add(BTN_USERS, BTN_REQS)
    kb.add(BTN_LOGOUT)
    return kb

def form_kb() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    kb.add(BTN_BACK, BTN_EXIT)
    return kb

def requests_list_inline(reqs: List[sqlite3.Row], page: int, total: int, per_page: int = 10) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=1)
    for r in reqs:
        title = f"#{r['id']} — {r['client_name'] or 'Без имени'}"
        ikb.add(InlineKeyboardButton(title, callback_data=f"{CB_OPEN}{r['id']}"))

    # пагинация
    pages = max(1, (total + per_page - 1) // per_page)
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("« Назад", callback_data=f"{CB_LIST_PAGE}{page-1}"))
    nav_row.append(InlineKeyboardButton(f"{page}/{pages}", callback_data=f"{CB_LIST_PAGE}{page}"))
    if page < pages:
        nav_row.append(InlineKeyboardButton("Вперёд »", callback_data=f"{CB_LIST_PAGE}{page+1}"))
    if nav_row:
        ikb.row(*nav_row)
    return ikb

def request_card_inline(req_id: int, is_owner: bool, is_admin: bool) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=2)
    if is_owner or is_admin:
        ikb.add(
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"{CB_EDIT}{req_id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"{CB_DELETE}{req_id}"),
        )
    ikb.add(InlineKeyboardButton("⬇️ Экспорт JSON", callback_data=f"{CB_EXPORT_ONE}{req_id}"))
    ikb.add(InlineKeyboardButton("⬅️ К списку", callback_data=CB_BACK_TO_LIST))
    return ikb

def edit_fields_inline(req_id: int) -> InlineKeyboardMarkup:
    ikb = InlineKeyboardMarkup(row_width=2)
    btns = [
        InlineKeyboardButton(title, callback_data=f"{CB_EDIT_FIELD}{req_id}_{field}")
        for field, title in EDITABLE_FIELDS.items()
    ]
    for i in range(0, len(btns), 2):
        ikb.row(*btns[i:i+2])
    ikb.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"{CB_OPEN}{req_id}"))
    return ikb

# =====================
# АНКЕТА: переходы назад
# =====================
PREV_STATE = {
    RequestForm.client_company: RequestForm.client_name,
    RequestForm.client_contact: RequestForm.client_company,
    RequestForm.site_company: RequestForm.client_contact,
    RequestForm.business_type: RequestForm.site_company,
    RequestForm.color_palette: RequestForm.business_type,
    RequestForm.site_contacts: RequestForm.color_palette,
    RequestForm.short_desc: RequestForm.site_contacts,
    RequestForm.work_hours: RequestForm.short_desc,
    RequestForm.structure: RequestForm.work_hours,
    RequestForm.images: RequestForm.structure,
    RequestForm.services: RequestForm.images,
}

async def prompt_for_state(state_name: State, message: types.Message):
    kb = form_kb()
    prompts = {
        RequestForm.client_name: "Введите <b>имя клиента</b>:",
        RequestForm.client_company: "Введите <b>название компании клиента</b>:",
        RequestForm.client_contact: "Введите <b>контактные данные клиента</b>:",
        RequestForm.site_company: "Введите <b>название компании для сайта</b>:",
        RequestForm.business_type: "Введите <b>тип бизнеса</b> (например: студия маникюра):",
        RequestForm.color_palette: "Введите <b>пожелания по цветовой гамме</b>:",
        RequestForm.site_contacts: "Укажите <b>контакты/адреса для сайта</b> (телефон, WhatsApp, Telegram, email и т.д.):",
        RequestForm.short_desc: "Введите <b>краткое описание</b> (1–2 предложения):",
        RequestForm.work_hours: "Введите <b>рабочие часы</b> (формат «Пн–Пт 10:00–19:00»):",
        RequestForm.structure: "Укажите <b>структуру сайта</b> (Hero, О нас, Услуги, Портфолио, Отзывы, FAQ, Контакты, Карта):",
        RequestForm.images: "Опишите <b>изображения</b> (например: «фото 1 — для Hero, фото 2 — для портфолио»). Загрузку добавим позже:",
        RequestForm.services: "Введите <b>услуги</b> (каждая с новой строки: Название — описание — цена):",
    }
    await message.answer(prompts[state_name], reply_markup=kb)

# =====================
# Глобальные выход/назад
# =====================
@dp.message_handler(lambda m: m.text in {BTN_EXIT, "/reset", "/cancel", "выйти", "отмена"}, state="*")
async def cmd_exit_form(message: types.Message, state: FSMContext):
    cur_state = await state.get_state()
    if cur_state is None:
        mode = get_mode(message.from_user.id)
        kb = admin_kb() if mode == "admin" else (manager_kb() if mode == "manager" else guest_kb())
        return await message.answer("Нет активной анкеты.", reply_markup=kb)
    await state.finish()
    set_mode(message.from_user.id, "manager")
    await message.answer("Анкета закрыта. Вы в режиме менеджера.", reply_markup=manager_kb())

@dp.message_handler(lambda m: m.text in {BTN_BACK, "назад", "/back"}, state="*")
async def go_back(message: types.Message, state: FSMContext):
    cur = await state.get_state()
    if not cur or not cur.startswith(RequestForm.__name__):
        return await message.answer("Сейчас не идёт заполнение анкеты.")
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
        await message.answer("Назад идти больше некуда.")

# =====================
# /start
# =====================
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
        set_mode(message.from_user.id, "manager")
        mode = "manager"
    await set_scope_cmds(message.chat.id, mode, is_reg)
    if mode == "admin":
        await message.answer("Здравствуйте! Режим: <b>Админ</b>", reply_markup=admin_kb())
    elif is_reg:
        await message.answer("Здравствуйте! Режим: <b>Менеджер</b>", reply_markup=manager_kb())
    else:
        await message.answer("Здравствуйте! Вы ещё не зарегистрированы.", reply_markup=guest_kb())

# =====================
# РЕГИСТРАЦИЯ
# =====================
@dp.message_handler(commands=["register"])
@dp.message_handler(lambda m: m.text == BTN_REG)
async def cmd_register(message: types.Message):
    if get_mode(message.from_user.id) == "admin":
        return await message.answer("Сейчас включён режим админа. Нажмите «🚪 Выйти из админки».", reply_markup=admin_kb())
    user = get_user_by_tgid(message.from_user.id)
    if user:
        set_mode(message.from_user.id, "manager")
        return await message.answer("Вы уже зарегистрированы.", reply_markup=manager_kb())
    await RegForm.first_name.set()
    await message.answer("Введите ваше <b>имя</b>:", reply_markup=guest_kb())

@dp.message_handler(state=RegForm.first_name)
async def reg_first_name(message: types.Message, state: FSMContext):
    await state.update_data(first_name=message.text.strip())
    await RegForm.next()
    await message.answer("Введите вашу <b>фамилию</b>:")

@dp.message_handler(state=RegForm.last_name)
async def reg_last_name(message: types.Message, state: FSMContext):
    await state.update_data(last_name=message.text.strip())
    await RegForm.next()
    await message.answer("Введите ваш <b>возраст</b> (числом):")

@dp.message_handler(state=RegForm.age)
async def reg_age(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    if not txt.isdigit() or not (0 < int(txt) < 120):
        return await message.answer("Возраст должен быть числом 1–119. Попробуйте снова:")
    await state.update_data(age=int(txt))
    await RegForm.next()
    await message.answer("Укажите ваш <b>контакт</b> (телефон/email/@username):")

@dp.message_handler(state=RegForm.contact)
async def reg_contact(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        create_user(
            tg_id=message.from_user.id,
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            age=data.get("age"),
            contact=message.text.strip(),
        )
        set_mode(message.from_user.id, "manager")
        await state.finish()
        await set_scope_cmds(message.chat.id, "manager", True)
        await message.answer("✅ Регистрация завершена!", reply_markup=manager_kb())
    except Exception:
        log.exception("Регистрация: ошибка сохранения")
        await state.finish()
        await message.answer("⚠️ Ошибка сохранения. Попробуйте ещё раз /register.")

# =====================
# СОЗДАНИЕ ЗАЯВКИ
# =====================
@dp.message_handler(commands=["new_request"])
@dp.message_handler(lambda m: m.text == BTN_NEW)
async def cmd_new_request(message: types.Message):
    if get_mode(message.from_user.id) != "manager":
        return await message.answer("Эта функция доступна только менеджеру.")
    user = get_user_by_tgid(message.from_user.id)
    if not user:
        return await message.answer("Сначала регистрация: «📝 Регистрация».", reply_markup=guest_kb())
    await RequestForm.client_name.set()
    await prompt_for_state(RequestForm.client_name, message)

@dp.message_handler(state=RequestForm.client_name)
async def q_client_name(message: types.Message, state: FSMContext):
    await state.update_data(client_name=message.text.strip())
    await RequestForm.client_company.set()
    await prompt_for_state(RequestForm.client_company, message)

@dp.message_handler(state=RequestForm.client_company)
async def q_client_company(message: types.Message, state: FSMContext):
    await state.update_data(client_company=message.text.strip())
    await RequestForm.client_contact.set()
    await prompt_for_state(RequestForm.client_contact, message)

@dp.message_handler(state=RequestForm.client_contact)
async def q_client_contact(message: types.Message, state: FSMContext):
    await state.update_data(client_contact=message.text.strip())
    await RequestForm.site_company.set()
    await prompt_for_state(RequestForm.site_company, message)

@dp.message_handler(state=RequestForm.site_company)
async def q_site_company(message: types.Message, state: FSMContext):
    await state.update_data(site_company=message.text.strip())
    await RequestForm.business_type.set()
    await prompt_for_state(RequestForm.business_type, message)

@dp.message_handler(state=RequestForm.business_type)
async def q_business_type(message: types.Message, state: FSMContext):
    await state.update_data(business_type=message.text.strip())
    await RequestForm.color_palette.set()
    await prompt_for_state(RequestForm.color_palette, message)

@dp.message_handler(state=RequestForm.color_palette)
async def q_color_palette(message: types.Message, state: FSMContext):
    await state.update_data(color_palette=message.text.strip())
    await RequestForm.site_contacts.set()
    await prompt_for_state(RequestForm.site_contacts, message)

@dp.message_handler(state=RequestForm.site_contacts)
async def q_site_contacts(message: types.Message, state: FSMContext):
    await state.update_data(site_contacts=message.text.strip())
    await RequestForm.short_desc.set()
    await prompt_for_state(RequestForm.short_desc, message)

@dp.message_handler(state=RequestForm.short_desc)
async def q_short_desc(message: types.Message, state: FSMContext):
    await state.update_data(short_desc=message.text.strip())
    await RequestForm.work_hours.set()
    await prompt_for_state(RequestForm.work_hours, message)

@dp.message_handler(state=RequestForm.work_hours)
async def q_work_hours(message: types.Message, state: FSMContext):
    await state.update_data(work_hours=message.text.strip())
    await RequestForm.structure.set()
    await prompt_for_state(RequestForm.structure, message)

@dp.message_handler(state=RequestForm.structure)
async def q_structure(message: types.Message, state: FSMContext):
    await state.update_data(structure=message.text.strip())
    await RequestForm.images.set()
    await prompt_for_state(RequestForm.images, message)

@dp.message_handler(state=RequestForm.images)
async def q_images(message: types.Message, state: FSMContext):
    await state.update_data(images=message.text.strip())
    await RequestForm.services.set()
    await prompt_for_state(RequestForm.services, message)

def parse_services(text: str) -> List[Dict[str, str]]:
    services = []
    for line in (text or "").splitlines():
        norm = line.replace("|", "—").replace(" - ", " — ").replace("-", "—")
        parts = [p.strip() for p in norm.split("—") if p.strip()]
        if not parts:
            continue
        item = {"name": parts[0]}
        if len(parts) > 1:
            item["desc"] = parts[1]
        if len(parts) > 2:
            item["price"] = parts[2]
        services.append(item)
    return services

@dp.message_handler(state=RequestForm.services)
async def q_services(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user = get_user_by_tgid(message.from_user.id)

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
        create_request(manager_id=user["id"], payload=payload)
        await state.finish()
        await message.answer("✅ Заявка сохранена!\nОткройте «📋 Мои заявки», чтобы посмотреть или отредактировать.", reply_markup=manager_kb())
    except Exception:
        log.exception("Ошибка сохранения заявки")
        await state.finish()
        await message.answer("⚠️ Не удалось сохранить заявку. Попробуйте ещё раз.", reply_markup=manager_kb())

# =====================
# МОИ ЗАЯВКИ (с пагинацией)
# =====================
def format_request_card(rec: sqlite3.Row, show_private: bool = True) -> str:
    site: Dict[str, Any] = json.loads(rec["site_params_json"] or "{}")
    services = site.get("services") or []
    services_txt = "\n".join(
        [f"• {e(s.get('name',''))}" + (f" — {e(s.get('desc',''))}" if s.get('desc') else "") + (f" — {e(s.get('price',''))}" if s.get('price') else "")
         for s in services]
    ) or "—"
    structure_txt = ", ".join([e(s) for s in (site.get("structure") or [])]) or "—"

    client_block = (
        f"Клиент: <b>{e(rec['client_name'])}</b>\n"
        f"Компания клиента: {e(rec['client_company'])}\n"
        f"Контакты клиента: {e(rec['client_contact'])}\n\n"
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
    if get_mode(message.from_user.id) != "manager":
        return await message.answer("Доступно только для менеджера.")
    user = get_user_by_tgid(message.from_user.id)
    if not user:
        return await message.answer("Сначала регистрация: «📝 Регистрация».", reply_markup=guest_kb())

    total = count_manager_requests(user["id"])
    if total == 0:
        return await message.answer("У вас пока нет заявок. Нажмите «➕ Создать заявку».", reply_markup=manager_kb())

    page = 1
    per_page = 10
    rows = list_manager_requests(user["id"], offset=(page-1)*per_page, limit=per_page)
    await message.answer("Список ваших заявок:", reply_markup=types.ReplyKeyboardRemove())
    await message.answer("Выберите заявку:", reply_markup=requests_list_inline(rows, page, total, per_page))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_LIST_PAGE))
async def cb_list_page(call: types.CallbackQuery):
    user = get_user_by_tgid(call.from_user.id)
    if not user:
        return await call.answer("Нет прав.", show_alert=True)
    try:
        page = int(call.data[len(CB_LIST_PAGE):])
    except ValueError:
        page = 1
    per_page = 10
    total = count_manager_requests(user["id"])
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(max(1, page), pages)
    rows = list_manager_requests(user["id"], offset=(page-1)*per_page, limit=per_page)
    await call.message.edit_reply_markup(requests_list_inline(rows, page, total, per_page))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_OPEN))
async def cb_open_request(call: types.CallbackQuery):
    req_id = int(call.data[len(CB_OPEN):])
    rec = get_request(req_id)
    if not rec:
        return await call.message.edit_text("Заявка не найдена.")
    user = get_user_by_tgid(call.from_user.id)
    is_owner = bool(user and rec["manager_id"] == user["id"])
    is_admin = get_mode(call.from_user.id) == "admin"
    show_private = is_owner or is_admin
    txt = format_request_card(rec, show_private=show_private)
    await call.message.edit_text(txt, reply_markup=request_card_inline(req_id, is_owner, is_admin))

@dp.callback_query_handler(lambda c: c.data and c.data == CB_BACK_TO_LIST)
async def cb_back_list(call: types.CallbackQuery):
    user = get_user_by_tgid(call.from_user.id)
    if not user:
        return await call.message.edit_text("Нет прав.")
    total = count_manager_requests(user["id"])
    if total == 0:
        return await call.message.edit_text("У вас пока нет заявок.")
    page = 1
    per_page = 10
    rows = list_manager_requests(user["id"], offset=0, limit=per_page)
    await call.message.edit_text("Список ваших заявок:", reply_markup=requests_list_inline(rows, page, total, per_page))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_DELETE))
async def cb_delete_request(call: types.CallbackQuery, state: FSMContext):
    req_id = int(call.data[len(CB_DELETE):])
    user = get_user_by_tgid(call.from_user.id)
    if not user:
        return await call.answer("Нет прав.", show_alert=True)
    if delete_request(req_id, manager_id=user["id"]) or get_mode(call.from_user.id) == "admin" and delete_request(req_id):
        await call.message.edit_text(f"Заявка #{req_id} удалена.")
    else:
        await call.answer("Не удалось удалить (возможно, нет прав).", show_alert=True)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_EDIT))
async def cb_edit_request(call: types.CallbackQuery):
    req_id = int(call.data[len(CB_EDIT):])
    rec = get_request(req_id)
    if not rec:
        return await call.message.edit_text("Заявка не найдена.")
    user = get_user_by_tgid(call.from_user.id)
    is_admin = get_mode(call.from_user.id) == "admin"
    if not (is_admin or (user and rec["manager_id"] == user["id"])):
        return await call.answer("Нет прав на редактирование.", show_alert=True)
    await call.message.edit_text(f"Редактирование заявки #{req_id}: выберите поле ниже.", reply_markup=edit_fields_inline(req_id))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_EDIT_FIELD))
async def cb_edit_field(call: types.CallbackQuery, state: FSMContext):
    payload = call.data[len(CB_EDIT_FIELD):]  # <id>_<field>
    req_id_str, field = payload.split("_", 1)
    req_id = int(req_id_str)
    rec = get_request(req_id)
    if not rec:
        return await call.message.edit_text("Заявка не найдена.")
    user = get_user_by_tgid(call.from_user.id)
    is_admin = get_mode(call.from_user.id) == "admin"
    if not (is_admin or (user and rec["manager_id"] == user["id"])):
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
    req_id = int(data.get("edit_req_id"))
    field = data.get("edit_field")
    rec = get_request(req_id)
    if not rec:
        await state.finish()
        return await message.answer("Заявка не найдена.", reply_markup=manager_kb())

    user = get_user_by_tgid(message.from_user.id)
    is_admin = get_mode(message.from_user.id) == "admin"
    if not (is_admin or (user and rec["manager_id"] == user["id"])):
        await state.finish()
        return await message.answer("Нет прав на редактирование.", reply_markup=manager_kb())

    site = json.loads(rec["site_params_json"] or "{}")
    value = (message.text or "").strip()

    if field == "structure":
        site["structure"] = [s.strip() for s in value.replace(";", ",").split(",") if s.strip()]
    elif field == "services":
        site["services"] = parse_services(value)
    else:
        site[field] = value

    update_request_site_json(req_id, site)
    await state.finish()
    await message.answer("✅ Сохранено.", reply_markup=manager_kb())

# =====================
# ЭКСПОРТ (inline, для менеджера/админа)
# =====================
def build_request_payload(rec: sqlite3.Row) -> Dict[str, Any]:
    return {
        "request_id": rec["id"],
        "manager_id": rec["manager_id"],
        "client": {
            "name": rec["client_name"],
            "company": rec["client_company"],
            "contact": rec["client_contact"],
        },
        "site": json.loads(rec["site_params_json"] or "{}"),
        "status": rec["status"],
        "created_at": rec["created_at"],
    }

@dp.callback_query_handler(lambda c: c.data and c.data.startswith(CB_EXPORT_ONE))
async def cb_export_one(call: types.CallbackQuery):
    req_id = int(call.data[len(CB_EXPORT_ONE):])
    rec = get_request(req_id)
    if not rec:
        return await call.answer("Заявка не найдена.", show_alert=True)

    user = get_user_by_tgid(call.from_user.id)
    is_admin = get_mode(call.from_user.id) == "admin"
    if not (is_admin or (user and rec["manager_id"] == user["id"])):
        return await call.answer("Нет прав на экспорт этой заявки.", show_alert=True)

    payload = build_request_payload(rec)
    json_str = json.dumps(payload, ensure_ascii=False, indent=2)

    fname = f"request_{req_id}.json"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(json_str)

    await call.message.answer_document(types.InputFile(fname), caption=f"Экспорт заявки #{req_id} (JSON)")

# =====================
# АДМИНКА
# =====================
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
        return await message.answer("Вы уже в админке.", reply_markup=admin_kb())
    await AdminLogin.password.set()
    await message.answer("Введите пароль администратора:")

@dp.message_handler(state=AdminLogin.password)
async def admin_check_pass(message: types.Message, state: FSMContext):
    if message.text.strip() != ADMIN_PASSWORD:
        await state.finish()
        return await message.answer("Пароль неверный.")
    set_mode(message.from_user.id, "admin")
    await state.finish()
    await set_scope_cmds(message.chat.id, "admin", True)
    await message.answer("Готово. Режим админа включён.", reply_markup=admin_kb())

@dp.message_handler(commands=["logout"])
@dp.message_handler(lambda m: m.text == BTN_LOGOUT)
async def cmd_logout(message: types.Message):
    if get_mode(message.from_user.id) != "admin":
        return await message.answer("Сейчас не режим админа.")
    set_mode(message.from_user.id, "manager")
    await set_scope_cmds(message.chat.id, "manager", True)
    await message.answer("Вы вышли из админки. Вернулся режим менеджера.", reply_markup=manager_kb())

@dp.message_handler(commands=["admin_panel"])
@dp.message_handler(lambda m: m.text == BTN_PANEL)
@require_admin
async def cmd_admin_panel(message: types.Message):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users"); users_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM requests"); reqs_count = cur.fetchone()[0]
    conn.close()
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
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, first_name, last_name, age, contact, created_at, tg_id FROM users ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return await message.answer("Пользователей пока нет.")

    lines = []
    for u in rows:
        name = f"{u['first_name'] or ''} {u['last_name'] or ''}".strip() or "—"
        lines.append(
            f"#{u['id']}: <b>{e(name)}</b> | {e(u['contact'])} | возраст: {e(u['age'])} | tg_id: {e(u['tg_id'])} | {e(u['created_at'])}"
        )
    text = "\n".join(lines)
    for part in chunks(text):
        await message.answer(part)

@dp.message_handler(commands=["admin_requests"])
@dp.message_handler(lambda m: m.text == BTN_REQS)
@require_admin
async def cmd_admin_requests(message: types.Message):
    total = count_all_requests()
    if total == 0:
        return await message.answer("Заявок пока нет.")
    rows = list_all_requests(0, 20)
    lines = []
    for r in rows:
        mgr = get_user_by_id(r["manager_id"])
        mgr_name = (f"{mgr['first_name'] or ''} {mgr['last_name'] or ''}".strip()) if mgr else "?"
        lines.append(
            f"#{r['id']}: <b>{e(r['client_name'])}</b> | менеджер: {e(mgr_name)} | {e(r['status'])} | {e(r['created_at'])}"
        )
    text = "\n".join(lines)
    for part in chunks(text):
        await message.answer(part)

@dp.message_handler(commands=["export_request"])
@require_admin
async def cmd_export_request(message: types.Message):
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await message.answer("Использование: /export_request <id>")
    req_id = int(parts[1])
    rec = get_request(req_id)
    if not rec:
        return await message.answer("Заявка не найдена.")

    payload = build_request_payload(rec)
    json_str = json.dumps(payload, ensure_ascii=False, indent=2)
    fname = f"request_{req_id}.json"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(json_str)
    await message.answer_document(types.InputFile(fname), caption=f"Экспорт заявки #{req_id}")

@dp.message_handler(commands=["export_all"])
@require_admin
async def cmd_export_all(message: types.Message):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM requests ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return await message.answer("Заявок нет для экспорта.")

    # формируем ZIP в памяти
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for r in rows:
            payload = build_request_payload(r)
            data = json.dumps(payload, ensure_ascii=False, indent=2)
            zf.writestr(f"request_{r['id']}.json", data)
    bio.seek(0)
    await message.answer_document(types.InputFile(bio, filename="requests_export.zip"), caption="Экспорт всех заявок (ZIP)")

# =====================
# on_startup
# =====================
async def on_startup(dp):
    init_db()
    # Безопасный дефолт на /start всё равно выставит нужные команды в чате
    await bot.set_my_commands(GUEST_CMDS)
    log.info("Bot is running…")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
