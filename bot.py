# -*- coding: utf-8 -*-
# Полный файл bot.py доработанный под ваши требования:
# - Кнопка «✉️ Ответить» работает (короткий callback reply:msg, поиск письма по tg_message_id)
# - Опрос входящих каждые 10 секунд, IMAP таймаут 10 сек
# - Лог отправки в точном формате (копируемый текст):
#   Сообщение <Subject>
#   <Body> успешно отправлено пользователю
#   <email>⚡️
# - «Умные пресеты»: кнопка «Показать пресеты» выводит полный текст по страницам, как на скринах.
#   Пагинация и безопасное редактирование, без ошибки "message is not modified".
# - Исправлены фильтры aiogram v3 (без & State, использован StateFilter там, где нужно).
# - При парсинге XLSX ники из чёрного списка отсекаются (используется smtp25.BLACKLIST_CACHE).
#
# ВАЖНО: файл использует ваши модели/утилиты/конфиг как в исходнике.

import asyncio
import random
from io import BytesIO
from typing import List, Dict, Any, Optional, Tuple
import imaplib
import re
import unicodedata
import math
from contextlib import contextmanager
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackContext,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
)

import pandas as pd
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, BotCommand
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import StateFilter
from aiogram.filters import StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

from email.header import decode_header, make_header
from email import message_from_bytes
from email.utils import parseaddr

from db import (
    SessionLocal, User, Account, Preset, SmartPreset, Subject, Proxy, IncomingMessage,
    get_or_create_user, approve_user,
    list_domains, set_domains_order, add_domain, delete_domains_by_indices, clear_domains,
    add_account, update_account, delete_account, clear_accounts,
    get_setting, set_setting,
)

import config
import smtp25
import socks
import socket

# ====== Constants ======
READ_INTERVAL = 10  # sec (опрос каждые 10 секунд)
IMAP_TIMEOUT = 10
IMAP_PORT_SSL = 993
IMAP_HOST_MAP = {
    "gmail.com": "imap.gmail.com",
    "googlemail.com": "imap.gmail.com",
    "gmx.de": "imap.gmx.net",
    "gmx.net": "imap.gmx.net",
    "gmx.at": "imap.gmx.net",
    "web.de": "imap.web.de",
    "yahoo.com": "imap.mail.yahoo.com",
    "yahoo.co.uk": "imap.mail.yahoo.com",
    "yandex.ru": "imap.yandex.com",
    "yandex.com": "imap.yandex.com",
    "mail.ru": "imap.mail.ru",
    "bk.ru": "imap.mail.ru",
    "list.ru": "imap.mail.ru",
    "inbox.ru": "imap.mail.ru",
    "outlook.com": "outlook.office365.com",
    "hotmail.com": "outlook.office365.com",
    "live.com": "outlook.office365.com",
    "office365.com": "outlook.office365.com",
    "icloud.com": "imap.mail.me.com",
    "me.com": "imap.mail.me.com",
    "aol.com": "imap.aol.com",
}

# ====== Access control ======
ADMIN_IDS: List[int] = []
try:
    if hasattr(config, "ADMIN_IDS") and isinstance(config.ADMIN_IDS, (list, tuple)):
        ADMIN_IDS = [int(x) for x in config.ADMIN_IDS]
    elif hasattr(config, "ADMIN_TELEGRAM_ID"):
        ADMIN_IDS = [int(config.ADMIN_TELEGRAM_ID)]
except Exception:
    ADMIN_IDS = []

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

async def ensure_approved(obj: types.Message | types.CallbackQuery) -> bool:
    if isinstance(obj, types.CallbackQuery):
        user = obj.from_user
        msg = obj.message
    else:
        user = obj.from_user
        msg = obj
    with SessionLocal() as s:
        u = get_or_create_user(s, user.id, user.username, user.first_name, user.last_name)
        if u.status != "approved":
            await msg.answer("Ваша заявка на доступ отправлена администратору. Ожидайте одобрения.")
            return False
    return True

# ====== FSM ======
class AddAccountFSM(StatesGroup):
    display_name = State()
    loginpass = State()
    
class ReplyFSM(StatesGroup):
    compose = State()  # ввод текста/фото
    html = State()     # ввод HTML

class EditAccountFSM(StatesGroup):
    account_id = State()
    display_name = State()
    loginpass = State()

class EmailDeleteFSM(StatesGroup):
    account_id = State()

class EmailsClearFSM(StatesGroup):
    confirm = State()

class PresetAddFSM(StatesGroup):
    title = State()
    body = State()

class PresetEditFSM(StatesGroup):
    preset_id = State()
    title = State()
    body = State()

class PresetDeleteFSM(StatesGroup):
    preset_id = State()

class PresetClearFSM(StatesGroup):
    confirm = State()

class SmartPresetAddFSM(StatesGroup):
    body = State()

class SmartPresetEditFSM(StatesGroup):
    preset_id = State()
    body = State()

class SmartPresetDeleteFSM(StatesGroup):
    preset_id = State()

class SmartPresetClearFSM(StatesGroup):
    confirm = State()

class SubjectAddFSM(StatesGroup):
    title = State()

class SubjectEditFSM(StatesGroup):
    subject_id = State()
    title = State()

class SubjectDeleteFSM(StatesGroup):
    subject_id = State()

class SubjectClearFSM(StatesGroup):
    confirm = State()

class CheckNicksFSM(StatesGroup):
    file = State()

class QuickAddFSM(StatesGroup):
    mode = State()   # one | many
    name = State()   # for "one"
    lines = State()  # bulk input

class DomainsFSM(StatesGroup):
    add = State()
    reorder = State()
    delete = State()
    clear = State()

class IntervalFSM(StatesGroup):
    set = State()

class ProxiesFSM(StatesGroup):
    add = State()
    edit_pick = State()
    edit_value = State()
    delete = State()
    clear = State()

class SingleSendFSM(StatesGroup):
    to = State()
    body = State()
    
class ReplyFSM(StatesGroup):
    compose = State()  # ввод текста/фото
    html = State()     # ввод HTML

# ====== Runtime ======
bot = Bot(
    token=config.TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())

LAST_XLSX_PER_CHAT: Dict[int, bytes] = {}
BASES_PER_CHAT: Dict[int, List[str]] = {}         # список баз (имя.фамилия) без домена
VERIFIED_ROWS_PER_CHAT: Dict[int, List[Dict[str, Any]]] = {}  # [{email, seller_name, title}]

IMAP_TASKS: Dict[int, asyncio.Task] = {}
IMAP_STATUS: Dict[int, Dict[str, Any]] = {}  # {"running": bool, "accounts": {email: {"active": bool, "last_ok": str, "last_err": str}}}

SEND_TASKS: Dict[int, asyncio.Task] = {}
SEND_STATUS: Dict[int, Dict[str, Any]] = {}

# ====== Helpers ======
def reply_main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="📖 Проверка ников"), KeyboardButton(text="🧾 HTML-шаблоны")],
            [KeyboardButton(text="Настройки⚙️")],
            [KeyboardButton(text="✉️ Отправить email"), KeyboardButton(text="➕ Быстрое добавление")],
        ]
    )
    
def _build_application() -> "Application":
    request = HTTPXRequest(
        connect_timeout=30.0,
        read_timeout=120.0,
        write_timeout=30.0,
        pool_timeout=30.0,
    )
    application = (
        Application.builder()
        .token(TOKEN)
        .get_updates_request(request)
        .request(request)
        .build()
    )
    return application

def _normalize_nick_local(nick: str) -> str:
    # Используем smtp25.normalize_nick, при отсутствии — локально
    try:
        return smtp25.normalize_nick(nick)
    except Exception:
        normalized = unicodedata.normalize('NFKD', str(nick))
        ascii_nick = normalized.encode('ascii', 'ignore').decode('ascii')
        return ascii_nick.lower()

def preview_bases_from_df(df: pd.DataFrame) -> List[str]:
    # Предпросмотр без сетевых вызовов, но с теми же фильтрами, что в smtp25
    bases: List[str] = []
    seen: set[str] = set()
    bl = getattr(smtp25, "BLACKLIST_CACHE", set())
    for _, row in df.iterrows():
        nick = str(row.get("seller_nick", "")).strip()
        if not nick:
            continue
        normalized = _normalize_nick_local(nick)
        if normalized in bl:
            continue
        parts = smtp25.extract_name_parts(nick)
        if not parts:
            continue
        first, last = parts
        if len(first) < 3 or (last and len(last) < 3):
            continue
        base = smtp25.generate_email(first, last)
        if base and base not in seen:
            seen.add(base)
            bases.append(base)
    return bases

def tg(text: str) -> str:
    # Экранирует HTML для Telegram (если нужно)
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def nav_row(back_cb: str) -> list[list[InlineKeyboardButton]]:
    return [[InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb),
             InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]]

async def delete_message_safe(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass
        
_BLACKLIST_INIT_DONE = False

def _ensure_blacklist_loaded_once():
    global _BLACKLIST_INIT_DONE
    if _BLACKLIST_INIT_DONE and getattr(smtp25, "BLACKLIST_CACHE", None):
        return
    try:
        cache = smtp25.load_blacklist()
        # Если load_blacklist возвращает set — используем его, иначе берём что уже внутри модуля
        if isinstance(cache, set):
            smtp25.BLACKLIST_CACHE = cache
        elif getattr(smtp25, "BLACKLIST_CACHE", None) is None:
            smtp25.BLACKLIST_CACHE = set()
        print(f"📋 Загружено {len(getattr(smtp25, 'BLACKLIST_CACHE', set()))} ников в черном списке")
    except Exception:
        if getattr(smtp25, "BLACKLIST_CACHE", None) is None:
            smtp25.BLACKLIST_CACHE = set()
    _BLACKLIST_INIT_DONE = True

def prepare_smtp25_from_db(user_id: int) -> List[str]:
    with SessionLocal() as s:
        domains = list_domains(s, user_id)
        smtp25.SEND_PROXY_LIST = [
            {"id": p.id, "host": p.host, "port": p.port, "user": p.user_login, "password": p.password}
            for p in s.query(Proxy).filter_by(user_id=user_id, type="send").all()
        ]
        smtp25.VERIFY_PROXY_LIST = [
            {"id": p.id, "host": p.host, "port": p.port, "user": p.user_login, "password": p.password}
            for p in s.query(Proxy).filter_by(user_id=user_id, type="verify").all()
        ]
        smtp25.EMAIL_ACCOUNTS = [
            {"id": a.id, "name": a.display_name, "email": a.email, "password": a.password}
            for a in s.query(Account).filter_by(user_id=user_id).all()
        ]
        smtp25.SUBJECTS = [x.title for x in s.query(Subject).filter_by(user_id=user_id).all()] or ["Ist OFFER noch verfügbar?"]
        smtp25.TEMPLATES = [x.body for x in s.query(SmartPreset).filter_by(user_id=user_id).all()] or ["Hi SELLER, ist OFFER noch verfügbar?"]

    # Раньше здесь каждый раз вызывался smtp25.load_blacklist() — убрано.
    _ensure_blacklist_loaded_once()
    return domains

# Безопасное редактирование (не падает на "message is not modified")
async def safe_edit_message(msg: types.Message, text: str, reply_markup: InlineKeyboardMarkup | None = None, parse_mode=None):
    try:
        await msg.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            try:
                await msg.edit_reply_markup(reply_markup=reply_markup)
            except TelegramBadRequest:
                pass
        else:
            raise
async def safe_cq_answer(cq: types.CallbackQuery, text: str | None = None, show_alert: bool = False, cache_time: int | None = None):
    """
    Безопасный ответ на callback_query: не падает, если запрос протух.
    """
    try:
        await cq.answer(text=text, show_alert=show_alert, cache_time=cache_time)
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if "query is too old" in msg or "query id is invalid" in msg or "response timeout expired" in msg:
            # игнорируем слишком старые/протухшие запросы
            return
        raise

# Лог успешной отправки (точный формат как на скринах)
async def log_send_ok(chat_id: int, subject: str, body: str, to_email: str):
    subject = subject or ""
    body = body or ""
    text = f"Сообщение {subject}\n{body} успешно отправлено пользователю\n{to_email}⚡️"
    await bot.send_message(chat_id, text, parse_mode=None)

# ====== START / ADMIN ======
@dp.message(Command("start"))
async def start_cmd(m: types.Message):
    with SessionLocal() as s:
        u = get_or_create_user(s, m.from_user.id, m.from_user.username, m.from_user.first_name, m.from_user.last_name)
        if u.status == "pending":
            for admin_id in ADMIN_IDS:
                try:
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin:approve:{u.id}"),
                         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin:deny:{u.id}")]
                    ])
                    await bot.send_message(admin_id, f"Новая заявка на доступ:\n@{u.username} ({u.first_name} {u.last_name})\nuser_id={u.id}", reply_markup=kb)
                except Exception:
                    pass
            await m.answer("Заявка на доступ отправлена администратору. Ожидайте одобрения.")
            return
        elif u.status == "denied":
            await m.answer("Доступ отклонён администратором.")
            return
    await m.answer("Готово. Выберите действие кнопками снизу.", reply_markup=reply_main_kb())

@dp.callback_query(F.data.startswith("admin:"))
async def admin_approve(c: types.CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав.", show_alert=True); return
    _, action, uid = c.data.split(":")
    user_id = int(uid)
    with SessionLocal() as s:
        approve_user(s, user_id, approved=(action == "approve"))
        u = s.query(User).filter_by(id=user_id).first()
    try:
        if u and u.tg_id:
            text = "Доступ одобрен. Добро пожаловать!" if action == "approve" else "К сожалению, доступ отклонён."
            await bot.send_message(u.tg_id, text)
    except Exception:
        pass
    await c.answer("Готово."); await delete_message_safe(c.message)

# ====== UI generic ======
@dp.callback_query(F.data == "ui:hide")
async def ui_hide(c: types.CallbackQuery):
    await delete_message_safe(c.message)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "settings:back")
async def settings_back(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text(settings_main_text(), reply_markup=settings_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "noop")
async def noop_cb(c: types.CallbackQuery):
    await safe_cq_answer(c)

@dp.message(F.text == "Настройки⚙️")
async def btn_settings(m: types.Message):
    if not await ensure_approved(m): return
    await m.answer(settings_main_text(), reply_markup=settings_kb())

@dp.message(Command("settings"))
async def cmd_settings(m: types.Message):
    await btn_settings(m)

# ====== Settings root ======
def settings_main_text() -> str:
    return "Настройки:"

def settings_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📑 Домены", callback_data="domains:open"),
         InlineKeyboardButton(text="📚 Пресеты (IMAP)", callback_data="presets:open")],
        [InlineKeyboardButton(text="📌 Темы", callback_data="subjects:open"),
         InlineKeyboardButton(text="📗 Умные пресеты", callback_data="smart:open")],
        [InlineKeyboardButton(text="📧 E‑mail", callback_data="emails:open"),
         InlineKeyboardButton(text="🌐 Прокси", callback_data="proxies:root")],
        [InlineKeyboardButton(text="⏱ Интервал", callback_data="interval:open")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ====== Domains ======
def domains_text_for_user(user_id: int) -> str:
    with SessionLocal() as s:
        doms = list_domains(s, user_id)
    if not doms:
        return "Текущие домены: список пуст."
    return "Текущие домены (по приоритету):\n\n" + "\n".join(f"{i+1}. {d}" for i, d in enumerate(doms))

def domains_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="domains:add"),
         InlineKeyboardButton(text="🔁 Изменить порядок", callback_data="domains:reorder")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="domains:delete"),
         InlineKeyboardButton(text="🧹 Удалить все", callback_data="domains:clear")],
        *nav_row("settings:back")
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "domains:open")
async def domains_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text(domains_text_for_user(c.from_user.id), reply_markup=domains_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "domains:add")
async def domains_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text(
        domains_text_for_user(c.from_user.id) + "\n\nВведите домен. Можно позицию: «gmail.com 1».",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open"))
    )
    await state.set_state(DomainsFSM.add); await safe_cq_answer(c)

@dp.message(DomainsFSM.add)
async def domains_add_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    parts = m.text.strip().split()
    if not parts:
        await m.answer("Пустой ввод.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open"))); return
    name = parts[0]
    pos = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
    with SessionLocal() as s:
        add_domain(s, m.from_user.id, name, pos)
    await m.answer(domains_text_for_user(m.from_user.id), reply_markup=domains_kb()); await state.clear()

@dp.callback_query(F.data == "domains:reorder")
async def domains_reorder(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text(
        domains_text_for_user(c.from_user.id) + "\n\nВведите новый порядок номеров (например: 3 1 2 4)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open"))
    )
    await state.set_state(DomainsFSM.reorder); await safe_cq_answer(c)

@dp.message(DomainsFSM.reorder)
async def domains_reorder_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    with SessionLocal() as s:
        names = list_domains(s, m.from_user.id)
    try:
        order = [int(x) for x in m.text.replace(",", " ").split()]
        if sorted(order) != list(range(1, len(names) + 1)):
            raise ValueError
        new_names = [names[i - 1] for i in order]
        with SessionLocal() as s:
            set_domains_order(s, m.from_user.id, new_names)
        await m.answer(domains_text_for_user(m.from_user.id), reply_markup=domains_kb()); await state.clear()
    except Exception:
        await m.answer("Неверный формат. Пример: 2 1 3", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))

@dp.callback_query(F.data == "domains:delete")
async def domains_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text(
        domains_text_for_user(c.from_user.id) + "\n\nВведите номера доменов для удаления (например: 1 4 6).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open"))
    )
    await state.set_state(DomainsFSM.delete); await safe_cq_answer(c)

@dp.message(DomainsFSM.delete)
async def domains_delete_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    try:
        nums = sorted({int(x) for x in m.text.replace(",", " ").split()}, reverse=True)
        with SessionLocal() as s:
            delete_domains_by_indices(s, m.from_user.id, list(nums))
        await m.answer(domains_text_for_user(m.from_user.id), reply_markup=domains_kb()); await state.clear()
    except Exception:
        await m.answer("Неверный ввод. Пример: 2 5 6", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))

@dp.callback_query(F.data == "domains:clear")
async def domains_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Подтвердите удаление всех доменов: ДА",
                              reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))
    await state.set_state(DomainsFSM.clear); await safe_cq_answer(c)

@dp.message(DomainsFSM.clear)
async def domains_clear_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if m.text.strip().upper() == "ДА":
        with SessionLocal() as s:
            clear_domains(s, m.from_user.id)
        await m.answer("Все домены удалены.\n\n" + domains_text_for_user(m.from_user.id), reply_markup=domains_kb())
    else:
        await m.answer("Отменено.", reply_markup=domains_kb())
    await state.clear()

# ====== INTERVAL ======
def interval_text(user_id: int) -> str:
    vmin = get_setting(user_id, "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    vmax = get_setting(user_id, "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    return f"Текущий интервал:\n\n[{vmin}, {vmax}]"

def interval_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="✏️ Изменить интервал", callback_data="interval:change"),
         InlineKeyboardButton(text="🔄 Сбросить интервал", callback_data="interval:reset")],
        *nav_row("settings:back")
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "interval:open")
async def interval_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text(interval_text(c.from_user.id), reply_markup=interval_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "interval:change")
async def interval_change(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text(
        interval_text(c.from_user.id) + "\n\nВведите два числа: MIN MAX (например: 3 6)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("interval:open"))
    )
    await state.set_state(IntervalFSM.set); await safe_cq_answer(c)

@dp.message(IntervalFSM.set)
async def interval_set_value(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    try:
        parts = [int(x) for x in m.text.replace(",", " ").split()]
        if len(parts) != 2:
            raise ValueError
        minv, maxv = parts
        if minv < 0 or maxv < 0 or minv >= maxv:
            raise ValueError
        set_setting(m.from_user.id, "send_delay_min", str(minv))
        set_setting(m.from_user.id, "send_delay_max", str(maxv))
        await m.answer(interval_text(m.from_user.id), reply_markup=interval_kb())
        await state.clear()
    except Exception:
        await m.answer("Неверный ввод. Пример: 3 6", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("interval:open")))

@dp.callback_query(F.data == "interval:reset")
async def interval_reset(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    set_setting(c.from_user.id, "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    set_setting(c.from_user.id, "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    await c.message.edit_text(interval_text(c.from_user.id), reply_markup=interval_kb())
    await c.answer("Сброшено")

# ====== PROXIES ======
def proxies_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛡 Verif прокси", callback_data="proxies:open:verify")],
        [InlineKeyboardButton(text="🚀 Send прокси", callback_data="proxies:open:send")],
        *nav_row("settings:back")
    ])

def proxies_section_kb(kind: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌐 Показать прокси", callback_data=f"proxies:list:{kind}")],
        [InlineKeyboardButton(text="➕ Добавить прокси", callback_data=f"proxies:add:{kind}"),
         InlineKeyboardButton(text="✏️ Изменить прокси", callback_data=f"proxies:edit:{kind}")],
        [InlineKeyboardButton(text="🗑 Удалить прокси", callback_data=f"proxies:delete:{kind}"),
         InlineKeyboardButton(text="🧹 Удалить все", callback_data=f"proxies:clear:{kind}")],
        *nav_row("proxies:root")
    ])

def render_proxies_text(user_id: int, kind: str) -> str:
    with SessionLocal() as s:
        items = s.query(Proxy).filter_by(user_id=user_id, type=kind).order_by(Proxy.id.asc()).all()
    if not items:
        return "Ваш список прокси пуст."
    title = "Verif прокси" if kind == "verify" else "Send прокси"
    lines = [f"{title}:\n"]
    for idx, p in enumerate(items, 1):
        host = p.host or ""
        login = p.user_login or ""
        pwd = p.password or ""
        lines.append(f"{idx}. {host}:{p.port}:{login}:{pwd}  (ID={p.id})")
    lines.append("\nДля изменений/удаления указывайте ID (в скобках).")
    return "\n".join(lines)

def parse_proxy_lines(text: str) -> List[Tuple[str, int, str, str]]:
    results = []
    for raw in [ln.strip() for ln in text.splitlines() if ln.strip()]:
        parts = raw.split(":")
        if len(parts) != 4:
            continue
        host, port, user, pwd = parts
        try:
            port_i = int(port)
        except Exception:
            continue
        results.append((host, port_i, user, pwd))
    return results

@dp.callback_query(F.data == "proxies:root")
async def proxies_root(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text("Настройки прокси:", reply_markup=proxies_root_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("proxies:open:"))
async def proxies_open_section(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]  # verify | send
    title = "Verif прокси" if kind == "verify" else "Send прокси"
    await c.message.edit_text(f"Настройки {title}:", reply_markup=proxies_section_kb(kind)); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("proxies:list:"))
async def proxies_list(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    text = render_proxies_text(c.from_user.id, kind)
    await c.message.edit_text(text, reply_markup=proxies_section_kb(kind)); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("proxies:add:"))
async def proxies_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await c.message.edit_text("Введите прокси в формате host:port:log:pass✍️\nМожно по одному на строку.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.add); await safe_cq_answer(c)

@dp.message(ProxiesFSM.add)
async def proxies_add_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    parsed = parse_proxy_lines(m.text)
    if not parsed:
        await m.answer("Не распознано ни одной строки. Ожидается host:port:log:pass",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
        return
    with SessionLocal() as s:
        for host, port, user, pwd in parsed:
            s.add(Proxy(user_id=m.from_user.id, host=host, port=port, user_login=user, password=pwd, type=kind, active=True))
        s.commit()
    await m.answer("Прокси добавлены.", reply_markup=proxies_section_kb(kind))
    await state.clear()

@dp.callback_query(F.data.startswith("proxies:edit:"))
async def proxies_edit_pick(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await c.message.edit_text("Введите ID прокси для изменения (смотрите его в списке в скобках):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.edit_pick); await safe_cq_answer(c)

@dp.message(ProxiesFSM.edit_pick)
async def proxies_edit_id(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("proxies:root")))
        return
    await state.update_data(proxy_id=int(m.text.strip()))
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    await m.answer("Введите новые данные в формате host:port:log:pass:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.edit_value)

@dp.message(ProxiesFSM.edit_value)
async def proxies_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    proxy_id = int(data.get("proxy_id"))
    parsed = parse_proxy_lines(m.text)
    if len(parsed) != 1:
        await m.answer("Ожидается одна строка формата host:port:log:pass.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
        return
    host, port, user, pwd = parsed[0]
    with SessionLocal() as s:
        pr = s.query(Proxy).filter_by(user_id=m.from_user.id, id=proxy_id, type=kind).first()
        if not pr:
            await m.answer("Прокси не найден.", reply_markup=proxies_section_kb(kind))
        else:
            pr.host = host; pr.port = port; pr.user_login = user; pr.password = pwd
            s.commit()
            await m.answer("Прокси обновлён.", reply_markup=proxies_section_kb(kind))
    await state.clear()

@dp.callback_query(F.data.startswith("proxies:delete:"))
async def proxies_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await c.message.edit_text("Введите ID прокси для удаления (можно несколько через пробел):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.delete); await safe_cq_answer(c)

@dp.message(ProxiesFSM.delete)
async def proxies_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    try:
        ids = [int(x) for x in m.text.replace(",", " ").split()]
    except Exception:
        await m.answer("Неверный ввод. Пример: 1 2 3",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
        return
    with SessionLocal() as s:
        for pid in ids:
            s.query(Proxy).filter_by(user_id=m.from_user.id, id=pid, type=kind).delete()
        s.commit()
    await m.answer("Удаление выполнено.", reply_markup=proxies_section_kb(kind))
    await state.clear()

@dp.callback_query(F.data.startswith("proxies:clear:"))
async def proxies_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await c.message.edit_text("Подтвердите удаление всех прокси: ДА",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.clear); await safe_cq_answer(c)

@dp.message(ProxiesFSM.clear)
async def proxies_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    if m.text.strip().upper() == "ДА":
        with SessionLocal() as s:
            s.query(Proxy).filter_by(user_id=m.from_user.id, type=kind).delete()
            s.commit()
        await m.answer("Все прокси удалены.", reply_markup=proxies_section_kb(kind))
    else:
        await m.answer("Отменено.", reply_markup=proxies_section_kb(kind))
    await state.clear()

# ====== EMAIL ACCOUNTS ======
def emails_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📬 Показать E‑mail", callback_data="emails:list")],
        [InlineKeyboardButton(text="➕ Добавить E‑mail", callback_data="emails:add"),
         InlineKeyboardButton(text="✏️ Изменить E‑mail", callback_data="emails:edit")],
        [InlineKeyboardButton(text="🗑 Удалить E‑mail", callback_data="emails:delete"),
         InlineKeyboardButton(text="🧹 Удалить все", callback_data="emails:clear")],
        *nav_row("settings:back")
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def render_emails_text(user_id: int) -> str:
    with SessionLocal() as s:
        items = s.query(Account).filter_by(user_id=user_id).order_by(Account.id.asc()).all()
    if not items:
        return "Пока аккаунтов нет."
    lines = []
    for acc in items:
        lines.append(f"E‑mail#{acc.id}\n{acc.display_name}\n{acc.email}:{acc.password}\n")
    return "\n".join(lines)

@dp.callback_query(F.data == "emails:open")
async def emails_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text("Настройки E‑mail:", reply_markup=emails_menu_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "emails:list")
async def emails_list(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text(render_emails_text(c.from_user.id), reply_markup=emails_menu_kb()); await safe_cq_answer(c)
    
async def _ensure_imap_started_for_user(uid: int, chat_id: int, started_for_email: Optional[str] = None):
    # Подготовить окружение (прокси/аккаунты)
    prepare_smtp25_from_db(uid)
    if uid not in IMAP_TASKS or IMAP_TASKS[uid].done():
        IMAP_TASKS[uid] = asyncio.create_task(imap_loop(uid, chat_id))
    # Лог о запуске для конкретного аккаунта (по требованию)
    if started_for_email:
        await bot.send_message(chat_id, f"Поток для {started_for_email} запущен⚡️", parse_mode=None)

@dp.callback_query(F.data == "emails:add")
async def emails_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите отображаемое имя и фамилию. Например: Jessy Jackson ✍️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(AddAccountFSM.display_name); await safe_cq_answer(c)

@dp.message(AddAccountFSM.display_name)
async def emails_add_name(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.update_data(display_name=m.text.strip())
    await m.answer("Введите E‑mail в формате login:pass ✍️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(AddAccountFSM.loginpass)

@dp.message(AddAccountFSM.loginpass)
async def emails_add_loginpass(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    disp = data.get("display_name", "").strip()
    if ":" not in m.text:
        await m.answer("Ожидаю формат login:pass. Попробуйте ещё раз.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
        return
    login, password = [x.strip() for x in m.text.split(":", 1)]
    with SessionLocal() as s:
        add_account(s, m.from_user.id, disp, login, password, auto_bind_proxy=True)
    await m.answer("Аккаунт сохранён.", reply_markup=emails_menu_kb())
    # Автозапуск потока чтения (через send‑прокси)
    await _ensure_imap_started_for_user(m.from_user.id, m.chat.id, started_for_email=login)
    await state.clear()

@dp.callback_query(F.data == "emails:edit")
async def emails_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите ID аккаунта для изменения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EditAccountFSM.account_id); await safe_cq_answer(c)

@dp.message(EditAccountFSM.account_id)
async def emails_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
        return
    await state.update_data(account_id=int(m.text.strip()))
    await m.answer("Новое отображаемое имя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EditAccountFSM.display_name)

@dp.message(EditAccountFSM.display_name)
async def emails_edit_name(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.update_data(display_name=m.text.strip())
    await m.answer("Новый login:pass:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EditAccountFSM.loginpass)

@dp.message(EditAccountFSM.loginpass)
async def emails_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    acc_id = int(data["account_id"])
    if ":" not in m.text:
        await m.answer("Ожидаю формат login:pass.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
        return
    login, password = [x.strip() for x in m.text.split(":", 1)]
    with SessionLocal() as s:
        update_account(s, m.from_user.id, acc_id, display_name=data["display_name"], email=login, password=password)
    await m.answer("Аккаунт обновлён.", reply_markup=emails_menu_kb())
    # Автозапуск/обновление потока
    await _ensure_imap_started_for_user(m.from_user.id, m.chat.id, started_for_email=login)
    await state.clear()

@dp.callback_query(F.data == "emails:delete")
async def emails_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите ID аккаунта для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EmailDeleteFSM.account_id); await safe_cq_answer(c)

@dp.message(EmailDeleteFSM.account_id)
async def emails_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
        return
    acc_id = int(m.text.strip())
    with SessionLocal() as s:
        delete_account(s, m.from_user.id, acc_id)
    await m.answer("Аккаунт удалён.", reply_markup=emails_menu_kb())
    await state.clear()

@dp.callback_query(F.data == "emails:clear")
async def emails_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Подтвердите удаление всех аккаунтов: ДА",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EmailsClearFSM.confirm); await safe_cq_answer(c)

@dp.message(EmailsClearFSM.confirm)
async def emails_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if m.text.strip().upper() == "ДА":
        with SessionLocal() as s:
            clear_accounts(s, m.from_user.id)
        await m.answer("Все аккаунты удалены.", reply_markup=emails_menu_kb())
    else:
        await m.answer("Отменено.", reply_markup=emails_menu_kb())
    await state.clear()

# ====== PRESETS (IMAP) ======
def presets_text(user_id: int) -> str:
    with SessionLocal() as s:
        items = s.query(Preset).filter_by(user_id=user_id).order_by(Preset.id.asc()).all()
    if not items:
        return "Пресетов пока нет."
    return "Ваши пресеты:\n\n" + "\n".join([f"#{p.id} {p.title}" for p in items])

def presets_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 Показать", callback_data="presets:list")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="presets:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="presets:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="presets:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="presets:clear")],
        *nav_row("settings:back")
    ])
    
def reply_actions_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📬 Отправить пресет", callback_data="reply:use_preset"),
         InlineKeyboardButton(text="5️⃣ Отправить HTML", callback_data="reply:use_html")],
        [InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]
    ])

def presets_inline_kb(user_id: int, back_cb: str) -> InlineKeyboardMarkup:
    with SessionLocal() as s:
        items = s.query(Preset).filter_by(user_id=user_id).order_by(Preset.id.asc()).all()
    rows = [[InlineKeyboardButton(text=f"📜 #{p.id} {p.title}", callback_data=f"presets:view:{p.id}:{back_cb}")] for p in items]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "presets:open")
async def presets_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text("Пресеты (IMAP):", reply_markup=presets_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "presets:list")
async def presets_list(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    await safe_edit_message(
        c.message,
        "Ваши пресеты:",
        reply_markup=presets_inline_kb(c.from_user.id, back_cb="presets:open"),
        parse_mode=None
    )
    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("presets:view:"))
async def presets_view_cb(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    # формат: presets:view:<preset_id>:<back_cb>
    _, _, pid, back_cb = c.data.split(":", 3)

    with SessionLocal() as s:
        p = s.query(Preset).filter_by(user_id=c.from_user.id, id=int(pid)).first()

    if not p:
        await c.answer("Не найдено", show_alert=True)
        return

    # Если пользователь в режиме ответа — сразу отправляем письмо из пресета
    cur_state = await state.get_state()
    if cur_state in {ReplyFSM.compose, ReplyFSM.html}:
        data = await state.get_data()
        acc_id = int(data["acc_id"])
        to_email = data["to"]
        subj = data.get("subject") or "Re:"
        body = (p.body or "").strip()
        is_html = (cur_state == ReplyFSM.html)
        ok = await send_email_via_account(c.from_user.id, acc_id, to_email, subj, body, html=is_html)
        if ok:
            await log_send_ok(c.message.chat.id, subj, body, to_email)
            await safe_edit_message(c.message, "Отправлено ✅", reply_markup=None, parse_mode=None)
        else:
            await safe_edit_message(c.message, "Ошибка отправки ❌", reply_markup=None, parse_mode=None)
        await state.clear()
        await safe_cq_answer(c)
        return

    # Обычный просмотр текста пресета
    await safe_edit_message(
        c.message,
        (p.body or "").strip(),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)]]
        ),
        parse_mode=None  # без HTML-разметки, чтобы показать текст полностью
    )
    await safe_cq_answer(c)

@dp.callback_query(F.data == "presets:add")
async def presets_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите заголовок пресета:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetAddFSM.title); await safe_cq_answer(c)

@dp.message(PresetAddFSM.title)
async def presets_add_title(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.update_data(title=m.text.strip())
    await m.answer("Введите текст пресета:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetAddFSM.body)

@dp.message(PresetAddFSM.body)
async def presets_add_body(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    with SessionLocal() as s:
        s.add(Preset(user_id=m.from_user.id, title=data["title"], body=m.text)); s.commit()
    await m.answer("Пресет добавлен.", reply_markup=presets_kb()); await state.clear()

@dp.callback_query(F.data == "presets:edit")
async def presets_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите ID пресета для изменения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetEditFSM.preset_id); await safe_cq_answer(c)

@dp.message(PresetEditFSM.preset_id)
async def presets_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))); return
    await state.update_data(preset_id=int(m.text.strip()))
    await m.answer("Новый заголовок:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetEditFSM.title)

@dp.message(PresetEditFSM.title)
async def presets_edit_title(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.update_data(title=m.text.strip())
    await m.answer("Новый текст:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetEditFSM.body)

@dp.message(PresetEditFSM.body)
async def presets_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    with SessionLocal() as s:
        p = s.query(Preset).filter_by(user_id=m.from_user.id, id=data["preset_id"]).first()
        if not p: await m.answer("Пресет не найден.", reply_markup=presets_kb()); await state.clear(); return
        p.title = data["title"]; p.body = m.text; s.commit()
    await m.answer("Пресет обновлён.", reply_markup=presets_kb()); await state.clear()

@dp.callback_query(F.data == "presets:delete")
async def presets_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите ID пресета для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetDeleteFSM.preset_id); await safe_cq_answer(c)

@dp.message(PresetDeleteFSM.preset_id)
async def presets_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))); return
    pid = int(m.text.strip())
    with SessionLocal() as s:
        s.query(Preset).filter_by(user_id=m.from_user.id, id=pid).delete(); s.commit()
    await m.answer("Удалено.", reply_markup=presets_kb()); await state.clear()

@dp.callback_query(F.data == "presets:clear")
async def presets_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Подтвердите очистку пресетов: ДА", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetClearFSM.confirm); await safe_cq_answer(c)

@dp.message(PresetClearFSM.confirm)
async def presets_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if m.text.strip().upper() == "ДА":
        with SessionLocal() as s: s.query(Preset).filter_by(user_id=m.from_user.id).delete(); s.commit()
        await m.answer("Все пресеты удалены.", reply_markup=presets_kb())
    else:
        await m.answer("Отменено.", reply_markup=presets_kb())
    await state.clear()

# ====== SMART PRESETS (ПОЛНЫЙ ПОКАЗ С ПАГИНАЦИЕЙ) ======
def smart_settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Показать пресеты", callback_data="smart:show:1")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="smart:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="smart:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="smart:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="smart:clear")],
        *nav_row("settings:back")
    ])

def smart_pager_kb(page: int, total_pages: int) -> list[list[InlineKeyboardButton]]:
    left_page = max(1, page - 1)
    right_page = min(total_pages, page + 1)
    return [[
        InlineKeyboardButton(text="◀️", callback_data=f"smart:show:{left_page}"),
        InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="smart:noop"),
        InlineKeyboardButton(text="▶️", callback_data=f"smart:show:{right_page}")
    ]]

def smart_manage_kb() -> list[list[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="smart:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="smart:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="smart:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="smart:clear")],
        *nav_row("smart:open")
    ]

def build_smart_text_and_kb(user_id: int, page: int = 1, per_page: int = 8) -> tuple[str, InlineKeyboardMarkup]:
    with SessionLocal() as s:
        items = s.query(SmartPreset).filter_by(user_id=user_id).order_by(SmartPreset.id.asc()).all()

    total = len(items)
    if total == 0:
        return "Пресетов пока нет.", smart_settings_kb()

    def compose_page(pp: int) -> tuple[str, int]:
        total_pages = max(1, math.ceil(total / pp))
        page_clamped = max(1, min(page, total_pages))
        start = (page_clamped - 1) * pp
        end = min(total, start + pp)
        slice_items = items[start:end]
        lines: list[str] = []
        for p in slice_items:
            lines.append(f"Пресет #{p.id}\n\nOFFER\n{(p.body or '').strip()}\n")
        return "\n".join(lines).strip(), total_pages

    text, total_pages = compose_page(per_page)
    while len(text) > 3800 and per_page > 3:
        per_page -= 1
        text, total_pages = compose_page(per_page)

    ik = smart_pager_kb(page, total_pages)
    ik += smart_manage_kb()
    return text, InlineKeyboardMarkup(inline_keyboard=ik)

def smart_kb() -> InlineKeyboardMarkup:
    # Используем новую панель настроек, как на скрине
    return smart_settings_kb()

@dp.callback_query(F.data == "smart:open")
async def smart_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_edit_message(c.message, "Настройки умных пресетов:", reply_markup=smart_settings_kb(), parse_mode=None)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "smart:list")
async def smart_list_legacy(c: types.CallbackQuery):
    # Легаси-кнопка — переводим на новую панель
    if not await ensure_approved(c): return
    await smart_open(c)

@dp.callback_query(F.data.startswith("smart:show"))
async def smart_show(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    parts = c.data.split(":")
    page = 1
    if len(parts) == 3 and parts[2].isdigit():
        page = int(parts[2])
    text, kb = build_smart_text_and_kb(c.from_user.id, page=page, per_page=8)
    await safe_edit_message(c.message, text, reply_markup=kb, parse_mode=None)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "smart:noop")
async def smart_noop(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_cq_answer(c)

@dp.callback_query(F.data == "smart:hide")
async def smart_hide(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_edit_message(c.message, "Настройки умных пресетов:", reply_markup=smart_settings_kb(), parse_mode=None)
    await safe_cq_answer(c)

# ====== SUBJECTS ======
def subjects_text(user_id: int) -> str:
    with SessionLocal() as s:
        items = s.query(Subject).filter_by(user_id=user_id).order_by(Subject.id.asc()).all()
    if not items:
        return "Тем пока нет."
    return "Ваши темы:\n\n" + "\n".join([f"#{x.id} {x.title}" for x in items])

def subjects_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 Показать", callback_data="subjects:list")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="subjects:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="subjects:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="subjects:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="subjects:clear")],
        *nav_row("settings:back")
    ])

@dp.callback_query(F.data == "subjects:open")
async def subjects_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text("Темы:", reply_markup=subjects_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "subjects:list")
async def subjects_list(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await c.message.edit_text(subjects_text(c.from_user.id), reply_markup=subjects_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "subjects:add")
async def subjects_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите название темы:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectAddFSM.title); await safe_cq_answer(c)

@dp.message(SubjectAddFSM.title)
async def subjects_add_title(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    with SessionLocal() as s:
        s.add(Subject(user_id=m.from_user.id, title=m.text.strip())); s.commit()
    await m.answer("Тема добавлена.", reply_markup=subjects_kb()); await state.clear()

@dp.callback_query(F.data == "subjects:edit")
async def subjects_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите ID темы:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectEditFSM.subject_id); await safe_cq_answer(c)

@dp.message(SubjectEditFSM.subject_id)
async def subjects_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open"))); return
    await state.update_data(subject_id=int(m.text.strip()))
    await m.answer("Новое название темы:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectEditFSM.title)

@dp.message(SubjectEditFSM.title)
async def subjects_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    with SessionLocal() as s:
        subj = s.query(Subject).filter_by(user_id=m.from_user.id, id=data["subject_id"]).first()
        if not subj: await m.answer("Тема не найдена.", reply_markup=subjects_kb()); await state.clear(); return
        subj.title = m.text.strip(); s.commit()
    await m.answer("Тема обновлена.", reply_markup=subjects_kb()); await state.clear()

@dp.callback_query(F.data == "subjects:delete")
async def subjects_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Введите ID темы для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectDeleteFSM.subject_id); await safe_cq_answer(c)

@dp.message(SubjectDeleteFSM.subject_id)
async def subjects_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if not m.text.strip().isdigit():
        await m.answer("Нужен числовой ID.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open"))); return
    sid = int(m.text.strip())
    with SessionLocal() as s:
        s.query(Subject).filter_by(user_id=m.from_user.id, id=sid).delete(); s.commit()
    await m.answer("Удалено.", reply_markup=subjects_kb()); await state.clear()

@dp.callback_query(F.data == "subjects:clear")
async def subjects_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await c.message.edit_text("Подтвердите очистку тем: ДА", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectClearFSM.confirm); await safe_cq_answer(c)

@dp.message(SubjectClearFSM.confirm)
async def subjects_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    if m.text.strip().upper() == "ДА":
        with SessionLocal() as s: s.query(Subject).filter_by(user_id=m.from_user.id).delete(); s.commit()
        await m.answer("Все темы удалены.", reply_markup=subjects_kb())
    else:
        await m.answer("Отменено.", reply_markup=subjects_kb())
    await state.clear()

# ====== CHECK NICKS (XLSX only) ======
def after_xlsx_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📘 Выполнить проверку email", callback_data="check:verify_emails")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ])

def after_verify_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Запустить сендинг", callback_data="send:start")],
        [InlineKeyboardButton(text="📊 Статус", callback_data="send:status"),
         InlineKeyboardButton(text="🛑 Стоп", callback_data="send:stop")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ])

@dp.message(F.text.in_({"📖 Проверка ников", "Проверка ников"}))
async def btn_check(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.set_state(CheckNicksFSM.file)
    await m.answer("Пришлите .xlsx файл для проверки.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("ui:hide")))

@dp.message(Command("check"))
async def cmd_check(m: types.Message, state: FSMContext):
    await btn_check(m, state)

@dp.message(F.text.regexp(r"(?i)проверка\s*ников"))
async def btn_check_regex(m: types.Message, state: FSMContext):
    await btn_check(m, state)

def pick_columns_via_smtp25(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    """
    Строгое определение колонок как в smtp25:
    - сперва smtp25.detect_columns
    - безопасный фолбэк по точным названиям
    """
    seller_col: Optional[str] = None
    title_col: Optional[str] = None

    try:
        col_map = smtp25.detect_columns(df) or {}
        seller_col = col_map.get("seller_nick")
        title_col = col_map.get("title")
    except Exception:
        col_map = {}

    if not seller_col:
        for cand in ("seller_nick", "Имя продавца"):
            if cand in df.columns:
                seller_col = cand
                break
    if not title_col:
        for cand in ("title", "Название", "Название товара"):
            if cand in df.columns:
                title_col = cand
                break

    rename = {}
    if seller_col: rename[seller_col] = "seller_nick"
    if title_col: rename[title_col] = "title"
    return df.rename(columns=rename).copy(), seller_col, title_col

@dp.message(CheckNicksFSM.file, F.document)
async def on_xlsx_received(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    filename = (m.document.file_name or "").lower()
    if not filename.endswith(".xlsx"):
        await m.answer("Ожидается .xlsx файл."); return
    buf = BytesIO(); await bot.download(m.document, destination=buf)
    LAST_XLSX_PER_CHAT[m.chat.id] = buf.getvalue()

    try:
        df = pd.read_excel(BytesIO(LAST_XLSX_PER_CHAT[m.chat.id]))
        df, seller_col, _ = pick_columns_via_smtp25(df)

        if not seller_col:
            cols = ", ".join([str(c) for c in df.columns])
            await m.answer(
                "Не удалось определить колонку с никами продавцов.\n"
                "Переименуйте столбец в один из вариантов: «Имя продавца» или «seller_nick».\n\n"
                f"Найденные столбцы: {cols}"
            )
            await state.clear()
            return

        # Для прозрачности покажем выбранную колонку
        await m.answer(f"Колонка ников: “{seller_col}”")

        # Подготовим окружение smtp25 (домены/прокси/аккаунты/шаблоны/blacklist)
        prepare_smtp25_from_db(m.from_user.id)

        # Предпросмотр баз с правилами smtp25 (без сетевой проверки)
        bases = preview_bases_from_df(df)
        BASES_PER_CHAT[m.chat.id] = bases

        if bases:
            for i in range(0, len(bases), 50):
                await m.answer("\n".join(bases[i:i+50]))
            await m.answer("Выполнено успешно✅", reply_markup=after_xlsx_kb())
        else:
            await m.answer(
                "Не удалось распознать ни одного валидного ника по правилам фильтрации.\n"
                "Проверьте содержимое столбца (длина, лишние символы)."
            )
    except Exception as e:
        await m.answer(f"Ошибка обработки XLSX: {e}")
    finally:
        await state.clear()

@dp.message(CheckNicksFSM.file)
async def ignore_non_xlsx(m: types.Message):
    pass

def verify_emails_from_df_for_user(user_id: int, df: pd.DataFrame) -> List[Dict[str, Any]]:
    # Подготовка окружения smtp25 + домены из БД
    domains = prepare_smtp25_from_db(user_id)

    # Грузим blacklist только один раз за запуск
    _ensure_blacklist_loaded_once()

    # На новый прогон чистим только кэш обработанных ников
    try:
        smtp25.PROCESSED_NICKS_CACHE.clear()
    except Exception:
        smtp25.PROCESSED_NICKS_CACHE = set()

    df2, _, _ = pick_columns_via_smtp25(df)
    keep = [c for c in ["seller_nick", "title"] if c in df2.columns]
    df2 = df2[keep].copy()

    results: List[Dict[str, Any]] = []
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=getattr(smtp25, "THREADS", 10)) as executor:
        futures = {executor.submit(smtp25.process_row, row, domains): idx for idx, row in df2.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                res = future.result()
                if res:
                    email_addr, seller_name = res
                    title = df2.at[idx, "title"] if "title" in df2.columns else ""
                    results.append({"email": email_addr, "seller_name": seller_name, "title": title})
            except Exception:
                continue
    return results

@dp.callback_query(F.data == "check:verify_emails")
async def verify_emails_btn(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    chat_id = c.message.chat.id
    xls = LAST_XLSX_PER_CHAT.get(chat_id)
    if not xls:
        await c.answer("Сначала загрузите XLSX через «Проверка ников».", show_alert=True); return
    status_msg = await bot.send_message(chat_id, "Проверка email выполняется…")
    try:
        df = pd.read_excel(BytesIO(xls))
        results = await asyncio.to_thread(verify_emails_from_df_for_user, c.from_user.id, df)
        VERIFIED_ROWS_PER_CHAT[chat_id] = results
        emails = [r["email"] for r in results]
        if emails:
            for i in range(0, len(emails), 50):
                await bot.send_message(chat_id, "\n".join(emails[i:i+50]))
        await delete_message_safe(status_msg)
        await bot.send_message(chat_id, "Выполнено успешно✅", reply_markup=after_verify_kb())
    except Exception as e:
        await delete_message_safe(status_msg)
        await bot.send_message(chat_id, f"Ошибка проверки email: {e}")

# ====== SEND: batch sending with logs ======
async def _quick_check_send_proxies(uid: int) -> str:
    prepare_smtp25_from_db(uid)
    if not smtp25.SEND_PROXY_LIST:
        return "Нет send‑прокси."
    bad: List[str] = []
    for p in smtp25.SEND_PROXY_LIST:
        try:
            s = socks.socksocket()
            s.set_proxy(socks.SOCKS5, p["host"], int(p["port"]), True, p.get("user"), p.get("password"))
            s.settimeout(5)
            s.connect(("smtp.gmail.com", 587))
            s.close()
        except Exception:
            bad.append(f"{p['host']}:{p['port']} (ID={p.get('id','?')})")
    if bad:
        return "Неработающие прокси:\n" + "\n".join(bad)
    return "✅ Все прокси валидны"

def _render_message(subject: str, template: str, seller_name: str, title: str) -> Tuple[str, str]:
    subj_in = subject or smtp25.get_random_subject()
    tmpl_in = template or smtp25.get_random_template()

    # Поддержка {SELLER}/{ITEM}/{OFFER} и плоских SELLER/OFFER
    def repl(txt: str) -> str:
        if seller_name:
            txt = txt.replace("{SELLER}", seller_name).replace("SELLER", seller_name)
        else:
            # если нет имени — просто убираем плейсхолдер
            txt = txt.replace("{SELLER}", "").replace("SELLER", "")
        return (txt
                .replace("{ITEM}", title or "")
                .replace("{OFFER}", title or "")
                .replace("OFFER", title or ""))

    return repl(subj_in).strip(), repl(tmpl_in)

async def _send_one(uid: int, to_email: str, subject: str, body: str) -> bool:
    # Используем аккаунты/прокси/SMTP из smtp25, но шаблоны — из БОТА (см. _render_message)
    prepare_smtp25_from_db(uid)
    acc = smtp25.get_random_account()
    proxy = smtp25.get_next_proxy("send")
    if not acc or not proxy:
        await bot.send_message(uid, "Нет аккаунтов или send‑прокси. Добавьте их в Настройках.")
        return False

    def _sync() -> bool:
        try:
            smtp = smtp25.initialize_smtp(acc, proxy)
            if not smtp: return False
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart()
            msg['From'] = f"{acc.get('name') or acc['email']} <{acc['email']}>"
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            smtp.sendmail(acc["email"], to_email, msg.as_string())
            try:
                smtp.quit()
            except Exception:
                pass
            return True
        except Exception:
            return False

    ok = await asyncio.to_thread(_sync)
    if not ok:
        await bot.send_message(uid, f"Ошибка подключения email с номером {acc.get('id','?')} к прокси с номером {proxy.get('id','?')}, проверьте их")
    return ok

async def send_loop(uid: int, chat_id: int):
    SEND_STATUS[uid] = {"running": True, "sent": 0, "failed": 0, "total": 0, "cancel": False, "last_err": None}
    results = VERIFIED_ROWS_PER_CHAT.get(chat_id, [])
    SEND_STATUS[uid]["total"] = len(results)

    proxy_report = await _quick_check_send_proxies(uid)
    await bot.send_message(chat_id, proxy_report)

    vmin = int(get_setting(uid, "send_delay_min", str(smtp25.MIN_SEND_DELAY)))
    vmax = int(get_setting(uid, "send_delay_max", str(smtp25.MAX_SEND_DELAY)))

    for r in results:
        if SEND_STATUS[uid].get("cancel"):
            break
        email = r["email"]; seller_name = r.get("seller_name", ""); title = r.get("title", "")
        subject, body = _render_message(smtp25.get_random_subject(), smtp25.get_random_template(), seller_name or "", title or "")

        ok = await _send_one(uid, email, subject, body)
        if ok:
            SEND_STATUS[uid]["sent"] += 1
            await log_send_ok(chat_id, subject, body, email)
        else:
            SEND_STATUS[uid]["failed"] += 1
            await bot.send_message(chat_id, f"Не удалось отправить пользователю {email}")
        await asyncio.sleep(random.uniform(vmin, vmax))

    SEND_STATUS[uid]["running"] = False
    if SEND_STATUS[uid].get("cancel"):
        await bot.send_message(chat_id, "Сендинг остановлен ⏹")
    else:
        await bot.send_message(chat_id, "Сендинг завершён ✅")

@dp.callback_query(F.data == "send:start")
async def send_start_cb(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    uid = c.from_user.id; chat_id = c.message.chat.id
    if chat_id not in VERIFIED_ROWS_PER_CHAT or not VERIFIED_ROWS_PER_CHAT[chat_id]:
        await c.answer("Сначала выполните проверку email.", show_alert=True); return
    if uid in SEND_TASKS and not SEND_TASKS[uid].done():
        await c.answer("Сендинг уже запущен.", show_alert=True); return
    SEND_STATUS[uid] = {"running": True, "sent": 0, "failed": 0, "total": len(VERIFIED_ROWS_PER_CHAT[chat_id]), "cancel": False}
    SEND_TASKS[uid] = asyncio.create_task(send_loop(uid, chat_id))
    await safe_cq_answer(c)
    await c.message.answer("Сендинг запущен 🚀")

@dp.callback_query(F.data == "send:status")
async def send_status_cb(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    st = SEND_STATUS.get(c.from_user.id)
    if not st:
        await c.answer("Сендинг не запускался.", show_alert=True); return
    await c.message.answer(f"Статус: {'идёт' if st.get('running') else 'остановлен'}\nОтправлено: {st.get('sent',0)}\nНе отправлено: {st.get('failed',0)}\nВсего: {st.get('total',0)}")
    await safe_cq_answer(c)

@dp.callback_query(F.data == "send:stop")
async def send_stop_cb(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    uid = c.from_user.id
    t = SEND_TASKS.get(uid)
    if t and not t.done():
        SEND_STATUS[uid]["cancel"] = True
        await c.answer("Останавливаю…")
    else:
        await c.answer("Сендинг не запущен.", show_alert=True)

# ====== ONE‑OFF SEND ======
def onesend_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Отмена", callback_data="onesend:cancel")]
    ])

@dp.message(F.text == "✉️ Отправить email")
async def onesend_entry(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.set_state(SingleSendFSM.to)
    await m.answer("Введите email получателя✍️", reply_markup=onesend_kb())

@dp.message(Command("send"))
async def cmd_send(m: types.Message, state: FSMContext):
    await onesend_entry(m, state)

@dp.message(SingleSendFSM.to)
async def onesend_got_to(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    to = m.text.strip()
    if "@" not in to:
        await m.answer("Некорректный email. Повторите."); return
    await state.update_data(to=to)
    await state.set_state(SingleSendFSM.body)
    await m.answer("Введите текст письма✍️", reply_markup=onesend_kb())

@dp.message(SingleSendFSM.body)
async def onesend_got_text(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    subject, body = _render_message(smtp25.get_random_subject(), m.text or smtp25.get_random_template(), "", "")
    ok = await _send_one(m.from_user.id, data.get("to"), subject, body)
    await m.answer("Отправлено ✅" if ok else "Ошибка отправки ❌")
    await state.clear()

@dp.callback_query(F.data == "onesend:cancel")
async def onesend_cancel(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Отменено.")
    await safe_cq_answer(c)
    
async def send_email_via_account(uid: int, acc_id: int, to_email: str, subject: str, body: str, html: bool = False, photo_bytes: Optional[bytes] = None, photo_name: Optional[str] = None) -> bool:
    prepare_smtp25_from_db(uid)
    with SessionLocal() as s:
        acc = s.query(Account).filter_by(user_id=uid, id=acc_id).first()
    if not acc:
        await bot.send_message(uid, "Аккаунт не найден."); return False
    proxy = smtp25.get_next_proxy("send")
    if not proxy:
        await bot.send_message(uid, "Нет send‑прокси."); return False

    def _sync() -> bool:
        try:
            smtp = smtp25.initialize_smtp({"email": acc.email, "password": acc.password, "name": acc.display_name}, proxy)
            if not smtp:
                return False
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            msg = MIMEMultipart()
            msg['From'] = f"{acc.display_name or acc.email} <{acc.email}>"
            msg['To'] = to_email
            msg['Subject'] = subject or "Re:"
            subtype = 'html' if html else 'plain'
            msg.attach(MIMEText(body or "", subtype))
            if photo_bytes:
                from email.mime.image import MIMEImage
                img = MIMEImage(photo_bytes, name=photo_name or "image.jpg")
                img.add_header('Content-Disposition', 'attachment', filename=photo_name or "image.jpg")
                msg.attach(img)
            smtp.sendmail(acc.email, to_email, msg.as_string())
            try: smtp.quit()
            except Exception: pass
            return True
        except Exception:
            return False

    return await asyncio.to_thread(_sync)
        
@dp.callback_query(F.data == "reply:msg")
async def reply_msg_cb(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    # текущий message_id = тот самый, где была кнопка
    tg_mid = c.message.message_id
    with SessionLocal() as s:
        row = (
            s.query(IncomingMessage)
            .filter_by(user_id=c.from_user.id, tg_message_id=tg_mid)
            .order_by(IncomingMessage.id.desc())
            .first()
        )
    if not row:
        await c.answer("Не нашёл данные письма", show_alert=True)
        return

    await state.set_state(ReplyFSM.compose)
    await state.update_data(acc_id=int(row.account_id), to=row.from_email, subject=f"Re: {row.subject or ''}")
    await c.message.answer("Введите сообщение✍️", reply_markup=reply_actions_kb())
    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("reply:start:"))
async def reply_start_cb(c: types.CallbackQuery, state: FSMContext):
    # Легаси-кнопка (не используется в новых сообщениях, но оставим совместимость)
    if not await ensure_approved(c): return
    _, _, acc_id, to_email = c.data.split(":", 3)
    await state.set_state(ReplyFSM.compose)
    await state.update_data(acc_id=int(acc_id), to=to_email)
    await c.message.answer("Введите сообщение✍️", reply_markup=reply_actions_kb())
    await safe_cq_answer(c)

@dp.callback_query(F.data == "reply:use_preset")
async def reply_use_preset(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await safe_edit_message(c.message, "Выберите пресет:", reply_markup=presets_inline_kb(c.from_user.id, back_cb="reply:back"), parse_mode=None)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "reply:use_html")
async def reply_use_html(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.set_state(ReplyFSM.html)
    await safe_edit_message(c.message, "Отправьте HTML✍️", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]]), parse_mode=None)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "reply:back")
async def reply_back(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.set_state(ReplyFSM.compose)
    await safe_edit_message(c.message, "Введите сообщение✍️", reply_markup=reply_actions_kb(), parse_mode=None)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "reply:cancel")
async def reply_cancel(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_message(c.message, "Отменено.", reply_markup=None, parse_mode=None)
    await safe_cq_answer(c)  # было: await safe_cq_answer(c)

@dp.message(ReplyFSM.compose)
async def reply_compose_text_or_photo(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    photo_bytes = None; photo_name = None
    body = ""
    if m.photo:
        ph = m.photo[-1]
        file = await bot.get_file(ph.file_id)
        buf = BytesIO()
        await bot.download_file(file.file_path, buf)
        photo_bytes = buf.getvalue()
        photo_name = "image.jpg"
        body = m.caption or ""
    else:
        body = m.text or ""
    subj = data.get("subject") or "Re:"
    ok = await send_email_via_account(m.from_user.id, int(data["acc_id"]), data["to"], subj, body, html=False, photo_bytes=photo_bytes, photo_name=photo_name)
    if ok:
        await log_send_ok(m.chat.id, subj, body, data["to"])
    await m.answer("Отправлено ✅" if ok else "Ошибка отправки ❌")
    await state.clear()

@dp.message(ReplyFSM.html)
async def reply_compose_html(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    subj = data.get("subject") or "Re:"
    body = m.text or ""
    ok = await send_email_via_account(m.from_user.id, int(data["acc_id"]), data["to"], subj, body, html=True)
    if ok:
        await log_send_ok(m.chat.id, subj, body, data["to"])
    await m.answer("Отправлено ✅" if ok else "Ошибка отправки ❌")
    await state.clear()

# ====== QUICK ADD ======
def quickadd_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Одно имя", callback_data="quickadd:one"),
         InlineKeyboardButton(text="1️⃣2️⃣3️⃣4️⃣ Разные имена", callback_data="quickadd:many")],
        *nav_row("ui:hide")
    ])

def quickadd_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="quickadd:cancel")]])

@dp.message(F.text == "➕ Быстрое добавление")
async def quickadd_start(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.set_state(QuickAddFSM.mode)
    await m.answer("Выберите опцию:", reply_markup=quickadd_menu_kb())

@dp.message(Command("quickadd"))
async def cmd_quickadd(m: types.Message, state: FSMContext):
    await quickadd_start(m, state)

@dp.callback_query(F.data == "quickadd:one")
async def quickadd_one(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.update_data(mode="one")
    await c.message.edit_text("Введите отображаемое имя и фамилию. Например: Jessy Jackson ✍️",
        reply_markup=quickadd_cancel_kb())
    await state.set_state(QuickAddFSM.name); await safe_cq_answer(c)

@dp.callback_query(F.data == "quickadd:many")
async def quickadd_many(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.update_data(mode="many")
    await c.message.edit_text("Отправьте данные текстом:\n\nemail1:password1:name1\nemail2:password2:name2",
        reply_markup=quickadd_cancel_kb())
    await state.set_state(QuickAddFSM.lines); await safe_cq_answer(c)

@dp.callback_query(F.data == "quickadd:cancel")
async def quickadd_cancel(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Отменено.")
    await safe_cq_answer(c)

@dp.message(QuickAddFSM.name)
async def quickadd_got_name(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await state.update_data(name=m.text.strip())
    await m.answer("Теперь отправьте строки вида:\nemail:password", reply_markup=quickadd_cancel_kb())
    await state.set_state(QuickAddFSM.lines)

def parse_lines_one(text: str) -> List[Tuple[str, str]]:
    rows = []
    for ln in [l.strip() for l in text.splitlines() if l.strip()]:
        parts = ln.split(":", 1)
        if len(parts) != 2:
            continue
        rows.append((parts[0].strip(), parts[1].strip()))
    return rows

def parse_lines_many(text: str) -> List[Tuple[str, str, str]]:
    rows = []
    for ln in [l.strip() for l in text.splitlines() if l.strip()]:
        parts = ln.split(":", 2)
        if len(parts) != 3:
            continue
        rows.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
    return rows

@dp.message(QuickAddFSM.lines)
async def quickadd_lines_text(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    mode = data.get("mode")
    added = 0; total = 0
    if mode == "one":
        name = data.get("name", "") or ""
        pairs = parse_lines_one(m.text)
        total = len(pairs)
        with SessionLocal() as s:
            for email_addr, password in pairs:
                try:
                    add_account(s, m.from_user.id, name or email_addr.split("@")[0], email_addr, password, auto_bind_proxy=True)
                    added += 1
                    # Старт потока чтения
                    await _ensure_imap_started_for_user(m.from_user.id, m.chat.id, started_for_email=email_addr)
                except Exception:
                    pass
    else:
        triples = parse_lines_many(m.text)
        total = len(triples)
        with SessionLocal() as s:
            for email_addr, password, name in triples:
                try:
                    add_account(s, m.from_user.id, name or email_addr.split("@")[0], email_addr, password, auto_bind_proxy=True)
                    added += 1
                    await _ensure_imap_started_for_user(m.from_user.id, m.chat.id, started_for_email=email_addr)
                except Exception:
                    pass
    await m.answer(f"Добавлено аккаунтов: {added} из {total}")
    await state.clear()

# ====== FALLBACK кнопки (текст) ======
@dp.message(F.text.regexp(r"(?i)\bпроверка\s+ников\b"))
async def fallback_btn_check(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await btn_check(m, state)

@dp.message(F.text == "🧾 HTML-шаблоны")
async def fallback_templates(m: types.Message):
    if not await ensure_approved(m): return
    await m.answer("HTML‑шаблоны — в разработке.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("settings:back")))

# ====== IMAP helpers (через send‑прокси) ======
def resolve_imap_host(email_addr: str) -> str:
    domain = (email_addr.split("@", 1)[1] if "@" in email_addr else "").lower()
    if domain in IMAP_HOST_MAP:
        return IMAP_HOST_MAP[domain]
    return f"imap.{domain}" if domain else "imap.gmail.com"

def _decode_header(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        return str(make_header(decode_header(s)))
    except Exception:
        return s

def _extract_body(msg) -> str:
    text_parts = []
    html_parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            if "attachment" in disp.lower():
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            except Exception:
                continue
            if ctype == "text/plain":
                text_parts.append(text)
            elif ctype == "text/html":
                html_parts.append(re.sub(r"<[^>]+>", " ", text))
    else:
        try:
            payload = msg.get_payload(decode=True) or b""
            text = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
            if msg.get_content_type() == "text/plain":
                text_parts.append(text)
            else:
                html_parts.append(re.sub(r"<[^>]+>", " ", text))
        except Exception:
            pass
    body = "\n".join(text_parts) if text_parts else "\n".join(html_parts)
    body = re.sub(r"\s+\n", "\n", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    return body[:3500]  # запас к лимиту Telegram
    
@contextmanager
def socks5_socket(host: str, port: int, user: str | None = None, password: str | None = None):
    """
    Временно перенаправляет ВСЕ TCP через SOCKS5, затем восстанавливает обычный socket.
    Нужен, потому что imaplib не умеет принимать кастомный сокет.
    """
    original_socket_cls = socket.socket
    try:
        socks.setdefaultproxy(socks.SOCKS5, host, int(port), True, user, password)
        socket.socket = socks.socksocket
        yield
    finally:
        socket.socket = original_socket_cls

def _set_socks_proxy_for_send() -> Optional[dict]:
    """
    Устанавливает SOCKS5 (send‑прокси) как глобальный socket для следующего подключения.
    Возвращает выбранный прокси или None.
    """
    proxy = smtp25.get_next_proxy("send")
    if not proxy:
        return None
    try:
        socks.setdefaultproxy(
            socks.SOCKS5,
            proxy["host"],
            int(proxy["port"]),
            True,
            proxy.get("user"),
            proxy.get("password")
        )
        socket.socket = socks.socksocket
        return proxy
    except Exception:
        return None

async def fetch_and_post_new_mails(user_id: int, acc: Account, chat_id: int) -> int:
    host = resolve_imap_host(acc.email)
    new_count = 0
    try:
        prepare_smtp25_from_db(user_id)
        proxy = smtp25.get_next_proxy("send")
        if not proxy:
            raise RuntimeError("Нет send‑прокси для IMAP")

        # ВАЖНО: локально подменяем socket только на время IMAP
        with socks5_socket(proxy["host"], proxy["port"], proxy.get("user"), proxy.get("password")):
            imap = imaplib.IMAP4_SSL(host, IMAP_PORT_SSL, timeout=IMAP_TIMEOUT)
            imap.login(acc.email, acc.password)
            imap.select("INBOX")
            typ, data = imap.uid("search", None, "UNSEEN")
            uid_bytes = data[0] or b""
            uids = [u for u in uid_bytes.split() if u]

            with SessionLocal() as s:
                existing = {x.uid for x in s.query(IncomingMessage.uid).filter_by(account_id=acc.id).all()}
            new_uids = [u for u in uids if u.decode() not in existing]

            for u in new_uids:
                typ, msg_data = imap.uid("fetch", u, "(RFC822)")
                if typ != "OK" or not msg_data or not isinstance(msg_data[0], tuple):
                    continue
                msg = message_from_bytes(msg_data[0][1])
                from_raw = msg.get("From", "")
                from_name, from_email = parseaddr(from_raw)
                subject = _decode_header(msg.get("Subject"))
                body = _extract_body(msg)

                text = (
                    f"⚡️ Получено сообщение на {acc.email} от {from_email}\n"
                    f"({from_name or ''} <{from_email}>)\n\n"
                    f"Тема:\n{subject}\n\n"
                    f"Текст:\n{body}"
                )
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✉️ Ответить", callback_data="reply:msg")]
                ])
                tg_msg = await bot.send_message(chat_id, text, parse_mode=None, reply_markup=kb)

                with SessionLocal() as s:
                    s.add(IncomingMessage(
                        user_id=user_id,
                        account_id=acc.id,
                        uid=u.decode(),
                        from_name=from_name or "",
                        from_email=from_email or "",
                        subject=subject,
                        body=body,
                        tg_message_id=tg_msg.message_id
                    ))
                    s.commit()

                try:
                    await bot.pin_chat_message(chat_id, tg_msg.message_id, disable_notification=True)
                except Exception:
                    pass

                new_count += 1

            try:
                imap.logout()
            except Exception:
                pass

        IMAP_STATUS.setdefault(user_id, {}).setdefault("accounts", {}).setdefault(acc.email, {})
        IMAP_STATUS[user_id]["accounts"][acc.email].update(
            {"active": True, "last_ok": f"+{new_count} new" if new_count else "нет новых", "last_err": None}
        )
        return new_count
    except Exception as e:
        IMAP_STATUS.setdefault(user_id, {}).setdefault("accounts", {}).setdefault(acc.email, {})
        IMAP_STATUS[user_id]["accounts"][acc.email].update({"active": False, "last_err": f"{acc.email}: {e}"})
        return 0

# ====== IMAP: /read /stop /status ======
async def imap_loop(user_id: int, chat_id: int):
    IMAP_STATUS[user_id] = {"running": True, "last_ok": None, "last_err": None, "accounts": IMAP_STATUS.get(user_id, {}).get("accounts", {})}
    while True:
        try:
            with SessionLocal() as s:
                accounts = s.query(Account).filter_by(user_id=user_id, active=True).all()
            total_new = 0
            for acc in accounts:
                cnt = await fetch_and_post_new_mails(user_id, acc, chat_id)
                total_new += cnt
            IMAP_STATUS[user_id]["last_ok"] = f"+{total_new} new" if total_new else "нет новых"
            await asyncio.sleep(READ_INTERVAL)
        except asyncio.CancelledError:
            IMAP_STATUS[user_id]["running"] = False
            raise
        except Exception as e:
            IMAP_STATUS[user_id]["last_err"] = str(e)
            await asyncio.sleep(READ_INTERVAL)

def _render_accounts_status(uid: int) -> str:
    st = IMAP_STATUS.get(uid) or {}
    accs = st.get("accounts", {}) or {}
    lines = []
    if not accs:
        lines.append("Аккаунтов не найдено.")
    else:
        for email, s in accs.items():
            mark = "✅" if s.get("active") else "❌"
            last_ok = s.get("last_ok") or ""
            last_err = s.get("last_err")
            if last_err:
                lines.append(f"{email} неактивен {mark}\n  Ошибка: {last_err}")
            else:
                lines.append(f"{email} активен {mark}\n  {last_ok}")
    return "\n".join(lines)

@dp.message(Command("read"))
async def cmd_read(m: types.Message):
    if not await ensure_approved(m): 
        return
    uid = m.from_user.id
    chat_id = m.chat.id

    # Быстрый ACK, чтобы видно было, что команда принята
    await m.answer("⏳ Запускаю чтение IMAP…", parse_mode=None)

    # Стартуем/перезапускаем фоновую задачу чтения
    try:
        await _ensure_imap_started_for_user(uid, chat_id)
    except Exception as e:
        await m.answer(f"❌ Не удалось запустить чтение: {e}", parse_mode=None)
        return

    # Покажем список активных аккаунтов
    try:
        with SessionLocal() as s:
            emails = [a.email for a in s.query(Account).filter_by(user_id=uid, active=True).order_by(Account.id.asc()).all()]
        if emails:
            await m.answer("Поток чтения запущен для аккаунтов:\n" + "\n".join(emails), parse_mode=None)
        else:
            await m.answer("Активных аккаунтов не найдено.", parse_mode=None)
    except Exception as e:
        await m.answer(f"⚠️ Ошибка выборки аккаунтов: {e}", parse_mode=None)

    # Дадим циклу секунду и выведем краткий статус аккаунтов
    await asyncio.sleep(1)
    status_text = _render_accounts_status(uid) or "Статус пока пуст."
    await m.answer(status_text, parse_mode=None)


@dp.message(Command("status"))
async def cmd_status(m: types.Message):
    if not await ensure_approved(m): 
        return
    uid = m.from_user.id

    st = IMAP_STATUS.get(uid)
    if not st:
        # Нет записи — покажем инфо по задаче, чтобы понять жив ли цикл
        task = IMAP_TASKS.get(uid)
        t_state = "нет" if not task else ("выполнена" if task.done() else "идёт")
        base = [f"IMAP статус: нет записи в кэше", f"Задача: {t_state}", ""]
        accs = _render_accounts_status(uid)
        await m.answer("\n".join([*base, accs] if accs else base), parse_mode=None)
        return

    running = st.get("running", False)
    last_ok = st.get("last_ok", "—")
    last_err = st.get("last_err", "—")
    txt = [
        f"IMAP статус: {'запущен' if running else 'остановлен'}",
        f"Последний OK: {last_ok}",
        f"Последняя ошибка: {last_err}",
        "",
        _render_accounts_status(uid)
    ]
    await m.answer("\n".join([t for t in txt if t is not None]), parse_mode=None)

@dp.message(Command("stop"))
async def cmd_stop(m: types.Message):
    uid = m.from_user.id
    t = IMAP_TASKS.get(uid)
    if t and not t.done():
        t.cancel()
        try:
            await t
        except Exception:
            pass
        await m.answer("IMAP чтение остановлено ⏹")
    else:
        await m.answer("IMAP не запущен.")

# ====== SEND commands duplicates ======
@dp.message(Command("sendstart"))
async def cmd_sendstart(m: types.Message):
    await send_start_cb(types.CallbackQuery(id="0", from_user=m.from_user, chat_instance="", message=m, data="send:start"))

@dp.message(Command("sendstatus"))
async def cmd_sendstatus(m: types.Message):
    st = SEND_STATUS.get(m.from_user.id)
    if not st:
        await m.answer("Сендинг не запускался."); return
    await m.answer(f"Статус: {'идёт' if st.get('running') else 'остановлен'}\nОтправлено: {st.get('sent',0)}\nНе отправлено: {st.get('failed',0)}\nВсего: {st.get('total',0)}")

@dp.message(Command("sendstop"))
async def cmd_sendstop(m: types.Message):
    uid = m.from_user.id
    t = SEND_TASKS.get(uid)
    if t and not t.done():
        SEND_STATUS[uid]["cancel"] = True
        await m.answer("Останавливаю…")
    else:
        await m.answer("Сендинг не запущен.")

# ====== MAIN ======
async def set_bot_commands(bot: Bot):
    commands = [
        BotCommand(command="start", description="Начать работу"),
        BotCommand(command="settings", description="Настройки"),
        BotCommand(command="check", description="Проверка ников (XLSX)"),
        BotCommand(command="send", description="Отправить email"),
        BotCommand(command="quickadd", description="Быстрое добавление"),
        BotCommand(command="sendstart", description="Сендинг: запустить"),
        BotCommand(command="sendstatus", description="Сендинг: статус"),
        BotCommand(command="sendstop", description="Сендинг: остановить"),
        BotCommand(command="read", description="IMAP: запустить чтение"),
        BotCommand(command="status", description="IMAP: статус"),
        BotCommand(command="stop", description="IMAP: остановить чтение"),
    ]
    await bot.set_my_commands(commands)

async def main() -> None:
    application = _build_application()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            SELECT_ACTION: [CallbackQueryHandler(button_handler)],
            EDIT_FILE: [
                MessageHandler(filters.TEXT | filters.Document.ALL, edit_file_content),
                CommandHandler('cancel', cancel)
            ],
            INPUT_PARAMS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_smtp_sender),
                CommandHandler('cancel', cancel)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    # Лонг‑поллинг: timeout должен быть меньше read_timeout выше
    application.run_polling(timeout=60, drop_pending_updates=False)