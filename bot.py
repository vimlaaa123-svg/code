"""
Big Basket Automated Distribution System
====================================================
Version: 3.0.0 (Enterprise Build - Big Basket Edition)
Description: A highly robust, asynchronous Telegram Bot built with aiogram 3.x.
Features include automated inventory handling, duplicate detection, advanced 
admin telemetry, .txt bulk imports, anti-spam middleware, seamless UI, 
strict session-based security challenges, and anti-leave penalties.

Author: Automated Systems
Date: 2026-03-04
"""

import asyncio
import logging
import sqlite3
import random
import string
import time
import os
import sys
import json
import hmac
import hashlib
import secrets
import psutil
from datetime import datetime, timedelta
from typing import Union, Dict, Any, List, Optional, Tuple
from urllib.parse import urlencode

# --- HIGH PERFORMANCE MODULES ---
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F, types, BaseMiddleware
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, IS_NOT_MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    FSInputFile,
    Message,
    WebAppInfo
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
API_TOKEN = "8653020435:AAHhtpiJa3h-TA2Lm8u8Ww2ALPm6D8A6Px8"
SUPER_ADMIN_ID = 7865072774
SUPPORT_USER = "@TECHXDEALSSUPPORTBOT"
UPDATES_CHANNEL_LINK = "https://t.me/UpdatesOnSupport11"

# PATHS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "production_core_bb.db")
PROOF_FOLDER = os.path.join(BASE_DIR, "payment_proofs")
TEMP_FOLDER = os.path.join(BASE_DIR, "temp_downloads")
WEB_VERIFY_HOST = os.getenv("WEB_VERIFY_HOST", "0.0.0.0")
WEB_VERIFY_PORT = int(os.getenv("WEB_VERIFY_PORT", "8080"))
WEB_VERIFY_BASE_URL = os.getenv("WEB_VERIFY_BASE_URL", f"https://bbverify.duckdns.org")
WEB_VERIFY_SECRET = os.getenv("WEB_VERIFY_SECRET", API_TOKEN)

# INITIALIZATION OF DIRECTORIES
for folder in [PROOF_FOLDER, TEMP_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# SYSTEM START TIME (For telemetry)
SYSTEM_START_TIME = time.time()

# DEFAULT MANDATORY CHANNELS
DEFAULT_CHANNELS = []

# ==========================================
# STATE MANAGEMENT & CACHE
# ==========================================
DB_CONN: Optional[aiosqlite.Connection] = None
CONFIG_CACHE: Dict[str, str] = {}
CHANNEL_CACHE: List[Dict[str, str]] = []
ADMIN_CACHE: List[int] = []
BOT_USERNAME: str = ""

class SystemStates(StatesGroup):
    """Finite State Machine definitions for bot flows."""
    captcha_verification = State()
    add_stock_input = State()
    broadcast_message = State()
    channel_add_id = State()
    channel_add_name = State()
    channel_add_link = State()
    manage_points_user = State()
    manage_points_amount = State()
    update_item_price = State()
    promote_admin = State()
    demote_admin = State()
    lookup_user = State()

# ==========================================
# LOGGING SETUP
# ==========================================
class CustomFormatter(logging.Formatter):
    """Custom logging format to maintain a professional terminal output."""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger("SystemCore")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

# ==========================================
# DATABASE ENGINE
# ==========================================
async def get_database_connection() -> aiosqlite.Connection:
    """Returns a singleton aiosqlite connection with WAL mode enabled."""
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = await aiosqlite.connect(DB_NAME)
        await DB_CONN.execute("PRAGMA journal_mode=WAL;")
        await DB_CONN.execute("PRAGMA synchronous=NORMAL;")
        DB_CONN.row_factory = aiosqlite.Row
    return DB_CONN

async def initialize_database() -> None:
    """Creates tables, applies migrations, and seeds default configurations."""
    logger.info(f"Initializing Database Engine at {DB_NAME}")
    db = await get_database_connection()

    # Users Table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            ref_by INTEGER,
            points INTEGER DEFAULT 0,
            total_redeemed INTEGER DEFAULT 0,
            join_date TEXT,
            is_banned INTEGER DEFAULT 0,
            last_msg_id INTEGER,
            is_verified INTEGER DEFAULT 0,
            reward_claimed INTEGER DEFAULT 0,
            verification_status TEXT DEFAULT 'pending'
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS device_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ad_id TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            user_agent TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # Safety migration for existing databases
    try:
        await db.execute("ALTER TABLE users ADD COLUMN reward_claimed INTEGER DEFAULT 0")
    except Exception:
        pass # Column already exists

    try:
        await db.execute("ALTER TABLE users ADD COLUMN verification_status TEXT DEFAULT 'pending'")
    except Exception:
        pass # Column already exists

    await db.execute("CREATE INDEX IF NOT EXISTS idx_device_verifications_ad_ip ON device_verifications(ad_id, ip_address)")

    # Inventory Table (Stock V2)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS stock_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT,
            code TEXT,
            added_date TEXT
        )
    """)

    # Channels Table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            chat_id TEXT,
            invite_link TEXT
        )
    """)

    # Admins Table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY,
            added_by INTEGER,
            added_date TEXT
        )
    """)
    await db.execute("INSERT OR IGNORE INTO admins (user_id, added_by, added_date) VALUES (?, ?, ?)", 
                     (SUPER_ADMIN_ID, 0, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Orders / Transactions Table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            user_id INTEGER,
            type TEXT,
            qty INTEGER,
            total REAL,
            status TEXT,
            codes TEXT,
            date TEXT
        )
    """)

    # Config Table
    await db.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")

    # Seed Default Configuration (Big Basket Edition)
    defaults = {
        "referral_reward": "1",
        "price_BB_CODE": "2",
        "maintenance_mode": "0",
        "allow_dupes": "0",
        "welcome_message": "Welcome to the Big Basket Automated Service.",
    }
    
    for key, value in defaults.items():
        await db.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (key, value))

    # Seed Default Channels if empty
    async with db.execute("SELECT COUNT(*) FROM channels") as cursor:
        count = (await cursor.fetchone())[0]
        if count == 0 and DEFAULT_CHANNELS:
            for c in DEFAULT_CHANNELS:
                await db.execute("INSERT INTO channels (name, chat_id, invite_link) VALUES (?, ?, ?)",
                            (c["name"], str(c["id"]), c["link"]))

    await db.commit()
    await synchronize_cache()

async def synchronize_cache() -> None:
    """Pulls essential configuration from DB into memory for instant access."""
    global CONFIG_CACHE, CHANNEL_CACHE, ADMIN_CACHE
    db = await get_database_connection()

    async with db.execute("SELECT key, value FROM config") as cursor:
        rows = await cursor.fetchall()
        CONFIG_CACHE = {row['key']: row['value'] for row in rows}

    async with db.execute("SELECT chat_id, invite_link, name FROM channels") as cursor:
        rows = await cursor.fetchall()
        CHANNEL_CACHE = [{'chat_id': r['chat_id'], 'link': r['invite_link'], 'name': r['name']} for r in rows]

    async with db.execute("SELECT user_id FROM admins") as cursor:
        rows = await cursor.fetchall()
        ADMIN_CACHE = [r['user_id'] for r in rows]
        
    logger.info("System Cache Synchronized Successfully.")

async def execute_query(query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False) -> Any:
    """A wrapper for database execution to reduce boilerplate code."""
    db = await get_database_connection()
    cursor = await db.execute(query, params)

    if fetchone:
        result = await cursor.fetchone()
        await db.commit()
        return dict(result) if result else None
    elif fetchall:
        results = await cursor.fetchall()
        await db.commit()
        return [dict(r) for r in results]

    await db.commit()
    return None

def fetch_config(key: str, default: Any = "0") -> Union[int, str]:
    """Retrieves a config value from the cache, attempting to cast to int."""
    val = CONFIG_CACHE.get(key, default)
    try:
        return int(val)
    except ValueError:
        return val

async def update_config(key: str, value: Union[str, int]) -> None:
    """Updates a config value in the database and the cache."""
    await execute_query("INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?", (key, str(value), str(value)))
    CONFIG_CACHE[key] = str(value)

def verify_admin(user_id: int) -> bool:
    """Checks if a given user_id exists within the admin cache."""
    return user_id in ADMIN_CACHE

# ==========================================
# MIDDLEWARES
# ==========================================
class ThrottlingMiddleware(BaseMiddleware):
    """
    Prevents users from spamming commands. Limits to 1 command per second.
    Does not apply to admins.
    """
    def __init__(self, limit: float = 1.0):
        self.limit = limit
        self.cache = {}

    async def __call__(self, handler, event: Message, data: Dict[str, Any]) -> Any:
        if not isinstance(event, Message):
            return await handler(event, data)
            
        user_id = event.from_user.id
        if verify_admin(user_id):
            return await handler(event, data)

        current_time = time.time()
        if user_id in self.cache:
            time_passed = current_time - self.cache[user_id]
            if time_passed < self.limit:
                # Silently drop spam
                return None
                
        self.cache[user_id] = current_time
        return await handler(event, data)

# ==========================================
# INITIALIZATION
# ==========================================
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.message.middleware(ThrottlingMiddleware(limit=0.8))

# ==========================================
# UI & UTILITY HELPERS
# ==========================================
def create_divider() -> str:
    """Returns a standardized professional UI divider."""
    return "<b>━━━━━━━━━━━━━━━━━━━━━━━━━━</b>"

def generate_security_captcha(length: int = 7) -> str:
    """Generates a secure alphanumeric captcha code."""
    characters = string.ascii_uppercase + string.digits
    return ''.join(random.choices(characters, k=length))

def create_verification_token(user_id: int, ttl_seconds: int = 1800) -> str:
    """Creates a signed short-lived token for the web verification endpoint."""
    payload = {
        "uid": user_id,
        "exp": int(time.time()) + ttl_seconds,
        "nonce": secrets.token_hex(8),
    }
    payload_raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    signature = hmac.new(WEB_VERIFY_SECRET.encode(), payload_raw.encode(), hashlib.sha256).hexdigest()
    return f"{payload_raw}.{signature}"


def parse_verification_token(token: str) -> Optional[Dict[str, Any]]:
    """Validates and decodes a signed token from the verification webapp."""
    if not token or "." not in token:
        return None

    payload_raw, signature = token.rsplit(".", 1)
    expected = hmac.new(WEB_VERIFY_SECRET.encode(), payload_raw.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None

    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        return None

    if int(payload.get("exp", 0)) < int(time.time()):
        return None

    return payload


def build_verification_url(user_id: int) -> str:
    """Builds a signed mini-web URL for device verification."""
    token = create_verification_token(user_id)
    return f"{WEB_VERIFY_BASE_URL}/verify?{urlencode({'token': token})}"


def render_verify_page(title: str, body: str, token: str = "", allow_submit: bool = True, return_url: str = "") -> str:
    button_html = ""
    if allow_submit and token:
        button_html = f'''
            <form method="post" action="/verify/submit">
              <input type="hidden" name="token" value="{token}">
              <label for="ad_id">Ad-ID</label>
              <input id="ad_id" name="ad_id" type="text" maxlength="128" required placeholder="Enter your ad-id">
              <button type="submit">Verify Device</button>
            </form>
        '''
    elif return_url:
        button_html = f'<a class="btn" href="{return_url}">Return to Bot</a>'

    return f'''
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Device Verification</title>
  <style>
    body {{ font-family: Arial,sans-serif; background:#0f172a; color:#fff; margin:0; }}
    .wrap {{ max-width:420px; margin:8vh auto; padding:24px; background:#1e293b; border-radius:14px; }}
    .loader {{ width:34px; height:34px; border:4px solid #334155; border-top-color:#22d3ee; border-radius:50%; animation:spin 1s linear infinite; margin: 0 auto 18px; }}
    @keyframes spin {{to{{transform:rotate(360deg)}}}}
    h2 {{ margin:6px 0 10px; text-align:center; }}
    p {{ color:#cbd5e1; text-align:center; line-height:1.4; }}
    form {{ display:flex; flex-direction:column; gap:10px; margin-top:16px; }}
    input {{ padding:11px; border-radius:8px; border:1px solid #334155; background:#0f172a; color:#fff; }}
    button,.btn {{ display:inline-block; text-align:center; padding:11px 12px; border:none; border-radius:8px; background:#22c55e; color:#03120a; font-weight:700; cursor:pointer; text-decoration:none; margin-top:16px; width:100%; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="loader"></div>
    <h2>{title}</h2>
    <p>{body}</p>
    {button_html}
  </div>
</body>
</html>
'''

async def web_verify_page(request: web.Request) -> web.Response:
    """Renders the mini verification page with loader + verify button."""
    token = request.query.get("token", "")
    payload = parse_verification_token(token)
    if not payload:
        html = render_verify_page(
            "Session Expired",
            "Verification link is invalid or expired. Return to the bot and run /start again.",
            allow_submit=False,
            return_url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "",
        )
        return web.Response(text=html, content_type="text/html")

    html = render_verify_page(
        "Click On Device Verify Button",
        "Secure loading finished. Enter ad-id and click verify to continue.",
        token=token,
        allow_submit=True,
    )
    return web.Response(text=html, content_type="text/html")


async def web_verify_submit(request: web.Request) -> web.Response:
    """Processes ad-id + IP duplicate checks and stores a secure verification log."""
    form = await request.post()
    token = form.get("token", "")
    ad_id = str(form.get("ad_id", "")).strip()
    payload = parse_verification_token(token)

    if not payload or not ad_id:
        html = render_verify_page(
            "Verification Failed",
            "Missing or invalid verification data. Return to bot and try again.",
            allow_submit=False,
            return_url=f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "",
        )
        return web.Response(text=html, content_type="text/html")

    user_id = int(payload["uid"])
    raw_ip = request.headers.get("X-Forwarded-For", request.remote or "")
    ip_address = raw_ip.split(",")[0].strip() if raw_ip else "unknown"
    user_agent = request.headers.get("User-Agent", "")[:512]

    duplicate = await execute_query(
        "SELECT id FROM device_verifications WHERE ad_id = ? AND ip_address = ? LIMIT 1",
        (ad_id, ip_address),
        fetchone=True,
    )

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return_url = f"https://t.me/{BOT_USERNAME}?start={user_id}" if BOT_USERNAME else ""

    if duplicate:
        await execute_query(
            "INSERT INTO device_verifications (user_id, ad_id, ip_address, user_agent, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, ad_id, ip_address, user_agent, "duplicate", now),
        )
        await execute_query(
            "UPDATE users SET verification_status = ?, reward_claimed = 1 WHERE user_id = ?",
            ("duplicate", user_id),
        )
        html = render_verify_page(
            "Duplicate Device",
            "This ad-id and IP already verified before. You are not going to get referral points.",
            allow_submit=False,
            return_url=return_url,
        )
        return web.Response(text=html, content_type="text/html")

    await execute_query(
        "INSERT INTO device_verifications (user_id, ad_id, ip_address, user_agent, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, ad_id, ip_address, user_agent, "approved", now),
    )
    await execute_query(
        "UPDATE users SET verification_status = ?, is_verified = 1, reward_claimed = 0 WHERE user_id = ?",
        ("approved", user_id),
    )

    html = render_verify_page(
        "Verification Success",
        "You are verified as a new real user. 1 referral point is now eligible after bot checks.",
        allow_submit=False,
        return_url=return_url,
    )
    return web.Response(text=html, content_type="text/html")


async def start_verification_webserver() -> web.AppRunner:
    """Starts the mini web verification server alongside the bot."""
    app = web.Application()
    app.add_routes([
        web.get("/verify", web_verify_page),
        web.post("/verify/submit", web_verify_submit),
    ])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEB_VERIFY_HOST, WEB_VERIFY_PORT)
    await site.start()
    logger.info(f"Verification web server listening on {WEB_VERIFY_HOST}:{WEB_VERIFY_PORT}")
    return runner

async def clear_previous_interface(user_id: int) -> None:
    """Deletes the previous bot interface message to keep chat clean."""
    res = await execute_query("SELECT last_msg_id FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    if res and res['last_msg_id']:
        try:
            await bot.delete_message(chat_id=user_id, message_id=res['last_msg_id'])
        except (TelegramBadRequest, TelegramForbiddenError):
            pass

async def send_smart_interface(user_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Sends a new interface message and tracks its ID for future cleanup."""
    await clear_previous_interface(user_id)
    try:
        msg = await bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup)
        await execute_query("UPDATE users SET last_msg_id = ? WHERE user_id = ?", (msg.message_id, user_id))
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        logger.warning(f"Failed to send interface to {user_id}: {e}")

async def evaluate_channel_subscriptions(user_id: int) -> List[Dict[str, str]]:
    """Evaluates if the user is a member of all required network channels."""
    missing_channels = []
    if not CHANNEL_CACHE:
        return []
        
    for channel in CHANNEL_CACHE:
        try:
            member = await bot.get_chat_member(chat_id=channel['chat_id'], user_id=user_id)
            if member.status in ['left', 'kicked', 'restricted']:
                missing_channels.append(channel)
        except Exception as e:
            logger.error(f"Error checking sub for {user_id} in {channel['chat_id']}: {e}")
            missing_channels.append(channel)
            
    return missing_channels

async def intercept_menu_navigation(message: types.Message, state: FSMContext) -> bool:
    """Intercepts bottom keyboard inputs globally to allow seamless navigation out of states."""
    if not message.text:
        return False
        
    nav_keywords = ["Redeem", "Profile", "Network", "Support", "Vouchers", "Affiliate"]
    if any(keyword in message.text for keyword in nav_keywords):
        await state.clear()
        
        text = message.text
        if "Redeem" in text:
            await display_redeem_menu(message)
        elif "Profile" in text:
            await display_user_profile(message)
        elif "Support" in text:
            await display_support_desk(message)
        elif "Vouchers" in text:
            await display_voucher_history(message)
        elif "Network" in text:
            await display_network_updates(message)
        elif "Affiliate" in text:
            await display_affiliate_program(message)
            
        return True
    return False

# ==========================================
# KEYBOARD BUILDERS
# ==========================================
def build_main_navigation() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🎁 Redeem BB Code"), KeyboardButton(text="🤝 Affiliate Program"))
    builder.row(KeyboardButton(text="👤 Profile"), KeyboardButton(text="🏆 Leaderboard"))
    builder.row(KeyboardButton(text="📞 Support Desk"), KeyboardButton(text="📢 Network Updates"))
    builder.row(KeyboardButton(text="🎟 My Vouchers"))
    return builder.as_markup(resize_keyboard=True)

def build_subscription_gateway(missing_channels: List[Dict[str, str]]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for channel in missing_channels:
        builder.row(InlineKeyboardButton(text=f"👉 Join {channel['name']}", url=channel['link']))
    builder.row(InlineKeyboardButton(text="✅ Verify Authorization", callback_data="verify_authorization"))
    return builder.as_markup()

def build_terms_acceptance() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="✅ Acknowledge & Proceed", callback_data="accept_terms"))
    return builder.as_markup()

def build_return_button(callback_data: str = "return_to_main") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data=callback_data))
    return builder.as_markup()

def build_admin_dashboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📊 Telemetry", callback_data="admin_telemetry"), 
        InlineKeyboardButton(text="📦 Inventory Status", callback_data="admin_inventory")
    )
    builder.row(
        InlineKeyboardButton(text="➕ Add Codes (.txt/Text)", callback_data="admin_add_stock"), 
        InlineKeyboardButton(text="🗑 Flush Inventory", callback_data="admin_flush_stock")
    )
    builder.row(
        InlineKeyboardButton(text="💰 Asset Pricing", callback_data="admin_pricing"), 
        InlineKeyboardButton(text="🎯 Ledger Management", callback_data="admin_ledger")
    )
    builder.row(
        InlineKeyboardButton(text="📢 Global Broadcast", callback_data="admin_broadcast"), 
        InlineKeyboardButton(text="🔗 Gateways (Channels)", callback_data="admin_gateways")
    )
    builder.row(
        InlineKeyboardButton(text="⚙️ System Settings", callback_data="admin_settings"), 
        InlineKeyboardButton(text="🔎 Inspect User", callback_data="admin_inspect")
    )
    builder.row(
        InlineKeyboardButton(text="👤 Promote Admin", callback_data="admin_promote"), 
        InlineKeyboardButton(text="🗑 Demote Admin", callback_data="admin_demote")
    )
    builder.row(InlineKeyboardButton(text="❌ Terminate Session", callback_data="admin_close"))
    return builder.as_markup()

# ==========================================
# STRICT SESSION-BASED VERIFICATION
# ==========================================
@dp.message(CommandStart())
async def command_start_handler(message: types.Message, command: CommandObject, state: FSMContext):
    """Entry point for users. Captcha then web device verification on every /start."""
    await state.clear()
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name

    # Initialize user in DB while preserving old records
    await execute_query("""
        INSERT OR IGNORE INTO users (user_id, username, points, join_date, is_verified, ref_by, verification_status)
        VALUES (?, ?, 0, ?, 0, NULL, 'pending')
    """, (user_id, username, time.strftime("%Y-%m-%d")))
    await execute_query("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))

    user_data = await execute_query("SELECT is_banned, ref_by FROM users WHERE user_id = ?", (user_id,), fetchone=True)

    # 1. Banned check
    if user_data['is_banned']:
        return await message.answer("🚫 <b>Access Denied.</b>\nYour ID has been permanently blacklisted from this service.")

    # 2. Affiliate Link Processing (Only assign if not already assigned)
    if user_data['ref_by'] is None:
        referrer_id = None
        if command.args and command.args.isdigit() and int(command.args) != user_id:
            referrer_id = int(command.args)
            await execute_query("UPDATE users SET ref_by = ? WHERE user_id = ?", (referrer_id, user_id))

    # 3. Maintenance Check
    maint_status = fetch_config("maintenance_mode")
    if maint_status == 1 and not verify_admin(user_id):
        text = "⚠️ <b>SERVICE UNAVAILABLE</b>\n\nSystems are currently undergoing scheduled maintenance. Please stand by."
        return await send_smart_interface(user_id, text)

    # 4. STRICT CHALLENGE: ALWAYS REQUIRE CAPTCHA AND PROFILE VERIFICATION ON START
    await initiate_captcha_protocol(message, state)

async def initiate_captcha_protocol(message: types.Message, state: FSMContext):
    """Generates and sends a mandatory security captcha."""
    security_code = generate_security_captcha()
    await state.update_data(captcha_ans=security_code)
    
    text = (
        f"🛡 <b>SECURITY PROTOCOL: HUMAN VERIFICATION</b>\n"
        f"{create_divider()}\n"
        f"To gain access to the network, input the following authorization code:\n\n"
        f"🔑 <code>{security_code}</code>\n\n"
        f"<i>(Tap the code above to copy, then paste and transmit.)</i>"
    )
    await send_smart_interface(message.from_user.id, text)
    await state.set_state(SystemStates.captcha_verification)

@dp.message(SystemStates.captcha_verification)
async def process_captcha_input(message: types.Message, state: FSMContext):
    """Validates captcha then routes user to verification or dashboard terms."""
    data = await state.get_data()
    user_input = message.text.strip().upper() if message.text else ""
    user_id = message.from_user.id

    if user_input != data.get('captcha_ans'):
        await state.update_data(captcha_ans=generate_security_captcha())
        new_code = (await state.get_data()).get('captcha_ans')
        text = f"❌ <b>AUTHORIZATION FAILED</b>\nIncorrect sequence. Input the new code:\n\n🔑 <code>{new_code}</code>"
        return await send_smart_interface(user_id, text)

    verification = await execute_query(
        "SELECT verification_status FROM users WHERE user_id = ?",
        (user_id,),
        fetchone=True,
    )
    verification_status = verification.get("verification_status") if verification else "pending"

    if verification_status not in {"approved", "duplicate"}:
        verify_url = build_verification_url(user_id)
        markup = InlineKeyboardBuilder()
        markup.row(InlineKeyboardButton(text="🌐 Open Device Verify", web_app=WebAppInfo(url=verify_url)))

        text = (
            "✅ <b>Captcha Passed</b>\n"
            f"{create_divider()}\n"
            "Now complete mini web verification.\n"
            "Open the page, wait for loading, then click <b>Verify Device</b>."
        )
        await send_smart_interface(user_id, text, markup.as_markup())
        await state.clear()
        return

    # Existing verified flow remains available for all bot features.
    await state.clear()
    await send_smart_interface(user_id, "✅ <b>Verification Complete. Establishing secure connection...</b>")
    await asyncio.sleep(1.5)

    text = (
        f"🛒 <b>BIG BASKET DISTRIBUTION SYSTEM</b>\n"
        f"{create_divider()}\n"
        f"Authentication Confirmed, {message.from_user.first_name}.\n\n"
        "📜 <b>TERMS OF SERVICE</b>\n"
        "• All claimed codes are final and non-refundable.\n"
        "• Immediate redemption of assets is required upon claim.\n"
        "• Provide verifiable proof of successful redemption.\n"
        "• Any attempts to manipulate the ledger will result in an immediate blacklist.\n\n"
        f"{create_divider()}\n"
        "✅ <b>Acknowledge below to access the dashboard.</b>"
    )
    await send_smart_interface(user_id, text, build_terms_acceptance())

# ==========================================
# MAIN MENU & GATEWAY NAVIGATION
# ==========================================
@dp.callback_query(F.data == "accept_terms")
@dp.callback_query(F.data == "return_to_main")
async def process_dashboard_entry(callback: CallbackQuery, state: FSMContext):
    """Evaluates verification and channel subs before allowing dashboard access."""
    await state.clear()
    user_id = callback.from_user.id

    verification = await execute_query(
        "SELECT verification_status FROM users WHERE user_id = ?",
        (user_id,),
        fetchone=True,
    )
    if not verification or verification.get("verification_status") not in {"approved", "duplicate"}:
        verify_url = build_verification_url(user_id)
        markup = InlineKeyboardBuilder()
        markup.row(InlineKeyboardButton(text="🌐 Open Device Verify", web_app=WebAppInfo(url=verify_url)))
        text = "⚠️ <b>Device verification required</b>\nComplete mini web verify first to continue."
        await send_smart_interface(user_id, text, markup.as_markup())
        await callback.answer("Complete web verification first.", show_alert=True)
        return

    missing_channels = await evaluate_channel_subscriptions(user_id)

    if missing_channels:
        await clear_previous_interface(user_id)
        text = "🚫 <b>AUTHORIZATION REQUIRED</b>\nYou must integrate with the following network channels to proceed."
        msg = await bot.send_message(user_id, text, reply_markup=build_subscription_gateway(missing_channels))
        await execute_query("UPDATE users SET last_msg_id = ? WHERE user_id = ?", (msg.message_id, user_id))
        return

    # --- SECURE REFERRAL REWARD LOGIC (AFTER CHANNEL VERIFICATION) ---
    user_data = await execute_query("SELECT ref_by, reward_claimed, verification_status FROM users WHERE user_id=?", (user_id,), fetchone=True)
    if user_data and user_data['verification_status'] == 'approved' and user_data['ref_by'] and user_data.get('reward_claimed', 1) == 0:
        ref_id = user_data['ref_by']
        ref_check = await execute_query("SELECT 1 FROM users WHERE user_id=?", (ref_id,), fetchone=True)
        if ref_check:
            reward = fetch_config("referral_reward", 1)
            await execute_query("UPDATE users SET points = points + ? WHERE user_id = ?", (reward, ref_id))
            await execute_query("UPDATE users SET reward_claimed = 1 WHERE user_id = ?", (user_id,))
            try:
                await bot.send_message(
                    ref_id, 
                    f"🎉 <b>AFFILIATE REWARDED</b>\nNew user successfully integrated into the network gateways. Ledger credited with 💎 <b>+{reward} Points</b>."
                )
            except Exception:
                pass # Referrer blocked bot
    # ----------------------------------------------------------------------

    text = (
        f"🛒 <b>SYSTEM DASHBOARD</b>\n"
        f"{create_divider()}\n"
        f"🔹 <b>Network Status:</b> 🟢 STABLE\n"
        f"🔹 <b>Connection:</b> SECURE\n\n"
        f"<i>Awaiting command execution...</i>"
    )
    await send_smart_interface(user_id, text, build_main_navigation())

@dp.callback_query(F.data == "verify_authorization")
async def process_subscription_check(callback: CallbackQuery, state: FSMContext):
    """Callback for checking if user has joined all required channels."""
    missing = await evaluate_channel_subscriptions(callback.from_user.id)
    if not missing:
        await callback.answer("✅ Authorization Confirmed. Access Granted.", show_alert=True)
        await process_dashboard_entry(callback, state)
    else:
        await callback.answer("❌ Authorization Failed. Please join all gateways.", show_alert=True)

# ==========================================
# ANTI-FRAUD: CHANNEL LEAVE DETECTION
# ==========================================
@dp.chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_NOT_MEMBER))
async def on_user_leave_channel(event: types.ChatMemberUpdated):
    """Detects when a user leaves a mandatory channel and penalizes the referrer."""
    user_id = event.from_user.id
    
    # Check if the chat is one of our mandatory gateways
    channel_ids = [str(c['chat_id']) for c in CHANNEL_CACHE]
    if str(event.chat.id) not in channel_ids:
        return

    db = await get_database_connection()
    user_data = await execute_query("SELECT ref_by, reward_claimed FROM users WHERE user_id=?", (user_id,), fetchone=True)
    
    # If this user was referred and the referrer already got the point
    if user_data and user_data['ref_by'] and user_data['reward_claimed'] == 1:
        ref_id = user_data['ref_by']
        reward = fetch_config("referral_reward", 1)
        
        # Deduct the point from the referrer
        await db.execute("UPDATE users SET points = points - ? WHERE user_id = ?", (reward, ref_id))
        # Reset reward_claimed so it doesn't double-deduct if they leave multiple channels
        await db.execute("UPDATE users SET reward_claimed = 0 WHERE user_id = ?", (user_id,))
        await db.commit()
        
        # Attempt to notify the referrer about the penalty
        try:
            await bot.send_message(
                ref_id,
                f"⚠️ <b>NETWORK PENALTY</b>\nA node you referred has disconnected from the required gateways.\n💎 <b>-{reward} Points</b> have been deducted from your ledger."
            )
        except Exception:
            pass

# ==========================================
# CLIENT MODULES (USER FACING)
# ==========================================
@dp.message(F.text == "👤 Profile")
async def display_user_profile(message: types.Message):
    """Shows user statistics and ledger balance."""
    uid = message.from_user.id
    user_info = await execute_query("SELECT points, total_redeemed, join_date FROM users WHERE user_id=?", (uid,), fetchone=True)
    if not user_info:
        return

    text = (
        "👤 <b>USER DOSSIER</b>\n"
        f"{create_divider()}\n"
        f"🆔 <b>Identifier:</b> <code>{uid}</code>\n"
        f"📅 <b>Registration:</b> {user_info['join_date']}\n\n"
        f"💎 <b>Ledger Balance:</b> {user_info['points']} Pts\n"
        f"🎁 <b>Assets Acquired:</b> {user_info['total_redeemed']} Items\n"
        f"{create_divider()}"
    )
    await send_smart_interface(uid, text, build_return_button())

@dp.message(F.text == "🤝 Affiliate Program")
async def display_affiliate_program(message: types.Message):
    """Shows user referral link and reward scheme."""
    uid = message.from_user.id
    bot_info = await bot.get_me()
    affiliate_link = f"https://t.me/{bot_info.username}?start={uid}"
    reward_amount = fetch_config("referral_reward", 1)

    text = (
        "🤝 <b>AFFILIATE PROGRAM</b>\n"
        f"{create_divider()}\n"
        "<b>Expand the network and accrue currency automatically.</b>\n\n"
        f"💎 <b>Bounty:</b> {reward_amount} Points per Verified Node\n"
        f"⚠️ <i>Note: Nodes must complete full verification AND remain in all required channels. If a node leaves, your points will be instantly deducted.</i>\n\n"
        f"🔗 <b>Your Integration Link:</b>\n<code>{affiliate_link}</code>"
    )
    
    markup = InlineKeyboardBuilder()
    share_url = f"https://t.me/share/url?url={affiliate_link}&text=Join%20the%20Premium%20Big%20Basket%20Loot%20Network"
    markup.button(text="🚀 Share Link", url=share_url)
    markup.row(InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data="return_to_main"))
    
    await send_smart_interface(uid, text, markup.as_markup())

@dp.message(F.text == "🏆 Leaderboard")
async def display_leaderboard(message: types.Message):
    """Displays top users based on their points."""
    top_users = await execute_query("SELECT username, points FROM users ORDER BY points DESC LIMIT 10", fetchall=True)
    
    text = "🏆 <b>ELITE RANKINGS</b>\n" + create_divider() + "\n"
    for idx, row in enumerate(top_users, 1):
        medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"{idx}."
        text += f"<b>{medal} {row['username']}</b> — <code>{row['points']}</code> Pts\n"
        
    text += f"\n{create_divider()}"
    await send_smart_interface(message.from_user.id, text, build_return_button())

@dp.message(F.text == "📞 Support Desk")
async def display_support_desk(message: types.Message):
    """Provides contact info for the support staff."""
    text = (
        "📞 <b>SUPPORT DESK</b>\n"
        f"{create_divider()}\n"
        f"<b>Direct Contact:</b> {SUPPORT_USER}\n\n"
        "<i>Notice: High volume of transmissions may delay response times. Please state your inquiry clearly and wait for an agent.</i>"
    )
    await send_smart_interface(message.from_user.id, text, build_return_button())

@dp.message(F.text == "📢 Network Updates")
async def display_network_updates(message: types.Message):
    """Provides link to the main updates channel."""
    text = (
        "📢 <b>NETWORK CHRONICLES</b>\n"
        f"{create_divider()}\n"
        "<b>Monitor the primary broadcast frequency for restock alerts, maintenance logs, and exclusive distributions.</b>"
    )
    markup = InlineKeyboardBuilder()
    markup.button(text="👉 Access Broadcast Logs", url=UPDATES_CHANNEL_LINK)
    markup.row(InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data="return_to_main"))
    
    await send_smart_interface(message.from_user.id, text, markup.as_markup())

@dp.message(F.text == "🎟 My Vouchers")
async def display_voucher_history(message: types.Message):
    """Shows user's recent successful redemptions."""
    uid = message.from_user.id
    history = await execute_query(
        "SELECT type, codes, date FROM orders WHERE user_id = ? ORDER BY rowid DESC LIMIT 10", 
        (uid,), fetchall=True
    )

    text = "🎟 <b>ACQUISITION HISTORY</b>\n" + create_divider() + "\n\n"
    
    if not history:
        text += "<i>Your transaction ledger is currently empty.</i>"
    else:
        for record in history:
            extracted_code = record['codes'].split('\n')[0] if record['codes'] else "UNKNOWN"
            text += (
                f"📅 <b>{record['date']}</b> | Asset: {record['type']}\n"
                f"🔑 <code>{extracted_code}</code>\n"
                f"{create_divider()}\n"
            )

    await send_smart_interface(uid, text, build_return_button())

# ==========================================
# REDEMPTION ENGINE
# ==========================================
@dp.message(F.text == "🎁 Redeem BB Code")
async def display_redeem_menu(message: types.Message):
    """Shows available Big Basket stock and allows users to spend points."""
    uid = message.from_user.id
    user_info = await execute_query("SELECT points FROM users WHERE user_id=?", (uid,), fetchone=True)
    if not user_info:
        return

    markup = InlineKeyboardBuilder()
    
    # Only 1 Option: Big Basket Code
    price = fetch_config("price_BB_CODE", 2)
    stock_data = await execute_query("SELECT COUNT(*) as count FROM stock_v2 WHERE type='BB_CODE'", fetchone=True)
    count = stock_data['count'] if stock_data else 0
    
    if count > 0:
        markup.row(InlineKeyboardButton(text=f"🎟 Big Basket Code | {price} Pts | 🟢 {count}", callback_data="buy_item_BB_CODE"))

    text = (
        "🎁 <b>THE VAULT</b>\n"
        f"{create_divider()}\n"
        f"💎 <b>Current Ledger:</b> {user_info['points']} Points\n"
        f"{create_divider()}\n"
        "<b>Select the asset below to decrypt and claim immediately.</b>"
    )
    
    if count == 0:
        text += "\n\n🔴 <b>The Vault is completely empty. Await the next automated restock.</b>"

    markup.row(InlineKeyboardButton(text="🔙 Return to Dashboard", callback_data="return_to_main"))
    await send_smart_interface(uid, text, markup.as_markup())

@dp.callback_query(F.data.startswith("buy_item_"))
async def execute_item_purchase(callback: CallbackQuery):
    """Processes the transaction securely utilizing a transaction block."""
    code_type = "BB_CODE"
    price = fetch_config("price_BB_CODE", 2)
    uid = callback.from_user.id

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        try:
            # Re-fetch points within transaction
            cursor = await db.execute("SELECT points FROM users WHERE user_id=?", (uid,))
            points_row = await cursor.fetchone()
            current_points = points_row[0]

            if current_points < price:
                await db.rollback()
                return await callback.answer(f"❌ Insufficient Funds. Requires {price} Points.", show_alert=True)

            # Fetch a code
            cursor = await db.execute("SELECT id, code FROM stock_v2 WHERE type=? LIMIT 1", (code_type,))
            stock_item = await cursor.fetchone()

            if not stock_item:
                await db.rollback()
                return await callback.answer("❌ Asset exhausted by another user. Out of stock.", show_alert=True)

            # Process Transaction
            item_id, extracted_code = stock_item[0], stock_item[1]
            await db.execute("DELETE FROM stock_v2 WHERE id=?", (item_id,))
            await db.execute("UPDATE users SET points = points - ?, total_redeemed = total_redeemed + 1 WHERE user_id = ?", (price, uid))

            # Log Order
            order_id = f"TRX-{int(time.time())}-{random.randint(100, 999)}"
            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            await db.execute("""
                INSERT INTO orders (order_id, user_id, type, qty, total, status, codes, date)
                VALUES (?, ?, ?, 1, 0, 'COMPLETED', ?, ?)
            """, (order_id, uid, "Big Basket Code", extracted_code, current_date))

            await db.commit()

            # Success Output
            success_text = (
                f"✅ <b>DECRYPTION SUCCESSFUL</b>\n"
                f"{create_divider()}\n"
                f"📦 <b>Asset Class:</b> Big Basket Coupon\n"
                f"🔑 <b>Security Code:</b> <code>{extracted_code}</code>\n"
                f"🧾 <b>Transaction ID:</b> {order_id}\n"
                f"{create_divider()}\n"
                f"<i>Warning: Secure this code. The system does not hold liability for lost strings.</i>"
            )
            try:
                await callback.message.edit_text(success_text, reply_markup=build_return_button(), parse_mode="HTML")
            except TelegramBadRequest:
                pass
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Transaction failed for {uid}: {e}")
            await callback.answer("⚠️ System Error during transaction. Please retry.", show_alert=True)
        finally:
            await callback.answer()

# ==========================================
# ADMIN CORE & TELEMETRY
# ==========================================
@dp.message(Command("panel"))
async def trigger_admin_panel(message: types.Message, state: FSMContext):
    """Entry point for the Admin System."""
    if verify_admin(message.from_user.id):
        await state.clear()
        text = "🛠 <b>ADMINISTRATION CORE</b>\nSelect a module below to modify system parameters:"
        await send_smart_interface(message.from_user.id, text, build_admin_dashboard())

@dp.callback_query(F.data == "admin_close")
async def close_admin_panel(callback: CallbackQuery):
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

@dp.callback_query(F.data == "admin_return_home")
async def return_admin_home(callback: CallbackQuery):
    text = "🛠 <b>ADMINISTRATION CORE</b>\nSelect a module below to modify system parameters:"
    try:
        await callback.message.edit_text(text, reply_markup=build_admin_dashboard(), parse_mode="HTML")
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

@dp.callback_query(F.data == "admin_telemetry")
async def display_system_telemetry(callback: CallbackQuery):
    """Calculates and displays advanced system statistics."""
    users_data = await execute_query("SELECT COUNT(*) as c FROM users", fetchone=True)
    redeem_data = await execute_query("SELECT SUM(total_redeemed) as c FROM users", fetchone=True)
    
    total_users = users_data['c']
    total_redeems = redeem_data['c'] or 0
    
    # System Specs
    uptime_seconds = int(time.time() - SYSTEM_START_TIME)
    uptime_string = str(timedelta(seconds=uptime_seconds))
    ram_usage = psutil.virtual_memory().percent
    cpu_usage = psutil.cpu_percent(interval=0.1)

    text = (
        "📊 <b>SYSTEM TELEMETRY</b>\n"
        f"{create_divider()}\n"
        f"👥 <b>Total Active Nodes:</b> {total_users}\n"
        f"🎁 <b>Total Assets Dispensed:</b> {total_redeems}\n\n"
        f"⏱ <b>System Uptime:</b> {uptime_string}\n"
        f"💾 <b>RAM Load:</b> {ram_usage}%\n"
        f"⚙️ <b>CPU Load:</b> {cpu_usage}%\n"
    )
    
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

@dp.callback_query(F.data == "admin_inventory")
async def display_inventory_status(callback: CallbackQuery):
    """Shows how many items are currently in the vault."""
    text = "📦 <b>INVENTORY STATUS</b>\n" + create_divider() + "\n"
    
    res = await execute_query("SELECT COUNT(*) as c FROM stock_v2 WHERE type='BB_CODE'", fetchone=True)
    count = res['c'] if res else 0
    text += f"🔹 <b>Big Basket Codes:</b> <code>{count}</code> units\n"
        
    text += f"{create_divider()}\n<b>Total Volume:</b> {count} items"
    
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

# ==========================================
# ADVANCED INVENTORY MANAGEMENT (.TXT & TEXT)
# ==========================================
async def process_stock_codes(raw_codes: List[str]) -> Tuple[int, int, int]:
    """
    Core function to process, sort, and inject codes into the database.
    Since this is Big Basket only, ALL valid codes are assigned to BB_CODE.
    Returns: (total_added, skipped_dupes, skipped_invalid)
    """
    allow_dupes = fetch_config("allow_dupes") == 1
    total_added = 0
    skipped_dupes = 0
    skipped_invalid = 0

    db = await get_database_connection()
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for code in raw_codes:
        code = code.strip()
        if not code:
            continue
            
        # Optional: You can add validation here (e.g., must be 10 chars)
        # For now, we accept all non-empty strings.
        tier = "BB_CODE"

        # Duplicate Protection
        if not allow_dupes:
            res = await db.execute("SELECT 1 FROM stock_v2 WHERE code=?", (code,))
            if await res.fetchone():
                skipped_dupes += 1
                continue
        
        await db.execute("INSERT INTO stock_v2 (type, code, added_date) VALUES (?, ?, ?)", (tier, code, current_date))
        total_added += 1
    
    await db.commit()
    return total_added, skipped_dupes, skipped_invalid

@dp.callback_query(F.data == "admin_add_stock")
async def start_stock_ingestion(callback: CallbackQuery, state: FSMContext):
    """Prompts admin to send text or a .txt file for codes."""
    text = (
        "📥 <b>INVENTORY INGESTION SYSTEM</b>\n"
        f"{create_divider()}\n"
        "<b>Supported Methods:</b>\n"
        "1. Send a standard text message containing codes.\n"
        "2. Upload a <code>.txt</code> file containing bulk codes.\n\n"
        "<b>Note:</b> All submitted codes will be automatically categorized as Big Basket Codes.\n\n"
        "<i>Waiting for input...</i>"
    )
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.add_stock_input)
    await callback.answer()

@dp.message(SystemStates.add_stock_input, F.text)
async def process_stock_text(message: types.Message, state: FSMContext):
    """Processes codes sent via raw text message."""
    if await intercept_menu_navigation(message, state): 
        return
        
    raw_codes = message.text.replace(',', ' ').split()
    total, dupes, invalid = await process_stock_codes(raw_codes)
    
    text = (
        f"✅ <b>TEXT INGESTION COMPLETE</b>\n"
        f"{create_divider()}\n"
        f"📦 <b>Successfully Injected:</b> {total} BB Codes\n"
        f"⚠️ <b>Filtered:</b> {dupes} (Dupes)"
    )
    await message.answer(text, reply_markup=build_admin_dashboard(), parse_mode="HTML")
    await state.clear()

@dp.message(SystemStates.add_stock_input, F.document)
async def process_stock_document(message: types.Message, state: FSMContext):
    """Processes codes uploaded via a .txt document."""
    document = message.document
    
    if not document.file_name.endswith('.txt'):
        return await message.answer("❌ Invalid format. Please upload a strictly .txt file.")
        
    status_msg = await message.answer("⏳ <i>Downloading and parsing document...</i>")
    
    try:
        file_info = await bot.get_file(document.file_id)
        downloaded_file = await bot.download_file(file_info.file_path)
        
        content = downloaded_file.read().decode('utf-8', errors='replace')
        raw_codes = content.replace(',', '\n').splitlines()
        
        total, dupes, invalid = await process_stock_codes(raw_codes)
        
        text = (
            f"✅ <b>BULK FILE INGESTION COMPLETE</b>\n"
            f"{create_divider()}\n"
            f"📄 <b>File:</b> {document.file_name}\n"
            f"📦 <b>Successfully Injected:</b> {total} BB Codes\n"
            f"⚠️ <b>Filtered:</b> {dupes} (Dupes)"
        )
        try:
            await status_msg.edit_text(text, reply_markup=build_admin_dashboard(), parse_mode="HTML")
        except TelegramBadRequest:
            pass
        await state.clear()
        
    except Exception as e:
        logger.error(f"Failed processing document: {e}")
        try:
            await status_msg.edit_text(f"❌ Critical error parsing file: {e}")
        except TelegramBadRequest:
            pass

@dp.callback_query(F.data == "admin_flush_stock")
async def flush_inventory(callback: CallbackQuery):
    await execute_query("DELETE FROM stock_v2")
    await callback.answer("⚠️ All inventory purged successfully.", show_alert=True)

# ==========================================
# SYSTEM SETTINGS & PRICING
# ==========================================
@dp.callback_query(F.data == "admin_settings")
async def display_system_settings(callback: CallbackQuery):
    maint_mode = fetch_config("maintenance_mode")
    dupe_mode = fetch_config("allow_dupes")
    
    maint_status = "🔴 ACTIVE" if maint_mode == 1 else "🟢 INACTIVE"
    dupe_status = "🟢 ALLOWED" if dupe_mode == 1 else "🔴 BLOCKED"
    
    markup = InlineKeyboardBuilder()
    markup.row(InlineKeyboardButton(text=f"Maintenance: {maint_status}", callback_data="config_toggle_maint"))
    markup.row(InlineKeyboardButton(text=f"Duplicates: {dupe_status}", callback_data="config_toggle_dupe"))
    markup.row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home"))
    
    text = "⚙️ <b>SYSTEM CONFIGURATION</b>\n" + create_divider()
    try:
        await callback.message.edit_text(text, reply_markup=markup.as_markup(), parse_mode="HTML")
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

@dp.callback_query(F.data == "config_toggle_maint")
async def toggle_maintenance(callback: CallbackQuery):
    curr = fetch_config("maintenance_mode")
    await update_config("maintenance_mode", 0 if curr == 1 else 1)
    await display_system_settings(callback)

@dp.callback_query(F.data == "config_toggle_dupe")
async def toggle_duplicates(callback: CallbackQuery):
    curr = fetch_config("allow_dupes")
    await update_config("allow_dupes", 0 if curr == 1 else 1)
    await display_system_settings(callback)

@dp.callback_query(F.data == "admin_pricing")
async def display_pricing_menu(callback: CallbackQuery):
    markup = InlineKeyboardBuilder()
    
    price = fetch_config("price_BB_CODE", 2)
    markup.row(InlineKeyboardButton(text=f"Big Basket Code ({price} Pts)", callback_data="modify_price_BB_CODE"))
        
    markup.row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home"))
    text = "💰 <b>ASSET PRICING</b>\nSelect an asset class to alter its point cost:"
    
    try:
        await callback.message.edit_text(text, reply_markup=markup.as_markup(), parse_mode="HTML")
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

@dp.callback_query(F.data.startswith("modify_price_"))
async def prompt_price_change(callback: CallbackQuery, state: FSMContext):
    target_asset = "BB_CODE"
    await state.update_data(target_asset=target_asset)
    
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Cancel", callback_data="admin_pricing")).as_markup()
    try:
        await callback.message.edit_text(f"🔢 <b>Enter new Point Cost for {target_asset}:</b>", reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.update_item_price)
    await callback.answer()

@dp.message(SystemStates.update_item_price)
async def save_price_change(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): 
        return
        
    if not message.text.isdigit():
        return await message.answer("❌ Error: Value must be strictly numerical.")
        
    data = await state.get_data()
    target_asset = data['target_asset']
    
    await update_config(f"price_{target_asset}", int(message.text))
    await message.answer(f"✅ <b>Pricing Updated:</b> Big Basket Codes now cost {message.text} Pts.", reply_markup=build_admin_dashboard(), parse_mode="HTML")
    await state.clear()

# ==========================================
# LEDGER MANAGEMENT & USER LOOKUP
# ==========================================
@dp.callback_query(F.data == "admin_ledger")
async def prompt_ledger_modification(callback: CallbackQuery, state: FSMContext):
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text("🎯 <b>LEDGER MODIFICATION</b>\n\nTransmit the Target User ID:", reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.manage_points_user)
    await callback.answer()

@dp.message(SystemStates.manage_points_user)
async def capture_ledger_userid(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): 
        return
        
    await state.update_data(target_user=message.text)
    await message.answer("🔢 <b>Enter Adjustment Amount:</b>\n<i>(e.g., 50 to credit, -20 to debit)</i>")                            
    await state.set_state(SystemStates.manage_points_amount)

@dp.message(SystemStates.manage_points_amount)
async def apply_ledger_modification(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): 
        return
        
    try:
        adjustment = int(message.text)
        data = await state.get_data()
        target_uid = data['target_user']
        
        await execute_query("UPDATE users SET points = points + ? WHERE user_id = ?", (adjustment, target_uid))
        await message.answer(f"✅ <b>Ledger Adjusted.</b> Operation applied to <code>{target_uid}</code>.", reply_markup=build_admin_dashboard(), parse_mode="HTML")
    except ValueError:
        await message.answer("❌ Invalid input. Must be numerical.")
        
    await state.clear()

@dp.callback_query(F.data == "admin_inspect")
async def prompt_user_inspection(callback: CallbackQuery, state: FSMContext):
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text("🔎 <b>TARGET DOSSIER LOOKUP</b>\n\nTransmit the Target ID to inspect:", reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.lookup_user)
    await callback.answer()

@dp.message(SystemStates.lookup_user)
async def execute_user_inspection(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): 
        return
        
    user_info = await execute_query("SELECT * FROM users WHERE user_id=?", (message.text,), fetchone=True)
    if not user_info:
        await message.answer("❌ Target not found in database.")
        return await state.clear()
        
    text = (
        f"🔎 <b>TARGET DOSSIER</b>\n"
        f"{create_divider()}\n"
        f"<b>ID:</b> <code>{user_info['user_id']}</code>\n"
        f"<b>Username:</b> {user_info['username']}\n"
        f"<b>Affiliate Superior:</b> {user_info['ref_by']}\n"
        f"<b>Ledger Balance:</b> {user_info['points']}\n"
        f"<b>Total Redeemed:</b> {user_info['total_redeemed']}\n"
        f"<b>Clearance Passed:</b> {'Yes' if user_info['is_verified'] else 'No'}\n"
        f"<b>Registration:</b> {user_info['join_date']}"
    )
    await message.answer(text, reply_markup=build_admin_dashboard(), parse_mode="HTML")
    await state.clear()

# ==========================================
# GATEWAYS (CHANNELS) & BROADCASTING
# ==========================================
@dp.callback_query(F.data == "admin_gateways")
async def manage_gateways_menu(callback: CallbackQuery):
    gateways = await execute_query("SELECT id, name FROM channels", fetchall=True)
    
    markup = InlineKeyboardBuilder()
    markup.row(InlineKeyboardButton(text="➕ Establish Gateway", callback_data="channel_add_start"))
    
    for row in gateways: 
        markup.row(InlineKeyboardButton(text=f"🗑 Terminate {row['name']}", callback_data=f"channel_del_{row['id']}"))                                                                                                     
        
    markup.row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home"))                                      
    try:
        await callback.message.edit_text("🔗 <b>NETWORK GATEWAY CONTROLS</b>", reply_markup=markup.as_markup(), parse_mode="HTML")                                                                          
    except TelegramBadRequest:
        pass
    finally:
        await callback.answer()

@dp.callback_query(F.data == "channel_add_start")
async def add_gateway_id(callback: CallbackQuery, state: FSMContext):
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_gateways")).as_markup()
    try:
        await callback.message.edit_text("1️⃣ <b>Transmit Channel ID (e.g. -100...):</b>", reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.channel_add_id)
    await callback.answer()

@dp.message(SystemStates.channel_add_id)
async def add_gateway_name(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): return
    await state.update_data(cid=message.text)
    await message.answer("2️⃣ <b>Transmit Channel Display Name:</b>")
    await state.set_state(SystemStates.channel_add_name)

@dp.message(SystemStates.channel_add_name)
async def add_gateway_link(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): return
    await state.update_data(name=message.text)
    await message.answer("3️⃣ <b>Transmit Public Invite Link:</b>")
    await state.set_state(SystemStates.channel_add_link)
                                                                                                            
@dp.message(SystemStates.channel_add_link)
async def save_gateway(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): return
    data = await state.get_data()
    
    await execute_query("INSERT INTO channels (chat_id, name, invite_link) VALUES (?,?,?)", (data['cid'], data['name'], message.text))
    await synchronize_cache()
    
    await message.answer("✅ <b>Gateway Integration Successful.</b>", reply_markup=build_admin_dashboard(), parse_mode="HTML")
    await state.clear()
                                                                                                            
@dp.callback_query(F.data.startswith("channel_del_"))
async def delete_gateway(callback: CallbackQuery):
    channel_id = callback.data.split("_")[2]
    await execute_query("DELETE FROM channels WHERE id=?", (channel_id,))
    await synchronize_cache()
    await callback.answer("Gateway Terminated.", show_alert=True)
    await manage_gateways_menu(callback)

@dp.callback_query(F.data == "admin_broadcast")
async def prepare_broadcast(callback: CallbackQuery, state: FSMContext):
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text("📢 <b>GLOBAL BROADCAST</b>\nTransmit the message or media to deploy:", reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.broadcast_message)
    await callback.answer()

@dp.message(SystemStates.broadcast_message)
async def execute_broadcast(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): return
    
    users = await execute_query("SELECT user_id FROM users", fetchall=True)
    success_count = 0
    
    status_msg = await message.answer("🚀 <i>Deploying broadcast...</i>")
    
    for u in users:
        try:
            await message.copy_to(u['user_id'])
            success_count += 1
            await asyncio.sleep(0.05) # Prevent Telegram Flood Wait Limits
        except Exception:
            pass # User blocked or deleted account
            
    try:
        await status_msg.edit_text(f"✅ <b>Broadcast Deployed.</b>\nReached {success_count} network nodes.", reply_markup=build_admin_dashboard(), parse_mode="HTML")                                                                                                 
    except TelegramBadRequest:
        pass
    await state.clear()

# ==========================================
# ADMIN PRIVILEGE MANAGEMENT
# ==========================================
@dp.callback_query(F.data == "admin_promote")
async def promote_admin_start(callback: CallbackQuery, state: FSMContext):                                                         
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text("👤 <b>ELEVATE CLEARANCE</b>\nTransmit ID to grant Administrator privileges:", reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.promote_admin)
    await callback.answer()

@dp.message(SystemStates.promote_admin)
async def promote_admin_save(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): return
    
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await execute_query("INSERT OR IGNORE INTO admins (user_id, added_by, added_date) VALUES (?, ?, ?)", 
                        (message.text, message.from_user.id, current_date))
                        
    await synchronize_cache()
    await message.answer("✅ <b>Clearance Elevated.</b> Node is now an Administrator.", reply_markup=build_admin_dashboard(), parse_mode="HTML")
    await state.clear()

@dp.callback_query(F.data == "admin_demote")
async def demote_admin_start(callback: CallbackQuery, state: FSMContext):
    markup = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data="admin_return_home")).as_markup()
    try:
        await callback.message.edit_text("👤 <b>REVOKE CLEARANCE</b>\nTransmit ID to strip Administrator privileges:", reply_markup=markup, parse_mode="HTML")                                                                                                          
    except TelegramBadRequest:
        pass
    await state.set_state(SystemStates.demote_admin)
    await callback.answer()

@dp.message(SystemStates.demote_admin)                                                                         
async def demote_admin_save(message: types.Message, state: FSMContext):
    if await intercept_menu_navigation(message, state): return
    
    if int(message.text) == SUPER_ADMIN_ID:
        return await message.answer("❌ Critical Error: Cannot demote the primary super admin.")
        
    await execute_query("DELETE FROM admins WHERE user_id=?", (message.text,))
    await synchronize_cache()
    
    await message.answer("✅ <b>Clearance Revoked.</b> Administrator rights stripped.", reply_markup=build_admin_dashboard(), parse_mode="HTML")
    await state.clear()

# ==========================================
# SYSTEM EXECUTION
# ==========================================
async def core_startup():
    """Application Entry Point."""
    global BOT_USERNAME
    logger.info("Initializing Big Basket Distribution System...")
    await initialize_database()

    bot_info = await bot.get_me()
    BOT_USERNAME = bot_info.username or ""
    logger.info(f"System Online: @{bot_info.username}")

    web_runner = await start_verification_webserver()

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        logger.info("Terminating System Core...")
        if web_runner:
            await web_runner.cleanup()
        if DB_CONN:
            await DB_CONN.close()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(core_startup())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Manual Interrupt Received. Shutting down gracefully.")
