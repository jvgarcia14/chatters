import os
import logging
from datetime import datetime
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise ValueError("Missing TELEGRAM_BOT_TOKEN")

if not DATABASE_URL:
    raise ValueError("Missing DATABASE_URL")


# =========================
# DB
# =========================
def get_conn():
    return psycopg.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Base tables
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS availability (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            private_chat_id BIGINT,
            name TEXT NOT NULL,
            telegram TEXT,
            available_date DATE NOT NULL,
            shift TEXT NOT NULL,
            page_type TEXT NOT NULL,
            preferred_page TEXT,
            status TEXT NOT NULL DEFAULT 'available',
            booked_by BIGINT,
            booked_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (user_id, available_date)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS manager_topics (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            topic_id BIGINT NOT NULL,
            title TEXT,
            registered_by BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chat_id, topic_id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS page_requests (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            topic_id BIGINT NOT NULL,
            request_date DATE NOT NULL,
            shift TEXT NOT NULL,
            page_name TEXT NOT NULL,
            created_by BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )

    # Migrations for older databases
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS private_chat_id BIGINT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS name TEXT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS telegram TEXT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS available_date DATE;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS shift TEXT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS page_type TEXT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS preferred_page TEXT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'available';")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS booked_by BIGINT;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS booked_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
    cur.execute("ALTER TABLE availability ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")

    # Backfill old rows just in case
    cur.execute("UPDATE availability SET status = 'available' WHERE status IS NULL;")

    # Indexes
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_availability_date
        ON availability (available_date);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_availability_status
        ON availability (status);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_page_requests_date_shift
        ON page_requests (request_date, shift);
        """
    )

    # Trigger
    cur.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    cur.execute("DROP TRIGGER IF EXISTS trg_set_updated_at ON availability;")

    cur.execute(
        """
        CREATE TRIGGER trg_set_updated_at
        BEFORE UPDATE ON availability
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Database initialized.")

# =========================
# HELPERS
# =========================
def is_private_chat(update: Update) -> bool:
    return update.effective_chat.type == "private"


def get_topic_id(update: Update):
    return getattr(update.message, "message_thread_id", None) if update.message else None


def get_callback_topic_id(query):
    return getattr(query.message, "message_thread_id", None) if query and query.message else None


def parse_friendly_date(text: str):
    text = text.strip().replace(",", "")
    formats = [
        "%Y-%m-%d",
        "%b %d %Y",
        "%B %d %Y",
        "%b %d",
        "%B %d",
    ]

    now = datetime.now()
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            if "%Y" not in fmt:
                parsed = parsed.replace(year=now.year)
            return parsed.date()
        except ValueError:
            continue
    return None


def pretty_date(date_value) -> str:
    if isinstance(date_value, str):
        date_value = datetime.strptime(date_value, "%Y-%m-%d").date()
    return f"{date_value.strftime('%B')} {date_value.day}, {date_value.year}"


def get_display_name(user) -> str:
    full = f"{user.first_name or ''} {user.last_name or ''}".strip()
    if full:
        return full
    if user.username:
        return user.username
    return f"User {user.id}"


def get_telegram_tag(user) -> str:
    return f"@{user.username}" if user.username else "(no @username)"


def get_manager_contact(user) -> str:
    if user.username:
        return f"@{user.username}"
    return get_display_name(user)


def is_registered_manager_topic(chat_id: int, topic_id):
    if topic_id is None:
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM manager_topics WHERE chat_id = %s AND topic_id = %s LIMIT 1",
        (chat_id, topic_id),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return bool(row)


async def reply_in_same_topic(message, text, **kwargs):
    await message.reply_text(
        text,
        message_thread_id=getattr(message, "message_thread_id", None),
        **kwargs,
    )


def parse_page_command_args(args):
    raw = " ".join(args).strip()
    if "|" not in raw:
        return None, None

    left, right = raw.split("|", 1)
    date_obj = parse_friendly_date(left.strip())
    if not date_obj:
        return None, None

    pages = [p.strip() for p in right.split(",") if p.strip()]
    if not pages:
        return date_obj, []

    return date_obj, pages


# =========================
# KEYBOARDS
# =========================
def start_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Submit Availability", callback_data="menu:available")],
            [InlineKeyboardButton("📋 View Available Pages", callback_data="menu:pages_help")],
            [InlineKeyboardButton("📌 View My Availability", callback_data="menu:myavailability_help")],
            [InlineKeyboardButton("🗑 Remove Availability", callback_data="menu:remove_help")],
        ]
    )


def shift_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔥 Prime", callback_data="shift:Prime")],
            [InlineKeyboardButton("🌤 Midshift", callback_data="shift:Midshift")],
            [InlineKeyboardButton("🌙 Closing", callback_data="shift:Closing")],
        ]
    )


def page_type_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔞 Nude", callback_data="page_type:Nude")],
            [InlineKeyboardButton("✨ Non-Nude", callback_data="page_type:Non-Nude")],
            [InlineKeyboardButton("🔄 Both", callback_data="page_type:Both")],
        ]
    )


def preferred_page_keyboard(page_names):
    keyboard = []
    for name in page_names:
        keyboard.append([InlineKeyboardButton(name[:50], callback_data=f"prefpage:{name}")])
    keyboard.append([InlineKeyboardButton("Any Page", callback_data="prefpage:Any Page")])
    return InlineKeyboardMarkup(keyboard)


def selection_keyboard(rows, iso_date):
    keyboard = []
    for row in rows:
        preferred = row.get("preferred_page") or "Any Page"
        label = f"{row['name']} | {row['shift']} | {preferred}"
        callback = f"select:{row['user_id']}:{iso_date}"
        keyboard.append([InlineKeyboardButton(label[:60], callback_data=callback)])
    return InlineKeyboardMarkup(keyboard)


# =========================
# COPY
# =========================
def chatter_welcome_text():
    return (
        "👋 Welcome to the Chatter Availability Bot\n\n"
        "You can use this bot to submit your availability, check open pages, and get selected for coverage.\n\n"
        "Available commands:\n"
        "/pages\n"
        "/pages Feb 10\n"
        "/available\n"
        "/myavailability Feb 10\n"
        "/remove Feb 10"
    )


def manager_help():
    return (
        "📋 Manager Commands\n\n"
        "/register\n"
        "/primepage Feb 10 | Dan, Autumn, Cat\n"
        "/midshiftpage Feb 10 | Autumn\n"
        "/closingpage Feb 10 | Carter\n"
        "/chattersprime Feb 10\n"
        "/chattersmidshift Feb 10\n"
        "/chattersclosing Feb 10\n"
        "/chattersall\n"
        "/chattersall Feb 10\n\n"
        "Use these inside the registered topic only."
    )


# =========================
# PAGE REQUESTS
# =========================
def save_page_requests(chat_id: int, topic_id: int, date_obj, shift: str, pages: list[str], created_by: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM page_requests
        WHERE chat_id = %s AND topic_id = %s AND request_date = %s AND shift = %s
        """,
        (chat_id, topic_id, date_obj.isoformat(), shift),
    )

    for page in pages:
        cur.execute(
            """
            INSERT INTO page_requests (chat_id, topic_id, request_date, shift, page_name, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (chat_id, topic_id, date_obj.isoformat(), shift, page, created_by),
        )

    conn.commit()
    cur.close()
    conn.close()


def fetch_page_requests_raw(date_iso: str | None = None):
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)

    if date_iso:
        cur.execute(
            """
            SELECT request_date, shift, page_name
            FROM page_requests
            WHERE request_date = %s
            ORDER BY
                CASE shift
                    WHEN 'Prime' THEN 1
                    WHEN 'Midshift' THEN 2
                    WHEN 'Closing' THEN 3
                    ELSE 99
                END,
                page_name ASC
            """,
            (date_iso,),
        )
    else:
        cur.execute(
            """
            SELECT request_date, shift, page_name
            FROM page_requests
            ORDER BY request_date ASC,
                CASE shift
                    WHEN 'Prime' THEN 1
                    WHEN 'Midshift' THEN 2
                    WHEN 'Closing' THEN 3
                    ELSE 99
                END,
                page_name ASC
            """
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_pages_for_shift_raw(date_iso: str, shift: str):
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)
    cur.execute(
        """
        SELECT page_name
        FROM page_requests
        WHERE request_date = %s AND shift = %s
        ORDER BY page_name ASC
        """,
        (date_iso, shift),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r["page_name"] for r in rows]


def fetch_booked_pages(date_iso: str, shift: str):
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)
    cur.execute(
        """
        SELECT preferred_page
        FROM availability
        WHERE available_date = %s
          AND shift = %s
          AND status = 'booked'
          AND preferred_page IS NOT NULL
          AND preferred_page <> 'Any Page'
        """,
        (date_iso, shift),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r["preferred_page"] for r in rows}


def fetch_open_pages_for_shift(date_iso: str, shift: str):
    requested = fetch_pages_for_shift_raw(date_iso, shift)
    booked = fetch_booked_pages(date_iso, shift)
    return [p for p in requested if p not in booked]


def fetch_open_page_requests(date_iso: str | None = None):
    raw_rows = fetch_page_requests_raw(date_iso)
    grouped = defaultdict(list)
    for row in raw_rows:
        key = (row["request_date"], row["shift"])
        grouped[key].append(row["page_name"])

    results = []
    for (request_date, shift), pages in grouped.items():
        booked = fetch_booked_pages(request_date.isoformat() if hasattr(request_date, "isoformat") else str(request_date), shift)
        for page in pages:
            if page not in booked:
                results.append(
                    {
                        "request_date": request_date,
                        "shift": shift,
                        "page_name": page,
                    }
                )
    return results


# =========================
# AVAILABILITY DB
# =========================
def save_availability(form):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO availability (
            user_id, private_chat_id, name, telegram, available_date, shift, page_type, preferred_page, status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'available')
        ON CONFLICT (user_id, available_date)
        DO UPDATE SET
            private_chat_id = EXCLUDED.private_chat_id,
            name = EXCLUDED.name,
            telegram = EXCLUDED.telegram,
            shift = EXCLUDED.shift,
            page_type = EXCLUDED.page_type,
            preferred_page = EXCLUDED.preferred_page,
            status = 'available',
            booked_by = NULL,
            booked_at = NULL,
            updated_at = NOW()
        """,
        (
            form["user_id"],
            form["private_chat_id"],
            form["name"],
            form["telegram"],
            form["date"],
            form["shift"],
            form["page_type"],
            form["preferred_page"],
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def fetch_user_availability(user_id: int, date_iso: str):
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)
    cur.execute(
        """
        SELECT available_date, shift, page_type, preferred_page, status
        FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (user_id, date_iso),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def remove_user_availability(user_id: int, date_iso: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (user_id, date_iso),
    )
    count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return count


def fetch_chatters_by_date(date_iso: str, shift=None):
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)

    if shift:
        cur.execute(
            """
            SELECT user_id, private_chat_id, name, telegram, available_date, shift, page_type, preferred_page, status
            FROM availability
            WHERE available_date = %s
              AND shift = %s
              AND status = 'available'
            ORDER BY name ASC
            """,
            (date_iso, shift),
        )
    else:
        cur.execute(
            """
            SELECT user_id, private_chat_id, name, telegram, available_date, shift, page_type, preferred_page, status
            FROM availability
            WHERE available_date = %s
              AND status = 'available'
            ORDER BY
                CASE shift
                    WHEN 'Prime' THEN 1
                    WHEN 'Midshift' THEN 2
                    WHEN 'Closing' THEN 3
                    ELSE 99
                END,
                name ASC
            """,
            (date_iso,),
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def fetch_all_chatters():
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)
    cur.execute(
        """
        SELECT user_id, private_chat_id, name, telegram, available_date, shift, page_type, preferred_page, status
        FROM availability
        WHERE status = 'available'
        ORDER BY available_date ASC,
            CASE shift
                WHEN 'Prime' THEN 1
                WHEN 'Midshift' THEN 2
                WHEN 'Closing' THEN 3
                ELSE 99
            END,
            name ASC
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def is_page_already_taken(date_iso: str, shift: str, preferred_page: str):
    if not preferred_page or preferred_page == "Any Page":
        return False

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM availability
        WHERE available_date = %s
          AND shift = %s
          AND preferred_page = %s
          AND status = 'booked'
        LIMIT 1
        """,
        (date_iso, shift, preferred_page),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return bool(row)


def book_chatter(user_id: int, date_iso: str, manager_id: int):
    conn = get_conn()
    cur = conn.cursor(row_factory=dict_row)

    cur.execute(
        """
        SELECT user_id, private_chat_id, name, telegram, shift, page_type, preferred_page, available_date, status
        FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (user_id, date_iso),
    )
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return None, "not_found"

    if row["status"] != "available":
        cur.close()
        conn.close()
        return None, "already_booked"

    preferred_page = row["preferred_page"]
    if preferred_page and preferred_page != "Any Page":
        cur.execute(
            """
            SELECT 1
            FROM availability
            WHERE available_date = %s
              AND shift = %s
              AND preferred_page = %s
              AND status = 'booked'
            LIMIT 1
            """,
            (date_iso, row["shift"], preferred_page),
        )
        page_taken = cur.fetchone()
        if page_taken:
            cur.close()
            conn.close()
            return None, "page_taken"

    cur.execute(
        """
        UPDATE availability
        SET status = 'booked',
            booked_by = %s,
            booked_at = NOW(),
            updated_at = NOW()
        WHERE user_id = %s
          AND available_date = %s
          AND status = 'available'
        RETURNING user_id, private_chat_id, name, telegram, shift, page_type, preferred_page, available_date, status
        """,
        (manager_id, user_id, date_iso),
    )
    booked_row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if not booked_row:
        return None, "already_booked"

    return booked_row, None


# =========================
# MANAGER GUARDS
# =========================
async def ensure_manager_topic(update: Update) -> bool:
    if is_private_chat(update):
        await update.message.reply_text("Use this command inside the registered manager topic.")
        return False

    topic_id = get_topic_id(update)
    if topic_id is None:
        await update.message.reply_text("Use this command inside the registered topic.")
        return False

    if not is_registered_manager_topic(update.effective_chat.id, topic_id):
        await reply_in_same_topic(
            update.message,
            "This topic is not registered yet. Type /register inside this topic first.",
        )
        return False

    return True


# =========================
# COMMANDS
# =========================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_private_chat(update):
        await update.message.reply_text(
            chatter_welcome_text(),
            reply_markup=start_keyboard(),
        )
    else:
        await reply_in_same_topic(update.message, manager_help())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_command(update, context)


async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_private_chat(update):
        await update.message.reply_text("Please use /register inside your manager topic.")
        return

    topic_id = get_topic_id(update)
    if topic_id is None:
        await update.message.reply_text("Please use /register inside a forum topic, not in the main group.")
        return

    chat = update.effective_chat
    user = update.effective_user

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO manager_topics (chat_id, topic_id, title, registered_by)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (chat_id, topic_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            registered_by = EXCLUDED.registered_by
        """,
        (chat.id, topic_id, chat.title, user.id),
    )
    conn.commit()
    cur.close()
    conn.close()

    await reply_in_same_topic(
        update.message,
        "✅ This topic has been registered successfully.\n\nYou can now use the manager commands here.",
    )


async def available_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        await reply_in_same_topic(update.message, "Please DM me and use /available there.")
        return

    user = update.effective_user
    context.user_data["availability_form"] = {
        "step": "awaiting_date",
        "user_id": user.id,
        "private_chat_id": update.effective_chat.id,
        "name": get_display_name(user),
        "telegram": get_telegram_tag(user),
    }

    await update.message.reply_text(
        "📅 Please enter your available date.\n\nExamples:\n• Feb 10\n• February 10\n• 2026-02-10"
    )


async def myavailability_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        await reply_in_same_topic(update.message, "Please use this in DM with the bot.")
        return

    if not context.args:
        await update.message.reply_text("Use: /myavailability Feb 10")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await update.message.reply_text("Invalid date. Example: /myavailability Feb 10")
        return

    row = fetch_user_availability(update.effective_user.id, parsed.isoformat())
    if not row:
        await update.message.reply_text(f"No saved availability found for {pretty_date(parsed)}.")
        return

    status_text = "Booked" if row["status"] == "booked" else "Available"

    await update.message.reply_text(
        "📌 Your Saved Availability\n\n"
        f"Date: {pretty_date(row['available_date'])}\n"
        f"Shift: {row['shift']}\n"
        f"Page Type: {row['page_type']}\n"
        f"Preferred Page: {row['preferred_page'] or 'Any Page'}\n"
        f"Status: {status_text}"
    )


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        await reply_in_same_topic(update.message, "Please use this in DM with the bot.")
        return

    if not context.args:
        await update.message.reply_text("Use: /remove Feb 10")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await update.message.reply_text("Invalid date. Example: /remove Feb 10")
        return

    deleted = remove_user_availability(update.effective_user.id, parsed.isoformat())
    if deleted == 0:
        await update.message.reply_text(f"No saved availability was found for {pretty_date(parsed)}.")
        return

    await update.message.reply_text(f"✅ Your availability for {pretty_date(parsed)} has been removed.")


async def pages_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        await reply_in_same_topic(update.message, "Please use /pages in DM with the bot.")
        return

    date_obj = None
    if context.args:
        date_obj = parse_friendly_date(" ".join(context.args))
        if not date_obj:
            await update.message.reply_text("Invalid date. Example: /pages Feb 10")
            return

    rows = fetch_open_page_requests(date_obj.isoformat() if date_obj else None)
    if not rows:
        if date_obj:
            await update.message.reply_text(f"No open pages found for {pretty_date(date_obj)}.")
        else:
            await update.message.reply_text("No open pages found right now.")
        return

    if date_obj:
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["shift"]].append(row["page_name"])

        parts = [f"📋 Available Pages for {pretty_date(date_obj)}", ""]

        if grouped["Prime"]:
            parts.append("🔥 Prime")
            for p in grouped["Prime"]:
                parts.append(f"• {p}")
            parts.append("")

        if grouped["Midshift"]:
            parts.append("🌤 Midshift")
            for p in grouped["Midshift"]:
                parts.append(f"• {p}")
            parts.append("")

        if grouped["Closing"]:
            parts.append("🌙 Closing")
            for p in grouped["Closing"]:
                parts.append(f"• {p}")
            parts.append("")

        await update.message.reply_text("\n".join(parts).strip())
        return

    by_date = defaultdict(list)
    for row in rows:
        by_date[row["request_date"]].append(row)

    chunks = []
    for date_key in sorted(by_date.keys()):
        chunks.append(f"📅 {pretty_date(date_key)}")

        grouped = defaultdict(list)
        for row in by_date[date_key]:
            grouped[row["shift"]].append(row["page_name"])

        if grouped["Prime"]:
            chunks.append("🔥 Prime")
            chunks.extend([f"• {p}" for p in grouped["Prime"]])
            chunks.append("")

        if grouped["Midshift"]:
            chunks.append("🌤 Midshift")
            chunks.extend([f"• {p}" for p in grouped["Midshift"]])
            chunks.append("")

        if grouped["Closing"]:
            chunks.append("🌙 Closing")
            chunks.extend([f"• {p}" for p in grouped["Closing"]])
            chunks.append("")

    text = "\n".join(chunks).strip()
    while len(text) > 3900:
        cut = text[:3900]
        last_break = cut.rfind("\n")
        if last_break == -1:
            last_break = 3900
        await update.message.reply_text(text[:last_break])
        text = text[last_break:].lstrip()

    if text:
        await update.message.reply_text(text)


# =========================
# MANAGER PAGE COMMANDS
# =========================
async def primepage_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    date_obj, pages = parse_page_command_args(context.args)
    if not date_obj or not pages:
        await reply_in_same_topic(update.message, "Use: /primepage Feb 10 | Dan, Autumn, Cat")
        return

    save_page_requests(
        update.effective_chat.id,
        get_topic_id(update),
        date_obj,
        "Prime",
        pages,
        update.effective_user.id,
    )

    await reply_in_same_topic(
        update.message,
        f"✅ Prime pages saved for {pretty_date(date_obj)}\n\n" + "\n".join([f"• {p}" for p in pages]),
    )


async def midshiftpage_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    date_obj, pages = parse_page_command_args(context.args)
    if not date_obj or not pages:
        await reply_in_same_topic(update.message, "Use: /midshiftpage Feb 10 | Autumn")
        return

    save_page_requests(
        update.effective_chat.id,
        get_topic_id(update),
        date_obj,
        "Midshift",
        pages,
        update.effective_user.id,
    )

    await reply_in_same_topic(
        update.message,
        f"✅ Midshift pages saved for {pretty_date(date_obj)}\n\n" + "\n".join([f"• {p}" for p in pages]),
    )


async def closingpage_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    date_obj, pages = parse_page_command_args(context.args)
    if not date_obj or not pages:
        await reply_in_same_topic(update.message, "Use: /closingpage Feb 10 | Cat, Carter")
        return

    save_page_requests(
        update.effective_chat.id,
        get_topic_id(update),
        date_obj,
        "Closing",
        pages,
        update.effective_user.id,
    )

    await reply_in_same_topic(
        update.message,
        f"✅ Closing pages saved for {pretty_date(date_obj)}\n\n" + "\n".join([f"• {p}" for p in pages]),
    )


# =========================
# MANAGER CHATTERS COMMANDS
# =========================
async def send_shift_list(message, date_obj, shift_name: str):
    rows = fetch_chatters_by_date(date_obj.isoformat(), shift_name)

    if not rows:
        await reply_in_same_topic(
            message,
            f"No {shift_name.lower()} shift chatters are available for {pretty_date(date_obj)}.",
        )
        return

    lines = [
        f"📅 {pretty_date(date_obj)}",
        f"🕒 {shift_name} Shift Coverage",
        "",
    ]

    for row in rows:
        lines.append(
            f"• {row['name']} — {row['telegram'] or '(no @username)'} — "
            f"{row['page_type']} — {row['preferred_page'] or 'Any Page'}"
        )

    await reply_in_same_topic(message, "\n".join(lines))
    await reply_in_same_topic(
        message,
        "Select a chatter to notify:",
        reply_markup=selection_keyboard(rows, date_obj.isoformat()),
    )


async def send_all_for_one_date(message, date_obj):
    rows = fetch_chatters_by_date(date_obj.isoformat())

    if not rows:
        await reply_in_same_topic(message, f"No chatters are available for {pretty_date(date_obj)}.")
        return

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["shift"]].append(
            f"• {row['name']} — {row['telegram'] or '(no @username)'} — "
            f"{row['page_type']} — {row['preferred_page'] or 'Any Page'}"
        )

    parts = [f"📅 Available Chatters for {pretty_date(date_obj)}", ""]

    if grouped["Prime"]:
        parts.append("🔥 Prime Shift")
        parts.extend(grouped["Prime"])
        parts.append("")

    if grouped["Midshift"]:
        parts.append("🌤 Midshift")
        parts.extend(grouped["Midshift"])
        parts.append("")

    if grouped["Closing"]:
        parts.append("🌙 Closing")
        parts.extend(grouped["Closing"])
        parts.append("")

    await reply_in_same_topic(message, "\n".join(parts).strip())
    await reply_in_same_topic(
        message,
        "Select a chatter to notify:",
        reply_markup=selection_keyboard(rows, date_obj.isoformat()),
    )


async def send_all_grouped_by_date(message):
    rows = fetch_all_chatters()

    if not rows:
        await reply_in_same_topic(message, "No availability records found.")
        return

    by_date = defaultdict(list)
    for row in rows:
        by_date[row["available_date"]].append(row)

    chunks = []
    for date_key in sorted(by_date.keys()):
        chunks.append(f"📅 {pretty_date(date_key)}")

        grouped = defaultdict(list)
        for row in by_date[date_key]:
            grouped[row["shift"]].append(
                f"• {row['name']} — {row['telegram'] or '(no @username)'} — "
                f"{row['page_type']} — {row['preferred_page'] or 'Any Page'}"
            )

        if grouped["Prime"]:
            chunks.append("🔥 Prime Shift")
            chunks.extend(grouped["Prime"])
            chunks.append("")

        if grouped["Midshift"]:
            chunks.append("🌤 Midshift")
            chunks.extend(grouped["Midshift"])
            chunks.append("")

        if grouped["Closing"]:
            chunks.append("🌙 Closing")
            chunks.extend(grouped["Closing"])
            chunks.append("")

    text = "\n".join(chunks).strip()
    while len(text) > 3900:
        cut = text[:3900]
        last_break = cut.rfind("\n")
        if last_break == -1:
            last_break = 3900
        await reply_in_same_topic(message, text[:last_break])
        text = text[last_break:].lstrip()

    if text:
        await reply_in_same_topic(message, text)


async def chattersprime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if not context.args:
        await reply_in_same_topic(update.message, "Use: /chattersprime Feb 10")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await reply_in_same_topic(update.message, "Invalid date. Example: /chattersprime Feb 10")
        return

    await send_shift_list(update.message, parsed, "Prime")


async def chattersmidshift_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if not context.args:
        await reply_in_same_topic(update.message, "Use: /chattersmidshift Feb 10")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await reply_in_same_topic(update.message, "Invalid date. Example: /chattersmidshift Feb 10")
        return

    await send_shift_list(update.message, parsed, "Midshift")


async def chattersclosing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if not context.args:
        await reply_in_same_topic(update.message, "Use: /chattersclosing Feb 10")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await reply_in_same_topic(update.message, "Invalid date. Example: /chattersclosing Feb 10")
        return

    await send_shift_list(update.message, parsed, "Closing")


async def chattersall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if context.args:
        parsed = parse_friendly_date(" ".join(context.args))
        if not parsed:
            await reply_in_same_topic(update.message, "Invalid date. Example: /chattersall Feb 10")
            return
        await send_all_for_one_date(update.message, parsed)
        return

    await send_all_grouped_by_date(update.message)


# =========================
# TEXT FLOW
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if update.message.text.startswith("/"):
        return

    if not is_private_chat(update):
        return

    form = context.user_data.get("availability_form")
    if not form:
        return

    if form.get("step") == "awaiting_date":
        parsed = parse_friendly_date(update.message.text.strip())
        if not parsed:
            await update.message.reply_text(
                "❌ Invalid date.\n\nPlease send your date like:\n• Feb 10\n• February 10\n• 2026-02-10"
            )
            return

        form["date"] = parsed.isoformat()
        form["step"] = "awaiting_shift"
        context.user_data["availability_form"] = form

        await update.message.reply_text(
            f"✅ Date Selected: {pretty_date(parsed)}\n\nPlease choose your shift:",
            reply_markup=shift_keyboard(),
        )


# =========================
# CALLBACKS
# =========================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data == "menu:available":
        user = query.from_user
        context.user_data["availability_form"] = {
            "step": "awaiting_date",
            "user_id": user.id,
            "private_chat_id": query.message.chat.id,
            "name": get_display_name(user),
            "telegram": get_telegram_tag(user),
        }
        await query.message.reply_text(
            "📅 Please enter your available date.\n\nExamples:\n• Feb 10\n• February 10\n• 2026-02-10"
        )
        return

    if data == "menu:pages_help":
        await query.message.reply_text("Use:\n/pages\nor\n/pages Feb 10")
        return

    if data == "menu:myavailability_help":
        await query.message.reply_text("Use:\n/myavailability Feb 10")
        return

    if data == "menu:remove_help":
        await query.message.reply_text("Use:\n/remove Feb 10")
        return

    if data.startswith("select:"):
        chat_id = query.message.chat.id
        topic_id = get_callback_topic_id(query)

        if not is_registered_manager_topic(chat_id, topic_id):
            await query.message.reply_text(
                "This topic is not registered yet. Type /register inside this topic first.",
                message_thread_id=topic_id,
            )
            return

        parts = data.split(":")
        if len(parts) < 3:
            await query.message.reply_text("Invalid selection data.", message_thread_id=topic_id)
            return

        _, user_id, date_str = parts[:3]
        manager_contact = get_manager_contact(query.from_user)

        booked_row, err = book_chatter(int(user_id), date_str, query.from_user.id)

        if err == "not_found":
            await query.message.reply_text(
                "This chatter is no longer available on that date.",
                message_thread_id=topic_id,
            )
            return

        if err == "already_booked":
            await query.message.reply_text(
                "This chatter has already been taken and removed from the list.",
                message_thread_id=topic_id,
            )
            return

        if err == "page_taken":
            await query.message.reply_text(
                "This page has already been taken by another booked chatter.",
                message_thread_id=topic_id,
            )
            return

        if not booked_row["private_chat_id"]:
            await query.message.reply_text(
                f"{booked_row['name']} was marked as booked, but the bot could not DM them because they have not started the bot in private.",
                message_thread_id=topic_id,
            )
            return

        dm_text = (
            f"Hello {booked_row['name']},\n\n"
            f"You have been selected for coverage on {pretty_date(booked_row['available_date'])}.\n\n"
            f"Shift: {booked_row['shift']}\n"
            f"Page Type: {booked_row['page_type']}\n"
            f"Preferred Page: {booked_row['preferred_page'] or 'Any Page'}\n\n"
            f"Please send a message to {manager_contact} so you can be added to the appropriate group chats.\n\n"
            f"Thank you."
        )

        try:
            await context.bot.send_message(chat_id=booked_row["private_chat_id"], text=dm_text)
            await query.message.reply_text(
                f"✅ {booked_row['name']} has been booked and removed from the available list.",
                message_thread_id=topic_id,
            )
        except Exception:
            await query.message.reply_text(
                f"✅ {booked_row['name']} has been booked and removed from the available list, but the DM could not be sent.",
                message_thread_id=topic_id,
            )
        return

    form = context.user_data.get("availability_form")
    if not form:
        await query.message.reply_text("This form expired. Please type /start again.")
        return

    if data.startswith("shift:"):
        shift = data.split(":", 1)[1]
        form["shift"] = shift
        form["step"] = "awaiting_page_type"
        context.user_data["availability_form"] = form

        await query.message.reply_text(
            "📄 Please choose your page type:",
            reply_markup=page_type_keyboard(),
        )
        return

    if data.startswith("page_type:"):
        page_type = data.split(":", 1)[1]
        form["page_type"] = page_type

        pages = fetch_open_pages_for_shift(form["date"], form["shift"])
        form["step"] = "awaiting_preferred_page"
        context.user_data["availability_form"] = form

        if pages:
            await query.message.reply_text(
                "📋 Please choose your preferred page:",
                reply_markup=preferred_page_keyboard(pages),
            )
        else:
            form["preferred_page"] = "Any Page"
            save_availability(form)
            saved_date = form["date"]
            saved_shift = form["shift"]
            saved_page_type = form["page_type"]
            preferred_page = form["preferred_page"]
            context.user_data.pop("availability_form", None)

            await query.message.reply_text(
                "✅ Availability Successfully Saved\n\n"
                f"Date: {pretty_date(saved_date)}\n"
                f"Shift: {saved_shift}\n"
                f"Page Type: {saved_page_type}\n"
                f"Preferred Page: {preferred_page}\n\n"
                "You may submit another date anytime by pressing /start."
            )
        return

    if data.startswith("prefpage:"):
        preferred_page = data.split(":", 1)[1]

        if preferred_page != "Any Page" and is_page_already_taken(form["date"], form["shift"], preferred_page):
            await query.message.reply_text(
                "That page has already been taken. Please type /available again and choose another page."
            )
            context.user_data.pop("availability_form", None)
            return

        form["preferred_page"] = preferred_page
        save_availability(form)

        saved_date = form["date"]
        saved_shift = form["shift"]
        saved_page_type = form["page_type"]
        context.user_data.pop("availability_form", None)

        await query.message.reply_text(
            "✅ Availability Successfully Saved\n\n"
            f"Date: {pretty_date(saved_date)}\n"
            f"Shift: {saved_shift}\n"
            f"Page Type: {saved_page_type}\n"
            f"Preferred Page: {preferred_page}\n\n"
            "You may submit another date anytime by pressing /start."
        )
        return


# =========================
# MAIN
# =========================
def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))

    app.add_handler(CommandHandler("register", register_command))

    app.add_handler(CommandHandler("available", available_command))
    app.add_handler(CommandHandler("myavailability", myavailability_command))
    app.add_handler(CommandHandler("remove", remove_command))
    app.add_handler(CommandHandler("pages", pages_command))

    app.add_handler(CommandHandler("primepage", primepage_command))
    app.add_handler(CommandHandler("midshiftpage", midshiftpage_command))
    app.add_handler(CommandHandler("closingpage", closingpage_command))

    app.add_handler(CommandHandler("chattersprime", chattersprime_command))
    app.add_handler(CommandHandler("chattersmidshift", chattersmidshift_command))
    app.add_handler(CommandHandler("chattersclosing", chattersclosing_command))
    app.add_handler(CommandHandler("chattersall", chattersall_command))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logging.info("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
