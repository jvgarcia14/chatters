import os
import re
import logging
from datetime import datetime
from collections import defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor
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


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

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
        CREATE INDEX IF NOT EXISTS idx_availability_date
        ON availability (available_date);
        """
    )

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
            [InlineKeyboardButton("🔞 Nude", callback_data="page:Nude")],
            [InlineKeyboardButton("✨ Non-Nude", callback_data="page:Non-Nude")],
            [InlineKeyboardButton("🔄 Both", callback_data="page:Both")],
        ]
    )


def start_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Submit Availability", callback_data="menu:available")],
            [InlineKeyboardButton("📌 View My Availability", callback_data="menu:myavailability_help")],
            [InlineKeyboardButton("🗑 Remove Availability", callback_data="menu:remove_help")],
        ]
    )


def selection_keyboard(rows, iso_date, manager_contact):
    keyboard = []
    for row in rows:
        label = f"{row['name']} | {row['shift']} | {row['page_type']}"
        callback = f"select:{row['user_id']}:{iso_date}:{manager_contact}"
        if len(callback) > 64:
            callback = f"select:{row['user_id']}:{iso_date}"
        keyboard.append([InlineKeyboardButton(label[:60], callback_data=callback)])
    return InlineKeyboardMarkup(keyboard)


def chatter_welcome_text():
    return (
        "👋 Welcome to the Chatter Availability Bot\n\n"
        "Please submit your availability so managers can review coverage quickly and contact you when needed.\n\n"
        "What you can do here:\n"
        "• Submit your available date\n"
        "• Choose your shift\n"
        "• Choose your page type\n"
        "• Check or remove your saved availability"
    )


def manager_help():
    return (
        "📋 Manager Commands\n\n"
        "/register — register this topic\n"
        "/chattersprime Feb 9\n"
        "/chattersmidshift Feb 9\n"
        "/chattersclosing Feb 9\n"
        "/chattersall\n"
        "/chattersall Feb 9\n\n"
        "Use these commands only inside the registered topic."
    )


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
        **kwargs
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_private_chat(update):
        await update.message.reply_text(
            chatter_welcome_text(),
            reply_markup=start_keyboard()
        )
    else:
        await reply_in_same_topic(update.message, manager_help())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_private_chat(update):
        await update.message.reply_text(
            chatter_welcome_text(),
            reply_markup=start_keyboard()
        )
    else:
        await reply_in_same_topic(update.message, manager_help())


async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_private_chat(update):
        await update.message.reply_text("Please use /register inside your manager topic.")
        return

    topic_id = get_topic_id(update)
    if topic_id is None:
        await update.message.reply_text(
            "Please use /register inside a forum topic, not in the main group."
        )
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
        "✅ This topic has been successfully registered for manager commands."
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
        "📅 Please enter your available date.\n\n"
        "Examples:\n"
        "• Feb 9\n"
        "• February 9\n"
        "• 2026-02-09"
    )


async def myavailability_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        await reply_in_same_topic(update.message, "Please use this in DM with the bot.")
        return

    if not context.args:
        await update.message.reply_text("Use: /myavailability Feb 9")
        return

    date_text = " ".join(context.args)
    parsed = parse_friendly_date(date_text)
    if not parsed:
        await update.message.reply_text("Invalid date. Example: Feb 9")
        return

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT available_date, shift, page_type
        FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (update.effective_user.id, parsed.isoformat()),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        await update.message.reply_text(
            f"No saved availability found for {pretty_date(parsed)}."
        )
        return

    await update.message.reply_text(
        f"📌 Your Saved Availability\n\n"
        f"Date: {pretty_date(row['available_date'])}\n"
        f"Shift: {row['shift']}\n"
        f"Page Type: {row['page_type']}"
    )


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        await reply_in_same_topic(update.message, "Please use this in DM with the bot.")
        return

    if not context.args:
        await update.message.reply_text("Use: /remove Feb 9")
        return

    date_text = " ".join(context.args)
    parsed = parse_friendly_date(date_text)
    if not parsed:
        await update.message.reply_text("Invalid date. Example: Feb 9")
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (update.effective_user.id, parsed.isoformat()),
    )
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    if deleted == 0:
        await update.message.reply_text(
            f"No saved availability was found for {pretty_date(parsed)}."
        )
        return

    await update.message.reply_text(
        f"✅ Your availability for {pretty_date(parsed)} has been removed."
    )


async def fetch_chatters_by_date(date_iso: str, shift=None):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if shift:
        cur.execute(
            """
            SELECT user_id, private_chat_id, name, telegram, available_date, shift, page_type
            FROM availability
            WHERE available_date = %s AND shift = %s
            ORDER BY name ASC
            """,
            (date_iso, shift),
        )
    else:
        cur.execute(
            """
            SELECT user_id, private_chat_id, name, telegram, available_date, shift, page_type
            FROM availability
            WHERE available_date = %s
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


async def fetch_all_chatters():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT user_id, private_chat_id, name, telegram, available_date, shift, page_type
        FROM availability
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


async def send_shift_list(message, manager_user, date_obj, shift_name: str):
    rows = await fetch_chatters_by_date(date_obj.isoformat(), shift_name)

    if not rows:
        await reply_in_same_topic(
            message,
            f"No {shift_name.lower()} shift chatters are available for {pretty_date(date_obj)}."
        )
        return

    lines = [
        f"📅 {pretty_date(date_obj)}",
        f"🕒 {shift_name} Shift Coverage",
        ""
    ]

    for row in rows:
        lines.append(f"• {row['name']} — {row['telegram'] or '(no @username)'} — {row['page_type']}")

    manager_contact = get_manager_contact(manager_user)

    await reply_in_same_topic(message, "\n".join(lines))
    await reply_in_same_topic(
        message,
        "Select a chatter to notify:",
        reply_markup=selection_keyboard(rows, date_obj.isoformat(), manager_contact),
    )


async def send_all_for_one_date(message, manager_user, date_obj):
    rows = await fetch_chatters_by_date(date_obj.isoformat())

    if not rows:
        await reply_in_same_topic(
            message,
            f"No chatters are available for {pretty_date(date_obj)}."
        )
        return

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["shift"]].append(
            f"• {row['name']} — {row['telegram'] or '(no @username)'} — {row['page_type']}"
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

    manager_contact = get_manager_contact(manager_user)

    await reply_in_same_topic(message, "\n".join(parts).strip())
    await reply_in_same_topic(
        message,
        "Select a chatter to notify:",
        reply_markup=selection_keyboard(rows, date_obj.isoformat(), manager_contact),
    )


async def send_all_grouped_by_date(message):
    rows = await fetch_all_chatters()

    if not rows:
        await reply_in_same_topic(message, "No availability records found.")
        return

    by_date = defaultdict(list)
    for row in rows:
        by_date[row["available_date"]].append(row)

    chunks = []
    for date_key in sorted(by_date.keys()):
        date_rows = by_date[date_key]
        chunks.append(f"📅 {pretty_date(date_key)}")

        grouped = defaultdict(list)
        for row in date_rows:
            grouped[row["shift"]].append(
                f"• {row['name']} — {row['telegram'] or '(no @username)'} — {row['page_type']}"
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
            "This topic is not registered yet. Type /register inside this topic first."
        )
        return False

    return True


async def chattersprime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if not context.args:
        await reply_in_same_topic(update.message, "Use: /chattersprime Feb 9")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await reply_in_same_topic(update.message, "Invalid date. Example: /chattersprime Feb 9")
        return

    await send_shift_list(update.message, update.effective_user, parsed, "Prime")


async def chattersmidshift_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if not context.args:
        await reply_in_same_topic(update.message, "Use: /chattersmidshift Feb 9")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await reply_in_same_topic(update.message, "Invalid date. Example: /chattersmidshift Feb 9")
        return

    await send_shift_list(update.message, update.effective_user, parsed, "Midshift")


async def chattersclosing_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if not context.args:
        await reply_in_same_topic(update.message, "Use: /chattersclosing Feb 9")
        return

    parsed = parse_friendly_date(" ".join(context.args))
    if not parsed:
        await reply_in_same_topic(update.message, "Invalid date. Example: /chattersclosing Feb 9")
        return

    await send_shift_list(update.message, update.effective_user, parsed, "Closing")


async def chattersall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_manager_topic(update):
        return

    if context.args:
        parsed = parse_friendly_date(" ".join(context.args))
        if not parsed:
            await reply_in_same_topic(update.message, "Invalid date. Example: /chattersall Feb 9")
            return
        await send_all_for_one_date(update.message, update.effective_user, parsed)
        return

    await send_all_grouped_by_date(update.message)


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
                "❌ Invalid date.\n\nPlease send your date like:\n• Feb 9\n• February 9\n• 2026-02-09"
            )
            return

        form["date"] = parsed.isoformat()
        form["step"] = "awaiting_shift"
        context.user_data["availability_form"] = form

        await update.message.reply_text(
            f"✅ Date Selected: {pretty_date(parsed)}\n\nPlease choose your shift:",
            reply_markup=shift_keyboard(),
        )


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
            "📅 Please enter your available date.\n\nExamples:\n• Feb 9\n• February 9\n• 2026-02-09"
        )
        return

    if data == "menu:myavailability_help":
        await query.message.reply_text("Use this command:\n/myavailability Feb 9")
        return

    if data == "menu:remove_help":
        await query.message.reply_text("Use this command:\n/remove Feb 9")
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
        if len(parts) >= 4:
            _, user_id, date_str, manager_contact = parts[0], parts[1], parts[2], ":".join(parts[3:])
        else:
            _, user_id, date_str = parts
            manager_contact = get_manager_contact(query.from_user)

        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT user_id, private_chat_id, name, telegram, shift, page_type, available_date
            FROM availability
            WHERE user_id = %s AND available_date = %s
            """,
            (int(user_id), date_str),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            await query.message.reply_text(
                "This chatter is no longer available on that date.",
                message_thread_id=topic_id,
            )
            return

        if not row["private_chat_id"]:
            await query.message.reply_text(
                f"Cannot DM {row['name']}. They need to start the bot in private first.",
                message_thread_id=topic_id,
            )
            return

        dm_text = (
            f"Hello {row['name']},\n\n"
            f"You have been selected for coverage on {pretty_date(row['available_date'])}.\n\n"
            f"Shift: {row['shift']}\n"
            f"Page Type: {row['page_type']}\n\n"
            f"Please send a message to {manager_contact} so you can be added to the appropriate group chats.\n\n"
            f"Thank you."
        )

        try:
            await context.bot.send_message(chat_id=row["private_chat_id"], text=dm_text)
            await query.message.reply_text(
                f"✅ Notification sent to {row['name']} {row['telegram'] or ''}",
                message_thread_id=topic_id,
            )
        except Exception:
            await query.message.reply_text(
                f"❌ Could not send a DM to {row['name']}. They may not have started the bot yet.",
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

    if data.startswith("page:"):
        page_type = data.split(":", 1)[1]
        form["page_type"] = page_type

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO availability (
                user_id, private_chat_id, name, telegram, available_date, shift, page_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, available_date)
            DO UPDATE SET
                private_chat_id = EXCLUDED.private_chat_id,
                name = EXCLUDED.name,
                telegram = EXCLUDED.telegram,
                shift = EXCLUDED.shift,
                page_type = EXCLUDED.page_type,
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
            ),
        )
        conn.commit()
        cur.close()
        conn.close()

        saved_date = form["date"]
        saved_shift = form["shift"]
        saved_page_type = form["page_type"]
        context.user_data.pop("availability_form", None)

        await query.message.reply_text(
            "✅ Availability Successfully Saved\n\n"
            f"Date: {pretty_date(saved_date)}\n"
            f"Shift: {saved_shift}\n"
            f"Page Type: {saved_page_type}\n\n"
            "You may submit another date anytime by pressing /start."
        )


def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("register", register_command))
    app.add_handler(CommandHandler("available", available_command))
    app.add_handler(CommandHandler("myavailability", myavailability_command))
    app.add_handler(CommandHandler("remove", remove_command))
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
