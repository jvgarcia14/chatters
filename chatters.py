import os
import re
import logging
from datetime import datetime
from collections import defaultdict

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
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
MANAGER_CHAT_IDS = [
    x.strip() for x in os.getenv("MANAGER_CHAT_IDS", "").split(",") if x.strip()
]
MANAGER_CONTACT = os.getenv("MANAGER_CONTACT", "@yourmanagerusername")

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


def is_valid_date(date_str: str) -> bool:
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_display_name(user) -> str:
    full = f"{user.first_name or ''} {user.last_name or ''}".strip()
    if full:
        return full
    if user.username:
        return user.username
    return f"User {user.id}"


def get_telegram_tag(user) -> str:
    return f"@{user.username}" if user.username else "(no @username)"


def is_manager_chat(chat_id: int) -> bool:
    if not MANAGER_CHAT_IDS:
        return True
    return str(chat_id) in MANAGER_CHAT_IDS


def shift_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Prime", callback_data="shift:Prime")],
            [InlineKeyboardButton("Midshift", callback_data="shift:Midshift")],
            [InlineKeyboardButton("Closing", callback_data="shift:Closing")],
        ]
    )


def page_type_keyboard():
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Nude", callback_data="page:Nude")],
            [InlineKeyboardButton("Non-Nude", callback_data="page:Non-Nude")],
            [InlineKeyboardButton("Both", callback_data="page:Both")],
        ]
    )


HELP_TEXT = """🤖 Chatter Availability Bot

Chatters:
/available - submit your available date, shift, and page type
/myavailability YYYY-MM-DD - see your saved availability
/remove YYYY-MM-DD - remove your availability for that date

Managers:
/chatters YYYY-MM-DD - show chatters available on that date and select one

Important:
Every chatter must press /start in the bot first so the bot can send them a DM.
"""


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)


async def available_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    context.user_data["availability_form"] = {
        "step": "awaiting_date",
        "user_id": user.id,
        "private_chat_id": update.effective_chat.id,
        "name": get_display_name(user),
        "telegram": get_telegram_tag(user),
    }

    await update.message.reply_text(
        "📅 Send your available date in this format:\nYYYY-MM-DD\n\nExample: 2026-04-01"
    )


async def myavailability_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Use: /myavailability YYYY-MM-DD")
        return

    date_str = context.args[0]
    if not is_valid_date(date_str):
        await update.message.reply_text("Invalid date. Use: YYYY-MM-DD")
        return

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT available_date, shift, page_type
        FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (update.effective_user.id, date_str),
    )

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        await update.message.reply_text(f"No saved availability found for {date_str}.")
        return

    await update.message.reply_text(
        f"📌 Your availability for {date_str}\n"
        f"Shift: {row['shift']}\n"
        f"Page type: {row['page_type']}"
    )


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Use: /remove YYYY-MM-DD")
        return

    date_str = context.args[0]
    if not is_valid_date(date_str):
        await update.message.reply_text("Invalid date. Use: YYYY-MM-DD")
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM availability
        WHERE user_id = %s AND available_date = %s
        """,
        (update.effective_user.id, date_str),
    )

    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    if deleted == 0:
        await update.message.reply_text(f"Nothing to remove for {date_str}.")
        return

    await update.message.reply_text(f"✅ Removed your availability for {date_str}.")


async def chatters_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_manager_chat(update.effective_chat.id):
        await update.message.reply_text(
            "❌ This chat is not allowed to use /chatters."
        )
        return

    if not context.args:
        await update.message.reply_text("Use: /chatters YYYY-MM-DD")
        return

    date_str = context.args[0]
    if not is_valid_date(date_str):
        await update.message.reply_text("Invalid date. Use: YYYY-MM-DD")
        return

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT user_id, private_chat_id, name, telegram, shift, page_type
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
        (date_str,),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        await update.message.reply_text(f"No chatters available for {date_str}.")
        return

    parts = [f"📅 Available Chatters — {date_str}", ""]

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["shift"]].append(
            f"• {row['name']} — {row['telegram'] or '(no @username)'} — {row['page_type']}"
        )

    if grouped["Prime"]:
        parts.append("🔥 Prime")
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

    await update.message.reply_text("\n".join(parts).strip())

    keyboard = []
    for row in rows:
        label = f"{row['name']} | {row['shift']} | {row['page_type']}"
        callback = f"select:{row['user_id']}:{date_str}"
        keyboard.append([InlineKeyboardButton(label[:60], callback_data=callback)])

    await update.message.reply_text(
        "Select a chatter to notify:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if update.message.text.startswith("/"):
        return

    form = context.user_data.get("availability_form")
    if not form:
        return

    if form.get("step") == "awaiting_date":
        date_str = update.message.text.strip()

        if not is_valid_date(date_str):
            await update.message.reply_text(
                "❌ Invalid date format. Send it like this: YYYY-MM-DD"
            )
            return

        form["date"] = date_str
        form["step"] = "awaiting_shift"
        context.user_data["availability_form"] = form

        await update.message.reply_text(
            "🕒 Select your shift:",
            reply_markup=shift_keyboard(),
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""

    # manager selects chatter
    if data.startswith("select:"):
        try:
            _, user_id, date_str = data.split(":", 2)
        except ValueError:
            await query.message.reply_text("Invalid selection.")
            return

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
            await query.message.reply_text("This chatter is no longer available on that date.")
            return

        if not row["private_chat_id"]:
            await query.message.reply_text(
                f"Cannot DM {row['name']}. They need to press /start in the bot first."
            )
            return

        dm_text = (
            f"Hi {row['name']}! We need you for {date_str}.\n\n"
            f"Shift: {row['shift']}\n"
            f"Page type: {row['page_type']}\n\n"
            f"Please DM our managers so we can add you to our GCs.\n"
            f"Manager contact: {MANAGER_CONTACT}"
        )

        try:
            await context.bot.send_message(chat_id=row["private_chat_id"], text=dm_text)
            await query.message.reply_text(
                f"✅ Message sent to {row['name']} {row['telegram'] or ''}"
            )
        except Exception:
            await query.message.reply_text(
                f"❌ Could not send DM to {row['name']}. They may not have started the bot in private."
            )
        return

    form = context.user_data.get("availability_form")
    if not form:
        await query.message.reply_text("This form expired. Type /available again.")
        return

    if data.startswith("shift:"):
        shift = data.split(":", 1)[1]
        form["shift"] = shift
        form["step"] = "awaiting_page_type"
        context.user_data["availability_form"] = form

        await query.message.reply_text(
            "📄 Select page type:",
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
                page_type = EXCLUDED.page_type
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

        context.user_data.pop("availability_form", None)

        await query.message.reply_text(
            "✅ Availability saved\n"
            f"Date: {form['date']}\n"
            f"Shift: {form['shift']}\n"
            f"Page type: {form['page_type']}"
        )


def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("available", available_command))
    app.add_handler(CommandHandler("myavailability", myavailability_command))
    app.add_handler(CommandHandler("remove", remove_command))
    app.add_handler(CommandHandler("chatters", chatters_command))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logging.info("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
