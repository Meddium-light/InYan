import os
import re
import asyncpg
import httpx
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TG_BOT_TOKEN")
MAX_TOKEN = os.getenv("MAX_BOT_TOKEN")

TG_API = f"https://api.telegram.org/bot{TG_TOKEN}"
MAX_API_URL = "https://platform-api.max.ru"

pool = None
http = httpx.AsyncClient(timeout=20.0)


# --- INIT ---

async def init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)


# --- UTILS ---

async def save_event(event_id, source):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM processed_events WHERE event_id=$1 AND source=$2",
            event_id, source
        )
        if row:
            return False
        await conn.execute(
            "INSERT INTO processed_events (event_id, source) VALUES ($1, $2)",
            event_id, source
        )
        return True


async def save_pending(code, source, chat_id):
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM pending_links WHERE chat_id=$1 AND source=$2",
            chat_id, source
        )
        await conn.execute(
            "INSERT INTO pending_links (code, source, chat_id) VALUES ($1, $2, $3)",
            code, source, chat_id
        )


async def try_link(code):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM pending_links WHERE code=$1",
            code
        )

        tg = [r for r in rows if r["source"] == "tg"]
        mx = [r for r in rows if r["source"] == "max"]

        if len(tg) == 1 and len(mx) == 1:
            tg_chat = tg[0]["chat_id"]
            mx_chat = mx[0]["chat_id"]

            await conn.execute(
                "INSERT INTO chat_map (tg_chat_id, max_chat_id) VALUES ($1, $2)",
                tg_chat, mx_chat
            )

            await conn.execute(
                "DELETE FROM pending_links WHERE code=$1",
                code
            )

            return tg_chat, mx_chat

    return None, None


async def get_mapping(source, chat_id):
    async with pool.acquire() as conn:
        if source == "tg":
            row = await conn.fetchrow(
                "SELECT max_chat_id FROM chat_map WHERE tg_chat_id=$1",
                chat_id
            )
            return row["max_chat_id"] if row else None
        else:
            row = await conn.fetchrow(
                "SELECT tg_chat_id FROM chat_map WHERE max_chat_id=$1",
                chat_id
            )
            return row["tg_chat_id"] if row else None


# --- SEND ---

async def send_to_tg(chat_id, text):
    await http.post(f"{TG_API}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })


async def send_to_max(chat_id, text):
    await http.post(
        f"{MAX_API_URL}/messages/send",
        headers={"Authorization": f"Bearer {MAX_TOKEN}"},
        json={
            "chat_id": chat_id,
            "text": text
        }
    )


# --- PARSE ---

def parse_code(text):
    m = re.match(r"/setcode\s+(\w+)", text or "")
    return m.group(1) if m else None


# --- MAIN HANDLER ---

async def handle_event(event):
    """
    Универсальный обработчик.
    ТУТ нужно адаптировать под формат Bothost.
    """

    # --- ОПРЕДЕЛЯЕМ ИСТОЧНИК ---
    source = event.get("source")  # "tg" или "max"

    if source == "tg":
        chat_id = str(event["chat_id"])
        text = event.get("text", "")
        user = event.get("user", "User")
        event_id = str(event.get("event_id"))

    else:  # MAX
        chat_id = str(event["chat_id"])
        text = event.get("text", "")
        user = event.get("user", "User")
        event_id = str(event.get("event_id"))

    # --- АНТИ-ДУБЛИ ---
    if not await save_event(event_id, source):
        return

    # --- /setcode ---
    code = parse_code(text)
    if code:
        await save_pending(code, source, chat_id)
        tg_chat, mx_chat = await try_link(code)

        if tg_chat and mx_chat:
            await send_to_tg(tg_chat, "✅ Чаты связаны")
            await send_to_max(mx_chat, "✅ Чаты связаны")
        return

    # --- ОБЫЧНОЕ СООБЩЕНИЕ ---
    target = await get_mapping(source, chat_id)
    if not target:
        return

    if source == "tg":
        await send_to_max(target, f"[TG | {user}]: {text}")
    else:
        await send_to_tg(target, f"[Max | {user}]: {text}")


# --- START ---

async def start():
    await init()
