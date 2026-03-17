import os
import re
import asyncio
from datetime import datetime, timedelta

import asyncpg
import httpx
from fastapi import FastAPI, Request
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
DATABASE_URL = os.getenv("DATABASE_URL")
TG_TOKEN = os.getenv("TG_BOT_TOKEN")
MAX_TOKEN = os.getenv("MAX_BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
CODE_TTL_MINUTES = int(os.getenv("CODE_TTL_MINUTES", "10"))
MAX_API_URL = os.getenv("MAX_API_URL", "https://platform-api.max.ru")

TG_API = f"https://api.telegram.org/bot{TG_TOKEN}"

app = FastAPI()
pool: asyncpg.Pool = None
http = httpx.AsyncClient(timeout=20.0)

# --- UTILS ---

async def save_event(event_id: str, source: str) -> bool:
    """Return True if new event, False if duplicate."""
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

async def save_pending(code: str, source: str, chat_id: str):
    async with pool.acquire() as conn:
        # удалим старые записи для этого чата (чтобы не копились)
        await conn.execute(
            "DELETE FROM pending_links WHERE chat_id=$1 AND source=$2",
            chat_id, source
        )
        await conn.execute(
            "INSERT INTO pending_links (code, source, chat_id) VALUES ($1, $2, $3)",
            code, source, chat_id
        )

async def cleanup_pending():
    async with pool.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM pending_links
            WHERE created_at < NOW() - ($1 || ' minutes')::interval
            """,
            CODE_TTL_MINUTES
        )

async def try_link(code: str):
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

            # проверки уникальности (1↔1)
            exists_tg = await conn.fetchrow(
                "SELECT 1 FROM chat_map WHERE tg_chat_id=$1", tg_chat
            )
            exists_mx = await conn.fetchrow(
                "SELECT 1 FROM chat_map WHERE max_chat_id=$1", mx_chat
            )
            if exists_tg or exists_mx:
                return ("exists", tg_chat, mx_chat)

            await conn.execute(
                "INSERT INTO chat_map (tg_chat_id, max_chat_id) VALUES ($1, $2)",
                tg_chat, mx_chat
            )
            await conn.execute(
                "DELETE FROM pending_links WHERE code=$1", code
            )
            return ("linked", tg_chat, mx_chat)

    return ("pending", None, None)

async def get_mapping(source: str, chat_id: str):
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

# --- SENDERS ---

async def send_to_tg(chat_id: str, text: str):
    await http.post(f"{TG_API}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

async def send_to_max(chat_id: str, text: str):
    await http.post(
        f"{MAX_API_URL}/messages/send",
        headers={"Authorization": f"Bearer {MAX_TOKEN}"},
        json={
            "chat_id": chat_id,
            "text": text
        }
    )

# --- COMMAND PARSER ---

def parse_setcode(text: str):
    m = re.match(r"^/setcode\s+([A-Za-z0-9_\-]+)", text or "")
    return m.group(1) if m else None

# --- WEBHOOKS ---

@app.post("/webhook/tg")
async def webhook_tg(req: Request):
    data = await req.json()

    message = data.get("message") or data.get("edited_message")
    if not message:
        return {"ok": True}

    chat_id = str(message["chat"]["id"])
    text = message.get("text") or ""
    user = message.get("from", {}).get("first_name", "User")

    event_id = str(message.get("message_id"))

    if not await save_event(event_id, "tg"):
        return {"ok": True}

    code = parse_setcode(text)
    if code:
        await cleanup_pending()
        await save_pending(code, "tg", chat_id)
        status, tg_chat, mx_chat = await try_link(code)

        if status == "linked":
            await send_to_tg(chat_id, "✅ Чаты связаны")
            await send_to_max(mx_chat, "✅ Чаты связаны")
        elif status == "exists":
            await send_to_tg(chat_id, "⚠️ Этот чат уже связан")
        else:
            await send_to_tg(chat_id, "⏳ Код сохранён, ждём вторую сторону")
        return {"ok": True}

    # обычное сообщение
    mx_chat = await get_mapping("tg", chat_id)
    if not mx_chat:
        return {"ok": True}

    await send_to_max(mx_chat, f"[TG | {user}]: {text}")
    return {"ok": True}


@app.post("/webhook/max")
async def webhook_max(req: Request):
    data = await req.json()

    # структура может отличаться — подправишь под реальный payload MAX
    event_id = str(data.get("event_id", ""))
    chat_id = str(data.get("chat_id"))
    text = data.get("text", "")
    user = data.get("user", {}).get("name", "User")

    if not await save_event(event_id, "max"):
        return {"ok": True}

    code = parse_setcode(text)
    if code:
        await cleanup_pending()
        await save_pending(code, "max", chat_id)
        status, tg_chat, mx_chat = await try_link(code)

        if status == "linked":
            await send_to_max(chat_id, "✅ Чаты связаны")
            await send_to_tg(tg_chat, "✅ Чаты связаны")
        elif status == "exists":
            await send_to_max(chat_id, "⚠️ Этот чат уже связан")
        else:
            await send_to_max(chat_id, "⏳ Код сохранён, ждём вторую сторону")
        return {"ok": True}

    # обычное сообщение
    tg_chat = await get_mapping("max", chat_id)
    if not tg_chat:
        return {"ok": True}

    await send_to_tg(tg_chat, f"[Max | {user}]: {text}")
    return {"ok": True}


# --- STARTUP ---

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)


@app.on_event("shutdown")
async def shutdown():
    await http.aclose()
    await pool.close()
