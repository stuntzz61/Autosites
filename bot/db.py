"""Database operations for the bot."""
import os
from typing import Optional, Dict
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")

pool: Optional[AsyncConnectionPool] = None


async def init_pool():
    global pool
    pool = AsyncConnectionPool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        open=False,
    )
    await pool.open()


async def close_pool():
    global pool
    if pool:
        await pool.close()


async def get_conn():
    return pool.connection()


async def get_user_by_tg_id(tg_id: int) -> Optional[Dict]:
    """Get user by Telegram ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role,
                          approval_status, is_blocked, created_at
                   FROM users WHERE tg_id = %s""",
                (tg_id,)
            )
            return await cur.fetchone()


async def create_user(
    tg_id: int,
    username: str,
    first_name: str,
    last_name: str,
    role: str = "manager",
    approval_status: str = "pending"
) -> Dict:
    """Create a new user."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO users (tg_id, username, first_name, last_name, role, approval_status)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   RETURNING id, tg_id, username, first_name, last_name, role, approval_status, created_at""",
                (tg_id, username, first_name, last_name, role, approval_status)
            )
            await conn.commit()
            return await cur.fetchone()


async def approve_user_by_tg_id(tg_id: int):
    """Approve a user by their Telegram ID."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'approved',
                   approved_at = NOW() WHERE tg_id = %s""",
                (tg_id,)
            )
            await conn.commit()


async def reject_user_by_tg_id(tg_id: int, reason: str):
    """Reject a user by their Telegram ID."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'rejected',
                   rejection_reason = %s WHERE tg_id = %s""",
                (reason, tg_id)
            )
            await conn.commit()


async def get_user_tg_id_by_id(user_id: str) -> Optional[int]:
    """Get Telegram ID by user UUID."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT tg_id FROM users WHERE id = %s",
                (user_id,)
            )
            row = await cur.fetchone()
            return row[0] if row else None


async def set_user_role(tg_id: int, role: str):
    """Set user role by Telegram ID."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET role = %s, approval_status = 'approved'
                   WHERE tg_id = %s""",
                (role, tg_id)
            )
            await conn.commit()


async def get_all_admins() -> list:
    """Get all admin users."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, tg_id, username, first_name FROM users WHERE role = 'admin'"
            )
            return await cur.fetchall()

