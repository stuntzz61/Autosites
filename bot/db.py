"""Database operations for the bot with robust connection handling."""
import os
import asyncio
from typing import Optional, Dict
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")

pool: Optional[AsyncConnectionPool] = None


async def init_pool():
    """Initialize the connection pool with health checks."""
    global pool
    pool = AsyncConnectionPool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        open=False,
        # Check connections before use
        check=AsyncConnectionPool.check_connection,
        # Timeout for getting connection
        timeout=30,
        # Max time connection can be idle
        max_idle=300,
        # Reconnect on failure
        reconnect_timeout=5,
    )
    await pool.open()


async def close_pool():
    """Close the connection pool."""
    global pool
    if pool:
        await pool.close()


@asynccontextmanager
async def get_conn():
    """Get a connection with automatic reconnection on failure."""
    global pool
    max_retries = 3
    retry_delay = 0.5

    for attempt in range(max_retries):
        try:
            async with pool.connection() as conn:
                yield conn
                return
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            if attempt < max_retries - 1:
                # Log and retry
                print(f"DB connection error (attempt {attempt + 1}): {e}")
                await asyncio.sleep(retry_delay * (attempt + 1))
                # Try to reset the pool
                try:
                    await pool.check()
                except:
                    pass
            else:
                raise


async def get_user_by_tg_id(tg_id: int) -> Optional[Dict]:
    """Get user by Telegram ID."""
    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role,
                          approval_status, created_at, is_blocked
                   FROM users WHERE tg_id = %s""",
                (tg_id,)
            )
            result = await cur.fetchone()
            if result and result.get('is_blocked') is None:
                result['is_blocked'] = False
            return result


async def create_user(
    tg_id: int,
    username: str,
    first_name: str,
    last_name: str,
    role: str = "manager",
    approval_status: str = "pending"
) -> Dict:
    """Create a new user."""
    async with get_conn() as conn:
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
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'approved',
                   approved_at = NOW() WHERE tg_id = %s""",
                (tg_id,)
            )
            await conn.commit()


async def reject_user_by_tg_id(tg_id: int, reason: str):
    """Reject a user by their Telegram ID."""
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'rejected',
                   rejection_reason = %s WHERE tg_id = %s""",
                (reason, tg_id)
            )
            await conn.commit()


async def get_user_tg_id_by_id(user_id: str) -> Optional[int]:
    """Get Telegram ID by user UUID."""
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT tg_id FROM users WHERE id = %s",
                (user_id,)
            )
            row = await cur.fetchone()
            return row[0] if row else None


async def set_user_role(tg_id: int, role: str):
    """Set user role by Telegram ID."""
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET role = %s, approval_status = 'approved'
                   WHERE tg_id = %s""",
                (role, tg_id)
            )
            await conn.commit()


async def get_all_admins() -> list:
    """Get all admin users."""
    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, tg_id, username, first_name FROM users WHERE role = 'admin'"
            )
            return await cur.fetchall()


async def get_user_by_id(user_id: str) -> Optional[Dict]:
    """Get user by UUID."""
    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role,
                          approval_status, created_at, is_blocked
                   FROM users WHERE id = %s""",
                (user_id,)
            )
            return await cur.fetchone()


async def validate_invite_code(code: str) -> tuple:
    """
    Validate an invite code.
    Returns (is_valid, invite_code_data or error_message)
    """
    from datetime import datetime, timezone

    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT ic.*,
                          ag.name as group_name,
                          u.first_name as creator_first_name,
                          u.last_name as creator_last_name
                   FROM invite_codes ic
                   LEFT JOIN admin_groups ag ON ag.id = ic.group_id
                   LEFT JOIN users u ON u.id = ic.created_by
                   WHERE ic.code = %s""",
                (code.upper(),)
            )
            invite = await cur.fetchone()

    if not invite:
        return False, "Неверный инвайт-код"

    if not invite['is_active']:
        return False, "Инвайт-код деактивирован"

    # Check expiration
    if invite['expires_at'] and invite['expires_at'].replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        return False, "Инвайт-код истёк"

    # Check usage limit
    if invite['max_uses'] is not None and invite['uses_count'] >= invite['max_uses']:
        return False, "Инвайт-код исчерпан (достигнут лимит использований)"

    return True, invite


async def create_user_with_invite(
    tg_id: int,
    username: str,
    first_name: str,
    last_name: str,
    invite_code: str
) -> Optional[Dict]:
    """Create user using an invite code."""
    # Validate invite first
    is_valid, result = await validate_invite_code(invite_code)
    if not is_valid:
        return None

    invite = result

    async with get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Determine approval status
            approval_status = 'approved' if invite.get('auto_approve') else 'pending'

            # Create user with group assignment
            await cur.execute(
                """INSERT INTO users (tg_id, username, first_name, last_name, role,
                                     approval_status, registered_via_code, admin_group_id)
                   VALUES (%s, %s, %s, %s, 'manager', %s, %s, %s)
                   RETURNING id, tg_id, username, first_name, last_name, role,
                             approval_status, created_at, admin_group_id""",
                (tg_id, username, first_name, last_name, approval_status,
                 invite['id'], invite.get('group_id'))
            )
            user = await cur.fetchone()
            user_id = str(user['id'])

            # Record invite code usage
            await cur.execute(
                """INSERT INTO invite_code_usage (invite_code_id, user_id)
                   VALUES (%s, %s)""",
                (invite['id'], user_id)
            )

            # Increment usage count
            await cur.execute(
                "UPDATE invite_codes SET uses_count = uses_count + 1 WHERE id = %s",
                (invite['id'],)
            )

            # Add user to group if specified
            if invite.get('group_id'):
                await cur.execute(
                    """INSERT INTO user_group_membership (user_id, group_id, role, added_by)
                       VALUES (%s, %s, 'member', %s)
                       ON CONFLICT (user_id, group_id) DO NOTHING""",
                    (user_id, invite['group_id'], invite.get('created_by'))
                )

            # Set approved_at if auto-approved
            if approval_status == 'approved':
                await cur.execute(
                    "UPDATE users SET approved_at = NOW() WHERE id = %s",
                    (user_id,)
                )

            await conn.commit()
            return user
