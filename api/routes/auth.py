import hmac
import hashlib
import json
from urllib.parse import parse_qsl
from typing import Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel
from psycopg.rows import dict_row

from config import settings
import db

router = APIRouter()


class VerifyRequest(BaseModel):
    initData: str
    invite_code: Optional[str] = None  # Optional invite code for registration


def verify_telegram_init_data(init_data: str) -> Optional[dict]:
    """Verify Telegram WebApp init data and extract user info."""
    try:
        # Parse the init data
        parsed = dict(parse_qsl(init_data))

        # Get the hash
        received_hash = parsed.pop('hash', None)
        if not received_hash:
            return None

        # Create data check string
        data_check_string = '\n'.join(
            f'{k}={v}' for k, v in sorted(parsed.items())
        )

        # Calculate secret key
        secret_key = hmac.new(
            b'WebAppData',
            settings.BOT_TOKEN.encode(),
            hashlib.sha256
        ).digest()

        # Calculate hash
        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256
        ).hexdigest()

        # Verify hash
        if not hmac.compare_digest(calculated_hash, received_hash):
            return None

        # Check auth_date (allow 24 hours)
        auth_date = int(parsed.get('auth_date', 0))
        if datetime.utcnow().timestamp() - auth_date > 86400:
            return None

        # Parse user data
        user_data = json.loads(parsed.get('user', '{}'))
        return user_data
    except Exception as e:
        print(f"Verification error: {e}")
        return None


async def get_current_user(x_telegram_init_data: str = Header(None)) -> dict:
    """Dependency to get current user from init data."""
    if not x_telegram_init_data:
        raise HTTPException(status_code=401, detail="Missing init data")

    user_data = verify_telegram_init_data(x_telegram_init_data)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid init data")

    tg_id = user_data.get('id')
    if not tg_id:
        raise HTTPException(status_code=401, detail="Invalid user data")

    # Get user from database
    user = await db.get_user_by_tg_id(tg_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    if user.get('is_blocked'):
        raise HTTPException(status_code=403, detail="User is blocked")

    return user


async def get_supervisor_user(user: dict = Depends(get_current_user)) -> dict:
    """Dependency to require supervisor role or higher (supervisor, director, owner)."""
    role = user.get('role')
    if role not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Supervisor access required")
    return user


async def get_director_user(user: dict = Depends(get_current_user)) -> dict:
    """Dependency to require director or owner role."""
    role = user.get('role')
    if role not in ('director', 'owner'):
        raise HTTPException(status_code=403, detail="Director access required")
    return user


async def get_owner_user(user: dict = Depends(get_current_user)) -> dict:
    """Dependency to require owner role."""
    if user.get('role') != 'owner':
        raise HTTPException(status_code=403, detail="Owner access required")
    return user


# Helper function to check if user has supervisor role or higher
def is_supervisor_role(role: str) -> bool:
    """Check if role is supervisor, director, or owner."""
    return role in ('supervisor', 'director', 'owner')

# Legacy alias for backward compatibility
async def get_admin_user(user: dict = Depends(get_current_user)) -> dict:
    """Legacy dependency - maps to get_supervisor_user for backward compatibility."""
    return await get_supervisor_user(user)


@router.post("/verify")
async def verify_init_data(request: VerifyRequest):
    """Verify Telegram init data and return user info."""
    user_data = verify_telegram_init_data(request.initData)

    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid init data")

    tg_id = user_data.get('id')
    username = user_data.get('username', '')
    first_name = user_data.get('first_name', '')
    last_name = user_data.get('last_name', '')

    # Check if user exists
    user = await db.get_user_by_tg_id(tg_id)

    if not user:
        # Create new user - optionally with invite code
        if request.invite_code:
            # Validate invite code first
            is_valid, result = await db.validate_invite_code(request.invite_code)
            if not is_valid:
                raise HTTPException(status_code=400, detail=result)

            # Create user with invite code
            user = await db.create_user_with_invite(
                tg_id, username, first_name, last_name,
                invite_code=request.invite_code
            )
        else:
            # Create user without invite code
            user = await db.create_user(tg_id, username, first_name, last_name)
    else:
        # User exists - update invite code if provided
        if request.invite_code:
            # Validate invite code first
            is_valid, result = await db.validate_invite_code(request.invite_code)
            if is_valid:
                invite = result
                # Update user's invite code and group
                async with await db.get_conn() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        # Check if user already used this invite
                        await cur.execute(
                            "SELECT id FROM invite_code_usage WHERE invite_code_id = %s AND user_id = %s",
                            (invite['id'], str(user['id']))
                        )
                        if not await cur.fetchone():
                            # Record usage
                            await cur.execute(
                                """INSERT INTO invite_code_usage (invite_code_id, user_id)
                                   VALUES (%s, %s)""",
                                (invite['id'], str(user['id']))
                            )
                            # Increment usage count
                            await cur.execute(
                                "UPDATE invite_codes SET uses_count = uses_count + 1 WHERE id = %s",
                                (invite['id'],)
                            )

                        # Update user's registered_via_code and group
                        await cur.execute(
                            """UPDATE users
                               SET registered_via_code = %s, admin_group_id = %s,
                                   approval_status = CASE
                                       WHEN %s THEN 'approved'
                                       ELSE approval_status
                                   END
                               WHERE id = %s
                               RETURNING *""",
                            (invite['id'], invite.get('group_id'), invite.get('auto_approve'), str(user['id']))
                        )
                        user = await cur.fetchone()

                        # Add to group if specified
                        if invite.get('group_id'):
                            await cur.execute(
                                """INSERT INTO user_group_membership (user_id, group_id, role, added_by)
                                   VALUES (%s, %s, 'member', %s)
                                   ON CONFLICT (user_id, group_id) DO UPDATE SET role = 'member'""",
                                (str(user['id']), invite['group_id'], invite['created_by'])
                            )

                        await conn.commit()

    # Check if blocked
    if user.get('is_blocked'):
        raise HTTPException(status_code=403, detail="User is blocked")

    # Assign roles based on config (only for initial setup, owner can manage roles later via UI)
    current_role = user.get('role', 'manager')

    # Check if user should be Owner (from OWNER_IDS env) - highest priority
    # Owner role can only be set via config, not via UI
    # If user is in OWNER_IDS, always set to owner regardless of current role
    is_owner_by_config = tg_id in settings.owner_tg_ids
    if is_owner_by_config:
        if current_role != 'owner':
            print(f"[DEBUG] Setting user {tg_id} to owner role (was {current_role})")
            await db.update_user_role(str(user['id']), 'owner')
            user['role'] = 'owner'
        else:
            print(f"[DEBUG] User {tg_id} already has owner role")
    # Check if user should be Director (from DIRECTOR_IDS env) - only if not owner
    # After initial setup, directors are managed by owner via UI
    elif not is_owner_by_config:
        is_director_by_config = tg_id in settings.director_tg_ids
        # Only assign director from config if user is still a manager (not manually assigned to supervisor/director)
        if is_director_by_config and current_role == 'manager':
            await db.update_user_role(str(user['id']), 'director')
            user['role'] = 'director'
        # Check if user should be Supervisor (from SUPERVISOR_IDS or ADMIN_IDS env) - only for initial setup
        elif not is_director_by_config and current_role == 'manager':
            is_supervisor_by_config = tg_id in settings.supervisor_tg_ids
            if is_supervisor_by_config:
                await db.update_user_role(str(user['id']), 'supervisor')
                user['role'] = 'supervisor'

    # Get user stats
    stats = await db.get_user_stats(str(user['id']))

    return {
        "user": {
            **user,
            "id": str(user['id']),
            "stats": stats,
        }
    }


@router.get("/me")
async def get_me(user: dict = Depends(get_current_user)):
    """Get current user info."""
    stats = await db.get_user_stats(str(user['id']))
    return {
        **user,
        "id": str(user['id']),
        "stats": stats,
    }


class AdminLoginRequest(BaseModel):
    password: str


class DevLoginRequest(BaseModel):
    tg_id: int


@router.post("/dev-login")
async def dev_login(request: DevLoginRequest):
    """Dev mode login - bypass Telegram verification for development."""
    if not settings.DEBUG:
        raise HTTPException(status_code=403, detail="Dev login only available in DEBUG mode")

    tg_id = request.tg_id

    # Get or create user
    user = await db.get_user_by_tg_id(tg_id)

    if not user:
        # Create test user
        user = await db.create_user(tg_id, 'testuser', 'Test', 'User')

    # Check if blocked
    if user.get('is_blocked'):
        raise HTTPException(status_code=403, detail="User is blocked")

    # Get user stats
    stats = await db.get_user_stats(str(user['id']))

    return {
        "user": {
            **user,
            "id": str(user['id']),
            "stats": stats,
        }
    }


@router.post("/admin-login")
async def admin_login(request: AdminLoginRequest, x_telegram_init_data: str = Header(None)):
    """Supervisor login with password verification.
    Note: Password alone does NOT grant admin access - user must be assigned role by owner/director."""
    # First verify the user via Telegram
    if not x_telegram_init_data:
        raise HTTPException(status_code=401, detail="Missing init data")

    user_data = verify_telegram_init_data(x_telegram_init_data)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid init data")

    tg_id = user_data.get('id')
    if not tg_id:
        raise HTTPException(status_code=401, detail="Invalid user data")

    # Check if user exists
    user = await db.get_user_by_tg_id(tg_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    # Verify password
    if request.password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")

    # Check if user has supervisor role or higher
    role = user.get('role')
    if role not in ('supervisor', 'director', 'owner'):
        raise HTTPException(
            status_code=403,
            detail="Access denied. You must be assigned supervisor role by owner or director."
        )

    return {"success": True, "role": role}
