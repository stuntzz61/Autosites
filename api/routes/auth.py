import hmac
import hashlib
import json
from urllib.parse import parse_qsl
from typing import Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel

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


async def get_admin_user(user: dict = Depends(get_current_user)) -> dict:
    """Dependency to require admin role."""
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


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

    # Check if blocked
    if user.get('is_blocked'):
        raise HTTPException(status_code=403, detail="User is blocked")

    # Check if user should be admin (from ADMIN_IDS env)
    is_admin_by_config = tg_id in settings.admin_tg_ids
    current_role = user.get('role', 'manager')

    # Update role to admin if in ADMIN_IDS and not already admin
    if is_admin_by_config and current_role != 'admin':
        await db.update_user_role(str(user['id']), 'admin')
        user['role'] = 'admin'

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
    """Admin login with password verification."""
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

    # Debug logging
    print(f"[DEBUG] admin-login: tg_id={tg_id}, admin_ids={settings.admin_tg_ids}, user_role={user.get('role')}")

    is_admin_by_config = tg_id in settings.admin_tg_ids
    is_admin_by_role = user.get('role') == 'admin'

    # Verify password first - if password correct, allow admin access
    if request.password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")

    # If password is correct but user not admin yet, make them admin
    if not is_admin_by_role:
        await db.update_user_role(str(user['id']), 'admin')
        user['role'] = 'admin'

    return {"success": True, "role": "admin"}
