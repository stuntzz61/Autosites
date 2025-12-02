"""
Telegram WebApp authentication
"""
import hashlib
import hmac
import json
import logging
from typing import Optional
from urllib.parse import parse_qs
from datetime import datetime, timedelta

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.config import settings
from app.database import get_user_by_tgid, create_user, get_user_by_id

log = logging.getLogger("auth")

security = HTTPBearer(auto_error=False)


def validate_telegram_init_data(init_data: str) -> dict:
    """
    Validate Telegram WebApp initData
    Returns user data if valid, raises exception if invalid
    """
    if not init_data:
        raise HTTPException(status_code=401, detail="Missing init data")

    # Parse the init data
    parsed = parse_qs(init_data)

    # Extract hash
    received_hash = parsed.get("hash", [None])[0]
    if not received_hash:
        raise HTTPException(status_code=401, detail="Missing hash")

    # Build data check string (sorted key=value pairs, excluding hash)
    data_pairs = []
    for key, values in parsed.items():
        if key != "hash":
            data_pairs.append(f"{key}={values[0]}")

    data_pairs.sort()
    data_check_string = "\n".join(data_pairs)

    # Calculate secret key
    secret_key = hmac.new(
        b"WebAppData",
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
    if calculated_hash != received_hash:
        log.warning(f"Invalid hash: expected {calculated_hash}, got {received_hash}")
        raise HTTPException(status_code=401, detail="Invalid init data")

    # Check auth_date (not older than 24 hours)
    auth_date = parsed.get("auth_date", [None])[0]
    if auth_date:
        auth_datetime = datetime.fromtimestamp(int(auth_date))
        if datetime.now() - auth_datetime > timedelta(hours=24):
            raise HTTPException(status_code=401, detail="Init data expired")

    # Parse user data
    user_data = parsed.get("user", [None])[0]
    if not user_data:
        raise HTTPException(status_code=401, detail="Missing user data")

    try:
        user = json.loads(user_data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=401, detail="Invalid user data")

    return {
        "user": user,
        "auth_date": auth_date,
        "start_param": parsed.get("start_param", [None])[0],
    }


async def get_current_user(request: Request) -> dict:
    """
    Get current user from Telegram initData
    """
    # Get init data from header
    init_data = request.headers.get("X-Telegram-Init-Data")

    if not init_data:
        # For development, allow API key auth
        if settings.DEBUG:
            api_key = request.headers.get("X-API-Key")
            if api_key == settings.SECRET_KEY:
                return {"id": "dev-user", "role": "admin"}

        raise HTTPException(status_code=401, detail="Not authenticated")

    # Validate init data
    validated = validate_telegram_init_data(init_data)
    tg_user = validated["user"]

    # Get or create user
    user = get_user_by_tgid(tg_user["id"])

    if not user:
        # Check if this is admin
        role = "admin" if tg_user["id"] in settings.ADMIN_TG_IDS else "manager"

        user = create_user(
            tg_id=tg_user["id"],
            first_name=tg_user.get("first_name", "Unknown"),
            last_name=tg_user.get("last_name"),
            username=tg_user.get("username"),
            role=role,
        )

        if not user:
            raise HTTPException(status_code=500, detail="Failed to create user")

        log.info(f"Created new user: {tg_user['id']} ({role})")

    return user


async def get_current_approved_user(user: dict = Depends(get_current_user)) -> dict:
    """
    Get current user, ensuring they are approved
    """
    if user.get("approval_status") == "pending":
        raise HTTPException(status_code=403, detail="Account pending approval")

    if user.get("approval_status") == "rejected":
        raise HTTPException(status_code=403, detail="Account rejected")

    if user.get("is_blocked"):
        raise HTTPException(status_code=403, detail="Account blocked")

    return user


async def get_admin_user(user: dict = Depends(get_current_user)) -> dict:
    """
    Get current user, ensuring they are admin
    """
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    return user

