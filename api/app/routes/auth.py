"""
Authentication routes
"""
import os
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from app.auth import get_current_user, validate_telegram_init_data
from app.database import (
    get_user_by_tgid, create_user, get_manager_stats, update_user,
    verify_admin_password, set_admin_password
)
from app.config import settings

router = APIRouter()


class TelegramAuthRequest(BaseModel):
    init_data: str


class AdminLoginRequest(BaseModel):
    password: str


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


class UserResponse(BaseModel):
    id: str
    tg_id: int
    username: Optional[str] = None
    first_name: str
    last_name: Optional[str] = None
    contact: Optional[str] = None
    role: str
    approval_status: str
    created_at: str
    stats: Optional[dict] = None


@router.post("/telegram")
async def auth_telegram(request: TelegramAuthRequest):
    """
    Authenticate via Telegram WebApp initData
    """
    # Validate init data
    validated = validate_telegram_init_data(request.init_data)
    tg_user = validated["user"]

    # Get or create user
    user = get_user_by_tgid(tg_user["id"])

    if not user:
        # Check if admin
        role = "admin" if tg_user["id"] in settings.ADMIN_TG_IDS else "manager"
        approval = "approved" if role == "admin" else "pending"

        user = create_user(
            tg_id=tg_user["id"],
            first_name=tg_user.get("first_name", "Unknown"),
            last_name=tg_user.get("last_name"),
            username=tg_user.get("username"),
            role=role,
        )

        if not user:
            raise HTTPException(status_code=500, detail="Failed to create user")

    # Get stats
    stats = get_manager_stats(str(user["id"])) if user["role"] == "manager" else None

    return {
        "user": {
            "id": str(user["id"]),
            "tg_id": user["tg_id"],
            "username": user.get("username"),
            "first_name": user["first_name"],
            "last_name": user.get("last_name"),
            "contact": user.get("contact"),
            "role": user["role"],
            "approval_status": user.get("approval_status", "approved"),
            "created_at": str(user["created_at"]),
            "stats": stats,
        }
    }


@router.get("/me")
async def get_me(user: dict = Depends(get_current_user)):
    """
    Get current user info
    """
    stats = get_manager_stats(str(user["id"])) if user["role"] == "manager" else None

    return {
        "user": {
            "id": str(user["id"]),
            "tg_id": user["tg_id"],
            "username": user.get("username"),
            "first_name": user["first_name"],
            "last_name": user.get("last_name"),
            "contact": user.get("contact"),
            "role": user["role"],
            "approval_status": user.get("approval_status", "approved"),
            "created_at": str(user["created_at"]),
            "stats": stats,
        }
    }


@router.post("/admin-login")
async def admin_login(request: AdminLoginRequest, user: dict = Depends(get_current_user)):
    """
    Login as admin with password (from database)
    """
    # Verify password against database
    if not verify_admin_password(request.password):
        raise HTTPException(status_code=401, detail="Неверный пароль")

    # Update user role to admin
    update_user(str(user["id"]), role="admin", approval_status="approved")

    return {"success": True, "message": "Вы авторизованы как администратор"}


@router.post("/admin-change-password")
async def admin_change_password(request: ChangePasswordRequest, user: dict = Depends(get_current_user)):
    """
    Change admin password (admin only)
    """
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Доступ запрещён")

    # Verify old password
    if not verify_admin_password(request.old_password):
        raise HTTPException(status_code=401, detail="Неверный текущий пароль")

    # Validate new password
    if len(request.new_password) < 6:
        raise HTTPException(status_code=400, detail="Пароль должен быть не менее 6 символов")

    # Set new password
    set_admin_password(request.new_password)

    return {"success": True, "message": "Пароль успешно изменён"}
