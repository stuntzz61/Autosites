"""
Profile routes
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_approved_user
from app.database import update_user, get_manager_stats

router = APIRouter()


class UpdateContactRequest(BaseModel):
    contact: str


@router.get("/me")
async def get_profile(user: dict = Depends(get_current_approved_user)):
    """Get current user profile with stats"""
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


@router.patch("/contact")
async def update_contact(
    data: UpdateContactRequest,
    user: dict = Depends(get_current_approved_user)
):
    """Update user contact"""
    # Validate contact
    contact = data.contact.strip()

    if len(contact) < 5:
        raise HTTPException(status_code=400, detail="Contact too short")

    if len(contact) > 100:
        raise HTTPException(status_code=400, detail="Contact too long")

    # Update
    update_user(str(user["id"]), contact=contact)

    return {"success": True}

