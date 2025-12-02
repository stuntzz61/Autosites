from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from routes.auth import get_current_user
import db

router = APIRouter()


class UpdateProfile(BaseModel):
    contact: Optional[str] = None


@router.get("")
async def get_profile(user: dict = Depends(get_current_user)):
    """Get current user profile."""
    stats = await db.get_user_stats(str(user['id']))
    return {
        **user,
        "id": str(user['id']),
        "stats": stats,
    }


@router.patch("")
async def update_profile(data: UpdateProfile, user: dict = Depends(get_current_user)):
    """Update current user profile."""
    if data.contact is not None:
        await db.update_user_contact(str(user['id']), data.contact)

    # Return updated user
    updated = await db.get_user_by_id(str(user['id']))
    stats = await db.get_user_stats(str(user['id']))

    return {
        **updated,
        "id": str(updated['id']),
        "stats": stats,
    }


@router.get("/stats")
async def get_stats(user: dict = Depends(get_current_user)):
    """Get current user statistics."""
    return await db.get_user_stats(str(user['id']))

