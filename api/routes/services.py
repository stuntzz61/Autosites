"""
Routes for additional services, service categories and manager feedback.
"""
from typing import Optional, List
import logging

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from routes.auth import get_current_user, get_admin_user
import db

log = logging.getLogger(__name__)

router = APIRouter()


# ==================== Models ====================

class AddServiceRequest(BaseModel):
    service_id: str
    notes: Optional[str] = None
    price: Optional[str] = None


class CreateCategoryRequest(BaseModel):
    name: str
    parent_id: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    sort_order: Optional[int] = 0


class UpdateCategoryRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None
    parent_id: Optional[str] = None


class UpdateServiceRequest(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    price: Optional[str] = None


class CreateFeedbackRequest(BaseModel):
    subject: str
    message: str
    category: Optional[str] = "general"
    priority: Optional[str] = "normal"
    request_id: Optional[str] = None


class RespondFeedbackRequest(BaseModel):
    response: str
    status: Optional[str] = "answered"


# ==================== Additional Services ====================

@router.get("/additional-services")
async def list_services(user: dict = Depends(get_current_user)):
    """List all available additional services."""
    services = await db.list_additional_services(active_only=True)
    return [
        {**s, "id": str(s["id"])}
        for s in services
    ]


@router.get("/requests/{request_id}/services")
async def get_request_services(request_id: str, user: dict = Depends(get_current_user)):
    """Get additional services for a request."""
    # Check request ownership
    request = await db.get_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    services = await db.get_request_additional_services(request_id)
    return [
        {**s, "id": str(s["id"]), "service_id": str(s["service_id"])}
        for s in services
    ]


@router.post("/requests/{request_id}/services")
async def add_service_to_request(
    request_id: str,
    data: AddServiceRequest,
    user: dict = Depends(get_current_user)
):
    """Add additional service to a request."""
    # Check request ownership
    request = await db.get_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    result = await db.add_request_additional_service(
        request_id=request_id,
        service_id=data.service_id,
        added_by=str(user['id']),
        notes=data.notes,
        price=data.price
    )

    return {**result, "id": str(result["id"])}


@router.patch("/requests/{request_id}/services/{service_id}")
async def update_request_service(
    request_id: str,
    service_id: str,
    data: UpdateServiceRequest,
    user: dict = Depends(get_current_user)
):
    """Update additional service for a request."""
    # Check request ownership
    request = await db.get_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    result = await db.update_request_additional_service(
        request_id=request_id,
        service_id=service_id,
        status=data.status,
        notes=data.notes,
        price=data.price
    )

    if not result:
        raise HTTPException(status_code=404, detail="Service not found")

    return {**result, "id": str(result["id"])}


@router.delete("/requests/{request_id}/services/{service_id}")
async def remove_service_from_request(
    request_id: str,
    service_id: str,
    user: dict = Depends(get_current_user)
):
    """Remove additional service from a request."""
    # Check request ownership
    request = await db.get_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    await db.remove_request_additional_service(request_id, service_id)
    return {"success": True}


# ==================== Manager Feedback ====================

async def notify_bot_new_feedback(manager_name: str, subject: str, priority: str):
    """Notify admins about new feedback."""
    import httpx
    from config import settings

    if not settings.BOT_WEBHOOK_URL:
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "new_feedback",
                    "manager_name": manager_name,
                    "subject": subject,
                    "priority": priority
                },
                timeout=5.0
            )
    except Exception as e:
        log.error(f"Failed to notify bot about feedback: {e}")


async def notify_bot_feedback_response(tg_id: int, subject: str, response: str):
    """Notify manager about admin response."""
    import httpx
    from config import settings

    if not settings.BOT_WEBHOOK_URL:
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "feedback_response",
                    "tg_id": tg_id,
                    "subject": subject,
                    "response": response
                },
                timeout=5.0
            )
    except Exception as e:
        log.error(f"Failed to notify manager about response: {e}")


@router.post("/feedback")
async def create_feedback(data: CreateFeedbackRequest, user: dict = Depends(get_current_user)):
    """Create a new feedback message from manager to admin."""
    feedback = await db.create_feedback(
        manager_id=str(user['id']),
        subject=data.subject,
        message=data.message,
        category=data.category,
        priority=data.priority,
        request_id=data.request_id
    )

    # Notify admins
    manager_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or 'Менеджер'
    await notify_bot_new_feedback(manager_name, data.subject, data.priority or 'normal')

    return {**feedback, "id": str(feedback["id"])}


@router.get("/feedback")
async def list_my_feedback(
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
    user: dict = Depends(get_current_user)
):
    """List feedback messages for current user."""
    offset = (page - 1) * limit

    feedback_list = await db.list_feedback(
        status=status,
        manager_id=str(user['id']),
        limit=limit,
        offset=offset
    )

    return {
        "items": [
            {**f, "id": str(f["id"]), "manager_id": str(f["manager_id"])}
            for f in feedback_list
        ],
        "page": page,
        "limit": limit
    }


@router.get("/feedback/{feedback_id}")
async def get_feedback_detail(feedback_id: str, user: dict = Depends(get_current_user)):
    """Get feedback details."""
    feedback = await db.get_feedback(feedback_id)

    if not feedback:
        raise HTTPException(status_code=404, detail="Feedback not found")

    # Check ownership (admin can view all)
    if user['role'] != 'admin' and str(feedback['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    return {**feedback, "id": str(feedback["id"]), "manager_id": str(feedback["manager_id"])}


# ==================== Admin Feedback Routes ====================

@router.get("/admin/feedback")
async def admin_list_feedback(
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    user: dict = Depends(get_admin_user)
):
    """List all feedback messages (admin only)."""
    offset = (page - 1) * limit

    feedback_list = await db.list_feedback(
        status=status,
        limit=limit,
        offset=offset
    )

    new_count = await db.count_new_feedback()

    return {
        "items": [
            {**f, "id": str(f["id"]), "manager_id": str(f["manager_id"])}
            for f in feedback_list
        ],
        "page": page,
        "limit": limit,
        "new_count": new_count
    }


@router.post("/admin/feedback/{feedback_id}/respond")
async def admin_respond_feedback(
    feedback_id: str,
    data: RespondFeedbackRequest,
    user: dict = Depends(get_admin_user)
):
    """Respond to feedback (admin only)."""
    # Get feedback to find manager tg_id
    feedback = await db.get_feedback(feedback_id)
    if not feedback:
        raise HTTPException(status_code=404, detail="Feedback not found")

    result = await db.respond_to_feedback(
        feedback_id=feedback_id,
        admin_id=str(user['id']),
        response=data.response,
        new_status=data.status
    )

    if not result:
        raise HTTPException(status_code=404, detail="Feedback not found")

    # Notify manager via Telegram
    if feedback.get('manager_tg_id'):
        await notify_bot_feedback_response(
            tg_id=feedback['manager_tg_id'],
            subject=feedback.get('subject', ''),
            response=data.response
        )

    return {**result, "id": str(result["id"])}


@router.patch("/admin/feedback/{feedback_id}/status")
async def admin_update_feedback_status(
    feedback_id: str,
    status: str,
    user: dict = Depends(get_admin_user)
):
    """Update feedback status (admin only)."""
    await db.update_feedback_status(feedback_id, status)
    return {"success": True}


@router.get("/admin/feedback/count")
async def admin_feedback_count(user: dict = Depends(get_admin_user)):
    """Get count of new feedback messages."""
    count = await db.count_new_feedback()
    return {"new_count": count}


# ==================== Service Categories ====================

@router.get("/categories")
async def list_categories(
    parent_id: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    """List service categories. If parent_id is provided, returns subcategories."""
    categories = await db.list_service_categories(parent_id=parent_id)
    return [
        {**c, "id": str(c["id"]), "parent_id": str(c["parent_id"]) if c.get("parent_id") else None}
        for c in categories
    ]


@router.get("/categories/tree")
async def get_category_tree(user: dict = Depends(get_current_user)):
    """Get full hierarchical category tree."""
    tree = await db.get_service_category_tree()
    return [
        {**c, "id": str(c["id"]), "parent_id": str(c["parent_id"]) if c.get("parent_id") else None}
        for c in tree
    ]


@router.post("/categories")
async def create_category(data: CreateCategoryRequest, user: dict = Depends(get_admin_user)):
    """Create a new service category (admin only)."""
    category = await db.create_service_category(
        name=data.name,
        parent_id=data.parent_id,
        description=data.description,
        icon=data.icon,
        sort_order=data.sort_order
    )
    return {**category, "id": str(category["id"])}


@router.patch("/categories/{category_id}")
async def update_category(
    category_id: str,
    data: UpdateCategoryRequest,
    user: dict = Depends(get_admin_user)
):
    """Update a service category (admin only)."""
    update_data = data.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(status_code=400, detail="No data to update")

    category = await db.update_service_category(category_id, update_data)
    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    return {**category, "id": str(category["id"])}


@router.delete("/categories/{category_id}")
async def delete_category(category_id: str, user: dict = Depends(get_admin_user)):
    """Delete a service category (admin only). Also deletes subcategories."""
    await db.delete_service_category(category_id)
    return {"success": True}

