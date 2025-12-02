"""
Request routes
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
import httpx

from app.auth import get_current_approved_user
from app.database import (
    get_requests_by_manager, count_requests_by_manager,
    get_request_by_id, create_request, update_request, delete_request
)
from app.config import settings
from app.s3 import upload_file_to_s3

log = logging.getLogger("requests")

router = APIRouter()


class RequestPayload(BaseModel):
    client: Optional[dict] = None
    site: Optional[dict] = None


class CreateRequestRequest(BaseModel):
    client: Optional[dict] = None
    site: Optional[dict] = None


class UpdateStatusRequest(BaseModel):
    status: str


# ==================== CRUD ====================

@router.get("")
async def list_requests(
    page: int = 1,
    per_page: int = 20,
    archived: bool = False,
    user: dict = Depends(get_current_approved_user)
):
    """List user's requests"""
    manager_id = str(user["id"])
    offset = (page - 1) * per_page

    requests = get_requests_by_manager(manager_id, archived, per_page, offset)
    total = count_requests_by_manager(manager_id, archived)

    return {
        "items": [
            {
                "id": str(r["id"]),
                "manager_id": str(r["manager_id"]),
                "status": r["status"],
                "client_name": r.get("client_name"),
                "company_name": r.get("company_name"),
                "business_type": r.get("business_type"),
                "result_url": r.get("result_url"),
                "created_at": str(r["created_at"]),
                "photos_count": 0,  # TODO: calculate
            }
            for r in requests
        ],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    }


@router.get("/{request_id}")
async def get_request(
    request_id: str,
    user: dict = Depends(get_current_approved_user)
):
    """Get request by ID"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership (unless admin)
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    return {
        "id": str(request["id"]),
        "manager_id": str(request["manager_id"]),
        "status": request["status"],
        "payload": request.get("payload", {}),
        "result_url": request.get("result_url"),
        "created_at": str(request["created_at"]),
        "updated_at": str(request["updated_at"]) if request.get("updated_at") else None,
    }


@router.post("")
async def create_new_request(
    data: CreateRequestRequest,
    user: dict = Depends(get_current_approved_user)
):
    """Create new request"""
    payload = {
        "client": data.client or {},
        "site": data.site or {},
    }

    request = create_request(str(user["id"]), payload)

    if not request:
        raise HTTPException(status_code=500, detail="Failed to create request")

    return {
        "id": str(request["id"]),
        "manager_id": str(request["manager_id"]),
        "status": request["status"],
        "created_at": str(request["created_at"]),
    }


@router.patch("/{request_id}")
async def update_existing_request(
    request_id: str,
    data: RequestPayload,
    user: dict = Depends(get_current_approved_user)
):
    """Update request"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    # Merge payload
    existing_payload = request.get("payload", {})
    if data.client:
        existing_payload["client"] = {**existing_payload.get("client", {}), **data.client}
    if data.site:
        existing_payload["site"] = {**existing_payload.get("site", {}), **data.site}

    update_request(request_id, payload=existing_payload)

    return {"success": True}


@router.delete("/{request_id}")
async def delete_existing_request(
    request_id: str,
    user: dict = Depends(get_current_approved_user)
):
    """Delete request"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership (admin can delete any)
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    delete_request(request_id)

    return {"success": True}


# ==================== Status ====================

@router.patch("/{request_id}/status")
async def update_status(
    request_id: str,
    data: UpdateStatusRequest,
    user: dict = Depends(get_current_approved_user)
):
    """Update request status"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    valid_statuses = [
        "draft", "collecting_info", "collecting_photos", "ready_to_generate",
        "queued", "generating", "generated_ok", "generated_error",
        "delivered", "closed", "archived"
    ]

    if data.status not in valid_statuses:
        raise HTTPException(status_code=400, detail="Invalid status")

    update_request(request_id, status=data.status)

    return {"success": True}


@router.post("/{request_id}/archive")
async def archive_request(
    request_id: str,
    user: dict = Depends(get_current_approved_user)
):
    """Archive request"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    update_request(request_id, status="archived")

    return {"success": True}


# ==================== Generation ====================

@router.post("/{request_id}/generate")
async def generate_site(
    request_id: str,
    user: dict = Depends(get_current_approved_user)
):
    """Trigger site generation"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if can generate
    if request["status"] not in ["draft", "collecting_info", "collecting_photos", "ready_to_generate"]:
        raise HTTPException(status_code=400, detail="Cannot generate from current status")

    # Update status
    update_request(request_id, status="queued")

    # Trigger N8N webhook (async)
    if settings.N8N_WEBHOOK_URL:
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    settings.N8N_WEBHOOK_URL,
                    json={
                        "request_id": request_id,
                        "payload": request.get("payload", {}),
                        "manager_tg_id": user["tg_id"],
                    },
                    timeout=10.0
                )
        except Exception as e:
            log.error(f"Failed to trigger N8N webhook: {e}")
            # Don't fail the request, just log the error

    return {"success": True, "status": "queued"}


# ==================== Photos ====================

@router.post("/{request_id}/photos")
async def upload_photo(
    request_id: str,
    file: UploadFile = File(...),
    category: str = Form(default="gallery"),
    user: dict = Depends(get_current_approved_user)
):
    """Upload photo to request"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    # Upload to S3
    try:
        url = await upload_file_to_s3(file, request_id, category)
    except Exception as e:
        log.error(f"Failed to upload photo: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload photo")

    # Update payload
    payload = request.get("payload", {})
    if "site" not in payload:
        payload["site"] = {}
    if "assets" not in payload["site"]:
        payload["site"]["assets"] = {}
    if "images" not in payload["site"]["assets"]:
        payload["site"]["assets"]["images"] = []

    payload["site"]["assets"]["images"].append({
        "url": url,
        "category": category,
        "alt": file.filename,
    })

    update_request(request_id, payload=payload)

    return {"url": url}


@router.delete("/{request_id}/photos")
async def delete_photo(
    request_id: str,
    url: str,
    user: dict = Depends(get_current_approved_user)
):
    """Delete photo from request"""
    request = get_request_by_id(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user["role"] != "admin" and str(request["manager_id"]) != str(user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    # Remove from payload
    payload = request.get("payload", {})
    images = payload.get("site", {}).get("assets", {}).get("images", [])

    payload["site"]["assets"]["images"] = [img for img in images if img.get("url") != url]

    update_request(request_id, payload=payload)

    # TODO: Delete from S3

    return {"success": True}

