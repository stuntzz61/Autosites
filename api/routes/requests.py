from typing import Optional, List
import json
import httpx
import logging

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form, Query
from pydantic import BaseModel

from config import settings
from routes.auth import get_current_user
import db
import s3

log = logging.getLogger(__name__)

router = APIRouter()


class CreateRequest(BaseModel):
    company_name: str
    client_name: str
    payload: Optional[dict] = None


class UpdateRequest(BaseModel):
    company_name: Optional[str] = None
    client_name: Optional[str] = None
    payload: Optional[dict] = None


class UpdateStatus(BaseModel):
    status: str


@router.get("")
async def list_requests(
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    user: dict = Depends(get_current_user)
):
    """List requests for current user."""
    offset = (page - 1) * limit

    # Regular users can only see their own requests
    user_id = str(user['id']) if user['role'] != 'admin' else None

    print(f"[DEBUG] list_requests: user_id={user_id}, role={user.get('role')}, status={status}")

    requests = await db.list_requests(
        user_id=user_id,
        status=status,
        limit=limit,
        offset=offset
    )

    print(f"[DEBUG] list_requests: found {len(requests)} requests")

    return {
        "items": [
            {**r, "id": str(r["id"])}
            for r in requests
        ],
        "page": page,
        "limit": limit,
    }


@router.get("/{request_id}")
async def get_request(request_id: str, user: dict = Depends(get_current_user)):
    """Get a specific request."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership (admin can view all)
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    return {**request, "id": str(request["id"])}


@router.post("")
async def create_request(data: CreateRequest, user: dict = Depends(get_current_user)):
    """Create a new request."""
    # Check if user is approved
    if user.get('approval_status') != 'approved' and user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Account not approved")

    request = await db.create_request(
        user_id=str(user['id']),
        company_name=data.company_name,
        client_name=data.client_name,
        payload=data.payload or {}
    )

    return {**request, "id": str(request["id"])}


@router.patch("/{request_id}")
async def update_request(
    request_id: str,
    data: UpdateRequest,
    user: dict = Depends(get_current_user)
):
    """Update a request."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    update_data = data.model_dump(exclude_none=True)
    updated = await db.update_request(request_id, update_data)

    return {**updated, "id": str(updated["id"])}


@router.patch("/{request_id}/status")
async def update_status(
    request_id: str,
    data: UpdateStatus,
    user: dict = Depends(get_current_user)
):
    """Update request status."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    await db.update_request_status(request_id, data.status)

    return {"success": True}


@router.post("/{request_id}/archive")
async def archive_request(request_id: str, user: dict = Depends(get_current_user)):
    """Archive a request."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    await db.archive_request(request_id)

    return {"success": True}


@router.delete("/{request_id}")
async def delete_request(request_id: str, user: dict = Depends(get_current_user)):
    """Delete a request (admin only for permanent delete)."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Only admin can delete, managers can only archive
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Only admins can delete requests")

    await db.delete_request(request_id)

    return {"success": True}


@router.post("/{request_id}/generate")
async def generate_site(request_id: str, user: dict = Depends(get_current_user)):
    """Send request to n8n for generation."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Update status
    await db.update_request_status(request_id, 'in_queue')

    # Send to n8n webhook
    if settings.N8N_WEBHOOK_URL:
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    settings.N8N_WEBHOOK_URL,
                    json={
                        "request_id": request_id,
                        "payload": request.get('payload', {}),
                    },
                    timeout=10.0
                )
        except Exception as e:
            print(f"Error sending to n8n: {e}")
            # Still return success - webhook might be processed later

    return {"success": True, "status": "in_queue"}


@router.post("/{request_id}/photos")
async def upload_photos(
    request_id: str,
    category: str = Form(...),
    files: List[UploadFile] = File(None),
    file: UploadFile = File(None),
    user: dict = Depends(get_current_user)
):
    """Upload photos for a request."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Handle both single file and multiple files
    upload_files = []
    if files:
        upload_files = files
    elif file:
        upload_files = [file]

    if not upload_files:
        raise HTTPException(status_code=422, detail="No files provided")

    # Upload files to S3
    uploaded_urls = []
    for f in upload_files:
        if not f.filename:
            continue
        try:
            url = await s3.upload_file_to_s3(f, request_id, category)
            uploaded_urls.append(url)
            log.info(f"Uploaded photo: {url}")
        except Exception as e:
            log.error(f"Failed to upload file: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")

    if not uploaded_urls:
        raise HTTPException(status_code=422, detail="No valid files to upload")

    # Update request payload with photo URLs
    payload = request.get('payload', {})
    site = payload.get('site', {})

    # Store photos in assets.images for consistency
    assets = site.get('assets', {})
    images = assets.get('images', [])

    for url in uploaded_urls:
        images.append({
            'url': url,
            'category': category,
            'alt': category
        })

    assets['images'] = images
    site['assets'] = assets
    payload['site'] = site

    await db.update_request(request_id, {'payload': payload})

    return {"urls": uploaded_urls}


@router.delete("/{request_id}/photos")
async def delete_photo(
    request_id: str,
    url: str = Query(...),
    user: dict = Depends(get_current_user)
):
    """Delete a photo from a request."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Delete from S3
    deleted = await s3.delete_file_from_s3(url)
    if not deleted:
        log.warning(f"Failed to delete photo from S3: {url}")

    # Remove from payload
    payload = request.get('payload', {})
    site = payload.get('site', {})
    assets = site.get('assets', {})
    images = assets.get('images', [])

    # Filter out the deleted image
    images = [img for img in images if img.get('url') != url]

    assets['images'] = images
    site['assets'] = assets
    payload['site'] = site

    await db.update_request(request_id, {'payload': payload})

    return {"success": True}

