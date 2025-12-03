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
    """Delete a request. Managers can delete their own drafts/errors, admins can delete any."""
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Managers can only delete drafts or error requests
    if user['role'] != 'admin':
        status = request.get('payload', {}).get('site', {}).get('meta', {}).get('status') or request.get('status', 'draft')
        if status not in ['draft', 'error', 'generated_error']:
            raise HTTPException(status_code=403, detail="Можно удалить только черновики или заявки с ошибками")

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
    webhook_url = settings.N8N_WEBHOOK_URL
    if not webhook_url:
        log.warning("N8N_WEBHOOK_URL not configured - skipping webhook call")
    else:
        try:
            # Build payload in the expected format
            payload = request.get('payload', {}) or {}
            webhook_data = {
                "request_id": request_id,
                "manager_id": str(request.get('user_id', '')),
                "client": payload.get('client', {}),
                "site": payload.get('site', {}),
            }

            log.info(f"Sending to n8n webhook: {webhook_url}")
            log.debug(f"Webhook data: {webhook_data}")

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    webhook_url,
                    json=webhook_data,
                    timeout=30.0
                )
                response.raise_for_status()
                log.info(f"Successfully sent to n8n webhook: {webhook_url}")
        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                error_json = e.response.json()
                error_detail = error_json.get('message', '')
                if 'not registered' in error_detail.lower() or 'not active' in error_detail.lower():
                    log.warning(f"n8n webhook not active/registered: {error_detail}")
                    log.warning("Workflow must be activated in n8n or use test URL (/webhook-test/)")
                else:
                    log.error(f"n8n webhook returned {e.response.status_code}: {error_detail}")
            except:
                log.error(f"n8n webhook returned {e.response.status_code}: {e.response.text}")

            # Don't fail the request - webhook can be configured later
            # Status is already updated to 'in_queue'
            log.warning("Request status updated to 'in_queue' but webhook call failed")
        except Exception as e:
            log.error(f"Error sending to n8n: {e}")
            # Don't fail the request - webhook can be configured later
            log.warning("Request status updated to 'in_queue' but webhook call failed")

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
    payload = request.get('payload', {}) or {}
    site = payload.get('site', {}) or {}

    # Store photos in assets.images for consistency
    assets = site.get('assets', {}) or {}
    images = assets.get('images', []) or []

    for url in uploaded_urls:
        images.append({
            'url': url,
            'category': category,
            'alt': category
        })

    assets['images'] = images
    site['assets'] = assets
    payload['site'] = site

    log.info(f"Updating request {request_id} with {len(images)} images")
    await db.update_request(request_id, {'payload': payload})

    # Verify update
    updated = await db.get_request(request_id)
    updated_images = updated.get('payload', {}).get('site', {}).get('assets', {}).get('images', [])
    log.info(f"Request {request_id} now has {len(updated_images)} images")

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

