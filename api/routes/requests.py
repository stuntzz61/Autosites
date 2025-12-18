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
    tariff: Optional[str] = 'standard'
    chat_id: Optional[int] = None  # Telegram chat ID


class UpdateRequest(BaseModel):
    company_name: Optional[str] = None
    client_name: Optional[str] = None
    payload: Optional[dict] = None
    tariff: Optional[str] = None


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
    user_id = str(user['id']) if user['role'] not in ('supervisor', 'director', 'owner') else None

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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    return {**request, "id": str(request["id"])}


async def notify_bot_request_created(tg_id: int, request_id: str, company_name: str):
    """Notify bot about new request to offer additional services."""
    from config import settings

    if not settings.BOT_WEBHOOK_URL:
        log.debug("BOT_WEBHOOK_URL not configured, skipping notification")
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "request_created",
                    "tg_id": tg_id,
                    "request_id": request_id,
                    "company_name": company_name
                },
                timeout=5.0
            )
            log.info(f"Notified bot about request {request_id}")
    except Exception as e:
        log.error(f"Failed to notify bot: {e}")


@router.post("")
async def create_request(data: CreateRequest, user: dict = Depends(get_current_user)):
    """Create a new request."""
    # Check if user is approved
    if user.get('approval_status') != 'approved' and user.get('role') not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Account not approved")

    request = await db.create_request(
        user_id=str(user['id']),
        company_name=data.company_name,
        client_name=data.client_name,
        payload=data.payload or {},
        tariff=data.tariff or 'standard',
        chat_id=data.chat_id
    )

    # Notify bot to offer additional services
    if user.get('tg_id'):
        await notify_bot_request_created(
            tg_id=user['tg_id'],
            request_id=str(request["id"]),
            company_name=data.company_name
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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Managers can only delete drafts or error requests
    if user['role'] not in ('supervisor', 'director', 'owner'):
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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Update status
    await db.update_request_status(request_id, 'in_queue')

    # Get tariff to determine which webhook to use
    tariff = request.get('tariff', 'standard')
    log.info(f"[GENERATE] Request {request_id}: tariff from DB = {tariff}, request keys = {list(request.keys())}")

    # Select webhook based on tariff
    if tariff == 'premium':
        webhook_url = settings.N8N_PREMIUM_WEBHOOK_URL
        if not webhook_url:
            log.warning("N8N_PREMIUM_WEBHOOK_URL not configured - falling back to standard webhook")
            webhook_url = settings.N8N_WEBHOOK_URL
    else:
        webhook_url = settings.N8N_WEBHOOK_URL

    log.info(f"[GENERATE] Request {request_id}: Selected webhook URL = {webhook_url} (tariff: {tariff})")

    # Send to n8n webhook
    if not webhook_url:
        log.warning("Webhook URL not configured - skipping webhook call")
    else:
        try:
            # Build payload in the expected format
            payload = request.get('payload', {}) or {}

            # Get manager's tg_id for notifications
            user_id = request.get('user_id')
            manager = None
            manager_tg_id = None
            if user_id:
                try:
                    manager = await db.get_user_by_id(str(user_id))
                    manager_tg_id = manager.get('tg_id') if manager else None
                except Exception as e:
                    log.warning(f"[GENERATE] Failed to get manager info for user_id {user_id}: {e}")
                    manager_tg_id = None

            # Get additional services for this request
            additional_services = await db.get_request_additional_services(request_id)
            # Format additional services for webhook (include code, name, status, notes)
            formatted_services = [
                {
                    "code": s.get('code'),
                    "name": s.get('name'),
                    "status": s.get('status'),
                    "notes": s.get('notes'),
                    "price": s.get('price')
                }
                for s in additional_services
            ]

            # Check if services have addons (for debugging)
            site_data = payload.get('site', {})
            services = site_data.get('services', [])
            services_with_addons = [s for s in services if isinstance(s, dict) and s.get('addons')]
            if services_with_addons:
                log.info(f"[GENERATE] Request {request_id}: Found {len(services_with_addons)} services with addons")
                for i, s in enumerate(services_with_addons):
                    addons_count = len(s.get('addons', []))
                    log.info(f"[GENERATE] Service {i+1} '{s.get('name', 'Unknown')}': {addons_count} addons")

            webhook_data = {
                "request_id": request_id,
                "manager_id": str(user_id) if user_id else "",
                "manager_tg_id": manager_tg_id,  # Chat ID for Telegram notifications
                "tariff": tariff,  # Pass tariff to n8n
                "client": payload.get('client', {}),
                "site": site_data,  # Full site object including services with addons
                "additional_services": formatted_services,  # Include additional services (e.g., logo_design)
            }

            # Log summary of data being sent
            webhook_json = json.dumps(webhook_data, default=str, ensure_ascii=False)
            webhook_size = len(webhook_json.encode('utf-8'))
            log.info(f"Sending to n8n webhook ({tariff} tariff): {webhook_url}")
            log.info(f"[GENERATE] Webhook payload size: {webhook_size} bytes")
            log.info(f"[GENERATE] Services count: {len(services)}")
            log.info(f"[GENERATE] Additional services count: {len(formatted_services)}")
            log.debug(f"Webhook data: {webhook_json[:500]}..." if len(webhook_json) > 500 else f"Webhook data: {webhook_json}")

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    webhook_url,
                    json=webhook_data,
                    timeout=30.0,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Autosites-API/1.0"
                    }
                )
                response.raise_for_status()
                log.info(f"Successfully sent to n8n webhook: {webhook_url}")
                log.info(f"[GENERATE] Response status: {response.status_code}")
        except httpx.ConnectError as e:
            log.error(f"Failed to connect to n8n at {webhook_url}: {e}")
            log.error(f"Check that N8N_WEBHOOK_URL is correct and n8n container is accessible")
            log.error(f"Common issues:")
            log.error(f"  1. Wrong hostname (should be 'n8n' or 'n8n-tg-bot' if in docker-compose)")
            log.error(f"  2. Wrong port (default is 5678)")
            log.error(f"  3. n8n container not running or not in same network")
            # Don't fail the request - webhook can be configured later
            log.warning("Request status updated to 'in_queue' but webhook call failed")
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
            log.error(f"Error sending to n8n at {webhook_url}: {e}", exc_info=True)
            # Don't fail the request - webhook can be configured later
            log.warning("Request status updated to 'in_queue' but webhook call failed")

    return {"success": True, "status": "in_queue"}


@router.post("/{request_id}/photos")
async def upload_photos(
    request_id: str,
    category: str = Form(...),
    files: List[UploadFile] = File(None),
    file: UploadFile = File(None),
    service_index: Optional[str] = Form(None),
    service_name: Optional[str] = Form(None),
    addon_index: Optional[str] = Form(None),  # Index of addon within service
    addon_name: Optional[str] = Form(None),   # Name of addon for validation
    user: dict = Depends(get_current_user)
):
    """Upload photos for a request.

    If service_index and service_name are provided, photos are attached to a specific service.
    If addon_index and addon_name are also provided, photos are attached to a specific addon within the service.
    Otherwise, photos are stored as general category photos in assets.images.
    """
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check ownership
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
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

    # Log received parameters for debugging
    log.info(f"[UPLOAD] Received photo upload params: service_index={service_index}, service_name={service_name}, addon_index={addon_index}, addon_name={addon_name}, category={category}")

    # If service_index is provided, attach photos to specific service or addon
    if service_index is not None and service_name is not None:
        try:
            service_idx = int(service_index)
            services = site.get('services', []) or []

            # Ensure services is a list
            if not isinstance(services, list):
                services = []

            # Ensure service exists at this index
            while len(services) <= service_idx:
                services.append({})

            # Get or create service object
            service = services[service_idx]
            if isinstance(service, str):
                service = {'name': service}

            # If addon_index is provided, attach photos to specific addon within service
            if addon_index is not None and addon_name is not None:
                try:
                    addon_idx = int(addon_index)
                    addons = service.get('addons', []) or []

                    # Ensure addons is a list
                    if not isinstance(addons, list):
                        addons = []

                    # Ensure addon exists at this index
                    while len(addons) <= addon_idx:
                        addons.append({'name': '', 'price': ''})

                    # Get or create addon object
                    addon = addons[addon_idx]
                    if isinstance(addon, str):
                        addon = {'name': addon, 'price': ''}

                    # Initialize photos array if not exists
                    if 'photos' not in addon:
                        addon['photos'] = []

                    # Add photos to addon
                    addon['photos'].extend(uploaded_urls)

                    # Update addon in list
                    addons[addon_idx] = addon
                    service['addons'] = addons

                    log.info(f"Attached {len(uploaded_urls)} photos to addon {addon_idx} ({addon_name}) in service {service_idx} ({service_name})")
                    log.debug(f"Addon photos after update: {addon.get('photos', [])}")
                    log.debug(f"Service addons after update: {[a.get('name', '') + ' (photos: ' + str(len(a.get('photos', []))) + ')' for a in addons]}")
                except (ValueError, IndexError) as e:
                    log.warning(f"Failed to attach photos to addon: {e}, attaching to service instead")
                    # Fall through to service attachment
                    # Initialize photos array if not exists
                    if 'photos' not in service:
                        service['photos'] = []
                    # Add photos to service
                    service['photos'].extend(uploaded_urls)
                    log.info(f"Attached {len(uploaded_urls)} photos to service {service_idx} ({service_name}) instead")
            else:
                # Attach photos to service (not addon)
                # Initialize photos array if not exists
                if 'photos' not in service:
                    service['photos'] = []

                # Add photos to service
                service['photos'].extend(uploaded_urls)

                log.info(f"Attached {len(uploaded_urls)} photos to service {service_idx} ({service_name})")

            # Update service in list
            services[service_idx] = service
            site['services'] = services

        except (ValueError, IndexError) as e:
            log.warning(f"Failed to attach photos to service: {e}, storing as general category photos")
            # Fall through to general category storage

    # Store photos in assets.images for general category photos
    # (or if service attachment failed)
    if service_index is None or service_name is None:
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
        log.info(f"Stored {len(uploaded_urls)} photos as general category '{category}' photos")

    payload['site'] = site

    # Debug: log addons with photos before save
    services_with_addon_photos = []
    for s in site.get('services', []):
        if isinstance(s, dict) and s.get('addons'):
            for a in s.get('addons', []):
                if isinstance(a, dict) and a.get('photos'):
                    services_with_addon_photos.append(f"{s.get('name', 'Unknown')} -> {a.get('name', 'Unknown')}: {len(a.get('photos', []))} photos")

    if services_with_addon_photos:
        log.info(f"[UPLOAD] Saving request with addon photos: {', '.join(services_with_addon_photos)}")
    else:
        log.debug(f"[UPLOAD] No addon photos found in services before save")

    log.info(f"Updating request {request_id} with photos")
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
    if user['role'] not in ('supervisor', 'director', 'owner') and str(request['user_id']) != str(user['id']):
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

