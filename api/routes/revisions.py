"""
Revisions Router - API для управления правками сайтов
Реализует полный цикл: создание правок → отправка в n8n → получение результата → редеплой
"""
from typing import Optional, List
import json
import uuid
import httpx
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form, BackgroundTasks
from pydantic import BaseModel, Field

from config import settings
from routes.auth import get_current_user
import db
import s3

log = logging.getLogger(__name__)

router = APIRouter()


# ==================== DTOs ====================

class ChangeLocationDTO(BaseModel):
    """Локация изменения на сайте."""
    area: Optional[str] = None  # hero, header, footer, about, contacts
    selector: Optional[str] = None  # CSS селектор
    description: Optional[str] = None  # Текстовое описание


class ScreenshotDTO(BaseModel):
    """Скриншот с комментарием."""
    file_key: Optional[str] = None  # S3 key
    url: Optional[str] = None  # Public URL
    comment: Optional[str] = None


class ChangeDTO(BaseModel):
    """Отдельная правка."""
    id: Optional[str] = None
    type: str = 'text_change'  # text_change, visual_change, layout_change, content_add, content_remove, style_change
    location: Optional[ChangeLocationDTO] = None
    client_description: str = Field(..., description="Описание правки от клиента")
    old_value: Optional[str] = None
    new_value_suggestion: Optional[str] = None
    screenshot: Optional[ScreenshotDTO] = None
    priority: str = 'normal'  # low, normal, high, critical


class CreateRevisionRequest(BaseModel):
    """Запрос на создание новой итерации правок."""
    site_id: str
    changes: List[ChangeDTO]
    source: str = 'telegram_bot'  # telegram_bot, webapp, api
    client_id: Optional[str] = None
    auto_submit: bool = False  # Автоматически отправить в n8n


class AddChangeRequest(BaseModel):
    """Добавление правки к существующей ревизии."""
    type: str = 'text_change'
    location: Optional[ChangeLocationDTO] = None
    client_description: str
    old_value: Optional[str] = None
    new_value_suggestion: Optional[str] = None
    screenshot_s3_key: Optional[str] = None
    screenshot_comment: Optional[str] = None
    priority: str = 'normal'


class SubmitRevisionRequest(BaseModel):
    """Запрос на отправку правок в n8n."""
    stop_preview: Optional[bool] = True  # Остановить preview сайт перед правками
    force: Optional[bool] = False  # Принудительная повторная отправка (если уже отправлялось)


class N8nRevisionCallbackRequest(BaseModel):
    """Webhook callback от n8n после обработки правок."""
    job_id: str  # revision_id или n8n_job_id
    revision_id: Optional[str] = None
    status: str  # completed, error, in_progress
    result_archive_s3_key: Optional[str] = None
    error_message: Optional[str] = None
    changes_applied: Optional[List[dict]] = None  # Информация о применённых правках
    ai_summary: Optional[str] = None  # AI-саммари изменений


# ==================== Helper Functions ====================

async def upload_revision_screenshot(
    file: UploadFile,
    site_id: str,
    iteration: int,
    index: int = 1
) -> tuple[str, str]:
    """Upload screenshot to S3 and return (s3_key, public_url)."""
    ext = file.filename.split(".")[-1] if file.filename and "." in file.filename else "png"
    s3_key = f"sites/{site_id}/revisions/{iteration}/screenshot_{index}.{ext}"

    content = await file.read()
    client = s3.get_s3_client()

    try:
        client.put_object(
            Bucket=settings.S3_BUCKET,
            Key=s3_key,
            Body=content,
            ContentType=file.content_type or "image/png",
            ACL="public-read",
        )
    except Exception as e:
        if "AccessControlListNotSupported" in str(e):
            client.put_object(
                Bucket=settings.S3_BUCKET,
                Key=s3_key,
                Body=content,
                ContentType=file.content_type or "image/png",
            )
        else:
            raise

    # Build public URL
    if settings.S3_PUBLIC_URL:
        url = f"{settings.S3_PUBLIC_URL.rstrip('/')}/{s3_key}"
    elif settings.S3_ENDPOINT:
        url = f"{settings.S3_ENDPOINT.rstrip('/')}/{settings.S3_BUCKET}/{s3_key}"
    else:
        url = f"https://{settings.S3_BUCKET}.s3.{settings.S3_REGION}.amazonaws.com/{s3_key}"

    return s3_key, url


async def get_archive_download_url(s3_key: str) -> str:
    """Get presigned download URL for archive."""
    client = s3.get_s3_client()
    try:
        url = client.generate_presigned_url(
            'get_object',
            Params={'Bucket': settings.S3_BUCKET, 'Key': s3_key},
            ExpiresIn=3600  # 1 hour
        )
        return url
    except Exception as e:
        log.error(f"Failed to generate presigned URL for {s3_key}: {e}")
        return None


async def stop_site_preview(site: dict) -> bool:
    """Stop preview site in deploy-node."""
    if not settings.DEPLOY_NODE_URL:
        log.warning("DEPLOY_NODE_URL not configured")
        return False

    if not site.get('preview_slug'):
        log.warning(f"Site {site['id']} has no preview_slug")
        return False

    try:
        async with httpx.AsyncClient() as client:
            # Use preview_slug as domain identifier
            domain = f"{site['preview_slug']}.autosites.ru"
            response = await client.post(
                f"{settings.DEPLOY_NODE_URL}/api/sites/{domain}/stop",
                timeout=30.0
            )

            if response.status_code == 200:
                log.info(f"Stopped preview for site {site['id']}")
                return True
            else:
                log.warning(f"Failed to stop preview: {response.status_code}")
                return False
    except Exception as e:
        log.error(f"Error stopping preview: {e}")
        return False


async def start_site_preview(site: dict) -> bool:
    """Start preview site in deploy-node."""
    if not settings.DEPLOY_NODE_URL:
        return False

    if not site.get('preview_slug'):
        return False

    try:
        async with httpx.AsyncClient() as client:
            domain = f"{site['preview_slug']}.autosites.ru"
            response = await client.post(
                f"{settings.DEPLOY_NODE_URL}/api/sites/{domain}/start",
                timeout=30.0
            )
            return response.status_code == 200
    except Exception as e:
        log.error(f"Error starting preview: {e}")
        return False


async def try_n8n_connection(url: str) -> bool:
    """Try to connect to n8n to verify it's accessible."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Try to access n8n health or root endpoint
            health_url = url.split('/webhook')[0] + '/healthz'  # n8n health endpoint
            response = await client.get(health_url)
            return response.status_code < 500
    except:
        return False


async def send_revision_to_n8n(revision: dict, changes: list, site: dict) -> dict:
    """Send revision to n8n for processing."""
    webhook_url = settings.N8N_REVISIONS_WEBHOOK_URL or settings.N8N_WEBHOOK_URL
    if not webhook_url:
        log.error("N8N_REVISIONS_WEBHOOK_URL and N8N_WEBHOOK_URL are not configured")
        raise HTTPException(status_code=500, detail="N8N webhook URL not configured")

    log.info(f"Sending revision {revision['id']} to n8n webhook: {webhook_url}")

    # Try alternative hostnames if connection fails
    # Based on docker-compose, n8n service is likely named 'n8n-main'
    alternative_hosts = ['n8n-main', 'n8n-tg-bot', 'n8n', 'n8n-tg']
    original_url = webhook_url

    # Get archive download URL if available
    archive_url = None
    if site.get('archive_s3_key'):
        archive_url = await get_archive_download_url(site['archive_s3_key'])

    # Build payload
    payload = {
        "action": "process_revision",
        "revision_id": str(revision['id']),
        "job_id": revision.get('n8n_job_id') or str(revision['id']),
        "site_id": str(revision['site_id']),
        "iteration": revision['iteration'],
        "s3_folder": revision['s3_folder'],
        "archive_url": archive_url,
        "archive_s3_key": site.get('archive_s3_key'),
        "callback_url": f"{settings.API_PUBLIC_URL}/api/revisions/webhook/n8n-callback" if settings.API_PUBLIC_URL else None,
        "site_info": {
            "company_name": site.get('company_name'),
            "domain": site.get('domain'),
            "preview_url": site.get('preview_url'),
        },
        "changes": [
            {
                "id": str(c['id']),
                "type": c['change_type'],
                "location": {
                    "area": c.get('location_area'),
                    "selector": c.get('location_selector'),
                    "description": c.get('location_description'),
                },
                "client_description": c['client_description'],
                "old_value": c.get('old_value'),
                "new_value_suggestion": c.get('new_value_suggestion'),
                "screenshot": {
                    "file_key": c.get('screenshot_s3_key'),
                    "comment": c.get('screenshot_comment'),
                } if c.get('screenshot_s3_key') else None,
                "priority": c.get('priority', 'normal'),
            }
            for c in changes
        ],
        "meta": {
            "source": revision.get('source', 'api'),
            "client_id": revision.get('client_id'),
            "manager_id": str(revision.get('manager_id')) if revision.get('manager_id') else None,
            "created_at": revision['created_at'].isoformat() if revision.get('created_at') else None,
        }
    }

    last_error = None
    tried_urls = []

    # Try original URL first
    urls_to_try = [webhook_url]

    # If original URL uses 'n8n' hostname, try alternatives
    if '://n8n:' in webhook_url or '://n8n/' in webhook_url:
        for alt_host in alternative_hosts:
            alt_url = webhook_url.replace('://n8n:', f'://{alt_host}:').replace('://n8n/', f'://{alt_host}/')
            if alt_url != webhook_url:
                urls_to_try.append(alt_url)

    for url in urls_to_try:
        tried_urls.append(url)
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    url,
                    json=payload,
                    timeout=60.0
                )
                response.raise_for_status()
                result = response.json()

                log.info(f"Sent revision {revision['id']} to n8n at {url}, response: {result}")
                # If we used alternative URL, log it for user to update config
                if url != original_url:
                    log.warning(f"Successfully connected using alternative hostname. Consider updating N8N_REVISIONS_WEBHOOK_URL to: {url}")
                return result
        except httpx.ConnectError as e:
            last_error = e
            log.warning(f"Failed to connect to {url}: {e}")
            continue
        except httpx.HTTPStatusError as e:
            # If we got HTTP response, connection works but endpoint might be wrong
            log.error(f"n8n at {url} returned error: {e.response.status_code} - {e.response.text}")
            raise HTTPException(status_code=502, detail=f"n8n error: {e.response.text}")

    # All URLs failed
    log.error(f"Failed to connect to n8n. Tried URLs: {tried_urls}")
    log.error(f"Last error: {last_error}")
    log.error(f"Possible solutions:")
    log.error(f"  1. Check docker-compose.yml - what is the n8n container name?")
    log.error(f"  2. Check if n8n container is running: docker ps | grep n8n")
    log.error(f"  3. Check if containers are in same network: docker network inspect <network_name>")
    log.error(f"  4. Update N8N_REVISIONS_WEBHOOK_URL in .env with correct hostname")

    raise HTTPException(
        status_code=502,
        detail=f"Cannot connect to n8n. Tried: {', '.join(tried_urls)}. "
               f"Check N8N_REVISIONS_WEBHOOK_URL in .env and ensure n8n container is running and accessible. "
               f"Error: {str(last_error)}"
    )


async def trigger_redeploy(site: dict, archive_s3_key: str, user_id: str = None) -> dict:
    """Trigger redeploy after revision processing."""
    if not settings.DEPLOY_NODE_URL:
        log.warning("DEPLOY_NODE_URL not configured")
        return None

    try:
        # Get presigned URL for archive
        archive_url = await get_archive_download_url(archive_s3_key)
        if not archive_url:
            log.error("Failed to get archive download URL")
            return None

        async with httpx.AsyncClient(timeout=300.0) as client:
            # First, download archive from S3
            archive_response = await client.get(archive_url, timeout=120.0)
            archive_response.raise_for_status()
            archive_content = archive_response.content

            # Then, deploy it to deploy-node
            files = {'archive': ('site.zip', archive_content, 'application/zip')}
            data = {
                'auto_select': 'true',
                'enable_ssl': 'true',  # SSL enabled by default for preview domains
                'client_site_id': str(site['id']),
                'request_id': str(site.get('request_id')) if site.get('request_id') else '',
            }

            # Use custom domain if set
            if site.get('domain'):
                data['domain'] = site['domain']

            response = await client.post(
                f"{settings.DEPLOY_NODE_URL}/api/deploy",
                files=files,
                data=data,
                timeout=120.0
            )
            response.raise_for_status()
            result = response.json()

            if result.get('success'):
                # Update site deploy status
                deployment = result.get('deployment', {})
                await db.update_site_deploy_status(
                    site_id=str(site['id']),
                    deploy_status='deploying',
                    deploy_id=deployment.get('id'),
                    preview_slug=deployment.get('preview_slug'),
                    preview_url=deployment.get('preview_url'),
                )

                # Create deploy history
                await db.create_deploy_history(
                    client_site_id=str(site['id']),
                    deploy_id=deployment.get('id'),
                    action='redeploy_after_revision',
                    initiated_by=user_id,
                    archive_s3_key=archive_s3_key
                )

                log.info(f"Triggered redeploy for site {site['id']}")
                return result

            return None

    except Exception as e:
        log.error(f"Failed to trigger redeploy: {e}")
        return None


async def notify_manager_revision_status(revision: dict, site: dict, status: str, message: str = None):
    """Notify manager about revision status via bot."""
    if not settings.BOT_WEBHOOK_URL:
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "revision_status",
                    "tg_id": revision.get('manager_tg_id') or site.get('manager_tg_id'),
                    "revision_id": str(revision['id']),
                    "site_id": str(revision['site_id']),
                    "company_name": site.get('company_name', 'Сайт'),
                    "iteration": revision['iteration'],
                    "status": status,
                    "message": message,
                    "preview_url": site.get('preview_url'),
                },
                timeout=10.0
            )
    except Exception as e:
        log.error(f"Failed to notify manager: {e}")


# ==================== CRUD Endpoints ====================

@router.get("")
async def list_revisions(
    site_id: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
    user: dict = Depends(get_current_user)
):
    """List revisions for current user or all (admin)."""
    offset = (page - 1) * limit

    if site_id:
        # Get specific site revisions
        site = await db.get_client_site(site_id)
        if not site:
            raise HTTPException(status_code=404, detail="Site not found")

        if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
            raise HTTPException(status_code=403, detail="Access denied")

        revisions = await db.get_site_revisions(site_id, status, limit, offset)
    else:
        # List active revisions
        manager_id = None if user['role'] == 'admin' else str(user['id'])
        revisions = await db.list_active_revisions(manager_id, limit, offset)

    return {
        "items": [{**r, "id": str(r["id"]), "site_id": str(r["site_id"])} for r in revisions],
        "page": page,
        "limit": limit
    }


@router.get("/stats")
async def get_revision_stats(user: dict = Depends(get_current_user)):
    """Get revision statistics (admin only)."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    stats = await db.get_revision_stats()
    return stats


@router.get("/{revision_id}")
async def get_revision(revision_id: str, user: dict = Depends(get_current_user)):
    """Get revision details with changes."""
    revision = await db.get_revision(revision_id)
    if not revision:
        raise HTTPException(status_code=404, detail="Revision not found")

    # Check access
    site = await db.get_client_site(str(revision['site_id']))
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Get changes
    changes = await db.get_revision_changes(revision_id)

    # Get history
    history = await db.get_revision_history(revision_id)

    return {
        **revision,
        "id": str(revision["id"]),
        "site_id": str(revision["site_id"]),
        "changes": changes,
        "history": history
    }


@router.post("")
async def create_revision(
    data: CreateRevisionRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user)
):
    """Create a new revision with changes."""
    # Verify site exists and user has access
    site = await db.get_client_site(data.site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if there's already an active revision
    active_revisions = await db.get_site_revisions(data.site_id, status='pending', limit=1)
    if active_revisions:
        raise HTTPException(
            status_code=400,
            detail="Site already has an active revision. Complete or cancel it first."
        )

    # Create revision
    revision = await db.create_revision(
        site_id=data.site_id,
        manager_id=str(user['id']),
        archive_s3_key=site.get('archive_s3_key'),
        source=data.source,
        client_id=data.client_id,
        request_id=str(site.get('request_id')) if site.get('request_id') else None
    )

    # Create changes
    created_changes = []
    for i, change in enumerate(data.changes):
        c = await db.create_revision_change(
            revision_id=str(revision['id']),
            client_description=change.client_description,
            change_type=change.type,
            location_area=change.location.area if change.location else None,
            location_selector=change.location.selector if change.location else None,
            location_description=change.location.description if change.location else None,
            old_value=change.old_value,
            new_value_suggestion=change.new_value_suggestion,
            screenshot_s3_key=change.screenshot.file_key if change.screenshot else None,
            screenshot_comment=change.screenshot.comment if change.screenshot else None,
            priority=change.priority
        )
        created_changes.append(c)

    log.info(f"Created revision {revision['id']} with {len(created_changes)} changes for site {data.site_id}")

    # Auto-submit if requested
    if data.auto_submit:
        background_tasks.add_task(
            auto_submit_revision,
            str(revision['id']),
            str(user['id'])
        )

    return {
        **revision,
        "id": str(revision["id"]),
        "site_id": str(revision["site_id"]),
        "changes": created_changes,
        "auto_submitted": data.auto_submit
    }


async def auto_submit_revision(revision_id: str, user_id: str):
    """Background task for auto-submitting revision."""
    try:
        revision = await db.get_revision(revision_id)
        if not revision:
            return

        site = await db.get_client_site(str(revision['site_id']))
        if not site:
            return

        changes = await db.get_revision_changes(revision_id)

        # Stop preview
        await stop_site_preview(site)

        # Update status
        await db.update_revision_status(
            revision_id,
            'in_progress',
            changed_by=user_id,
            change_source='auto_submit'
        )

        # Generate job ID
        job_id = str(uuid.uuid4())
        await db.update_revision(revision_id, {
            'n8n_job_id': job_id,
            'n8n_sent_at': datetime.now(timezone.utc)
        })

        # Send to n8n
        await send_revision_to_n8n(revision, changes, site)

        # Update status
        await db.update_revision_status(
            revision_id,
            'processing',
            changed_by=user_id,
            change_source='n8n'
        )

    except Exception as e:
        log.error(f"Auto-submit failed for revision {revision_id}: {e}")
        await db.update_revision_status(
            revision_id,
            'failed',
            error_message=str(e),
            changed_by=user_id,
            change_source='auto_submit'
        )


@router.post("/{revision_id}/changes")
async def add_change(
    revision_id: str,
    data: AddChangeRequest,
    user: dict = Depends(get_current_user)
):
    """Add a change to existing revision."""
    revision = await db.get_revision(revision_id)
    if not revision:
        raise HTTPException(status_code=404, detail="Revision not found")

    if revision['status'] not in ('pending', 'draft'):
        raise HTTPException(status_code=400, detail="Cannot add changes to revision in progress")

    # Check access
    site = await db.get_client_site(str(revision['site_id']))
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    change = await db.create_revision_change(
        revision_id=revision_id,
        client_description=data.client_description,
        change_type=data.type,
        location_area=data.location.area if data.location else None,
        location_selector=data.location.selector if data.location else None,
        location_description=data.location.description if data.location else None,
        old_value=data.old_value,
        new_value_suggestion=data.new_value_suggestion,
        screenshot_s3_key=data.screenshot_s3_key,
        screenshot_comment=data.screenshot_comment,
        priority=data.priority
    )

    return change


@router.post("/{revision_id}/upload-screenshot")
async def upload_screenshot(
    revision_id: str,
    file: UploadFile = File(...),
    comment: Optional[str] = Form(None),
    user: dict = Depends(get_current_user)
):
    """Upload screenshot for revision."""
    revision = await db.get_revision(revision_id)
    if not revision:
        raise HTTPException(status_code=404, detail="Revision not found")

    # Check access
    site = await db.get_client_site(str(revision['site_id']))
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Get existing changes count for index
    changes = await db.get_revision_changes(revision_id)
    screenshot_index = sum(1 for c in changes if c.get('screenshot_s3_key')) + 1

    # Upload to S3
    s3_key, url = await upload_revision_screenshot(
        file,
        str(revision['site_id']),
        revision['iteration'],
        screenshot_index
    )

    return {
        "s3_key": s3_key,
        "url": url,
        "comment": comment
    }


@router.post("/{revision_id}/submit")
async def submit_revision(
    revision_id: str,
    data: Optional[SubmitRevisionRequest] = None,
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(get_current_user)
):
    """Submit revision for processing by n8n."""
    # Handle case when no body is sent
    if data is None:
        data = SubmitRevisionRequest(stop_preview=True)

    revision = await db.get_revision(revision_id)
    if not revision:
        raise HTTPException(status_code=404, detail="Revision not found")

    # Allow resubmission if:
    # 1. Force flag is set
    # 2. Status is failed or error
    # 3. Status is in_progress/processing but there's an error (n8n failed)
    can_resubmit = (
        (data.force if data else False) or
        revision['status'] in ('failed', 'error') or
        (revision['status'] in ('in_progress', 'processing') and revision.get('error_message'))
    )

    if revision['status'] not in ('pending', 'draft') and not can_resubmit:
        raise HTTPException(
            status_code=400,
            detail=f"Revision already submitted or completed. Current status: {revision['status']}. "
                   f"Use force=true to resubmit, or wait for completion."
        )

    # Check access
    site = await db.get_client_site(str(revision['site_id']))
    if not site:
        raise HTTPException(status_code=404, detail="Site not found for this revision")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Get changes
    changes = await db.get_revision_changes(revision_id)
    if not changes:
        raise HTTPException(status_code=400, detail="Revision has no changes")

    # Stop preview if requested (default: True)
    stop_preview = data.stop_preview if data.stop_preview is not None else True
    if stop_preview:
        stopped = await stop_site_preview(site)
        if stopped:
            await db.update_site_deploy_status(str(site['id']), 'stopped')

    # Update status to in_progress
    await db.update_revision_status(
        revision_id,
        'in_progress',
        changed_by=str(user['id']),
        change_source='user'
    )

    # Generate job ID for correlation
    job_id = str(uuid.uuid4())
    await db.update_revision(revision_id, {
        'n8n_job_id': job_id,
        'n8n_sent_at': datetime.now(timezone.utc)
    })

    # Send to n8n
    try:
        result = await send_revision_to_n8n(revision, changes, site)

        # Update status to processing (clear any previous errors)
        await db.update_revision_status(
            revision_id,
            'processing',
            changed_by=str(user['id']),
            change_source='n8n',
            error_message=None  # Clear previous errors
        )

        # Notify manager
        background_tasks.add_task(
            notify_manager_revision_status,
            revision,
            site,
            'processing',
            'Правки отправлены на обработку'
        )

        return {
            "success": True,
            "message": "Revision submitted for processing",
            "job_id": job_id,
            "n8n_response": result
        }

    except HTTPException as e:
        # If n8n connection failed, mark as failed so user can retry
        if e.status_code in (502, 500):  # Connection errors
            await db.update_revision_status(
                revision_id,
                'failed',
                error_message=e.detail,
                changed_by=str(user['id']),
                change_source='system'
            )
        raise
    except Exception as e:
        # Revert status to failed so user can retry
        error_msg = str(e)
        await db.update_revision_status(
            revision_id,
            'failed',
            error_message=error_msg,
            changed_by=str(user['id']),
            change_source='system'
        )
        log.error(f"Failed to submit revision {revision_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/{revision_id}/cancel")
async def cancel_revision(
    revision_id: str,
    user: dict = Depends(get_current_user)
):
    """Cancel a pending or in_progress revision."""
    revision = await db.get_revision(revision_id)
    if not revision:
        raise HTTPException(status_code=404, detail="Revision not found")

    if revision['status'] in ('completed', 'cancelled'):
        raise HTTPException(status_code=400, detail="Cannot cancel completed or already cancelled revision")

    # Check access
    site = await db.get_client_site(str(revision['site_id']))
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    await db.update_revision_status(
        revision_id,
        'cancelled',
        changed_by=str(user['id']),
        change_source='user'
    )

    # Start preview if it was stopped
    if site.get('deploy_status') == 'stopped':
        await start_site_preview(site)
        await db.update_site_deploy_status(str(site['id']), 'active')

    return {"success": True, "message": "Revision cancelled"}


@router.delete("/{revision_id}")
async def delete_revision(
    revision_id: str,
    user: dict = Depends(get_current_user)
):
    """Delete a revision (admin or owner, only if pending/cancelled)."""
    revision = await db.get_revision(revision_id)
    if not revision:
        raise HTTPException(status_code=404, detail="Revision not found")

    if revision['status'] not in ('pending', 'cancelled', 'failed'):
        raise HTTPException(status_code=400, detail="Cannot delete active or completed revision")

    # Check access
    site = await db.get_client_site(str(revision['site_id']))
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    await db.delete_revision(revision_id)

    return {"success": True}


# ==================== n8n Webhook Callback ====================

@router.post("/webhook/n8n-callback")
async def n8n_revision_callback(
    data: N8nRevisionCallbackRequest,
    background_tasks: BackgroundTasks
):
    """
    Webhook callback from n8n after processing revision.
    n8n sends result archive and status.
    """
    log.info(f"n8n callback received: job_id={data.job_id}, status={data.status}")

    # Find revision by job_id or revision_id
    revision = None
    if data.revision_id:
        revision = await db.get_revision(data.revision_id)
    if not revision:
        revision = await db.get_revision_by_n8n_job(data.job_id)

    if not revision:
        log.warning(f"Revision not found for job_id={data.job_id}")
        raise HTTPException(status_code=404, detail="Revision not found")

    site = await db.get_client_site(str(revision['site_id']))
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Update revision based on status
    if data.status == 'completed':
        # Update revision
        await db.update_revision(str(revision['id']), {
            'status': 'completed',
            'result_archive_s3_key': data.result_archive_s3_key,
            'n8n_response_at': datetime.now(timezone.utc),
            'completed_at': datetime.now(timezone.utc)
        })

        # Update site archive
        if data.result_archive_s3_key:
            await db.update_client_site(str(site['id']), {
                'archive_s3_key': data.result_archive_s3_key,
                'revision_status': 'completed'
            })

            # Trigger redeploy
            background_tasks.add_task(
                trigger_redeploy,
                site,
                data.result_archive_s3_key,
                str(revision.get('manager_id'))
            )

        # Update change statuses if provided
        if data.changes_applied:
            for change_info in data.changes_applied:
                if change_info.get('id'):
                    await db.update_revision_change(change_info['id'], {
                        'status': change_info.get('status', 'applied'),
                        'ai_interpretation': change_info.get('ai_interpretation'),
                        'ai_confidence': change_info.get('ai_confidence')
                    })

        # Notify manager
        background_tasks.add_task(
            notify_manager_revision_status,
            revision,
            site,
            'completed',
            data.ai_summary or 'Правки успешно применены'
        )

        log.info(f"Revision {revision['id']} completed successfully")

    elif data.status == 'error':
        await db.update_revision_status(
            str(revision['id']),
            'failed',
            error_message=data.error_message,
            change_source='n8n'
        )

        # Start preview back if it was stopped
        if site.get('deploy_status') == 'stopped':
            await start_site_preview(site)
            await db.update_site_deploy_status(str(site['id']), 'active')

        # Notify manager
        background_tasks.add_task(
            notify_manager_revision_status,
            revision,
            site,
            'failed',
            data.error_message or 'Ошибка при обработке правок'
        )

        log.error(f"Revision {revision['id']} failed: {data.error_message}")

    elif data.status == 'in_progress':
        await db.update_revision_status(
            str(revision['id']),
            'processing',
            change_source='n8n'
        )

    return {"success": True, "revision_id": str(revision['id'])}


# ==================== Site Revisions Shortcuts ====================

@router.get("/site/{site_id}")
async def get_site_revisions_list(
    site_id: str,
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 20,
    user: dict = Depends(get_current_user)
):
    """Get revisions for a specific site."""
    site = await db.get_client_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    offset = (page - 1) * limit
    revisions = await db.get_site_revisions(site_id, status, limit, offset)

    return {
        "items": [{**r, "id": str(r["id"]), "site_id": str(r["site_id"])} for r in revisions],
        "page": page,
        "limit": limit
    }


@router.get("/site/{site_id}/stats")
async def get_site_revision_stats_endpoint(
    site_id: str,
    user: dict = Depends(get_current_user)
):
    """Get revision statistics for a specific site."""
    site = await db.get_client_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    stats = await db.get_site_revision_stats(site_id)
    return stats

