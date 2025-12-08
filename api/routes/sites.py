"""
Client Sites Router - CRUD операции для сайтов клиентов
Связывает заявки с деплоями и управляет хостингом
"""
from typing import Optional, List
import json
import httpx
import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form, BackgroundTasks
from pydantic import BaseModel

from config import settings
from routes.auth import get_current_user
import db

log = logging.getLogger(__name__)

router = APIRouter()


# ==================== DTOs ====================

class CreateSiteRequest(BaseModel):
    request_id: str
    company_name: str
    client_name: Optional[str] = None
    client_contact: Optional[str] = None
    hosting_plan: str = 'trial'
    notes: Optional[str] = None


class UpdateSiteRequest(BaseModel):
    company_name: Optional[str] = None
    client_name: Optional[str] = None
    client_contact: Optional[str] = None
    domain: Optional[str] = None
    notes: Optional[str] = None
    hosting_auto_renew: Optional[bool] = None


class AssignDomainRequest(BaseModel):
    domain: str
    enable_ssl: bool = True


class ExtendHostingRequest(BaseModel):
    plan: str
    months: int = 1


class DeployCallbackRequest(BaseModel):
    """Webhook callback from deploy-node"""
    deploy_id: str
    status: str  # pending, running, completed, failed
    request_id: Optional[str] = None  # ID заявки для связи
    client_site_id: Optional[str] = None  # ID сайта клиента для связи
    preview_slug: Optional[str] = None
    preview_url: Optional[str] = None
    server_id: Optional[str] = None
    server_name: Optional[str] = None
    server_host: Optional[str] = None
    port: Optional[int] = None
    domain: Optional[str] = None
    ssl_enabled: Optional[bool] = None
    error_message: Optional[str] = None
    build_output: Optional[str] = None


class GenerationCallbackRequest(BaseModel):
    """Webhook callback from n8n after generation"""
    request_id: str
    status: str  # completed, error
    archive_s3_key: Optional[str] = None
    archive_size_bytes: Optional[int] = None
    error_message: Optional[str] = None


# ==================== Helper Functions ====================

async def trigger_deploy(site: dict, archive_path: str, user_id: str = None):
    """Trigger deployment to deploy-node."""
    deploy_url = settings.DEPLOY_NODE_URL
    if not deploy_url:
        log.warning("DEPLOY_NODE_URL not configured, skipping deployment")
        return None

    try:
        async with httpx.AsyncClient() as client:
            # Prepare multipart form data
            files = {'archive': open(archive_path, 'rb')}
            data = {
                'auto_select': 'true',
                'enable_ssl': 'false',  # Will enable after domain assignment
                'request_id': site.get('request_id'),  # Pass request_id for callback
                'client_site_id': str(site['id']),  # Pass client_site_id for callback
            }

            # Add domain if set
            if site.get('domain'):
                data['domain'] = site['domain']
                data['enable_ssl'] = 'true'

            response = await client.post(
                f"{deploy_url}/api/deploy",
                files=files,
                data=data,
                timeout=60.0
            )
            response.raise_for_status()
            result = response.json()

            if result.get('success'):
                deployment = result.get('deployment', {})

                # Update site with deploy info
                await db.update_site_deploy_status(
                    site_id=str(site['id']),
                    deploy_status='deploying',
                    deploy_id=deployment.get('id'),
                    preview_slug=deployment.get('preview_slug'),
                    preview_url=deployment.get('preview_url'),
                    server_id=deployment.get('server_id'),
                    server_name=deployment.get('server_name')
                )

                # Create deploy history
                await db.create_deploy_history(
                    client_site_id=str(site['id']),
                    deploy_id=deployment.get('id'),
                    action='deploy',
                    initiated_by=user_id,
                    archive_s3_key=site.get('archive_s3_key')
                )

                log.info(f"Deployment started: {deployment.get('id')} for site {site['id']}")
                return deployment
            else:
                error = result.get('error', 'Unknown error')
                await db.update_site_deploy_status(
                    site_id=str(site['id']),
                    deploy_status='failed',
                    error=error
                )
                log.error(f"Deploy failed for site {site['id']}: {error}")
                return None

    except Exception as e:
        log.error(f"Error triggering deploy for site {site['id']}: {e}")
        await db.update_site_deploy_status(
            site_id=str(site['id']),
            deploy_status='failed',
            error=str(e)
        )
        return None


async def assign_domain_to_deploy(site: dict, domain: str, enable_ssl: bool = True):
    """Assign domain to existing deployment in deploy-node."""
    deploy_url = settings.DEPLOY_NODE_URL
    if not deploy_url or not site.get('deploy_id'):
        return None

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{deploy_url}/api/sites/by-id/{site['deploy_id']}/domain",
                json={
                    'domain': domain,
                    'enable_ssl': enable_ssl
                },
                timeout=30.0
            )
            response.raise_for_status()
            result = response.json()

            if result.get('success'):
                await db.update_client_site(str(site['id']), {
                    'domain': domain,
                    'domain_status': 'active',
                    'ssl_enabled': enable_ssl
                })
                return result

    except Exception as e:
        log.error(f"Error assigning domain for site {site['id']}: {e}")
        await db.update_client_site(str(site['id']), {
            'domain_status': 'failed',
            'last_error': str(e)
        })

    return None


# ==================== CRUD Endpoints ====================

@router.get("")
async def list_sites(
    deploy_status: Optional[str] = None,
    hosting_plan: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    user: dict = Depends(get_current_user)
):
    """List client sites."""
    offset = (page - 1) * limit

    # Regular users see only their sites
    manager_id = str(user['id']) if user['role'] != 'admin' else None

    sites = await db.list_client_sites(
        manager_id=manager_id,
        deploy_status=deploy_status,
        hosting_plan=hosting_plan,
        limit=limit,
        offset=offset
    )

    return {
        "items": [{**s, "id": str(s["id"])} for s in sites],
        "page": page,
        "limit": limit
    }


@router.get("/stats")
async def get_sites_stats(user: dict = Depends(get_current_user)):
    """Get sites statistics (admin only)."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    stats = await db.get_sites_stats()
    return stats


@router.get("/expiring")
async def get_expiring_sites(
    days: int = 7,
    user: dict = Depends(get_current_user)
):
    """Get sites with expiring hosting."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    sites = await db.get_expiring_sites(days)
    return {"items": sites}


@router.get("/plans")
async def list_hosting_plans():
    """List available hosting plans."""
    plans = await db.list_hosting_plans()
    return {"items": plans}


@router.get("/by-request/{request_id}")
async def get_site_by_request(request_id: str, user: dict = Depends(get_current_user)):
    """Get client site by request ID."""
    # Verify request belongs to user
    request = await db.get_request(request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    site = await db.get_client_site_by_request(request_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found for this request")

    return {**site, "id": str(site["id"])}


@router.get("/{site_id}")
async def get_site(site_id: str, user: dict = Depends(get_current_user)):
    """Get a specific client site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Check ownership
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    return {**site, "id": str(site["id"])}


@router.get("/{site_id}/history")
async def get_site_history(
    site_id: str,
    limit: int = 10,
    user: dict = Depends(get_current_user)
):
    """Get deployment history for a site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    history = await db.get_deploy_history(site_id, limit)
    return {"items": history}


@router.post("")
async def create_site(data: CreateSiteRequest, user: dict = Depends(get_current_user)):
    """Create a new client site from a request."""
    # Verify request exists and belongs to user
    request = await db.get_request(data.request_id)

    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if site already exists for this request
    existing = await db.get_client_site_by_request(data.request_id)
    if existing:
        return {**existing, "id": str(existing["id"]), "already_exists": True}

    # Create site
    site = await db.create_client_site(
        request_id=data.request_id,
        manager_id=str(user['id']),
        company_name=data.company_name,
        client_name=data.client_name,
        client_contact=data.client_contact,
        hosting_plan=data.hosting_plan,
        notes=data.notes
    )

    log.info(f"Created client site {site['id']} for request {data.request_id}")
    return {**site, "id": str(site["id"])}


@router.patch("/{site_id}")
async def update_site(
    site_id: str,
    data: UpdateSiteRequest,
    user: dict = Depends(get_current_user)
):
    """Update a client site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    update_data = data.model_dump(exclude_none=True)
    updated = await db.update_client_site(site_id, update_data)

    return {**updated, "id": str(updated["id"])}


@router.post("/{site_id}/deploy")
async def deploy_site(
    site_id: str,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user)
):
    """Trigger deployment for a site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if site has archive
    if not site.get('archive_s3_key'):
        raise HTTPException(status_code=400, detail="Site has no generated archive")

    # Check if already deploying
    if site.get('deploy_status') == 'deploying':
        raise HTTPException(status_code=400, detail="Deployment already in progress")

    # Update status
    await db.update_site_deploy_status(site_id, 'pending')

    # TODO: Download archive from S3 and trigger deploy
    # For now, return pending status

    return {"success": True, "status": "pending", "message": "Deployment queued"}


@router.post("/{site_id}/domain")
async def assign_domain(
    site_id: str,
    data: AssignDomainRequest,
    user: dict = Depends(get_current_user)
):
    """Assign a custom domain to a site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check hosting plan allows custom domains
    plan = await db.get_hosting_plan(site.get('hosting_plan', 'trial'))
    if plan and not plan.get('custom_domain'):
        raise HTTPException(
            status_code=400,
            detail="Your hosting plan doesn't support custom domains. Please upgrade."
        )

    # Update local record
    await db.assign_domain_to_site(site_id, data.domain, data.enable_ssl)

    # If site is deployed, update in deploy-node
    if site.get('deploy_status') == 'active' and site.get('deploy_id'):
        result = await assign_domain_to_deploy(site, data.domain, data.enable_ssl)
        if not result:
            return {
                "success": False,
                "message": "Domain saved but failed to configure in deploy system"
            }

    return {
        "success": True,
        "domain": data.domain,
        "ssl_enabled": data.enable_ssl
    }


@router.post("/{site_id}/extend")
async def extend_hosting(
    site_id: str,
    data: ExtendHostingRequest,
    user: dict = Depends(get_current_user)
):
    """Extend hosting for a site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Verify plan exists
    plan = await db.get_hosting_plan(data.plan)
    if not plan:
        raise HTTPException(status_code=400, detail="Invalid hosting plan")

    updated = await db.extend_hosting(site_id, data.plan, data.months)

    return {
        "success": True,
        "hosting_plan": updated['hosting_plan'],
        "hosting_expires_at": updated['hosting_expires_at'].isoformat() if updated.get('hosting_expires_at') else None
    }


@router.post("/{site_id}/stop")
async def stop_site(site_id: str, user: dict = Depends(get_current_user)):
    """Stop a deployed site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    if site.get('deploy_status') != 'active':
        raise HTTPException(status_code=400, detail="Site is not active")

    # TODO: Call deploy-node to stop the site

    await db.update_site_deploy_status(site_id, 'stopped')

    return {"success": True, "message": "Site stopped"}


@router.delete("/{site_id}")
async def delete_site(site_id: str, user: dict = Depends(get_current_user)):
    """Delete a client site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Only admin can delete
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    # TODO: Stop and cleanup in deploy-node if active

    await db.delete_client_site(site_id)

    return {"success": True}


# ==================== Webhook Callbacks ====================

@router.post("/webhook/deploy-callback")
async def deploy_callback(data: DeployCallbackRequest):
    """
    Webhook callback from deploy-node.
    Called when deployment status changes.
    """
    log.info(f"Deploy callback received: {data.deploy_id} -> {data.status}")

    # Find site by deploy_id, client_site_id, or request_id
    site = None
    if data.client_site_id:
        site = await db.get_client_site(data.client_site_id)
    elif data.request_id:
        site = await db.get_client_site_by_request(data.request_id)

    if not site:
        site = await db.get_client_site_by_deploy_id(data.deploy_id)

    if not site:
        log.warning(f"Site not found for deploy_id: {data.deploy_id}, request_id: {data.request_id}, client_site_id: {data.client_site_id}")
        raise HTTPException(status_code=404, detail="Site not found")

    # Map deploy-node status to our status
    status_map = {
        'pending': 'pending',
        'running': 'deploying',
        'completed': 'active',
        'failed': 'failed'
    }
    deploy_status = status_map.get(data.status, data.status)

    # Update site with server_host for domain configuration instructions
    update_data = {
        'deploy_status': deploy_status,
    }
    if data.preview_slug:
        update_data['preview_slug'] = data.preview_slug
    if data.preview_url:
        update_data['preview_url'] = data.preview_url
    if data.server_id:
        update_data['server_id'] = data.server_id
    if data.server_name:
        update_data['server_name'] = data.server_name
    if data.server_host:
        update_data['server_host'] = data.server_host
    if data.port:
        update_data['container_port'] = data.port
    if data.error_message:
        update_data['last_error'] = data.error_message

    await db.update_client_site(str(site['id']), update_data)

    # Also update deploy status explicitly
    await db.update_site_deploy_status(
        site_id=str(site['id']),
        deploy_status=deploy_status,
        preview_slug=data.preview_slug,
        preview_url=data.preview_url,
        server_id=data.server_id,
        server_name=data.server_name,
        error=data.error_message
    )

    # Update domain status if domain was assigned
    if data.domain and deploy_status == 'active':
        await db.update_client_site(str(site['id']), {
            'domain': data.domain,
            'domain_status': 'active',
            'ssl_enabled': data.ssl_enabled or False
        })

    # Update request status based on deploy result
    if site.get('request_id'):
        if deploy_status == 'active':
            # Site is live - update request to success if not already
            await db.update_request_status(str(site['request_id']), 'success')
            log.info(f"Updated request {site['request_id']} status to success after successful deploy")

    # Update deploy history if exists
    history = await db.get_deploy_history(str(site['id']), limit=1)
    if history and history[0].get('deploy_id') == data.deploy_id:
        await db.update_deploy_history(
            history_id=str(history[0]['id']),
            status='success' if deploy_status == 'active' else ('failed' if deploy_status == 'failed' else 'running'),
            build_output=data.build_output,
            error_message=data.error_message
        )

    # Get manager info for notifications
    if not site.get('manager_tg_id'):
        full_site = await db.get_client_site(str(site['id']))
        if full_site:
            site = full_site

    # Notify manager via bot if configured
    if site.get('manager_tg_id') and deploy_status in ('active', 'failed', 'deploying'):
        await notify_manager_deploy_status(site, deploy_status, data)

    return {"success": True}


@router.post("/webhook/generation-callback")
async def generation_callback(data: GenerationCallbackRequest):
    """
    Webhook callback from n8n after site generation.
    Creates client_site record and optionally triggers deploy.
    """
    log.info(f"Generation callback received: {data.request_id} -> {data.status}")

    # Get the request
    request = await db.get_request(data.request_id)
    if not request:
        log.warning(f"Request not found: {data.request_id}")
        raise HTTPException(status_code=404, detail="Request not found")

    # Get user info for notifications
    user = await db.get_user_by_id(str(request['user_id']))

    # Get or create client site
    site = await db.get_client_site_by_request(data.request_id)

    if not site:
        # Create new client site
        payload = request.get('payload', {})
        site_data = payload.get('site', {})
        client_data = payload.get('client', {})

        site = await db.create_client_site(
            request_id=data.request_id,
            manager_id=str(request['user_id']),
            company_name=site_data.get('company', client_data.get('company', 'Unknown')),
            client_name=client_data.get('name'),
            client_contact=client_data.get('contact'),
            hosting_plan='trial'
        )
        log.info(f"Created client site {site['id']} from generation callback")

    # Add manager_tg_id to site for notifications
    if user:
        site['manager_tg_id'] = user.get('tg_id')

    # Update generation status
    if data.status == 'completed':
        await db.update_site_generation_status(
            site_id=str(site['id']),
            status='completed',
            archive_s3_key=data.archive_s3_key,
            archive_size_bytes=data.archive_size_bytes
        )

        # Update request status
        await db.update_request_status(data.request_id, 'success')

        # Notify manager about generation complete
        await notify_manager_generation_complete(site, 'completed')

        # Auto-deploy if configured
        if settings.AUTO_DEPLOY_ENABLED:
            log.info(f"Auto-deploy enabled, triggering deploy for site {site['id']}")
            # TODO: Download archive and trigger deploy
            await db.update_site_deploy_status(str(site['id']), 'pending')

    elif data.status == 'error':
        await db.update_site_generation_status(
            site_id=str(site['id']),
            status='error',
            error=data.error_message
        )
        await db.update_request_status(data.request_id, 'error')

        # Notify manager about generation error
        await notify_manager_generation_complete(site, 'error', data.error_message)

    return {"success": True, "site_id": str(site['id'])}


async def notify_manager_deploy_status(site: dict, status: str, data: DeployCallbackRequest):
    """Notify manager about deployment status via bot."""
    if not settings.BOT_WEBHOOK_URL:
        log.warning("BOT_WEBHOOK_URL not configured, skipping notification")
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "deploy_status",
                    "tg_id": site.get('manager_tg_id'),
                    "site_id": str(site['id']),
                    "company_name": site.get('company_name', 'Сайт'),
                    "status": status,
                    "preview_url": data.preview_url,
                    "domain": data.domain,
                    "error": data.error_message
                },
                timeout=10.0
            )
            log.info(f"Sent deploy status notification to bot for site {site['id']}")
    except Exception as e:
        log.error(f"Failed to notify manager via bot: {e}")


async def notify_manager_generation_complete(site: dict, status: str, error: str = None):
    """Notify manager about generation completion via bot."""
    if not settings.BOT_WEBHOOK_URL:
        return

    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{settings.BOT_WEBHOOK_URL}/webhook",
                json={
                    "action": "generation_complete",
                    "tg_id": site.get('manager_tg_id'),
                    "request_id": str(site.get('request_id')),
                    "company_name": site.get('company_name', 'Сайт'),
                    "status": status,
                    "error": error
                },
                timeout=10.0
            )
    except Exception as e:
        log.error(f"Failed to notify manager about generation: {e}")


# ==================== Admin Endpoints ====================

@router.get("/admin/all")
async def admin_list_all_sites(
    deploy_status: Optional[str] = None,
    manager_id: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    user: dict = Depends(get_current_user)
):
    """List all sites (admin only)."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    offset = (page - 1) * limit

    sites = await db.list_client_sites(
        manager_id=manager_id,
        deploy_status=deploy_status,
        limit=limit,
        offset=offset
    )

    return {
        "items": [{**s, "id": str(s["id"])} for s in sites],
        "page": page,
        "limit": limit
    }


@router.post("/admin/{site_id}/force-deploy")
async def admin_force_deploy(
    site_id: str,
    user: dict = Depends(get_current_user)
):
    """Force re-deploy a site (admin only)."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    site = await db.get_client_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if not site.get('archive_s3_key'):
        raise HTTPException(status_code=400, detail="Site has no archive")

    # Reset status and trigger deploy
    await db.update_site_deploy_status(site_id, 'pending')

    # TODO: Download archive and trigger deploy

    return {"success": True, "message": "Force deploy initiated"}


@router.post("/{site_id}/sync-status")
async def sync_site_status(
    site_id: str,
    user: dict = Depends(get_current_user)
):
    """Manually sync deployment status from deploy-node."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    if not site.get('deploy_id'):
        raise HTTPException(status_code=400, detail="Site has no deploy_id")

    if not settings.DEPLOY_NODE_URL:
        raise HTTPException(status_code=500, detail="DEPLOY_NODE_URL not configured")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get deployment status from deploy-node
            response = await client.get(
                f"{settings.DEPLOY_NODE_URL}/api/deploy/{site['deploy_id']}",
                timeout=10.0
            )

            if response.status_code == 404:
                await db.update_site_deploy_status(
                    site_id=site_id,
                    deploy_status='failed',
                    error="Deployment not found in deploy-node"
                )
                return {"success": True, "message": "Deployment not found, marked as failed"}

            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to get deployment status: {response.status_code}"
                )

            result = response.json()
            if not result.get('success'):
                raise HTTPException(
                    status_code=502,
                    detail=result.get('error', 'Unknown error from deploy-node')
                )

            deployment = result.get('data', {})
            deploy_status_raw = deployment.get('status', '')

            # Map deploy-node status to Autosites status
            status_map = {
                'pending': 'pending',
                'uploading': 'deploying',
                'building': 'deploying',
                'deploying': 'deploying',
                'completed': 'active',
                'failed': 'failed',
                'rollback': 'failed',
            }
            deploy_status = status_map.get(deploy_status_raw, deploy_status_raw)

            # Update site with all available data
            update_data = {
                'deploy_status': deploy_status,
            }

            if deployment.get('preview_slug'):
                update_data['preview_slug'] = deployment.get('preview_slug')
                # Try to get preview_url from deployment, or construct it
                preview_url = deployment.get('preview_url')
                if not preview_url:
                    preview_slug = deployment.get('preview_slug', '')
                    if preview_slug:
                        preview_url = f"https://{preview_slug}.autosites.ru"
                if preview_url:
                    update_data['preview_url'] = preview_url

            if deployment.get('server_id'):
                update_data['server_id'] = deployment.get('server_id')
            if deployment.get('server_name'):
                update_data['server_name'] = deployment.get('server_name')
            if deployment.get('server_host'):
                update_data['server_host'] = deployment.get('server_host')
            if deployment.get('port'):
                update_data['container_port'] = deployment.get('port')

            if deploy_status == 'failed' and deployment.get('error_message'):
                update_data['last_error'] = deployment.get('error_message')
                update_data['last_error_at'] = datetime.now(timezone.utc)

            if deployment.get('domain') and deployment.get('domain') != site.get('domain'):
                update_data['domain'] = deployment.get('domain')
                update_data['domain_status'] = 'active' if deploy_status == 'active' else 'pending'

            await db.update_client_site(site_id, update_data)

            # Update request status if site is now active
            if deploy_status == 'active' and site.get('request_id'):
                await db.update_request_status(str(site['request_id']), 'success')

            log.info(f"Synced status for site {site_id}: {deploy_status}")

            return {
                "success": True,
                "message": "Status synced",
                "status": deploy_status,
                "deployment": deployment
            }

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Timeout connecting to deploy-node")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Failed to connect to deploy-node: {str(e)}")
    except Exception as e:
        log.error(f"Error syncing site status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/admin/sync-all-statuses")
async def admin_sync_all_statuses(
    user: dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = None
):
    """Sync all deployment statuses from deploy-node (admin only)."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    if not settings.DEPLOY_NODE_URL:
        raise HTTPException(status_code=500, detail="DEPLOY_NODE_URL not configured")

    # Run sync in background
    from cron_jobs import sync_deploy_statuses
    background_tasks.add_task(sync_deploy_statuses)

    return {
        "success": True,
        "message": "Status sync started in background"
    }
