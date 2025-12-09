"""
Client Sites Router - CRUD операции для сайтов клиентов
Связывает заявки с деплоями и управляет хостингом
"""
from typing import Optional, List, Any
import json
import httpx
import logging
import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form, BackgroundTasks
from pydantic import BaseModel

from config import settings
from routes.auth import get_current_user
import db

log = logging.getLogger(__name__)

router = APIRouter()


def serialize_site(site: dict) -> dict:
    """Convert UUID fields and other non-JSON-serializable types to strings."""
    if not site:
        return site
    result = {}
    for key, value in site.items():
        if value is None:
            result[key] = None
        elif isinstance(value, UUID):
            result[key] = str(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, dict):
            # Recursively serialize nested dicts
            result[key] = serialize_site(value)
        elif isinstance(value, list):
            # Serialize list items
            result[key] = [serialize_site(item) if isinstance(item, dict) else
                          str(item) if isinstance(item, UUID) else item
                          for item in value]
        elif hasattr(value, '__str__') and not isinstance(value, (str, int, float, bool)):
            # Convert any other objects to string
            result[key] = str(value)
        else:
            result[key] = value
    return result


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
    request_id: Optional[str] = None  # Может приходить как request_id или requestId
    requestId: Optional[str] = None  # Альтернативное поле (camelCase)
    status: str  # completed, error
    archive_s3_key: Optional[str] = None
    archiveSize: Optional[str] = None  # Альтернативное поле (camelCase)
    archive_size_bytes: Optional[int] = None
    archiveSizeBytes: Optional[int] = None  # Альтернативное поле (camelCase)
    error_message: Optional[str] = None
    error: Optional[str] = None  # Альтернативное поле
    errorMessage: Optional[str] = None  # Альтернативное поле (camelCase)

    def get_request_id(self) -> Optional[str]:
        """Get request_id from either field"""
        return self.request_id or self.requestId

    def get_archive_s3_key(self) -> Optional[str]:
        """Get archive_s3_key from either field"""
        return self.archive_s3_key or self.archiveSize

    def get_archive_size_bytes(self) -> Optional[int]:
        """Get archive_size_bytes from either field"""
        return self.archive_size_bytes or self.archiveSizeBytes

    def get_error_message(self) -> Optional[str]:
        """Get error message from either field"""
        return self.error_message or self.error or self.errorMessage


# ==================== Helper Functions ====================

async def trigger_deploy(site: dict, archive_path: str = None, user_id: str = None):
    """
    Trigger deployment to deploy-node.

    Args:
        site: Client site dict
        archive_path: Optional local path to archive (if None, will download from S3)
        user_id: Optional user ID who initiated deploy
    """
    deploy_url = settings.DEPLOY_NODE_URL
    if not deploy_url:
        log.warning("DEPLOY_NODE_URL not configured, skipping deployment")
        return None

    import tempfile
    import os
    from io import BytesIO

    temp_file = None
    archive_file = None

    try:
        # If archive_path not provided, download from S3
        if not archive_path and site.get('archive_s3_key'):
            log.info(f"Downloading archive from S3: {site['archive_s3_key']}")
            import s3
            archive_bytes = await s3.download_file_from_s3(site['archive_s3_key'])

            # Create temp file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
            temp_file.write(archive_bytes)
            temp_file.close()
            archive_path = temp_file.name
            archive_file = open(archive_path, 'rb')
            log.info(f"Downloaded archive to temp file: {archive_path}")
        elif archive_path:
            archive_file = open(archive_path, 'rb')
        else:
            raise ValueError("No archive_path or archive_s3_key provided")

        async with httpx.AsyncClient(timeout=300.0) as client:
            # Prepare multipart form data
            # Extract filename from S3 key or use default
            filename = 'site.zip'
            if site.get('archive_s3_key'):
                filename = os.path.basename(site['archive_s3_key']) or 'site.zip'
            elif archive_path:
                filename = os.path.basename(archive_path) or 'site.zip'

            # Read file content
            archive_file.seek(0)
            archive_content = archive_file.read()
            archive_file.close()

            # Prepare multipart form
            files = {'archive': (filename, archive_content, 'application/zip')}
            data = {
                'auto_select': 'true',
                'enable_ssl': 'true',  # SSL включён по умолчанию для preview доменов (*.autosites.ru)
                'request_id': str(site.get('request_id')) if site.get('request_id') else '',  # Convert UUID to string
                'client_site_id': str(site['id']),  # Pass client_site_id for callback
            }

            # Add domain if set
            if site.get('domain'):
                data['domain'] = site['domain']

            log.info(f"Sending deploy request to {deploy_url}/api/deploy for site {site['id']}")
            response = await client.post(
                f"{deploy_url}/api/deploy",
                files=files,
                data=data,
                timeout=300.0
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
        log.error(f"Error triggering deploy for site {site['id']}: {e}", exc_info=True)
        await db.update_site_deploy_status(
            site_id=str(site['id']),
            deploy_status='failed',
            error=str(e)
        )
        return None
    finally:
        # Cleanup
        if archive_file and not archive_file.closed:
            archive_file.close()
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass


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
        "items": [serialize_site(s) for s in sites],
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
    return {"items": [serialize_site(s) for s in sites]}


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

    return serialize_site(site)


@router.get("/{site_id}")
async def get_site(site_id: str, user: dict = Depends(get_current_user)):
    """Get a specific client site."""
    site = await db.get_client_site(site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Check ownership
    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    return serialize_site(site)


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
        return {**serialize_site(existing), "already_exists": True}

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
    return serialize_site(site)


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

    return serialize_site(updated)


@router.post("/{site_id}/deploy")
async def deploy_site(
    site_id: str,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user)
):
    """Trigger deployment for a site."""
    log.info(f"Deploy request for site {site_id} from user {user['id']}")
    site = await db.get_client_site(site_id)

    if not site:
        log.warning(f"Site {site_id} not found for deploy")
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] != 'admin' and str(site['manager_id']) != str(user['id']):
        log.warning(f"Access denied for user {user['id']} to deploy site {site_id}")
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if site has archive
    if not site.get('archive_s3_key'):
        log.error(f"Site {site_id} has no archive_s3_key. Current site data: {site}")
        raise HTTPException(status_code=400, detail="Site has no generated archive")

    log.info(f"Site {site_id} has archive: {site.get('archive_s3_key')}, proceeding with deploy")

    # Check if already deploying
    if site.get('deploy_status') == 'deploying':
        raise HTTPException(status_code=400, detail="Deployment already in progress")

    # Update status
    await db.update_site_deploy_status(site_id, 'pending')

    # Trigger deploy (will download from S3 automatically)
    deployment = await trigger_deploy(site, user_id=str(user['id']))

    if deployment:
        return {
            "success": True,
            "status": "deploying",
            "message": "Deployment started",
            "deployment": deployment
        }
    else:
        return {
            "success": False,
            "status": "failed",
            "message": "Failed to start deployment"
        }


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

    # Call deploy-node to stop the container
    if settings.DEPLOY_NODE_URL and site.get('deploy_id'):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{settings.DEPLOY_NODE_URL}/api/sites/by-id/{site['deploy_id']}/stop"
                )
                if response.status_code not in (200, 404):
                    log.warning(f"Failed to stop site on deploy-node: {response.status_code}")
        except Exception as e:
            log.error(f"Error stopping site on deploy-node: {e}")
            # Continue to update local status anyway

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

    # Stop and cleanup in deploy-node if deployed
    if settings.DEPLOY_NODE_URL and site.get('deploy_id'):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Delete site (stops container, removes nginx config, cleans up files)
                response = await client.delete(
                    f"{settings.DEPLOY_NODE_URL}/api/sites/by-id/{site['deploy_id']}"
                )
                if response.status_code not in (200, 404):
                    log.warning(f"Failed to delete site on deploy-node: {response.status_code}")
                else:
                    log.info(f"Site {site_id} deleted from deploy-node")
        except Exception as e:
            log.error(f"Error deleting site on deploy-node: {e}")
            # Continue to delete from our DB anyway

    await db.delete_client_site(site_id)
    log.info(f"Site {site_id} deleted from database")

    return {"success": True}


# ==================== Webhook Callbacks ====================

@router.post("/webhook/deploy-callback")
async def deploy_callback(data: DeployCallbackRequest):
    """
    Webhook callback from deploy-node.
    Called when deployment status changes.
    Creates client_site if not exists.
    """
    log.info(f"Deploy callback received: {data.deploy_id} -> {data.status}")

    # Find site by deploy_id, client_site_id, or request_id
    site = None
    if data.client_site_id:
        site = await db.get_client_site(data.client_site_id)

    if not site and data.request_id:
        site = await db.get_client_site_by_request(data.request_id)

    if not site:
        site = await db.get_client_site_by_deploy_id(data.deploy_id)

    # If site not found - CREATE IT automatically
    if not site:
        log.info(f"Site not found for deploy_id: {data.deploy_id}, creating new client_site...")

        # Try to get request info if request_id provided
        request = None
        manager_id = None
        company_name = data.preview_slug or data.domain or f"Site-{data.deploy_id[:8]}"

        if data.request_id:
            request = await db.get_request(data.request_id)
            if request:
                manager_id = str(request.get('user_id'))
                payload = request.get('payload', {})
                site_data = payload.get('site', {})
                client_data = payload.get('client', {})
                company_name = site_data.get('company', client_data.get('company', request.get('company_name', company_name)))

        # If no manager_id, try to get from first admin
        if not manager_id:
            admins = await db.list_admins()
            if admins:
                manager_id = str(admins[0]['id'])
            else:
                log.error("No admin users found, cannot create site")
                raise HTTPException(status_code=500, detail="No admin users available")

        # Create the client_site
        site = await db.create_client_site(
            request_id=data.request_id,
            manager_id=manager_id,
            company_name=company_name,
            client_name=request.get('client_name') if request else None,
            client_contact=request.get('client_contact') if request else None,
            hosting_plan='trial'
        )

        # Update with deploy_id
        await db.update_client_site(str(site['id']), {
            'deploy_id': data.deploy_id
        })

        log.info(f"Created client_site {site['id']} for deploy {data.deploy_id}")

    # Map deploy-node status to our status
    status_map = {
        'pending': 'pending',
        'uploading': 'deploying',
        'building': 'deploying',
        'deploying': 'deploying',
        'running': 'active',
        'completed': 'active',
        'active': 'active',
        'stopped': 'stopped',
        'failed': 'failed',
        'error': 'failed',
    }
    deploy_status = status_map.get(data.status, data.status)

    log.info(f"Mapped status {data.status} -> {deploy_status}")

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
    # Log raw data for debugging
    import json
    log.info(f"Generation callback raw data: {json.dumps(data.dict(), indent=2, default=str)}")

    # Get request_id from either field
    request_id = data.get_request_id()
    if not request_id:
        log.error("Generation callback missing request_id")
        raise HTTPException(status_code=422, detail="request_id is required")

    log.info(f"Generation callback received: {request_id} -> {data.status}")

    # Get the request
    request = await db.get_request(request_id)
    if not request:
        log.warning(f"Request not found: {request_id}")
        raise HTTPException(status_code=404, detail="Request not found")

    # Get user info for notifications
    user = await db.get_user_by_id(str(request['user_id']))

    # Get or create client site
    site = await db.get_client_site_by_request(request_id)

    if not site:
        # Create new client site
        payload = request.get('payload', {})
        site_data = payload.get('site', {})
        client_data = payload.get('client', {})

        site = await db.create_client_site(
            request_id=request_id,
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
        archive_s3_key = data.get_archive_s3_key()
        archive_size_bytes = data.get_archive_size_bytes()

        log.info(f"Parsed archive data - archive_s3_key: {archive_s3_key}, archive_size_bytes: {archive_size_bytes}")
        log.info(f"Raw fields - archive_s3_key: {data.archive_s3_key}, archiveSize: {data.archiveSize}")

        if not archive_s3_key:
            log.warning(f"⚠️ WARNING: archive_s3_key is None! Site {site['id']} cannot be deployed without archive.")
            log.warning(f"Full callback data: {json.dumps(data.dict(), indent=2, default=str)}")

        log.info(f"Updating site {site['id']} with archive: {archive_s3_key}, size: {archive_size_bytes}")

        # Update site generation status (this also updates archive_s3_key in client_sites)
        await db.update_site_generation_status(
            site_id=str(site['id']),
            status='completed',
            archive_s3_key=archive_s3_key,
            archive_size_bytes=archive_size_bytes
        )

        # Reload site to get updated archive_s3_key
        site = await db.get_client_site(str(site['id']))
        log.info(f"Site {site['id']} generation completed. Archive: {site.get('archive_s3_key')}, Size: {archive_size_bytes} bytes")

        # Update request status
        await db.update_request_status(request_id, 'success')

        # Auto-deploy if configured
        if settings.AUTO_DEPLOY_ENABLED:
            log.info(f"Auto-deploy enabled, triggering deploy for site {site['id']}")
            try:
                # Update site with archive key if not set
                if archive_s3_key and not site.get('archive_s3_key'):
                    await db.update_client_site(str(site['id']), {
                        'archive_s3_key': archive_s3_key
                    })
                    site['archive_s3_key'] = archive_s3_key

                # Trigger deploy
                await trigger_deploy(site, user_id=str(request['user_id']))
                log.info(f"Auto-deploy triggered successfully for site {site['id']}")
            except Exception as e:
                log.error(f"Failed to auto-deploy site {site['id']}: {e}", exc_info=True)
                # Don't fail the callback, just log the error
                await db.update_site_deploy_status(str(site['id']), 'failed', error=str(e))
        else:
            # Notify manager about generation complete only when auto-deploy is disabled
            # (when auto-deploy is enabled, deploy status notifications will be sent instead)
            await notify_manager_generation_complete(site, 'completed')
            log.info(f"Auto-deploy disabled. Site {site['id']} ready for manual deployment. Archive: {archive_s3_key}")

    elif data.status == 'error':
        error_message = data.get_error_message()
        await db.update_site_generation_status(
            site_id=str(site['id']),
            status='error',
            error=error_message
        )
        await db.update_request_status(request_id, 'error')

        # Notify manager about generation error
        await notify_manager_generation_complete(site, 'error', error_message)

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
        log.debug("BOT_WEBHOOK_URL not configured, skipping notification")
        return

    # Check if manager_tg_id is available
    if not site.get('manager_tg_id'):
        log.debug(f"No manager_tg_id for site {site.get('id')}, skipping notification")
        return

    try:
        async with httpx.AsyncClient() as client:
            # Use proper URL format (with or without /webhook)
            webhook_url = settings.BOT_WEBHOOK_URL
            if not webhook_url.endswith('/webhook'):
                webhook_url = f"{webhook_url.rstrip('/')}/webhook"

            log.debug(f"Sending notification to {webhook_url} for manager {site.get('manager_tg_id')}")

            await client.post(
                webhook_url,
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
            log.info(f"Successfully notified manager {site.get('manager_tg_id')} about generation")
    except httpx.ConnectError as e:
        log.warning(f"Failed to connect to bot webhook at {webhook_url}: {e}. Check BOT_WEBHOOK_URL in .env")
    except Exception as e:
        log.error(f"Failed to notify manager about generation: {e}", exc_info=True)


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
        "items": [serialize_site(s) for s in sites],
        "page": page,
        "limit": limit
    }


@router.post("/admin/{site_id}/force-deploy")
async def admin_force_deploy(
    site_id: str,
    background_tasks: BackgroundTasks,
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

    # Check if already deploying
    if site.get('deploy_status') == 'deploying':
        raise HTTPException(status_code=400, detail="Deployment already in progress")

    # Reset status and trigger deploy
    await db.update_site_deploy_status(site_id, 'pending')

    # Trigger deploy (downloads from S3 automatically)
    deployment = await trigger_deploy(site, user_id=str(user['id']))

    if deployment:
        return {
            "success": True,
            "message": "Force deploy initiated",
            "deployment": deployment
        }
    else:
        return {
            "success": False,
            "message": "Failed to start deployment"
        }


class CreateSiteForRequestRequest(BaseModel):
    """Create client_site for existing request"""
    deploy_id: Optional[str] = None  # Optional: link to existing deploy


@router.post("/create-for-request/{request_id}")
async def create_site_for_request(
    request_id: str,
    data: CreateSiteForRequestRequest = None,
    user: dict = Depends(get_current_user)
):
    """
    Create client_site record for a request.
    Use this when client_site wasn't created automatically.
    """
    # Check if request exists
    request = await db.get_request(request_id)
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")

    # Check access
    if user['role'] != 'admin' and str(request['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if site already exists
    existing = await db.get_client_site_by_request(request_id)
    if existing:
        return {
            "success": True,
            "message": "Site already exists",
            "site": existing
        }

    # Extract data from request payload
    payload = request.get('payload', {})
    site_data = payload.get('site', {})
    client_data = payload.get('client', {})

    # Create client site
    site = await db.create_client_site(
        request_id=request_id,
        manager_id=str(request['user_id']),
        company_name=site_data.get('company', client_data.get('company', request.get('company_name', 'Unknown'))),
        client_name=client_data.get('name', request.get('client_name')),
        client_contact=client_data.get('contact', request.get('client_contact')),
        hosting_plan='trial'
    )

    # If deploy_id provided, link to existing deploy
    if data and data.deploy_id:
        await db.update_client_site(str(site['id']), {
            'deploy_id': data.deploy_id,
            'deploy_status': 'pending'  # Will be synced
        })

    log.info(f"Created client site {site['id']} for request {request_id}")

    return {
        "success": True,
        "message": "Site created",
        "site": serialize_site(site)
    }


@router.post("/admin/import-from-deploy-node")
async def admin_import_from_deploy_node(
    user: dict = Depends(get_current_user)
):
    """
    Import existing deployments from deploy-node and create client_sites.
    Admin only.
    """
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    if not settings.DEPLOY_NODE_URL:
        raise HTTPException(status_code=500, detail="DEPLOY_NODE_URL not configured")

    imported = []
    errors = []

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get all deployments from deploy-node
            response = await client.get(
                f"{settings.DEPLOY_NODE_URL}/api/deploy",
                timeout=10.0
            )

            if response.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to get deployments: {response.status_code}"
                )

            result = response.json()
            deployments = result.get('data', []) if isinstance(result, dict) else result

            for deploy in deployments:
                deploy_id = deploy.get('id')
                if not deploy_id:
                    continue

                # Check if already linked
                existing = await db.get_client_site_by_deploy_id(deploy_id)
                if existing:
                    continue  # Already imported

                # Try to find request by preview_slug or other identifier
                # For now, create orphan client_site that can be linked later
                preview_slug = deploy.get('preview_slug', '')
                domain = deploy.get('domain', '')

                # Create minimal client_site
                try:
                    site = await db.create_client_site(
                        request_id=None,  # No request linked
                        manager_id=str(user['id']),  # Current admin as owner
                        company_name=domain or preview_slug or f"Import-{deploy_id[:8]}",
                        client_name=None,
                        client_contact=None,
                        hosting_plan='trial'
                    )

                    # Update with deploy info
                    status_map = {
                        'pending': 'pending',
                        'uploading': 'deploying',
                        'building': 'deploying',
                        'deploying': 'deploying',
                        'completed': 'active',
                        'failed': 'failed',
                    }

                    await db.update_client_site(str(site['id']), {
                        'deploy_id': deploy_id,
                        'deploy_status': status_map.get(deploy.get('status', ''), deploy.get('status', 'none')),
                        'preview_slug': preview_slug,
                        'preview_url': f"https://{preview_slug}.autosites.ru" if preview_slug else None,
                        'domain': domain if domain and '.autosites.ru' not in domain else None,
                        'server_id': deploy.get('server_id'),
                        'server_name': deploy.get('server_name'),
                        'server_host': deploy.get('server_host'),
                        'container_port': deploy.get('port'),
                    })

                    imported.append({
                        'deploy_id': deploy_id,
                        'site_id': str(site['id']),
                        'preview_slug': preview_slug
                    })
                    log.info(f"Imported deploy {deploy_id} as site {site['id']}")

                except Exception as e:
                    errors.append({
                        'deploy_id': deploy_id,
                        'error': str(e)
                    })
                    log.error(f"Failed to import deploy {deploy_id}: {e}")

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Timeout connecting to deploy-node")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Failed to connect to deploy-node: {str(e)}")

    return {
        "success": True,
        "imported": len(imported),
        "errors": len(errors),
        "details": {
            "imported": imported,
            "errors": errors
        }
    }


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
                'running': 'active',  # Container is running = active
                'completed': 'active',
                'active': 'active',
                'stopped': 'stopped',
                'failed': 'failed',
                'rollback': 'failed',
                'error': 'failed',
            }
            deploy_status = status_map.get(deploy_status_raw, deploy_status_raw)

            # If status not in map, log it for debugging
            if deploy_status_raw and deploy_status_raw not in status_map:
                log.warning(f"Unknown deploy status from deploy-node: {deploy_status_raw}")

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
