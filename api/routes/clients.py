"""
Client Registration Router - Создание клиентских аккаунтов для client-editor-ui
Интеграция между Autosites и cms-service/auth-service
"""
import logging
import secrets
import string
import httpx
from typing import Optional
from pydantic import BaseModel

from fastapi import APIRouter, HTTPException, Depends

from config import settings
from routes.auth import get_current_user
import db

log = logging.getLogger(__name__)

router = APIRouter()


def generate_password(length: int = 12) -> str:
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def generate_login(company_name: str) -> str:
    """Generate login from company name."""
    # Транслитерация и очистка
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
        'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya', ' ': '_', '-': '_'
    }

    result = []
    for char in company_name.lower():
        if char in translit_map:
            result.append(translit_map[char])
        elif char.isalnum():
            result.append(char)

    login = ''.join(result)[:30]  # Ограничиваем длину
    return login or 'client'


# ==================== DTOs ====================

class RegisterClientRequest(BaseModel):
    """Request to register a client for site editing."""
    site_id: str  # ID сайта в client_sites
    company_name: str
    client_name: Optional[str] = None
    client_contact: Optional[str] = None
    telegram_id: Optional[str] = None
    # Можно указать свои логин/пароль или сгенерировать автоматически
    login: Optional[str] = None
    password: Optional[str] = None


class ProvisionSiteRequest(BaseModel):
    """Request to provision site in CMS after deploy."""
    site_id: str  # ID сайта в client_sites
    initial_config: Optional[dict] = None


class ClientCredentials(BaseModel):
    """Client credentials for site editor."""
    login: str
    password: str
    editor_url: str


# ==================== Internal Functions ====================

async def create_auth_user(login: str, password: str, role: str = 'client', telegram_id: Optional[str] = None) -> dict:
    """Create user in auth-service."""
    if not settings.AUTH_SERVICE_URL:
        raise HTTPException(status_code=500, detail="AUTH_SERVICE_URL not configured")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.AUTH_SERVICE_URL}/api/admin/users",
                json={
                    "login": login,
                    "password": password,
                    "role": role,
                    "telegram_id": telegram_id
                },
                headers={
                    "X-Admin-Secret": settings.AUTH_SERVICE_ADMIN_SECRET
                } if settings.AUTH_SERVICE_ADMIN_SECRET else {}
            )

            if response.status_code == 409:
                # User already exists
                log.warning(f"User {login} already exists in auth-service")
                return {"id": None, "login": login, "already_exists": True}

            if response.status_code >= 400:
                log.error(f"Failed to create user in auth-service: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to create user in auth-service: {response.text}"
                )

            return response.json().get("user", response.json())

    except httpx.RequestError as e:
        log.error(f"Auth-service request failed: {e}")
        raise HTTPException(status_code=502, detail=f"Auth-service unavailable: {str(e)}")


async def provision_cms_site(
    user_id: str,
    request_id: str,
    domain: str,
    base_archive_s3_key: str,
    initial_config: dict
) -> dict:
    """Provision site in CMS service."""
    if not settings.CMS_SERVICE_URL:
        raise HTTPException(status_code=500, detail="CMS_SERVICE_URL not configured")

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{settings.CMS_SERVICE_URL}/internal/sites/provision",
                json={
                    "user_id": user_id,
                    "request_id": request_id,
                    "domain": domain,
                    "base_archive_s3_key": base_archive_s3_key,
                    "initial_config": initial_config
                }
            )

            if response.status_code >= 400:
                log.error(f"Failed to provision site in CMS: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to provision site in CMS: {response.text}"
                )

            return response.json()

    except httpx.RequestError as e:
        log.error(f"CMS-service request failed: {e}")
        raise HTTPException(status_code=502, detail=f"CMS-service unavailable: {str(e)}")


# ==================== Endpoints ====================

@router.post("/register")
async def register_client(
    data: RegisterClientRequest,
    user: dict = Depends(get_current_user)
):
    """
    Register a client account for site editing.
    Creates user in auth-service and returns login credentials.

    This is typically called after a site is deployed and ready for client editing.
    """
    # Check permission (supervisor+ can register clients)
    if user['role'] not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Supervisor access required")

    # Get site info
    site = await db.get_client_site(data.site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Generate or use provided credentials
    login = data.login or generate_login(data.company_name)
    password = data.password or generate_password()

    # Check if client already registered for this site
    existing_client = await db.get_client_by_site_id(data.site_id)
    if existing_client:
        log.info(f"Client already registered for site {data.site_id}")
        return {
            "success": True,
            "message": "Client already registered",
            "already_exists": True,
            "credentials": {
                "login": existing_client.get('login'),
                "editor_url": "https://studio.wenlix.ru/"
            }
        }

    # Create user in auth-service
    auth_user = await create_auth_user(
        login=login,
        password=password,
        role='client',
        telegram_id=data.telegram_id
    )

    if auth_user.get('already_exists'):
        # Try with numbered suffix
        for i in range(1, 100):
            try:
                auth_user = await create_auth_user(
                    login=f"{login}_{i}",
                    password=password,
                    role='client',
                    telegram_id=data.telegram_id
                )
                login = f"{login}_{i}"
                break
            except HTTPException as e:
                if e.status_code != 502 or "already exists" not in str(e.detail).lower():
                    raise
        else:
            raise HTTPException(status_code=409, detail="Could not create unique login")

    # Save client info to our database
    client_record = await db.register_site_client(
        site_id=data.site_id,
        auth_user_id=auth_user.get('id'),
        login=login,
        company_name=data.company_name,
        client_name=data.client_name,
        client_contact=data.client_contact,
        telegram_id=data.telegram_id
    )

    log.info(f"Registered client {login} for site {data.site_id}")

    return {
        "success": True,
        "message": "Client registered successfully",
        "credentials": ClientCredentials(
            login=login,
            password=password,  # Only returned once!
            editor_url=f"https://wenlix.ru/editor/"
        ),
        "client_id": client_record.get('id') if client_record else None
    }


@router.post("/provision-site")
async def provision_site_for_editing(
    data: ProvisionSiteRequest,
    user: dict = Depends(get_current_user)
):
    """
    Provision a deployed site in CMS service for editing.
    Should be called after site deploy is complete.

    This:
    1. Gets site info from client_sites
    2. Creates site record in cms-service with archive key
    3. Links client account (if registered) to the CMS site
    """
    # Check permission
    if user['role'] not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Supervisor access required")

    # Get site info
    site = await db.get_client_site(data.site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Check if site is deployed
    if site.get('deploy_status') != 'active':
        raise HTTPException(
            status_code=400,
            detail=f"Site must be deployed first. Current status: {site.get('deploy_status')}"
        )

    # Check if site has archive
    archive_key = site.get('archive_s3_key')
    if not archive_key:
        raise HTTPException(status_code=400, detail="Site has no archive key")

    # Get client info (if registered)
    client = await db.get_client_by_site_id(data.site_id)
    if not client:
        raise HTTPException(
            status_code=400,
            detail="Client not registered. Call /clients/register first."
        )

    # Provision in CMS
    domain = site.get('domain') or f"{site.get('preview_slug')}.{settings.PREVIEW_DOMAIN}"
    request_id = str(site.get('request_id') or site.get('id'))

    initial_config = data.initial_config or {
        "site": {
            "title": site.get('company_name', 'Сайт'),
            "domain": domain
        }
    }

    cms_result = await provision_cms_site(
        user_id=client.get('auth_user_id'),
        request_id=request_id,
        domain=domain,
        base_archive_s3_key=archive_key,
        initial_config=initial_config
    )

    # Update our database with CMS site ID
    if cms_result.get('success') and cms_result.get('site'):
        await db.update_site_cms_id(
            site_id=data.site_id,
            cms_site_id=cms_result['site'].get('id')
        )

    log.info(f"Provisioned site {data.site_id} in CMS")

    return {
        "success": True,
        "message": "Site provisioned for editing",
        "cms_site": cms_result.get('site'),
        "editor_url": "https://studio.wenlix.ru/"
    }


@router.post("/setup-complete/{site_id}")
async def complete_client_setup(
    site_id: str,
    initial_config: Optional[dict] = None,
    user: dict = Depends(get_current_user)
):
    """
    Complete setup for a deployed site:
    1. Register client (if not already)
    2. Provision in CMS

    Returns credentials and editor URL.
    """
    if user['role'] not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Supervisor access required")

    # Get site
    site = await db.get_client_site(site_id)
    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Check deploy status
    if site.get('deploy_status') != 'active':
        raise HTTPException(
            status_code=400,
            detail=f"Site must be deployed. Current status: {site.get('deploy_status')}"
        )

    company_name = site.get('company_name', 'Компания')

    # Step 1: Register client if needed
    client = await db.get_client_by_site_id(site_id)
    credentials = None

    if not client:
        login = generate_login(company_name)
        password = generate_password()

        auth_user = await create_auth_user(login, password, 'client')

        if auth_user.get('already_exists'):
            for i in range(1, 100):
                try:
                    auth_user = await create_auth_user(f"{login}_{i}", password, 'client')
                    login = f"{login}_{i}"
                    break
                except:
                    continue

        client = await db.register_site_client(
            site_id=site_id,
            auth_user_id=auth_user.get('id'),
            login=login,
            company_name=company_name,
            client_name=site.get('client_name'),
            client_contact=site.get('client_contact')
        )

        credentials = {
            "login": login,
            "password": password
        }

        log.info(f"Created client {login} for site {site_id}")

    # Step 2: Provision in CMS
    archive_key = site.get('archive_s3_key')
    if not archive_key:
        raise HTTPException(status_code=400, detail="Site has no archive key")

    domain = site.get('domain') or f"{site.get('preview_slug')}.{settings.PREVIEW_DOMAIN}"

    cms_result = await provision_cms_site(
        user_id=client.get('auth_user_id'),
        request_id=str(site.get('request_id') or site_id),
        domain=domain,
        base_archive_s3_key=archive_key,
        initial_config=initial_config or {"site": {"title": company_name}}
    )

    if cms_result.get('success'):
        await db.update_site_cms_id(site_id, cms_result['site'].get('id'))

    return {
        "success": True,
        "message": "Client setup complete",
        "credentials": credentials,  # None if client already existed
        "editor_url": "https://wenlix.ru/editor/",
        "cms_site_id": cms_result.get('site', {}).get('id')
    }


@router.get("/generate-password")
async def generate_random_password(
    length: int = 12,
    user: dict = Depends(get_current_user)
):
    """Generate a random secure password."""
    if user['role'] not in ('supervisor', 'director', 'owner', 'manager'):
        raise HTTPException(status_code=403, detail="Access denied")

    password = generate_password(length)
    return {"password": password}


@router.get("/generate-login")
async def generate_login_from_name(
    company_name: str,
    user: dict = Depends(get_current_user)
):
    """Generate login from company name (transliteration)."""
    if user['role'] not in ('supervisor', 'director', 'owner', 'manager'):
        raise HTTPException(status_code=403, detail="Access denied")

    login = generate_login(company_name)
    return {"login": login}


@router.get("")
async def list_clients(
    page: int = 1,
    limit: int = 50,
    search: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    """List all registered clients."""
    if user['role'] not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Supervisor access required")

    clients = await db.list_site_clients(page=page, limit=limit, search=search)
    return {
        "items": clients,
        "page": page,
        "limit": limit
    }


@router.post("/{site_id}/reset-password")
async def reset_client_password(
    site_id: str,
    new_password: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    """Reset client password via auth-service."""
    if user['role'] not in ('supervisor', 'director', 'owner'):
        raise HTTPException(status_code=403, detail="Supervisor access required")

    client = await db.get_client_by_site_id(site_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Generate new password if not provided
    password = new_password or generate_password()

    # Reset password in auth-service
    if not settings.AUTH_SERVICE_URL:
        raise HTTPException(status_code=500, detail="AUTH_SERVICE_URL not configured")

    try:
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            response = await http_client.post(
                f"{settings.AUTH_SERVICE_URL}/api/admin/users/{client.get('auth_user_id')}/reset-password",
                json={"new_password": password},
                headers={
                    "X-Admin-Secret": settings.AUTH_SERVICE_ADMIN_SECRET
                } if settings.AUTH_SERVICE_ADMIN_SECRET else {}
            )

            if response.status_code >= 400:
                log.error(f"Failed to reset password: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to reset password in auth-service: {response.text}"
                )

    except httpx.RequestError as e:
        log.error(f"Auth-service request failed: {e}")
        raise HTTPException(status_code=502, detail=f"Auth-service unavailable: {str(e)}")

    log.info(f"Password reset for client {client.get('login')} (site {site_id})")

    return {
        "success": True,
        "new_password": password,
        "login": client.get('login')
    }


@router.get("/{site_id}")
async def get_client_info(
    site_id: str,
    user: dict = Depends(get_current_user)
):
    """Get client info for a site."""
    if user['role'] not in ('supervisor', 'director', 'owner'):
        site = await db.get_client_site(site_id)
        if not site or str(site.get('manager_id')) != str(user['id']):
            raise HTTPException(status_code=403, detail="Access denied")

    client = await db.get_client_by_site_id(site_id)

    if not client:
        return {"registered": False}

    return {
        "registered": True,
        "login": client.get('login'),
        "company_name": client.get('company_name'),
        "client_name": client.get('client_name'),
        "created_at": client.get('created_at'),
        "cms_site_id": client.get('cms_site_id'),
        "editor_url": "https://studio.wenlix.ru/"
    }

