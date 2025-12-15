"""
Domain Registration Router - REG.RU integration via deploy-node
Provides domain check, registration, and DNS configuration for requests
"""
from typing import Optional, List
import httpx
import logging

from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel

from config import settings
from routes.auth import get_current_user
import db

log = logging.getLogger(__name__)

router = APIRouter()


# ==================== DTOs ====================

class CheckDomainRequest(BaseModel):
    domain: str
    tlds: Optional[List[str]] = None
    max_suggestions: Optional[int] = 10


class CheckDomainResponse(BaseModel):
    status: str  # "available", "taken", "invalid_domain"
    domain: str
    price: Optional[dict] = None  # {"currency": "RUB", "amount": 199.00}
    is_premium: Optional[bool] = False
    alternatives: Optional[List[dict]] = None  # [{"domain": "...", "price": {...}}]
    message: Optional[str] = None


class RegisterDomainRequest(BaseModel):
    domain: str
    period: Optional[int] = 1  # years
    configure_dns: Optional[bool] = True  # auto-configure DNS


class RegisterDomainResponse(BaseModel):
    status: str  # "registered", "bill_created", "taken", "failed"
    domain: str
    service_id: Optional[str] = None
    bill_id: Optional[str] = None
    dns: Optional[dict] = None  # {"status": "configuring"} or {"status": "configured"}
    message: Optional[str] = None


class DomainStatusResponse(BaseModel):
    domain: Optional[str] = None
    registration: Optional[dict] = None
    dns: Optional[dict] = None
    updated_at: Optional[str] = None


# ==================== Helper Functions ====================

def get_client_ip(request: Request) -> str:
    """Extract client IP from request headers."""
    # Try X-Forwarded-For first (may contain multiple IPs)
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        # Take the first IP (original client)
        ip = xff.split(",")[0].strip()
        if ip:
            return ip

    # Try X-Real-IP
    xri = request.headers.get("X-Real-IP", "")
    if xri:
        return xri

    # Fall back to client host
    if request.client:
        return request.client.host

    return "127.0.0.1"


async def proxy_to_deploy_node(method: str, endpoint: str, data: dict = None, client_ip: str = None) -> dict:
    """Proxy request to deploy-node domain API."""
    deploy_url = settings.DEPLOY_NODE_URL
    if not deploy_url:
        raise HTTPException(status_code=503, detail="Domain service not configured")

    url = f"{deploy_url}/api/domains{endpoint}"
    headers = {}

    # Pass client IP for REG.RU partner API
    if client_ip:
        headers["X-Real-IP"] = client_ip
        headers["X-Forwarded-For"] = client_ip

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method == "GET":
                response = await client.get(url, headers=headers)
            elif method == "POST":
                response = await client.post(url, json=data, headers=headers)
            else:
                raise ValueError(f"Unsupported method: {method}")

            result = response.json()

            # Log non-2xx responses
            if response.status_code >= 400:
                log.warning(f"Deploy-node domain API error: {response.status_code} - {result}")

            return {
                "status_code": response.status_code,
                "data": result
            }

    except httpx.TimeoutException:
        log.error(f"Timeout connecting to deploy-node: {url}")
        raise HTTPException(status_code=504, detail="Domain service timeout")
    except httpx.RequestError as e:
        log.error(f"Failed to connect to deploy-node: {e}")
        raise HTTPException(status_code=502, detail="Domain service unavailable")
    except Exception as e:
        log.error(f"Error in domain proxy: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal error")


# ==================== Domain Endpoints ====================

@router.post("/{request_id}/domain/check", response_model=CheckDomainResponse)
async def check_domain(
    request_id: str,
    data: CheckDomainRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Check domain availability and get alternatives if taken.

    Returns:
    - status: "available" | "taken" | "invalid_domain"
    - domain: normalized domain name
    - price: registration price if available
    - alternatives: list of available alternatives if domain is taken
    """
    # Verify request belongs to user
    req = await db.get_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(req['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    client_ip = get_client_ip(request)
    log.info(f"Domain check for request {request_id}: {data.domain} from {client_ip}")

    # Proxy to deploy-node
    result = await proxy_to_deploy_node(
        "POST",
        "/check",
        {
            "domain": data.domain,
            "tlds": data.tlds,
            "max_suggest": data.max_suggestions
        },
        client_ip
    )

    response_data = result["data"]

    # Map deploy-node response to our format
    if result["status_code"] == 200 and response_data.get("success"):
        if response_data.get("available"):
            return CheckDomainResponse(
                status="available",
                domain=response_data.get("domain", data.domain),
                price={"currency": "RUB", "amount": response_data.get("price")} if response_data.get("price") else None,
                is_premium=False
            )
        else:
            # Domain is taken - return alternatives
            alternatives = []
            for alt in response_data.get("alternatives", []):
                alternatives.append({
                    "domain": alt.get("domain"),
                    "price": {"currency": "RUB", "amount": alt.get("price")} if alt.get("price") else None
                })

            return CheckDomainResponse(
                status="taken",
                domain=response_data.get("domain", data.domain),
                alternatives=alternatives
            )

    # Handle errors
    error_code = response_data.get("error", "")
    if error_code == "invalid_domain":
        return CheckDomainResponse(
            status="invalid_domain",
            domain=data.domain,
            message="Некорректный формат домена"
        )
    elif error_code == "tld_disabled":
        return CheckDomainResponse(
            status="invalid_domain",
            domain=data.domain,
            message="Эта доменная зона недоступна для регистрации"
        )

    # Generic error
    raise HTTPException(
        status_code=result["status_code"] or 500,
        detail=response_data.get("message", "Failed to check domain")
    )


@router.post("/{request_id}/domain/register", response_model=RegisterDomainResponse)
async def register_domain(
    request_id: str,
    data: RegisterDomainRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    """
    Register a domain via REG.RU.
    Domain will be registered on company profile (no user data required).

    Returns:
    - status: "registered" | "bill_created" | "taken" | "failed"
    - service_id: REG.RU service ID if registered
    - bill_id: Bill ID if payment required
    - dns: DNS configuration status
    """
    # Verify request belongs to user
    req = await db.get_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(req['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    client_ip = get_client_ip(request)
    log.info(f"Domain registration for request {request_id}: {data.domain} from {client_ip}")

    # Check if site exists for this request
    site = await db.get_client_site_by_request(request_id)
    project_id = str(site['id']) if site else request_id

    # Register domain via deploy-node
    result = await proxy_to_deploy_node(
        "POST",
        "/register",
        {
            "domain": data.domain,
            "period": data.period,
            "project_id": project_id
        },
        client_ip
    )

    response_data = result["data"]

    if result["status_code"] in (200, 202):
        status = response_data.get("status", "failed")

        response = RegisterDomainResponse(
            status=status,
            domain=data.domain,
            service_id=response_data.get("service_id"),
            bill_id=response_data.get("bill_id"),
            message=response_data.get("message")
        )

        # If registration successful and DNS config requested
        if status == "registered" and data.configure_dns:
            # Trigger DNS configuration asynchronously
            try:
                dns_result = await proxy_to_deploy_node(
                    "POST",
                    "/configure-dns",
                    {"domain": data.domain},
                    client_ip
                )
                if dns_result["status_code"] == 200:
                    response.dns = {"status": "configuring"}
                else:
                    response.dns = {"status": "pending", "error": "DNS configuration queued"}
            except Exception as e:
                log.warning(f"DNS configuration failed for {data.domain}: {e}")
                response.dns = {"status": "pending", "error": str(e)}

        # Update site with domain info if registered
        if status == "registered" and site:
            try:
                await db.update_client_site(str(site['id']), {
                    'domain': data.domain,
                    'domain_status': 'pending'
                })
            except Exception as e:
                log.error(f"Failed to update site domain: {e}")

        return response

    # Handle specific errors
    error_code = response_data.get("error", "")

    if error_code == "domain_taken":
        return RegisterDomainResponse(
            status="taken",
            domain=data.domain,
            message="Домен уже занят"
        )
    elif error_code == "not_enough_money":
        # This shouldn't happen with ok_if_no_money=1, but handle anyway
        return RegisterDomainResponse(
            status="bill_created",
            domain=data.domain,
            bill_id=response_data.get("bill_id"),
            message="Недостаточно средств на балансе. Создан счёт на оплату."
        )

    # Generic error
    error_message = response_data.get("message", "Ошибка регистрации домена")
    log.error(f"Domain registration failed: {error_code} - {error_message}")

    return RegisterDomainResponse(
        status="failed",
        domain=data.domain,
        message=error_message
    )


@router.get("/{request_id}/domain/status", response_model=DomainStatusResponse)
async def get_domain_status(
    request_id: str,
    user: dict = Depends(get_current_user)
):
    """
    Get domain registration and DNS status for a request.
    """
    # Verify request belongs to user
    req = await db.get_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")

    if user['role'] != 'admin' and str(req['user_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Get site for this request
    site = await db.get_client_site_by_request(request_id)
    if not site:
        return DomainStatusResponse(
            domain=None,
            registration={"status": "none"},
            dns={"status": "none"}
        )

    domain = site.get('domain')
    if not domain:
        return DomainStatusResponse(
            domain=None,
            registration={"status": "none"},
            dns={"status": "none"}
        )

    # Return current status from DB
    domain_status = site.get('domain_status', 'pending')
    ssl_enabled = site.get('ssl_enabled', False)

    return DomainStatusResponse(
        domain=domain,
        registration={
            "status": "registered" if domain_status in ('active', 'pending') else domain_status
        },
        dns={
            "status": domain_status,
            "ssl_enabled": ssl_enabled
        },
        updated_at=site.get('updated_at', '').isoformat() if site.get('updated_at') else None
    )


@router.get("/domain/prices")
async def get_domain_prices(
    request: Request,
    user: dict = Depends(get_current_user)
):
    """Get TLD pricing information."""
    client_ip = get_client_ip(request)

    result = await proxy_to_deploy_node("GET", "/prices", client_ip=client_ip)

    if result["status_code"] == 200:
        return result["data"]

    raise HTTPException(
        status_code=result["status_code"],
        detail="Failed to get pricing"
    )

