"""
Search routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth import get_current_approved_user
from app.database import search_requests

router = APIRouter()


@router.get("")
async def search(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
    user: dict = Depends(get_current_approved_user)
):
    """Search requests by company name or client name"""
    results = search_requests(q, limit)

    # Filter by ownership for non-admin users
    if user["role"] != "admin":
        results = [r for r in results if str(r.get("manager_id")) == str(user["id"])]

    return [
        {
            "id": str(r["id"]),
            "manager_id": str(r["manager_id"]) if r.get("manager_id") else None,
            "status": r["status"],
            "client_name": r.get("client_name"),
            "company_name": r.get("company_name"),
            "business_type": r.get("business_type"),
            "result_url": r.get("result_url"),
            "created_at": str(r["created_at"]),
        }
        for r in results
    ]

