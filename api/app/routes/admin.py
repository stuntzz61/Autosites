"""
Admin routes
"""
import logging
import io
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth import get_admin_user
from app.database import (
    get_all_requests, count_all_requests, get_all_managers,
    get_pending_registrations, approve_user, reject_user,
    update_user, get_stats, get_user_by_id, delete_request, update_request
)
from app.config import settings

log = logging.getLogger("admin")

router = APIRouter()


class ApproveRejectRequest(BaseModel):
    reason: Optional[str] = None


class BlockRequest(BaseModel):
    reason: Optional[str] = None


class BroadcastRequest(BaseModel):
    message: str
    manager_ids: Optional[List[str]] = None


class MassArchiveRequest(BaseModel):
    type: str  # 'completed' or 'old'


# ==================== Stats ====================

@router.get("/stats")
async def admin_stats(user: dict = Depends(get_admin_user)):
    """Get admin statistics"""
    return get_stats()


# ==================== Managers ====================

@router.get("/managers")
async def list_managers(
    page: int = 1,
    per_page: int = 20,
    user: dict = Depends(get_admin_user)
):
    """List all managers"""
    offset = (page - 1) * per_page
    managers = get_all_managers(per_page, offset)

    return {
        "items": [
            {
                "id": str(m["id"]),
                "tg_id": m["tg_id"],
                "username": m.get("username"),
                "first_name": m["first_name"],
                "last_name": m.get("last_name"),
                "contact": m.get("contact"),
                "is_blocked": m.get("is_blocked", False),
                "approval_status": m.get("approval_status", "approved"),
                "created_at": str(m["created_at"]),
                "total_requests": m.get("total_requests", 0),
                "completed_requests": m.get("completed_requests", 0),
            }
            for m in managers
        ],
        "total": len(managers),  # TODO: proper count
        "page": page,
        "per_page": per_page,
        "pages": 1,  # TODO: calculate
    }


@router.get("/managers/{manager_id}")
async def get_manager(
    manager_id: str,
    user: dict = Depends(get_admin_user)
):
    """Get manager details"""
    manager = get_user_by_id(manager_id)

    if not manager:
        raise HTTPException(status_code=404, detail="Manager not found")

    return {
        "id": str(manager["id"]),
        "tg_id": manager["tg_id"],
        "username": manager.get("username"),
        "first_name": manager["first_name"],
        "last_name": manager.get("last_name"),
        "contact": manager.get("contact"),
        "is_blocked": manager.get("is_blocked", False),
        "approval_status": manager.get("approval_status", "approved"),
        "created_at": str(manager["created_at"]),
    }


@router.post("/managers/{manager_id}/block")
async def block_manager(
    manager_id: str,
    data: BlockRequest,
    user: dict = Depends(get_admin_user)
):
    """Block a manager"""
    manager = get_user_by_id(manager_id)

    if not manager:
        raise HTTPException(status_code=404, detail="Manager not found")

    update_user(manager_id, is_blocked=True)

    return {"success": True}


@router.post("/managers/{manager_id}/unblock")
async def unblock_manager(
    manager_id: str,
    user: dict = Depends(get_admin_user)
):
    """Unblock a manager"""
    manager = get_user_by_id(manager_id)

    if not manager:
        raise HTTPException(status_code=404, detail="Manager not found")

    update_user(manager_id, is_blocked=False)

    return {"success": True}


@router.delete("/managers/{manager_id}")
async def delete_manager(
    manager_id: str,
    user: dict = Depends(get_admin_user)
):
    """Delete a manager"""
    manager = get_user_by_id(manager_id)

    if not manager:
        raise HTTPException(status_code=404, detail="Manager not found")

    # TODO: Soft delete or handle related data

    return {"success": True}


# ==================== Pending ====================

@router.get("/pending")
async def list_pending(user: dict = Depends(get_admin_user)):
    """List pending registrations"""
    pending = get_pending_registrations()

    return [
        {
            "id": str(p["id"]),
            "tg_id": p["tg_id"],
            "username": p.get("username"),
            "first_name": p["first_name"],
            "last_name": p.get("last_name"),
            "contact": p.get("contact"),
            "created_at": str(p["created_at"]),
        }
        for p in pending
    ]


@router.post("/users/{user_id}/approve")
async def approve_registration(
    user_id: str,
    user: dict = Depends(get_admin_user)
):
    """Approve user registration"""
    target_user = get_user_by_id(user_id)

    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    approve_user(user_id)

    return {"success": True}


@router.post("/users/{user_id}/reject")
async def reject_registration(
    user_id: str,
    data: ApproveRejectRequest,
    user: dict = Depends(get_admin_user)
):
    """Reject user registration"""
    target_user = get_user_by_id(user_id)

    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")

    reject_user(user_id, data.reason)

    return {"success": True}


# ==================== Requests ====================

@router.get("/requests")
async def list_all_requests(
    page: int = 1,
    per_page: int = 20,
    user: dict = Depends(get_admin_user)
):
    """List all requests (admin)"""
    offset = (page - 1) * per_page
    requests = get_all_requests(per_page, offset)
    total = count_all_requests()

    return {
        "items": [
            {
                "id": str(r["id"]),
                "manager_id": str(r["manager_id"]) if r.get("manager_id") else None,
                "status": r["status"],
                "client_name": r.get("client_name"),
                "company_name": r.get("company_name"),
                "business_type": r.get("business_type"),
                "result_url": r.get("result_url"),
                "created_at": str(r["created_at"]),
                "manager_name": r.get("manager_first_name"),
                "manager_username": r.get("manager_username"),
            }
            for r in requests
        ],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    }


# ==================== Mass Operations ====================

@router.post("/mass-archive")
async def mass_archive(
    data: MassArchiveRequest,
    user: dict = Depends(get_admin_user)
):
    """Mass archive requests"""
    requests = get_all_requests(limit=10000)
    archived_count = 0

    if data.type == "completed":
        for r in requests:
            if r["status"] in ["generated_ok", "delivered", "closed"]:
                update_request(str(r["id"]), status="archived")
                archived_count += 1

    elif data.type == "old":
        from datetime import datetime, timedelta, timezone
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        for r in requests:
            created_at = r["created_at"]
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            if created_at < cutoff and r["status"] not in ["archived"]:
                update_request(str(r["id"]), status="archived")
                archived_count += 1

    return {"success": True, "archived": archived_count}


# ==================== Export ====================

@router.get("/export/{format}")
async def export_stats(
    format: str,
    user: dict = Depends(get_admin_user)
):
    """Export statistics to Excel or PDF"""
    stats = get_stats()

    if format == "excel":
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "Statistics"

        ws.append(["Metric", "Value"])
        ws.append(["Total Users", stats["total_users"]])
        ws.append(["Total Managers", stats["total_managers"]])
        ws.append(["Total Requests", stats["total_requests"]])
        ws.append(["Requests Today", stats["requests_today"]])
        ws.append(["Requests This Week", stats["requests_this_week"]])

        # Add status sheet
        ws2 = wb.create_sheet("By Status")
        ws2.append(["Status", "Count"])
        for item in stats.get("by_status", []):
            ws2.append([item["status"], item["count"]])

        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=report.xlsx"}
        )

    elif format == "pdf":
        from reportlab.lib.pagesizes import A4
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib import colors
        from reportlab.lib.units import cm

        output = io.BytesIO()
        doc = SimpleDocTemplate(output, pagesize=A4)
        styles = getSampleStyleSheet()
        elements = []

        # Title
        elements.append(Paragraph("<b>AutoSites Report</b>", styles['Title']))
        elements.append(Spacer(1, 0.5 * cm))

        # Stats table
        data = [
            ["Metric", "Value"],
            ["Total Users", stats["total_users"]],
            ["Total Managers", stats["total_managers"]],
            ["Total Requests", stats["total_requests"]],
            ["Requests Today", stats["requests_today"]],
            ["Requests This Week", stats["requests_this_week"]],
        ]

        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        elements.append(table)

        doc.build(elements)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=report.pdf"}
        )

    else:
        raise HTTPException(status_code=400, detail="Invalid format")

