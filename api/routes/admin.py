from typing import Optional, List
import io
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from routes.auth import get_admin_user
import db

router = APIRouter()


class RejectRequest(BaseModel):
    reason: str


class BroadcastRequest(BaseModel):
    message: str
    photo: Optional[str] = None
    recipient_ids: Optional[List[str]] = None


class MassActionRequest(BaseModel):
    ids: List[str]


# ==================== Dashboard ====================

@router.get("/dashboard")
async def get_dashboard(user: dict = Depends(get_admin_user)):
    """Get admin dashboard data."""
    return await db.get_dashboard_stats()


# ==================== Managers ====================

@router.get("/managers")
async def list_managers(user: dict = Depends(get_admin_user)):
    """List all managers."""
    return await db.list_managers()


@router.get("/managers/{manager_id}")
async def get_manager(manager_id: str, user: dict = Depends(get_admin_user)):
    """Get manager details."""
    manager = await db.get_user_by_id(manager_id)
    if not manager:
        raise HTTPException(status_code=404, detail="Manager not found")

    stats = await db.get_user_stats(manager_id)
    return {**manager, "id": str(manager["id"]), "stats": stats}


@router.post("/managers/{manager_id}/block")
async def block_manager(manager_id: str, user: dict = Depends(get_admin_user)):
    """Block a manager."""
    await db.block_user(manager_id)
    return {"success": True}


@router.post("/managers/{manager_id}/unblock")
async def unblock_manager(manager_id: str, user: dict = Depends(get_admin_user)):
    """Unblock a manager."""
    await db.unblock_user(manager_id)
    return {"success": True}


@router.delete("/managers/{manager_id}")
async def delete_manager(manager_id: str, user: dict = Depends(get_admin_user)):
    """Delete a manager."""
    await db.delete_user(manager_id)
    return {"success": True}


# ==================== Pending Registrations ====================

@router.get("/pending")
async def list_pending(user: dict = Depends(get_admin_user)):
    """List pending registrations."""
    return await db.list_pending_registrations()


@router.post("/pending/{user_id}/approve")
async def approve_registration(user_id: str, user: dict = Depends(get_admin_user)):
    """Approve a pending registration."""
    await db.approve_user(user_id)
    return {"success": True}


@router.post("/pending/{user_id}/reject")
async def reject_registration(
    user_id: str,
    data: RejectRequest,
    user: dict = Depends(get_admin_user)
):
    """Reject a pending registration."""
    await db.reject_user(user_id, data.reason)
    return {"success": True}


# ==================== Requests ====================

@router.get("/requests")
async def list_all_requests(
    status: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    user: dict = Depends(get_admin_user)
):
    """List all requests (admin view)."""
    offset = (page - 1) * limit
    requests = await db.list_requests(status=status, limit=limit, offset=offset)

    return {
        "items": [{**r, "id": str(r["id"])} for r in requests],
        "page": page,
        "limit": limit,
    }


@router.get("/requests/search")
async def search_requests(q: str, user: dict = Depends(get_admin_user)):
    """Search requests by company or client name."""
    # Simple search - can be enhanced with full-text search
    all_requests = await db.list_requests(limit=1000)
    q_lower = q.lower()

    results = [
        {**r, "id": str(r["id"])}
        for r in all_requests
        if q_lower in (r.get('company_name', '') or '').lower() or
           q_lower in (r.get('client_name', '') or '').lower()
    ]

    return {"items": results[:50]}


@router.post("/requests/mass-archive")
async def mass_archive(data: MassActionRequest, user: dict = Depends(get_admin_user)):
    """Archive multiple requests."""
    await db.mass_archive_requests(data.ids)
    return {"success": True, "count": len(data.ids)}


@router.post("/requests/mass-delete")
async def mass_delete(data: MassActionRequest, user: dict = Depends(get_admin_user)):
    """Delete multiple requests."""
    await db.mass_delete_requests(data.ids)
    return {"success": True, "count": len(data.ids)}


# ==================== Broadcast ====================

@router.post("/broadcast")
async def send_broadcast(data: BroadcastRequest, user: dict = Depends(get_admin_user)):
    """Send broadcast message to managers."""
    # Get recipients
    if data.recipient_ids:
        # Send to specific managers
        recipients = data.recipient_ids
    else:
        # Send to all active managers
        managers = await db.list_managers()
        recipients = [str(m['id']) for m in managers if not m.get('is_blocked')]

    # TODO: Implement actual message sending via bot
    # This would require calling the bot's API or using a message queue

    return {
        "success": True,
        "recipients_count": len(recipients),
    }


# ==================== Stats ====================

@router.get("/stats/overview")
async def get_stats_overview(user: dict = Depends(get_admin_user)):
    """Get overview statistics."""
    return await db.get_overview_stats()


@router.get("/stats/by-status")
async def get_stats_by_status(user: dict = Depends(get_admin_user)):
    """Get statistics by status."""
    return await db.get_stats_by_status()


@router.get("/stats/by-day")
async def get_stats_by_day(days: int = 7, user: dict = Depends(get_admin_user)):
    """Get statistics by day."""
    return await db.get_stats_by_day(days)


@router.get("/stats/managers")
async def get_manager_stats(user: dict = Depends(get_admin_user)):
    """Get manager statistics."""
    return await db.get_manager_stats()


# ==================== Export ====================

@router.get("/export/excel")
async def export_excel(user: dict = Depends(get_admin_user)):
    """Export statistics to Excel."""
    try:
        import openpyxl
        from openpyxl.styles import Font, Alignment
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl not installed")

    # Create workbook
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Statistics"

    # Get data
    stats = await db.get_overview_stats()
    by_status = await db.get_stats_by_status()
    managers = await db.get_manager_stats()

    # Overview
    ws['A1'] = 'AutoSites Statistics'
    ws['A1'].font = Font(bold=True, size=14)
    ws['A2'] = f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}'

    ws['A4'] = 'Overview'
    ws['A4'].font = Font(bold=True)
    ws['A5'] = 'Total Requests'
    ws['B5'] = stats.get('total_requests', 0)
    ws['A6'] = 'Total Managers'
    ws['B6'] = stats.get('total_managers', 0)
    ws['A7'] = 'This Month'
    ws['B7'] = stats.get('this_month', 0)

    # By Status
    ws['A9'] = 'By Status'
    ws['A9'].font = Font(bold=True)
    row = 10
    for item in by_status:
        ws[f'A{row}'] = item.get('status', 'unknown')
        ws[f'B{row}'] = item.get('count', 0)
        row += 1

    # Top Managers
    ws['A{}'.format(row + 1)] = 'Top Managers'
    ws['A{}'.format(row + 1)].font = Font(bold=True)
    row += 2
    ws[f'A{row}'] = 'Name'
    ws[f'B{row}'] = 'Username'
    ws[f'C{row}'] = 'Requests'
    row += 1
    for m in managers[:10]:
        ws[f'A{row}'] = f"{m.get('first_name', '')} {m.get('last_name', '')}"
        ws[f'B{row}'] = f"@{m.get('username', '')}" if m.get('username') else '-'
        ws[f'C{row}'] = m.get('request_count', 0)
        row += 1

    # Save to buffer
    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=stats_{datetime.now().strftime('%Y%m%d')}.xlsx"}
    )


@router.get("/export/pdf")
async def export_pdf(user: dict = Depends(get_admin_user)):
    """Export statistics to PDF."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
    except ImportError:
        raise HTTPException(status_code=500, detail="reportlab not installed")

    # Get data
    stats = await db.get_overview_stats()
    by_status = await db.get_stats_by_status()
    managers = await db.get_manager_stats()

    # Create PDF
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    elements.append(Paragraph("AutoSites Statistics", styles['Title']))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['Normal']))
    elements.append(Spacer(1, 20))

    # Overview
    elements.append(Paragraph("Overview", styles['Heading2']))
    overview_data = [
        ['Metric', 'Value'],
        ['Total Requests', str(stats.get('total_requests', 0))],
        ['Total Managers', str(stats.get('total_managers', 0))],
        ['This Month', str(stats.get('this_month', 0))],
    ]
    overview_table = Table(overview_data, colWidths=[200, 100])
    overview_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(overview_table)
    elements.append(Spacer(1, 20))

    # By Status
    elements.append(Paragraph("By Status", styles['Heading2']))
    status_data = [['Status', 'Count']]
    for item in by_status:
        status_data.append([item.get('status', 'unknown'), str(item.get('count', 0))])
    status_table = Table(status_data, colWidths=[200, 100])
    status_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(status_table)
    elements.append(Spacer(1, 20))

    # Top Managers
    elements.append(Paragraph("Top Managers", styles['Heading2']))
    manager_data = [['Name', 'Username', 'Requests']]
    for m in managers[:10]:
        manager_data.append([
            f"{m.get('first_name', '')} {m.get('last_name', '')}",
            f"@{m.get('username', '')}" if m.get('username') else '-',
            str(m.get('request_count', 0))
        ])
    manager_table = Table(manager_data, colWidths=[150, 100, 80])
    manager_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(manager_table)

    doc.build(elements)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=stats_{datetime.now().strftime('%Y%m%d')}.pdf"}
    )

