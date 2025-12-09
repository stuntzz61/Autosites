from typing import Optional, List
import io
import base64
import httpx
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from config import settings
from routes.auth import get_admin_user
import db

log = logging.getLogger(__name__)

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


@router.post("/managers/{manager_id}/make-admin")
async def make_manager_admin(manager_id: str, user: dict = Depends(get_admin_user)):
    """Promote manager to admin role."""
    await db.update_user_role(manager_id, 'admin')
    return {"success": True}


# ==================== Pending Registrations ====================

@router.get("/pending")
async def list_pending(user: dict = Depends(get_admin_user)):
    """List pending registrations."""
    return await db.list_pending_registrations()


@router.post("/pending/{user_id}/approve")
async def approve_registration(user_id: str, user: dict = Depends(get_admin_user)):
    """Approve a pending registration."""
    # Check current status first
    current_status = await db.get_user_approval_status(user_id)
    if not current_status:
        raise HTTPException(status_code=404, detail="User not found")

    if current_status == 'approved':
        return {"success": True, "message": "Already approved"}

    if current_status == 'rejected':
        raise HTTPException(status_code=400, detail="User was already rejected by another admin")

    result = await db.approve_user(user_id)
    if not result:
        raise HTTPException(status_code=400, detail="Status already changed by another admin")

    return {"success": True}


@router.post("/pending/{user_id}/reject")
async def reject_registration(
    user_id: str,
    data: RejectRequest,
    user: dict = Depends(get_admin_user)
):
    """Reject a pending registration."""
    # Check current status first
    current_status = await db.get_user_approval_status(user_id)
    if not current_status:
        raise HTTPException(status_code=404, detail="User not found")

    if current_status == 'rejected':
        return {"success": True, "message": "Already rejected"}

    if current_status == 'approved':
        raise HTTPException(status_code=400, detail="User was already approved by another admin")

    result = await db.reject_user(user_id, data.reason)
    if not result:
        raise HTTPException(status_code=400, detail="Status already changed by another admin")

    return {"success": True}


@router.post("/pending/{user_id}/reset")
async def reset_registration(user_id: str, user: dict = Depends(get_admin_user)):
    """Reset a rejected user's status to pending (allow re-application)."""
    current_status = await db.get_user_approval_status(user_id)
    if not current_status:
        raise HTTPException(status_code=404, detail="User not found")

    if current_status != 'rejected':
        raise HTTPException(status_code=400, detail="Can only reset rejected users")

    await db.reset_user_approval(user_id)
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

async def send_telegram_message(tg_id: int, text: str, photo_data: Optional[str] = None) -> bool:
    """Send message to user via Telegram Bot API."""
    try:
        async with httpx.AsyncClient() as client:
            if photo_data:
                # Check if it's a base64 data URL
                if photo_data.startswith('data:'):
                    # Extract base64 content
                    # Format: data:image/jpeg;base64,/9j/4AAQ...
                    try:
                        header, b64_content = photo_data.split(',', 1)
                        image_bytes = base64.b64decode(b64_content)

                        # Determine file extension from header
                        ext = 'jpg'
                        if 'png' in header:
                            ext = 'png'
                        elif 'gif' in header:
                            ext = 'gif'
                        elif 'webp' in header:
                            ext = 'webp'

                        # Send as multipart/form-data
                        files = {
                            'photo': (f'photo.{ext}', image_bytes, f'image/{ext}')
                        }
                        data = {
                            'chat_id': str(tg_id),
                            'caption': text,
                            'parse_mode': 'HTML'
                        }

                        response = await client.post(
                            f"https://api.telegram.org/bot{settings.BOT_TOKEN}/sendPhoto",
                            data=data,
                            files=files,
                            timeout=30.0
                        )
                    except Exception as e:
                        log.error(f"Failed to decode base64 image: {e}")
                        # Fallback to text-only message
                        response = await client.post(
                            f"https://api.telegram.org/bot{settings.BOT_TOKEN}/sendMessage",
                            json={
                                "chat_id": tg_id,
                                "text": text,
                                "parse_mode": "HTML"
                            },
                            timeout=10.0
                        )
                else:
                    # It's a regular URL
                    response = await client.post(
                        f"https://api.telegram.org/bot{settings.BOT_TOKEN}/sendPhoto",
                        json={
                            "chat_id": tg_id,
                            "photo": photo_data,
                            "caption": text,
                            "parse_mode": "HTML"
                        },
                        timeout=10.0
                    )
            else:
                # Send text message
                response = await client.post(
                    f"https://api.telegram.org/bot{settings.BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": tg_id,
                        "text": text,
                        "parse_mode": "HTML"
                    },
                    timeout=10.0
                )

            result = response.json()
            if not result.get('ok'):
                log.warning(f"Failed to send message to {tg_id}: {result}")
                return False
            return True
    except Exception as e:
        log.error(f"Error sending message to {tg_id}: {e}")
        return False


@router.post("/broadcast")
async def send_broadcast(data: BroadcastRequest, user: dict = Depends(get_admin_user)):
    """Send broadcast message to managers."""
    if not data.message:
        raise HTTPException(status_code=400, detail="Message is required")

    # Get recipients
    if data.recipient_ids:
        # Get specific managers by ID
        managers = await db.list_managers()
        recipients = [m for m in managers if str(m['id']) in data.recipient_ids and not m.get('is_blocked')]
    else:
        # Send to all active managers
        managers = await db.list_managers()
        recipients = [m for m in managers if not m.get('is_blocked')]

    if not recipients:
        return {
            "success": False,
            "error": "No recipients found",
            "sent_count": 0,
            "failed_count": 0,
        }

    # Send messages
    sent_count = 0
    failed_count = 0

    for manager in recipients:
        tg_id = manager.get('tg_id')
        if not tg_id:
            failed_count += 1
            continue

        success = await send_telegram_message(tg_id, data.message, data.photo)
        if success:
            sent_count += 1
        else:
            failed_count += 1

    log.info(f"Broadcast sent: {sent_count} success, {failed_count} failed")

    return {
        "success": True,
        "sent_count": sent_count,
        "failed_count": failed_count,
        "total_recipients": len(recipients),
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

