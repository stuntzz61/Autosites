"""
Payments Router - управление платежами за хостинг
"""
from typing import Optional
import json
import base64
import logging
import io
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from config import settings
from routes.auth import get_current_user
import db
import httpx

log = logging.getLogger(__name__)

router = APIRouter()

# Try to import qrcode, fallback to simple implementation if not available
try:
    import qrcode
    from qrcode.image.pil import PilImage
    HAS_QRCODE = True
except ImportError:
    HAS_QRCODE = False
    log.warning("qrcode library not installed, using fallback QR generation")


# ==================== DTOs ====================

class CreatePaymentRequest(BaseModel):
    site_id: str
    plan: str
    months: int = 1


class PaymentResponse(BaseModel):
    id: str
    site_id: str
    amount: float
    currency: str
    status: str
    qr_code_url: Optional[str] = None
    payment_url: Optional[str] = None
    expires_at: Optional[str] = None


# ==================== QR Code Generation ====================

def generate_qr_code(data: str) -> tuple[str, bytes]:
    """
    Генерирует QR код с использованием библиотеки qrcode.
    Возвращает (data_url, png_bytes).
    """
    if HAS_QRCODE:
        # Создаём QR код
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(data)
        qr.make(fit=True)

        # Создаём изображение
        img = qr.make_image(fill_color="black", back_color="white")

        # Конвертируем в PNG bytes
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        png_bytes = buffer.getvalue()

        # Создаём data URL
        qr_base64 = base64.b64encode(png_bytes).decode()
        qr_url = f"data:image/png;base64,{qr_base64}"

        return qr_url, png_bytes
    else:
        # Fallback: создаём простой SVG QR код (placeholder)
        qr_svg = f'''<svg width="256" height="256" xmlns="http://www.w3.org/2000/svg">
            <rect width="256" height="256" fill="white"/>
            <rect x="20" y="20" width="50" height="50" fill="black"/>
            <rect x="186" y="20" width="50" height="50" fill="black"/>
            <rect x="20" y="186" width="50" height="50" fill="black"/>
            <rect x="30" y="30" width="30" height="30" fill="white"/>
            <rect x="196" y="30" width="30" height="30" fill="white"/>
            <rect x="30" y="196" width="30" height="30" fill="white"/>
            <rect x="38" y="38" width="14" height="14" fill="black"/>
            <rect x="204" y="38" width="14" height="14" fill="black"/>
            <rect x="38" y="204" width="14" height="14" fill="black"/>
            <text x="128" y="128" font-family="Arial" font-size="10" text-anchor="middle" fill="black">
                QR CODE
            </text>
        </svg>'''

        qr_base64 = base64.b64encode(qr_svg.encode()).decode()
        qr_url = f"data:image/svg+xml;base64,{qr_base64}"

        return qr_url, qr_svg.encode()


def generate_sbp_qr_data(bank_account: str, amount: float, description: str, payment_id: str) -> str:
    """
    Генерирует данные для QR кода СБП (Система Быстрых Платежей).
    Формат: https://qr.nspk.ru/... (для реальной интеграции нужен банковский API)
    """
    # Для тестирования используем простой формат
    # В продакшене здесь будет формат НСПК или API банка
    payment_data = {
        "type": "sbp_payment",
        "id": payment_id,
        "amount": amount,
        "description": description,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(payment_data, ensure_ascii=False)


async def generate_payment_qr(payment_id: str, amount: float, description: str) -> dict:
    """
    Генерирует QR код для оплаты.
    Может быть расширено для интеграции с ЮKassa, CloudPayments, СБП.
    """
    # Формируем данные для QR кода
    qr_data = generate_sbp_qr_data(
        bank_account=settings.PAYMENT_BANK_ACCOUNT if hasattr(settings, 'PAYMENT_BANK_ACCOUNT') else "",
        amount=amount,
        description=description,
        payment_id=payment_id
    )

    # Генерируем QR код
    qr_url, qr_image = generate_qr_code(qr_data)

    # URL для оплаты (для интеграции с платёжной системой)
    payment_url = None
    if hasattr(settings, 'PAYMENT_BASE_URL') and settings.PAYMENT_BASE_URL:
        payment_url = f"{settings.PAYMENT_BASE_URL}/pay/{payment_id}"

    expires_at = datetime.now(timezone.utc) + timedelta(hours=24)

    return {
        "qr_code_url": qr_url,
        "qr_image_data": base64.b64encode(qr_image).decode(),
        "payment_url": payment_url,
        "expires_at": expires_at.isoformat()
    }


# ==================== Endpoints ====================

@router.post("")
async def create_payment(
    data: CreatePaymentRequest,
    user: dict = Depends(get_current_user)
):
    """Create a new payment."""
    # Get site
    site = await db.get_client_site(data.site_id)

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    # Check ownership
    if user['role'] not in ('supervisor', 'director', 'owner') and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Get plan
    plan = await db.get_hosting_plan(data.plan)
    if not plan:
        raise HTTPException(status_code=400, detail="Invalid hosting plan")

    # Calculate amount
    if data.months == 12 and plan.get('price_yearly'):
        amount = float(plan['price_yearly'])
    else:
        amount = float(plan['price_monthly'] or 0) * data.months

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Invalid payment amount")

    # Create payment record
    payment_data = {
        'client_site_id': data.site_id,
        'type': 'payment',
        'amount': amount,
        'currency': 'RUB',
        'plan_id': data.plan,
        'period_months': data.months,
        'status': 'pending',
        'payment_method': 'qr_code',
        'payment_system': 'manual'
    }

    # Generate QR code
    qr_info = await generate_payment_qr(
        payment_id="",  # Will be set after creation
        amount=amount,
        description=f"Оплата хостинга: {site['company_name']} ({data.plan}, {data.months} мес.)"
    )

    payment_data.update({
        'qr_code_url': qr_info['qr_code_url'],
        'payment_url': qr_info['payment_url'],
        'expires_at': datetime.fromisoformat(qr_info['expires_at'].replace('Z', '+00:00'))
    })

    # Create transaction in DB
    payment = await db.create_hosting_transaction(
        client_site_id=data.site_id,
        type='payment',
        amount=amount,
        currency='RUB',
        plan_id=data.plan,
        period_months=data.months,
        qr_code_url=qr_info['qr_code_url'],
        payment_url=qr_info['payment_url'],
        expires_at=payment_data['expires_at']
    )

    log.info(f"Created payment {payment['id']} for site {data.site_id}, amount: {amount} RUB")

    return {
        "id": str(payment['id']),
        "site_id": data.site_id,
        "amount": amount,
        "currency": "RUB",
        "status": "pending",
        "qr_code_url": qr_info['qr_code_url'],
        "payment_url": qr_info['payment_url'],
        "expires_at": qr_info['expires_at'],
        "plan": data.plan,
        "period_months": data.months
    }


@router.get("/{payment_id}")
async def get_payment(payment_id: str, user: dict = Depends(get_current_user)):
    """Get payment details."""
    payment = await db.get_hosting_transaction(payment_id)

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    # Get site to check ownership
    site = await db.get_client_site(str(payment['client_site_id']))

    if not site:
        raise HTTPException(status_code=404, detail="Site not found")

    if user['role'] not in ('supervisor', 'director', 'owner') and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    return payment


@router.get("/{payment_id}/qr")
async def get_payment_qr(payment_id: str, user: dict = Depends(get_current_user)):
    """Get payment QR code."""
    payment = await db.get_hosting_transaction(payment_id)

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    # Check ownership
    site = await db.get_client_site(str(payment['client_site_id']))

    if user['role'] not in ('supervisor', 'director', 'owner') and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    # Check if expired
    if payment.get('expires_at'):
        expires_at = payment['expires_at']
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))

        if expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=400, detail="QR code expired")

    return {
        "qr_url": payment.get('qr_code_url'),
        "payment_url": payment.get('payment_url'),
        "amount": float(payment['amount']),
        "currency": payment.get('currency', 'RUB'),
        "expires_at": payment.get('expires_at')
    }


@router.post("/{payment_id}/verify")
async def verify_payment(payment_id: str, user: dict = Depends(get_current_user)):
    """
    Verify payment status.
    TODO: Интеграция с платежной системой для проверки реального статуса
    """
    payment = await db.get_hosting_transaction(payment_id)

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    # Check ownership
    site = await db.get_client_site(str(payment['client_site_id']))

    if user['role'] not in ('supervisor', 'director', 'owner') and str(site['manager_id']) != str(user['id']):
        raise HTTPException(status_code=403, detail="Access denied")

    if payment['status'] == 'completed':
        return {"status": "completed", "message": "Payment already completed"}

    # TODO: Здесь должна быть проверка в платежной системе
    # Для заглушки - ручная проверка админом

    # Check if manually marked as paid (by admin)
    updated_payment = await db.get_hosting_transaction(payment_id)

    if updated_payment['status'] == 'completed':
        # Extend hosting
        await db.extend_hosting(
            site_id=str(payment['client_site_id']),
            plan=payment['plan_id'],
            months=payment['period_months']
        )

        # Update payment
        await db.update_hosting_transaction(payment_id, {
            'status': 'completed',
            'verified_at': datetime.now(timezone.utc)
        })

        log.info(f"Payment {payment_id} verified, hosting extended for site {payment['client_site_id']}")

        return {"status": "completed", "message": "Payment verified and hosting extended"}

    return {"status": "pending", "message": "Payment not yet completed"}


@router.get("")
async def list_payments(
    site_id: Optional[str] = None,
    status: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    """List payments."""
    if site_id:
        # Verify ownership
        site = await db.get_client_site(site_id)

        if not site:
            raise HTTPException(status_code=404, detail="Site not found")

        if user['role'] not in ('supervisor', 'director', 'owner') and str(site['manager_id']) != str(user['id']):
            raise HTTPException(status_code=403, detail="Access denied")

        payments = await db.list_hosting_transactions(client_site_id=site_id, status=status)
    else:
        if user['role'] not in ('supervisor', 'director', 'owner'):
            raise HTTPException(status_code=403, detail="Admin access required")

        payments = await db.list_hosting_transactions(status=status)

    return {"items": payments}


# ==================== Admin Endpoints ====================

@router.post("/{payment_id}/complete")
async def admin_complete_payment(payment_id: str, user: dict = Depends(get_current_user)):
    """Manually mark payment as completed (admin only)."""
    if user['role'] != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")

    payment = await db.get_hosting_transaction(payment_id)

    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")

    if payment['status'] == 'completed':
        raise HTTPException(status_code=400, detail="Payment already completed")

    # Update payment
    await db.update_hosting_transaction(payment_id, {
        'status': 'completed',
        'verified_at': datetime.now(timezone.utc),
        'completed_at': datetime.now(timezone.utc)
    })

    # Extend hosting
    await db.extend_hosting(
        site_id=str(payment['client_site_id']),
        plan=payment['plan_id'],
        months=payment['period_months']
    )

    log.info(f"Admin {user['id']} manually completed payment {payment_id}")

    return {"success": True, "message": "Payment marked as completed"}

