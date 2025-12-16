"""
Manager Registration Router - Handle manager registration via invite links
Includes workspace provisioning and profile management
"""
from typing import Optional
import re
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, field_validator

from config import settings
from routes.auth import get_current_user
import db
from psycopg.rows import dict_row

log = logging.getLogger(__name__)

router = APIRouter()


# ==================== DTOs ====================

class InviteStatusResponse(BaseModel):
    status: str  # "new", "activated", "invalid", "expired"
    manager: Optional[dict] = None
    workspace: Optional[dict] = None
    invite_name: Optional[str] = None
    message: Optional[str] = None


class RegisterManagerRequest(BaseModel):
    full_name: str
    phone: str
    email: str
    agree_terms: bool = True

    @field_validator('full_name')
    @classmethod
    def validate_full_name(cls, v):
        v = v.strip()
        if len(v) < 3:
            raise ValueError('ФИО должно содержать минимум 3 символа')
        if len(v) > 255:
            raise ValueError('ФИО слишком длинное')
        # Soft validation: at least 2 parts (first + last name)
        parts = v.split()
        if len(parts) < 2:
            raise ValueError('Укажите полное ФИО (имя и фамилию)')
        return v

    @field_validator('phone')
    @classmethod
    def validate_phone(cls, v):
        # Normalize phone: remove spaces, dashes, parentheses
        v = re.sub(r'[\s\-\(\)]', '', v)

        # Accept formats: +79991234567, 89991234567, 79991234567
        if v.startswith('8') and len(v) == 11:
            v = '+7' + v[1:]
        elif v.startswith('7') and len(v) == 11:
            v = '+' + v
        elif not v.startswith('+'):
            v = '+7' + v

        # Validate format
        if not re.match(r'^\+7\d{10}$', v):
            raise ValueError('Некорректный формат телефона. Используйте: +7 (XXX) XXX-XX-XX')

        return v

    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        v = v.strip().lower()
        if not v:
            raise ValueError('Email обязателен для заполнения')
        # Basic email validation
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, v):
            raise ValueError('Некорректный формат email')
        return v


class RegisterManagerResponse(BaseModel):
    status: str  # "ok", "error"
    manager: Optional[dict] = None
    workspace: Optional[dict] = None
    redirect_url: Optional[str] = None
    message: Optional[str] = None


# ==================== Helper Functions ====================

def generate_workspace_slug(full_name: str, user_id: str) -> str:
    """Generate URL-friendly workspace slug from name."""
    import unicodedata
    import re

    # Transliterate Cyrillic to Latin
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }

    name = full_name.lower()
    result = ''
    for char in name:
        if char in translit_map:
            result += translit_map[char]
        elif char.isalnum():
            result += char
        elif char in ' -_':
            result += '-'

    # Clean up
    result = re.sub(r'-+', '-', result).strip('-')

    # Add unique suffix from user_id
    suffix = user_id[:8] if len(user_id) >= 8 else user_id

    return f"{result}-{suffix}"[:100]


async def create_workspace_for_manager(manager_id: str, full_name: str) -> dict:
    """
    Create workspace/tenant for a manager.
    Idempotent: returns existing workspace if already created.
    """
    async with await db.get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check if workspace already exists for this manager
            await cur.execute(
                "SELECT * FROM workspaces WHERE owner_id = %s LIMIT 1",
                (manager_id,)
            )
            existing = await cur.fetchone()
            if existing:
                log.info(f"Workspace already exists for manager {manager_id}: {existing['id']}")
                return dict(existing)

            # Generate unique slug
            slug = generate_workspace_slug(full_name, manager_id)

            # Ensure slug is unique
            await cur.execute("SELECT id FROM workspaces WHERE slug = %s", (slug,))
            if await cur.fetchone():
                # Add random suffix
                import secrets
                slug = f"{slug}-{secrets.token_hex(4)}"

            # Create workspace
            await cur.execute(
                """INSERT INTO workspaces (name, slug, owner_id, settings, status)
                   VALUES (%s, %s, %s, %s, 'active')
                   RETURNING *""",
                (f"Workspace {full_name}", slug, manager_id, '{}')
            )
            workspace = await cur.fetchone()

            # Link manager to workspace
            await cur.execute(
                "UPDATE users SET workspace_id = %s WHERE id = %s",
                (workspace['id'], manager_id)
            )

            await conn.commit()

            log.info(f"Created workspace {workspace['id']} for manager {manager_id}")
            return dict(workspace)


async def update_manager_profile(user_id: str, full_name: str, phone: str, email: str) -> dict:
    """Update manager profile with registration data."""
    async with await db.get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """UPDATE users
                   SET full_name = %s, phone = %s, email = %s, registration_completed_at = NOW()
                   WHERE id = %s
                   RETURNING *""",
                (full_name, phone, email, user_id)
            )
            user = await cur.fetchone()
            await conn.commit()
            return dict(user) if user else None


async def mark_invite_activated(code: str, user_id: str):
    """Mark invite code as activated by this user."""
    async with await db.get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE invite_codes
                   SET activated_at = NOW(), activated_by = %s
                   WHERE code = %s AND activated_at IS NULL""",
                (user_id, code)
            )
            await conn.commit()


async def get_invite_status(code: str) -> dict:
    """Get detailed invite code status."""
    invite = await db.get_invite_code(code)

    if not invite:
        return {"status": "invalid", "message": "Ссылка недействительна"}

    if not invite.get('is_active'):
        return {"status": "invalid", "message": "Ссылка деактивирована"}

    # Check expiration
    if invite.get('expires_at'):
        expires = invite['expires_at']
        if isinstance(expires, str):
            from datetime import datetime
            expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
        if expires < datetime.now(timezone.utc):
            return {"status": "expired", "message": "Срок действия ссылки истёк"}

    # Check usage limit
    if invite.get('max_uses') and invite.get('uses_count', 0) >= invite['max_uses']:
        return {"status": "invalid", "message": "Лимит использований исчерпан"}

    # Check if already activated (has associated user with completed registration)
    if invite.get('activated_by'):
        user = await db.get_user_by_id(str(invite['activated_by']))
        # If user was deleted, reset activated_by
        if not user:
            async with await db.get_conn() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE invite_codes SET activated_by = NULL, activated_at = NULL WHERE id = %s",
                        (invite['id'],)
                    )
                    await conn.commit()
        elif user and user.get('registration_completed_at'):
            workspace = None
            if user.get('workspace_id'):
                async with await db.get_conn() as conn:
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT id, name, slug FROM workspaces WHERE id = %s",
                            (user['workspace_id'],)
                        )
                        ws = await cur.fetchone()
                        if ws:
                            workspace = {"id": str(ws['id']), "name": ws['name'], "slug": ws['slug']}

            return {
                "status": "activated",
                "manager": {
                    "id": str(user['id']),
                    "full_name": user.get('full_name'),
                    "phone": user.get('phone')
                },
                "workspace": workspace
            }

    return {
        "status": "new",
        "invite_name": invite.get('name'),
        "message": "Требуется регистрация"
    }


# ==================== API Endpoints ====================

@router.get("/invite/{token}", response_model=InviteStatusResponse)
async def check_invite_status(token: str):
    """
    Check invite link status.

    Returns:
    - status: "new" (requires registration), "activated" (already registered),
              "invalid" (bad token), "expired"
    - manager: manager info if already activated
    - workspace: workspace info if already provisioned
    """
    log.info(f"Checking invite status for token: {token[:8]}...")

    result = await get_invite_status(token.upper())
    return InviteStatusResponse(**result)


@router.post("/invite/{token}/register", response_model=RegisterManagerResponse)
async def register_manager(
    token: str,
    data: RegisterManagerRequest,
    request: Request
):
    """
    Register manager via invite link.

    Creates:
    1. Manager profile (FIO, phone)
    2. Workspace/tenant for the manager
    3. Marks invite as activated

    Returns redirect_url for the manager dashboard.
    """
    token = token.upper()
    log.info(f"Registration attempt for token: {token[:8]}...")

    # Validate invite
    invite_status = await get_invite_status(token)

    if invite_status["status"] == "activated":
        # Already registered - return existing data
        return RegisterManagerResponse(
            status="ok",
            manager=invite_status.get("manager"),
            workspace=invite_status.get("workspace"),
            redirect_url="/",
            message="Вы уже зарегистрированы"
        )

    if invite_status["status"] != "new":
        raise HTTPException(
            status_code=400,
            detail=invite_status.get("message", "Недействительная ссылка")
        )

    # Get invite data
    invite = await db.get_invite_code(token)
    if not invite:
        raise HTTPException(status_code=400, detail="Ссылка недействительна")

    try:
        # Use invite code to create/get user
        # First check if there's already a user with this invite
        async with await db.get_conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Check for existing user created via this invite
                await cur.execute(
                    "SELECT * FROM users WHERE registered_via_code = %s LIMIT 1",
                    (invite['id'],)
                )
                user = await cur.fetchone()

                if not user:
                    # Check if there's a pending user (created via bot but not registered via invite form)
                    # This handles the case where user was created via bot, then deleted, then tries to register with new invite
                    # We'll create a new user since the old one was deleted

                    # Also check if invite was previously activated but user was deleted
                    # Reset activated_by if user doesn't exist
                    if invite.get('activated_by'):
                        await cur.execute(
                            "SELECT id FROM users WHERE id = %s",
                            (invite['activated_by'],)
                        )
                        if not await cur.fetchone():
                            # User was deleted, reset invite activation
                            await cur.execute(
                                "UPDATE invite_codes SET activated_by = NULL, activated_at = NULL WHERE id = %s",
                                (invite['id'],)
                            )

                    # Try to get Telegram user ID from request if available (user opened form from Telegram)
                    tg_id = None
                    try:
                        init_data = request.headers.get('X-Telegram-Init-Data')
                        if init_data:
                            from routes.auth import verify_telegram_init_data
                            user_data = verify_telegram_init_data(init_data)
                            if user_data:
                                tg_id = user_data.get('id')
                                # Check if user exists with this tg_id
                                await cur.execute(
                                    "SELECT * FROM users WHERE tg_id = %s LIMIT 1",
                                    (tg_id,)
                                )
                                existing_user = await cur.fetchone()
                                if existing_user:
                                    # Update existing user instead of creating new one
                                    await cur.execute(
                                        """UPDATE users
                                           SET full_name = %s, phone = %s, email = %s,
                                               registered_via_code = %s, group_id = %s,
                                               approval_status = %s, registration_completed_at = NOW()
                                           WHERE id = %s
                                           RETURNING *""",
                                        (
                                            data.full_name,
                                            data.phone,
                                            data.email,
                                            invite['id'],
                                            invite.get('group_id'),
                                            'approved' if invite.get('auto_approve') else 'pending',
                                            existing_user['id']
                                        )
                                    )
                                    user = await cur.fetchone()

                                    # Record invite usage if not already recorded
                                    await cur.execute(
                                        "SELECT id FROM invite_code_usage WHERE invite_code_id = %s AND user_id = %s",
                                        (invite['id'], str(user['id']))
                                    )
                                    if not await cur.fetchone():
                                        await cur.execute(
                                            """INSERT INTO invite_code_usage (invite_code_id, user_id)
                                               VALUES (%s, %s)""",
                                            (invite['id'], str(user['id']))
                                        )
                                        await cur.execute(
                                            "UPDATE invite_codes SET uses_count = uses_count + 1 WHERE id = %s",
                                            (invite['id'],)
                                        )

                                    # Add to group if specified
                                    if invite.get('group_id'):
                                        await cur.execute(
                                            """INSERT INTO user_group_membership (user_id, group_id, role, added_by)
                                               VALUES (%s, %s, 'member', %s)
                                               ON CONFLICT (user_id, group_id) DO UPDATE SET role = 'member'""",
                                            (str(user['id']), invite['group_id'], invite['created_by'])
                                        )

                                    await conn.commit()
                                    log.info(f"Updated existing user {user['id']} with new invite {token[:8]}")
                                    # Skip to workspace creation
                                    user = dict(user)
                                    user_id = str(user['id'])
                                    workspace = await create_workspace_for_manager(user_id, data.full_name)
                                    await mark_invite_activated(token, user_id)
                                    await db.use_invite_code(token, user_id)
                                    return RegisterManagerResponse(
                                        status="ok",
                                        manager={
                                            "id": user_id,
                                            "full_name": user.get('full_name'),
                                            "phone": user.get('phone'),
                                            "email": user.get('email')
                                        },
                                        workspace={
                                            "id": str(workspace['id']),
                                            "name": workspace['name'],
                                            "slug": workspace['slug']
                                        },
                                        redirect_url="/",
                                        message="Регистрация успешно завершена"
                                    )
                    except Exception as e:
                        log.warning(f"Could not get Telegram user ID from request: {e}")

                    # Create new user without telegram data (will be linked later)
                    # For now, generate a placeholder tg_id
                    import secrets
                    temp_tg_id = tg_id or int(secrets.token_hex(8), 16) % (10**10)

                    await cur.execute(
                        """INSERT INTO users
                           (tg_id, username, first_name, last_name, full_name, phone, email,
                            role, approval_status, registered_via_code, group_id,
                            registration_completed_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, 'manager', %s, %s, %s, NOW())
                           RETURNING *""",
                        (
                            temp_tg_id,
                            f"manager_{token[:8].lower()}",
                            data.full_name.split()[0],
                            data.full_name.split()[-1] if len(data.full_name.split()) > 1 else '',
                            data.full_name,
                            data.phone,
                            data.email,
                            'approved' if invite.get('auto_approve') else 'pending',
                            invite['id'],
                            invite.get('group_id')
                        )
                    )
                    user = await cur.fetchone()
                    await conn.commit()
                    log.info(f"Created new user {user['id']} via invite {token[:8]}")
                else:
                    # Update existing user
                    await cur.execute(
                        """UPDATE users
                           SET full_name = %s, phone = %s, email = %s, registration_completed_at = NOW()
                           WHERE id = %s
                           RETURNING *""",
                        (data.full_name, data.phone, data.email, user['id'])
                    )
                    user = await cur.fetchone()
                    await conn.commit()
                    log.info(f"Updated user {user['id']} registration")

        user = dict(user)
        user_id = str(user['id'])

        # Create workspace (idempotent)
        workspace = await create_workspace_for_manager(user_id, data.full_name)

        # Mark invite as activated
        await mark_invite_activated(token, user_id)

        # Record invite usage
        await db.use_invite_code(token, user_id)

        log.info(f"Manager {user_id} registered successfully with workspace {workspace['id']}")

        return RegisterManagerResponse(
            status="ok",
            manager={
                "id": user_id,
                "full_name": user.get('full_name'),
                "phone": user.get('phone'),
                "email": user.get('email')
            },
            workspace={
                "id": str(workspace['id']),
                "name": workspace['name'],
                "slug": workspace['slug']
            },
            redirect_url="/",
            message="Регистрация успешно завершена"
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Registration failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Ошибка при регистрации. Попробуйте ещё раз."
        )


@router.get("/invite/{token}/session")
async def get_session_for_invite(token: str):
    """
    Get session/redirect for an activated invite.
    Use this after registration to get access to the dashboard.
    """
    token = token.upper()
    invite_status = await get_invite_status(token)

    if invite_status["status"] != "activated":
        raise HTTPException(
            status_code=400,
            detail="Invite not activated. Please complete registration first."
        )

    return {
        "status": "ok",
        "redirect_url": "/",
        "manager": invite_status.get("manager"),
        "workspace": invite_status.get("workspace")
    }


@router.get("/profile")
async def get_manager_profile(user: dict = Depends(get_current_user)):
    """Get current manager's profile and workspace info."""
    workspace = None

    if user.get('workspace_id'):
        async with await db.get_conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """SELECT id, name, slug, settings, max_requests, max_sites, status
                       FROM workspaces WHERE id = %s""",
                    (user['workspace_id'],)
                )
                ws = await cur.fetchone()
                if ws:
                    workspace = dict(ws)
                    workspace['id'] = str(workspace['id'])

    return {
        "manager": {
            "id": str(user['id']),
            "full_name": user.get('full_name'),
            "phone": user.get('phone'),
            "email": user.get('email'),
            "username": user.get('username'),
            "first_name": user.get('first_name'),
            "last_name": user.get('last_name'),
            "tg_id": user.get('tg_id'),
            "registration_completed": user.get('registration_completed_at') is not None
        },
        "workspace": workspace
    }


@router.post("/register", response_model=RegisterManagerResponse)
async def register_approved_manager(
    data: RegisterManagerRequest,
    user: dict = Depends(get_current_user)
):
    """
    Register an approved manager with profile data.
    This endpoint is for managers who were approved but haven't completed registration yet.
    """
    # Check if user is a manager
    if user.get('role') != 'manager':
        raise HTTPException(status_code=403, detail="Only managers can register")

    # Check if already registered
    if user.get('registration_completed_at'):
        return RegisterManagerResponse(
            status="ok",
            manager={
                "id": str(user['id']),
                "full_name": user.get('full_name'),
                "phone": user.get('phone'),
                "email": user.get('email')
            },
            redirect_url="/",
            message="Вы уже зарегистрированы"
        )

    # Check if user is approved
    if user.get('approval_status') != 'approved':
        raise HTTPException(
            status_code=400,
            detail="Ваша заявка еще не одобрена. Дождитесь одобрения администратором."
        )

    try:
        user_id = str(user['id'])

        # Update user profile
        async with await db.get_conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """UPDATE users
                       SET full_name = %s, phone = %s, email = %s, registration_completed_at = NOW()
                       WHERE id = %s
                       RETURNING *""",
                    (data.full_name, data.phone, data.email, user_id)
                )
                updated_user = await cur.fetchone()
                await conn.commit()

        # Create workspace if not exists
        workspace = await create_workspace_for_manager(user_id, data.full_name)

        log.info(f"Manager {user_id} completed registration")

        return RegisterManagerResponse(
            status="ok",
            manager={
                "id": user_id,
                "full_name": updated_user.get('full_name'),
                "phone": updated_user.get('phone'),
                "email": updated_user.get('email')
            },
            workspace={
                "id": str(workspace['id']),
                "name": workspace['name'],
                "slug": workspace['slug']
            },
            redirect_url="/",
            message="Регистрация успешно завершена"
        )

    except Exception as e:
        log.error(f"Registration failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Ошибка при регистрации. Попробуйте ещё раз."
        )


@router.patch("/profile")
async def update_profile(
    data: dict,
    user: dict = Depends(get_current_user)
):
    """Update manager profile."""
    allowed_fields = {'full_name', 'phone', 'email'}
    update_data = {k: v for k, v in data.items() if k in allowed_fields}

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    # Validate phone if provided
    if 'phone' in update_data:
        phone = update_data['phone']
        phone = re.sub(r'[\s\-\(\)]', '', phone)
        if phone.startswith('8') and len(phone) == 11:
            phone = '+7' + phone[1:]
        elif phone.startswith('7') and len(phone) == 11:
            phone = '+' + phone
        elif not phone.startswith('+'):
            phone = '+7' + phone

        if not re.match(r'^\+7\d{10}$', phone):
            raise HTTPException(status_code=400, detail="Некорректный формат телефона")
        update_data['phone'] = phone

    # Validate email if provided
    if 'email' in update_data:
        email = update_data['email'].strip().lower()
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            raise HTTPException(status_code=400, detail="Некорректный формат email")
        update_data['email'] = email

    async with await db.get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            set_clause = ", ".join(f"{k} = %s" for k in update_data.keys())
            values = list(update_data.values()) + [user['id']]

            await cur.execute(
                f"UPDATE users SET {set_clause} WHERE id = %s RETURNING *",
                values
            )
            updated = await cur.fetchone()
            await conn.commit()

    return {
        "status": "ok",
        "manager": {
            "id": str(updated['id']),
            "full_name": updated.get('full_name'),
            "phone": updated.get('phone'),
            "email": updated.get('email')
        }
    }

