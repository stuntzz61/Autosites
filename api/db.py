import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from config import settings

pool: Optional[AsyncConnectionPool] = None


async def init_pool():
    global pool
    pool = AsyncConnectionPool(
        settings.database_url,
        min_size=2,
        max_size=10,
        open=False,
    )
    await pool.open()


async def close_pool():
    global pool
    if pool:
        await pool.close()


async def get_conn():
    return pool.connection()


# ==================== Users ====================

async def get_user_by_tg_id(tg_id: int) -> Optional[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role,
                          approval_status, created_at, COALESCE(is_blocked, FALSE) as is_blocked,
                          full_name, phone, email, registration_completed_at
                   FROM users WHERE tg_id = %s""",
                (tg_id,)
            )
            return await cur.fetchone()


async def get_user_by_id(user_id: str) -> Optional[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role,
                          approval_status, created_at, COALESCE(is_blocked, FALSE) as is_blocked,
                          full_name, phone, email, registration_completed_at
                   FROM users WHERE id = %s""",
                (user_id,)
            )
            return await cur.fetchone()


async def create_user(tg_id: int, username: str, first_name: str, last_name: str) -> Dict:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO users (tg_id, username, first_name, last_name, role, approval_status)
                   VALUES (%s, %s, %s, %s, 'manager', 'pending')
                   RETURNING id, tg_id, username, first_name, last_name, contact, role, approval_status, created_at""",
                (tg_id, username, first_name, last_name)
            )
            await conn.commit()
            return await cur.fetchone()


async def list_managers() -> List[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get managers with their group info (if assigned)
            await cur.execute(
                """SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name, u.full_name,
                          u.phone, u.email, u.contact, u.role, u.approval_status, u.created_at,
                          COALESCE(u.is_blocked, FALSE) as is_blocked,
                          COUNT(DISTINCT r.id) as request_count,
                          ag.id as group_id, ag.name as group_name
                   FROM users u
                   LEFT JOIN projects p ON p.manager_id = u.id
                   LEFT JOIN requests r ON r.project_id = p.id
                   LEFT JOIN user_group_membership ugm ON ugm.user_id = u.id
                   LEFT JOIN admin_groups ag ON ag.id = ugm.group_id AND ag.is_active = TRUE
                   WHERE u.role = 'manager' AND u.approval_status = 'approved'
                   GROUP BY u.id, u.tg_id, u.username, u.first_name, u.last_name, u.full_name,
                            u.phone, u.email, u.contact, u.role, u.approval_status, u.created_at, u.is_blocked,
                            ag.id, ag.name
                   ORDER BY request_count DESC"""
            )
            return await cur.fetchall()


async def list_admins() -> List[Dict]:
    """Get all admin users (supervisor, director, owner)."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role, created_at
                   FROM users
                   WHERE role IN ('supervisor', 'director', 'owner')
                   ORDER BY
                     CASE role
                       WHEN 'owner' THEN 1
                       WHEN 'director' THEN 2
                       WHEN 'supervisor' THEN 3
                     END,
                     created_at ASC"""
            )
            return await cur.fetchall()


async def list_pending_registrations(admin_id: str = None) -> List[Dict]:
    """List pending registrations. If admin_id is provided, only show users from admin's groups."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            if admin_id:
                # Show only users registered via invite codes from admin's groups or in admin's groups
                await cur.execute(
                    """SELECT DISTINCT u.id, u.tg_id, u.username, u.first_name, u.last_name,
                              u.contact, u.role, u.approval_status, u.created_at
                       FROM users u
                       LEFT JOIN invite_codes ic ON ic.id = u.registered_via_code
                       LEFT JOIN admin_groups ag1 ON ag1.id = ic.group_id
                       LEFT JOIN user_group_membership ugm ON ugm.user_id = u.id
                       LEFT JOIN admin_groups ag2 ON ag2.id = ugm.group_id
                       WHERE u.role = 'manager'
                         AND u.approval_status = 'pending'
                         AND (
                           (ic.id IS NOT NULL AND ag1.created_by = %s)
                           OR (ugm.id IS NOT NULL AND ag2.created_by = %s)
                         )
                       ORDER BY u.created_at DESC""",
                    (admin_id, admin_id)
                )
            else:
                await cur.execute(
                    """SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name,
                              u.contact, u.role, u.approval_status, u.created_at
                       FROM users u
                       WHERE u.role = 'manager' AND u.approval_status = 'pending'
                       ORDER BY u.created_at DESC"""
                )
            return await cur.fetchall()


async def approve_user(user_id: str) -> bool:
    """Approve user registration. Returns False if already approved/rejected."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            # Only approve if status is 'pending'
            await cur.execute(
                """UPDATE users SET approval_status = 'approved',
                   approved_at = NOW(), rejection_reason = NULL
                   WHERE id = %s AND approval_status = 'pending'
                   RETURNING id""",
                (user_id,)
            )
            result = await cur.fetchone()
            await conn.commit()
            return result is not None


async def reject_user(user_id: str, reason: str) -> bool:
    """Reject user registration. Returns False if already approved/rejected."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            # Only reject if status is 'pending'
            await cur.execute(
                """UPDATE users SET approval_status = 'rejected',
                   rejection_reason = %s
                   WHERE id = %s AND approval_status = 'pending'
                   RETURNING id""",
                (reason, user_id)
            )
            result = await cur.fetchone()
            await conn.commit()
            return result is not None


async def reset_user_approval(user_id: str):
    """Reset user approval status to pending (allow re-application)."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'pending',
                   rejection_reason = NULL, approved_at = NULL
                   WHERE id = %s AND approval_status = 'rejected'""",
                (user_id,)
            )
            await conn.commit()


async def get_user_approval_status(user_id: str) -> Optional[str]:
    """Get user's current approval status."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT approval_status FROM users WHERE id = %s",
                (user_id,)
            )
            result = await cur.fetchone()
            return result[0] if result else None


async def block_user(user_id: str):
    """Block user."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET is_blocked = TRUE WHERE id = %s",
                (user_id,)
            )
            await conn.commit()
            print(f"[DEBUG] User {user_id} blocked successfully")


async def unblock_user(user_id: str):
    """Unblock user."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET is_blocked = FALSE WHERE id = %s",
                (user_id,)
            )
            await conn.commit()
            print(f"[DEBUG] User {user_id} unblocked successfully")


async def update_user_role(user_id: str, role: str):
    """Update user role."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET role = %s WHERE id = %s",
                (role, user_id)
            )
            await conn.commit()


async def delete_user(user_id: str):
    """Delete user and clean up related invite code data."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            # Get invite codes that were activated by this user
            await cur.execute(
                "SELECT id FROM invite_codes WHERE activated_by = %s",
                (user_id,)
            )
            invite_ids = [row[0] for row in await cur.fetchall()]

            # Reset activated_by for invite codes
            if invite_ids:
                await cur.execute(
                    "UPDATE invite_codes SET activated_by = NULL, activated_at = NULL WHERE id = ANY(%s)",
                    (invite_ids,)
                )

            # Delete invite code usage records
            await cur.execute(
                "DELETE FROM invite_code_usage WHERE user_id = %s",
                (user_id,)
            )

            # Delete user group memberships
            await cur.execute(
                "DELETE FROM user_group_membership WHERE user_id = %s",
                (user_id,)
            )

            # Delete user
            await cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
            await conn.commit()


async def update_user_contact(user_id: str, contact: str):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET contact = %s WHERE id = %s",
                (contact, user_id)
            )
            await conn.commit()


# ==================== Requests ====================

async def list_requests(
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict]:
    conditions = []
    params = []

    if user_id:
        conditions.append("p.manager_id = %s")
        params.append(user_id)

    if status:
        if status == 'archived':
            conditions.append("(r.payload_json->'site'->'meta'->>'status' = 'archived' OR r.status = 'archived')")
        else:
            conditions.append("(r.payload_json->'site'->'meta'->>'status' = %s OR r.status = %s)")
            params.extend([status, status])

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    params.extend([limit, offset])

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT r.id,
                           COALESCE(r.payload_json->'site'->>'company', '') as company_name,
                           COALESCE(r.payload_json->'client'->>'name', '') as client_name,
                           COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) as status,
                           r.payload_json,
                           COALESCE(r.tariff, 'standard') as tariff,
                           r.created_at
                    FROM requests r
                    JOIN projects p ON p.id = r.project_id
                    WHERE {where_clause}
                    ORDER BY r.created_at DESC
                    LIMIT %s OFFSET %s""",
                params
            )
            rows = await cur.fetchall()
            for row in rows:
                if row.get('payload_json'):
                    payload = row.pop('payload_json')
                    # Parse JSON if it's a string
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except:
                            payload = {}
                    row['payload'] = payload
            return rows


async def get_request(request_id: str) -> Optional[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT r.id,
                          COALESCE(r.payload_json->'site'->>'company', '') as company_name,
                          COALESCE(r.payload_json->'client'->>'name', '') as client_name,
                          COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) as status,
                          r.payload_json,
                          COALESCE(r.tariff, 'standard') as tariff,
                          p.manager_id as user_id,
                          r.created_at
                   FROM requests r
                   JOIN projects p ON p.id = r.project_id
                   WHERE r.id = %s""",
                (request_id,)
            )
            row = await cur.fetchone()
            if row:
                if row.get('payload_json'):
                    # Parse JSON if it's a string
                    payload = row.pop('payload_json')
                    if isinstance(payload, str):
                        import json
                        try:
                            payload = json.loads(payload)
                        except:
                            payload = {}
                    row['payload'] = payload

                    # Debug: log images
                    images = payload.get('site', {}).get('assets', {}).get('images', [])
                    if images:
                        print(f"[DEBUG] get_request {request_id}: found {len(images)} images")
                else:
                    row['payload'] = {}
            return row


async def create_request(user_id: str, company_name: str, client_name: str, payload: Dict, tariff: str = 'standard') -> Dict:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # First, get or create a project for this manager
            await cur.execute(
                """SELECT id FROM projects
                   WHERE manager_id = %s AND status IN ('draft', 'active')
                   ORDER BY created_at DESC LIMIT 1""",
                (user_id,)
            )
            project = await cur.fetchone()

            if project:
                project_id = project['id']
            else:
                # Create new project
                await cur.execute(
                    """INSERT INTO projects (manager_id, title, status)
                       VALUES (%s, %s, 'draft')
                       RETURNING id""",
                    (user_id, company_name or 'Новый проект')
                )
                project = await cur.fetchone()
                project_id = project['id']

            # Create the request with tariff
            await cur.execute(
                """INSERT INTO requests (project_id, status, payload_json, tariff)
                   VALUES (%s, 'draft', %s, %s)
                   RETURNING id, status, payload_json, tariff, created_at""",
                (project_id, json.dumps(payload), tariff)
            )
            await conn.commit()
            row = await cur.fetchone()
            if row:
                row['company_name'] = company_name
                row['client_name'] = client_name
                if row.get('payload_json'):
                    payload = row.pop('payload_json')
                    # Parse JSON if it's a string
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except:
                            payload = {}
                    row['payload'] = payload
            return row


async def update_request(request_id: str, data: Dict) -> Optional[Dict]:
    """Update request - payload_json and tariff are updatable. company_name and client_name are stored in payload_json."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Build update query dynamically based on what's provided
            update_fields = []
            update_values = []

            # Handle payload update - merge with existing payload if needed
            if 'payload' in data:
                payload_to_save = data['payload']

                # If company_name or client_name are provided separately, update them in payload
                if 'company_name' in data:
                    if 'site' not in payload_to_save:
                        payload_to_save['site'] = {}
                    payload_to_save['site']['company'] = data['company_name']

                if 'client_name' in data:
                    if 'client' not in payload_to_save:
                        payload_to_save['client'] = {}
                    payload_to_save['client']['name'] = data['client_name']

                # Debug: log images before save
                images = payload_to_save.get('site', {}).get('assets', {}).get('images', [])
                print(f"[DEBUG] update_request {request_id}: saving {len(images)} images")
                update_fields.append("payload_json = %s::jsonb")
                update_values.append(json.dumps(payload_to_save))
            elif 'company_name' in data or 'client_name' in data:
                # If only company_name or client_name provided without payload, merge with existing
                existing_request = await get_request(request_id)
                if existing_request:
                    existing_payload = existing_request.get('payload', {}) or {}
                    if 'site' not in existing_payload:
                        existing_payload['site'] = {}
                    if 'client' not in existing_payload:
                        existing_payload['client'] = {}

                    if 'company_name' in data:
                        existing_payload['site']['company'] = data['company_name']
                    if 'client_name' in data:
                        existing_payload['client']['name'] = data['client_name']

                    update_fields.append("payload_json = %s::jsonb")
                    update_values.append(json.dumps(existing_payload))

            if 'tariff' in data:
                update_fields.append("tariff = %s")
                update_values.append(data['tariff'])
                print(f"[DEBUG] update_request {request_id}: updating tariff to {data['tariff']}")

            if not update_fields:
                return await get_request(request_id)

            # Add request_id for WHERE clause
            update_values.append(request_id)

            query = f"""UPDATE requests SET {', '.join(update_fields)}
                    WHERE id = %s
                    RETURNING id, status, payload_json, tariff, created_at"""

            await cur.execute(query, update_values)
            await conn.commit()
            row = await cur.fetchone()
            if row:
                payload = row.get('payload_json') or {}
                # Parse JSON if it's a string
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except:
                        payload = {}

                row['payload'] = payload
                # Extract company_name and client_name from payload for backward compatibility
                row['company_name'] = payload.get('site', {}).get('company', '')
                row['client_name'] = payload.get('client', {}).get('name', '')

                # Debug: log tariff after save
                saved_tariff = row.get('tariff', 'standard')
                print(f"[DEBUG] update_request {request_id}: saved tariff = {saved_tariff}")

                # Debug: verify images after save
                if 'payload' in data or 'company_name' in data or 'client_name' in data:
                    saved_images = payload.get('site', {}).get('assets', {}).get('images', [])
                    print(f"[DEBUG] update_request {request_id}: saved {len(saved_images)} images")

                if 'payload_json' in row:
                    del row['payload_json']
            return row


def normalize_status(status: str) -> str:
    """Normalize legacy status names to current standard."""
    mapping = {
        'generated_ok': 'success',
        'generated_error': 'error',
        'ready': 'ready_to_generate',
        'in_progress': 'generating',
        'pending': 'draft',
    }
    return mapping.get(status.lower(), status)


async def update_request_status(request_id: str, status: str):
    # Normalize status before saving
    normalized_status = normalize_status(status)

    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            # Update both status field and payload status
            await cur.execute(
                """UPDATE requests
                   SET status = %s,
                       payload_json = jsonb_set(
                           COALESCE(payload_json, '{}'::jsonb),
                           '{site,meta,status}',
                           %s::jsonb
                       )
                   WHERE id = %s""",
                (normalized_status, json.dumps(normalized_status), request_id)
            )
            await conn.commit()


async def archive_request(request_id: str):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE requests
                   SET status = 'archived',
                       payload_json = jsonb_set(
                           COALESCE(payload_json, '{}'::jsonb),
                           '{site,meta,status}',
                           '"archived"'::jsonb
                       )
                   WHERE id = %s""",
                (request_id,)
            )
            await conn.commit()


async def delete_request(request_id: str):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM requests WHERE id = %s", (request_id,))
            await conn.commit()


async def mass_archive_requests(request_ids: List[str]):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE requests
                   SET status = 'archived',
                       payload_json = jsonb_set(
                           COALESCE(payload_json, '{}'::jsonb),
                           '{site,meta,status}',
                           '"archived"'::jsonb
                       )
                   WHERE id = ANY(%s)""",
                (request_ids,)
            )
            await conn.commit()


async def mass_delete_requests(request_ids: List[str]):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM requests WHERE id = ANY(%s)",
                (request_ids,)
            )
            await conn.commit()


# ==================== Stats ====================

async def get_dashboard_stats() -> Dict:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Total counts
            await cur.execute(
                """SELECT
                   COUNT(*) FILTER (WHERE role = 'manager' AND approval_status = 'approved') as total_managers,
                   (SELECT COUNT(*) FROM requests) as total_requests,
                   (SELECT COUNT(*) FROM requests WHERE status = 'success' OR payload_json->'site'->'meta'->>'status' = 'generated_ok') as completed_requests,
                   (SELECT COUNT(*) FROM requests WHERE status NOT IN ('success', 'archived', 'closed', 'generated_ok')) as pending_requests,
                   (SELECT COUNT(*) FROM requests WHERE DATE(created_at) = CURRENT_DATE) as today_requests,
                   (SELECT COUNT(*) FROM requests WHERE DATE(created_at) = CURRENT_DATE AND (status = 'success' OR payload_json->'site'->'meta'->>'status' = 'generated_ok')) as today_generated,
                   (SELECT COUNT(*) FROM requests WHERE status = 'archived' OR payload_json->'site'->'meta'->>'status' = 'archived') as today_archived
                   FROM users"""
            )
            stats = await cur.fetchone()

            # Top managers
            await cur.execute(
                """SELECT u.id, u.first_name, u.last_name, u.username, COUNT(r.id) as request_count
                   FROM users u
                   LEFT JOIN projects p ON p.manager_id = u.id
                   LEFT JOIN requests r ON r.project_id = p.id
                   WHERE u.role = 'manager' AND u.approval_status = 'approved'
                   GROUP BY u.id
                   ORDER BY request_count DESC
                   LIMIT 5"""
            )
            top_managers = await cur.fetchall()

            # Status counts
            await cur.execute(
                """SELECT
                   COUNT(*) FILTER (WHERE status = 'draft') as draft_count,
                   COUNT(*) FILTER (WHERE status = 'ready_to_generate' OR payload_json->'site'->'meta'->>'status' = 'ready_to_generate') as ready_count,
                   COUNT(*) FILTER (WHERE status IN ('generating', 'in_queue')) as generating_count,
                   COUNT(*) FILTER (WHERE status = 'success' OR payload_json->'site'->'meta'->>'status' = 'generated_ok') as success_count,
                   COUNT(*) FILTER (WHERE status = 'error' OR payload_json->'site'->'meta'->>'status' = 'generated_error') as error_count
                   FROM requests WHERE status != 'archived' AND (payload_json->'site'->'meta'->>'status' IS NULL OR payload_json->'site'->'meta'->>'status' != 'archived')"""
            )
            status_counts = await cur.fetchone()

            return {
                'stats': {**stats, **status_counts},
                'top_managers': top_managers,
            }


async def get_stats_by_status() -> List[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT COALESCE(NULLIF(payload_json->'site'->'meta'->>'status', ''), status, 'draft') as status,
                          COUNT(*) as count
                   FROM requests
                   GROUP BY COALESCE(NULLIF(payload_json->'site'->'meta'->>'status', ''), status, 'draft')
                   ORDER BY count DESC"""
            )
            return await cur.fetchall()


async def get_stats_by_day(days: int = 7) -> List[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT DATE(created_at)::text as date, COUNT(*) as count
                   FROM requests
                   WHERE created_at >= CURRENT_DATE - INTERVAL '%s days'
                   GROUP BY DATE(created_at)
                   ORDER BY date""",
                (days,)
            )
            return await cur.fetchall()


async def get_manager_stats() -> List[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT u.id, u.first_name, u.last_name, u.username,
                          COUNT(r.id) as request_count
                   FROM users u
                   LEFT JOIN projects p ON p.manager_id = u.id
                   LEFT JOIN requests r ON r.project_id = p.id
                   WHERE u.role = 'manager' AND u.approval_status = 'approved'
                   GROUP BY u.id
                   ORDER BY request_count DESC"""
            )
            return await cur.fetchall()


async def get_user_stats(user_id: str) -> Dict:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT
                   COUNT(r.id) as total_requests,
                   COUNT(r.id) FILTER (WHERE COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) = 'generated_ok') as completed_requests,
                   COUNT(r.id) FILTER (WHERE COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) NOT IN ('generated_ok', 'archived', 'closed')) as pending_requests,
                   COUNT(r.id) FILTER (WHERE r.created_at >= CURRENT_DATE - INTERVAL '7 days') as this_week,
                   COUNT(r.id) FILTER (WHERE DATE(r.created_at) = CURRENT_DATE) as today
                   FROM projects p
                   LEFT JOIN requests r ON r.project_id = p.id
                   WHERE p.manager_id = %s""",
                (user_id,)
            )
            return await cur.fetchone()


async def get_overview_stats() -> Dict:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT
                   COUNT(*) as total_requests,
                   (SELECT COUNT(*) FROM users WHERE role = 'manager' AND approval_status = 'approved') as total_managers,
                   COUNT(*) FILTER (WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)) as this_month,
                   COALESCE(COUNT(*) / NULLIF(EXTRACT(DAY FROM CURRENT_DATE), 0), 0) as avg_per_day
                   FROM requests"""
            )
            return await cur.fetchone()


# ==================== Additional Services ====================

async def list_additional_services(active_only: bool = True) -> List[Dict]:
    """List all additional services."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            query = """SELECT id, code, name, description, price_info, icon, is_active, sort_order
                       FROM additional_services"""
            if active_only:
                query += " WHERE is_active = TRUE"
            query += " ORDER BY sort_order, name"
            await cur.execute(query)
            return await cur.fetchall()


async def get_request_additional_services(request_id: str) -> List[Dict]:
    """Get additional services for a request."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT ras.id, ras.request_id, ras.service_id, ras.status,
                          ras.notes, ras.price, ras.created_at,
                          s.code, s.name, s.description, s.icon
                   FROM request_additional_services ras
                   JOIN additional_services s ON s.id = ras.service_id
                   WHERE ras.request_id = %s
                   ORDER BY s.sort_order""",
                (request_id,)
            )
            return await cur.fetchall()


async def add_request_additional_service(
    request_id: str,
    service_id: str,
    added_by: str,
    notes: str = None,
    price: str = None
) -> Dict:
    """Add additional service to a request."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO request_additional_services
                   (request_id, service_id, added_by, notes, price)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (request_id, service_id)
                   DO UPDATE SET notes = EXCLUDED.notes, price = EXCLUDED.price, updated_at = NOW()
                   RETURNING id, request_id, service_id, status, notes, price, created_at""",
                (request_id, service_id, added_by, notes, price)
            )
            await conn.commit()
            return await cur.fetchone()


async def update_request_additional_service(
    request_id: str,
    service_id: str,
    status: str = None,
    notes: str = None,
    price: str = None
) -> Optional[Dict]:
    """Update additional service for a request."""
    updates = []
    params = []

    if status:
        updates.append("status = %s")
        params.append(status)
    if notes is not None:
        updates.append("notes = %s")
        params.append(notes)
    if price is not None:
        updates.append("price = %s")
        params.append(price)

    if not updates:
        return None

    params.extend([request_id, service_id])

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE request_additional_services
                    SET {", ".join(updates)}
                    WHERE request_id = %s AND service_id = %s
                    RETURNING id, request_id, service_id, status, notes, price""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def remove_request_additional_service(request_id: str, service_id: str):
    """Remove additional service from a request."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM request_additional_services WHERE request_id = %s AND service_id = %s",
                (request_id, service_id)
            )
            await conn.commit()


# ==================== Manager Feedback ====================

async def create_feedback(
    manager_id: str,
    subject: str,
    message: str,
    category: str = "general",
    priority: str = "normal",
    request_id: str = None
) -> Dict:
    """Create a new feedback from manager."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO manager_feedback
                   (manager_id, subject, message, category, priority, request_id)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   RETURNING id, manager_id, subject, message, category, priority,
                             status, request_id, created_at""",
                (manager_id, subject, message, category, priority, request_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def list_feedback(
    status: str = None,
    manager_id: str = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict]:
    """List feedback messages."""
    conditions = []
    params = []

    if status:
        conditions.append("f.status = %s")
        params.append(status)
    if manager_id:
        conditions.append("f.manager_id = %s")
        params.append(manager_id)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    params.extend([limit, offset])

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT f.id, f.subject, f.message, f.category, f.priority,
                           f.status, f.request_id, f.admin_response, f.responded_at,
                           f.created_at, f.updated_at,
                           u.id as manager_id, u.first_name as manager_first_name,
                           u.last_name as manager_last_name, u.username as manager_username,
                           u.tg_id as manager_tg_id
                    FROM manager_feedback f
                    JOIN users u ON u.id = f.manager_id
                    WHERE {where_clause}
                    ORDER BY
                        CASE f.status WHEN 'new' THEN 0 ELSE 1 END,
                        CASE f.priority
                            WHEN 'urgent' THEN 0
                            WHEN 'high' THEN 1
                            WHEN 'normal' THEN 2
                            ELSE 3
                        END,
                        f.created_at DESC
                    LIMIT %s OFFSET %s""",
                params
            )
            return await cur.fetchall()


async def get_feedback(feedback_id: str) -> Optional[Dict]:
    """Get feedback by ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT f.id, f.subject, f.message, f.category, f.priority,
                          f.status, f.request_id, f.admin_response, f.responded_at,
                          f.created_at, f.updated_at,
                          u.id as manager_id, u.first_name as manager_first_name,
                          u.last_name as manager_last_name, u.username as manager_username,
                          u.tg_id as manager_tg_id
                   FROM manager_feedback f
                   JOIN users u ON u.id = f.manager_id
                   WHERE f.id = %s""",
                (feedback_id,)
            )
            return await cur.fetchone()


async def respond_to_feedback(
    feedback_id: str,
    admin_id: str,
    response: str,
    new_status: str = "answered"
) -> Optional[Dict]:
    """Respond to a feedback."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """UPDATE manager_feedback
                   SET admin_response = %s, responded_by = %s,
                       responded_at = NOW(), status = %s
                   WHERE id = %s
                   RETURNING id, subject, message, admin_response, status""",
                (response, admin_id, new_status, feedback_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def update_feedback_status(feedback_id: str, status: str):
    """Update feedback status."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE manager_feedback SET status = %s WHERE id = %s",
                (status, feedback_id)
            )
            await conn.commit()


async def count_new_feedback() -> int:
    """Count new/unread feedback."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM manager_feedback WHERE status = 'new'"
            )
            result = await cur.fetchone()
            return result[0] if result else 0


# ==================== Client Sites ====================

async def create_client_site(
    request_id: Optional[str],  # Can be None for imported sites
    manager_id: str,
    company_name: str,
    client_name: str = None,
    client_contact: str = None,
    hosting_plan: str = 'trial',
    notes: str = None
) -> Dict:
    """Create a new client site record."""
    from datetime import timedelta

    # Calculate hosting expiration based on plan
    hosting_expires = None
    if hosting_plan == 'trial':
        hosting_expires = datetime.now(timezone.utc) + timedelta(days=7)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO client_sites
                   (request_id, manager_id, company_name, client_name, client_contact,
                    hosting_plan, hosting_expires_at, notes)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING *""",
                (request_id, manager_id, company_name, client_name, client_contact,
                 hosting_plan, hosting_expires, notes)
            )
            await conn.commit()
            return await cur.fetchone()


async def get_client_site(site_id: str) -> Optional[Dict]:
    """Get client site by ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT cs.*,
                          u.first_name as manager_first_name,
                          u.last_name as manager_last_name,
                          u.tg_id as manager_tg_id
                   FROM client_sites cs
                   JOIN users u ON u.id = cs.manager_id
                   WHERE cs.id = %s""",
                (site_id,)
            )
            return await cur.fetchone()


async def get_client_site_by_request(request_id: str) -> Optional[Dict]:
    """Get client site by request ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT cs.*,
                          u.first_name as manager_first_name,
                          u.last_name as manager_last_name,
                          u.tg_id as manager_tg_id
                   FROM client_sites cs
                   JOIN users u ON u.id = cs.manager_id
                   WHERE cs.request_id = %s""",
                (request_id,)
            )
            return await cur.fetchone()


async def get_client_site_by_deploy_id(deploy_id: str) -> Optional[Dict]:
    """Get client site by deploy ID from deploy-node."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM client_sites WHERE deploy_id = %s",
                (deploy_id,)
            )
            return await cur.fetchone()


async def list_client_sites(
    manager_id: str = None,
    deploy_status: str = None,
    hosting_plan: str = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict]:
    """List client sites with filters."""
    conditions = []
    params = []

    if manager_id:
        conditions.append("cs.manager_id = %s")
        params.append(manager_id)

    if deploy_status:
        conditions.append("cs.deploy_status = %s")
        params.append(deploy_status)

    if hosting_plan:
        conditions.append("cs.hosting_plan = %s")
        params.append(hosting_plan)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    params.extend([limit, offset])

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT cs.*,
                           u.first_name as manager_first_name,
                           u.last_name as manager_last_name,
                           u.username as manager_username
                    FROM client_sites cs
                    JOIN users u ON u.id = cs.manager_id
                    WHERE {where_clause}
                    ORDER BY cs.created_at DESC
                    LIMIT %s OFFSET %s""",
                params
            )
            return await cur.fetchall()


async def update_client_site(site_id: str, data: Dict) -> Optional[Dict]:
    """Update client site."""
    from uuid import UUID as UUID_type

    allowed_fields = [
        'company_name', 'client_name', 'client_contact',
        'deploy_id', 'preview_slug', 'preview_url',
        'domain', 'domain_status', 'ssl_enabled',
        'generation_status', 'deploy_status',
        'hosting_plan', 'hosting_expires_at', 'hosting_auto_renew',
        'archive_s3_key', 'archive_size_bytes',
        'server_id', 'server_name', 'server_host', 'container_port',
        'last_error', 'last_error_at',
        'notes', 'metadata',
        'generated_at', 'deployed_at'
    ]

    updates = []
    params = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            value = data[field]
            # Convert dict to JSON string for JSONB fields
            if field == 'metadata' and isinstance(value, dict):
                value = json.dumps(value)
            # Convert UUID to string
            elif isinstance(value, UUID_type):
                value = str(value)
            params.append(value)

    if not updates:
        return await get_client_site(site_id)

    params.append(site_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE client_sites
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def update_site_deploy_status(
    site_id: str,
    deploy_status: str,
    deploy_id: str = None,
    preview_slug: str = None,
    preview_url: str = None,
    server_id: str = None,
    server_name: str = None,
    error: str = None
) -> Optional[Dict]:
    """Update deployment status of a site."""
    updates = ["deploy_status = %s"]
    params = [deploy_status]

    if deploy_id:
        updates.append("deploy_id = %s")
        params.append(deploy_id)

    if preview_slug:
        updates.append("preview_slug = %s")
        params.append(preview_slug)

    if preview_url:
        updates.append("preview_url = %s")
        params.append(preview_url)

    if server_id:
        updates.append("server_id = %s")
        params.append(server_id)

    if server_name:
        updates.append("server_name = %s")
        params.append(server_name)

    if deploy_status == 'active':
        updates.append("deployed_at = NOW()")

    if error:
        updates.append("last_error = %s")
        updates.append("last_error_at = NOW()")
        params.append(error)

    params.append(site_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE client_sites
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def update_site_generation_status(
    site_id: str,
    status: str,
    archive_s3_key: str = None,
    archive_size_bytes: int = None,
    error: str = None
) -> Optional[Dict]:
    """Update generation status of a site."""
    updates = ["generation_status = %s"]
    params = [status]

    if archive_s3_key:
        updates.append("archive_s3_key = %s")
        params.append(archive_s3_key)

    if archive_size_bytes:
        updates.append("archive_size_bytes = %s")
        params.append(archive_size_bytes)

    if status == 'completed':
        updates.append("generated_at = NOW()")

    if error:
        updates.append("last_error = %s")
        updates.append("last_error_at = NOW()")
        params.append(error)

    params.append(site_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE client_sites
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def assign_domain_to_site(
    site_id: str,
    domain: str,
    ssl_enabled: bool = False
) -> Optional[Dict]:
    """Assign a custom domain to a site."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """UPDATE client_sites
                   SET domain = %s, domain_status = 'pending', ssl_enabled = %s
                   WHERE id = %s
                   RETURNING *""",
                (domain, ssl_enabled, site_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def extend_hosting(
    site_id: str,
    plan: str,
    months: int = 1
) -> Optional[Dict]:
    """Extend hosting for a site."""
    from datetime import timedelta

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get current expiration
            await cur.execute(
                "SELECT hosting_expires_at FROM client_sites WHERE id = %s",
                (site_id,)
            )
            current = await cur.fetchone()

            if not current:
                return None

            # Calculate new expiration
            base_date = current['hosting_expires_at']
            if not base_date or base_date < datetime.now(timezone.utc):
                base_date = datetime.now(timezone.utc)

            new_expiration = base_date + timedelta(days=30 * months)

            # Update
            await cur.execute(
                """UPDATE client_sites
                   SET hosting_plan = %s, hosting_expires_at = %s
                   WHERE id = %s
                   RETURNING *""",
                (plan, new_expiration, site_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def get_expiring_sites(days: int = 7) -> List[Dict]:
    """Get sites with expiring hosting."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT cs.*,
                          u.first_name as manager_first_name,
                          u.last_name as manager_last_name,
                          u.tg_id as manager_tg_id,
                          EXTRACT(DAY FROM cs.hosting_expires_at - NOW()) as days_remaining
                   FROM client_sites cs
                   JOIN users u ON u.id = cs.manager_id
                   WHERE cs.deploy_status = 'active'
                     AND cs.hosting_expires_at IS NOT NULL
                     AND cs.hosting_expires_at <= NOW() + INTERVAL '%s days'
                   ORDER BY cs.hosting_expires_at ASC""",
                (days,)
            )
            return await cur.fetchall()


async def get_sites_stats() -> Dict:
    """Get statistics about client sites."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT
                   COUNT(*) as total_sites,
                   COUNT(*) FILTER (WHERE deploy_status = 'active') as active_sites,
                   COUNT(*) FILTER (WHERE deploy_status IN ('pending', 'deploying')) as pending_sites,
                   COUNT(*) FILTER (WHERE deploy_status = 'failed') as failed_sites,
                   COUNT(*) FILTER (WHERE generation_status = 'generating') as generating_sites,
                   COUNT(*) FILTER (WHERE hosting_plan = 'trial') as trial_sites,
                   COUNT(*) FILTER (WHERE hosting_plan != 'trial' AND hosting_plan IS NOT NULL) as paid_sites,
                   COUNT(*) FILTER (WHERE hosting_expires_at < NOW() AND deploy_status = 'active') as expired_sites
                   FROM client_sites"""
            )
            return await cur.fetchone()


async def delete_client_site(site_id: str):
    """Delete a client site."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM client_sites WHERE id = %s", (site_id,))
            await conn.commit()


# ==================== Deploy History ====================

async def create_deploy_history(
    client_site_id: str,
    deploy_id: str,
    action: str,
    initiated_by: str = None,
    archive_s3_key: str = None
) -> Dict:
    """Create a deploy history record."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO deploy_history
                   (client_site_id, deploy_id, action, status, initiated_by, archive_s3_key)
                   VALUES (%s, %s, %s, 'pending', %s, %s)
                   RETURNING *""",
                (client_site_id, deploy_id, action, initiated_by, archive_s3_key)
            )
            await conn.commit()
            return await cur.fetchone()


async def update_deploy_history(
    history_id: str,
    status: str,
    build_output: str = None,
    error_message: str = None
) -> Optional[Dict]:
    """Update deploy history record."""
    updates = ["status = %s"]
    params = [status]

    if build_output:
        updates.append("build_output = %s")
        params.append(build_output)

    if error_message:
        updates.append("error_message = %s")
        params.append(error_message)

    if status in ('success', 'failed'):
        updates.append("completed_at = NOW()")

    params.append(history_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE deploy_history
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def get_deploy_history(client_site_id: str, limit: int = 10) -> List[Dict]:
    """Get deploy history for a site."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT dh.*, u.first_name, u.last_name
                   FROM deploy_history dh
                   LEFT JOIN users u ON u.id = dh.initiated_by
                   WHERE dh.client_site_id = %s
                   ORDER BY dh.started_at DESC
                   LIMIT %s""",
                (client_site_id, limit)
            )
            return await cur.fetchall()


# ==================== Hosting Plans ====================

async def list_hosting_plans(active_only: bool = True) -> List[Dict]:
    """List available hosting plans."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            query = "SELECT * FROM hosting_plans"
            if active_only:
                query += " WHERE is_active = TRUE"
            query += " ORDER BY sort_order"
            await cur.execute(query)
            return await cur.fetchall()


async def get_hosting_plan(plan_id: str) -> Optional[Dict]:
    """Get a specific hosting plan."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM hosting_plans WHERE id = %s",
                (plan_id,)
            )
            return await cur.fetchone()


# ==================== Hosting Transactions ====================

async def create_hosting_transaction(
    client_site_id: str,
    type: str,
    amount: float,
    currency: str,
    plan_id: str,
    period_months: int,
    qr_code_url: str = None,
    payment_url: str = None,
    expires_at: datetime = None,
    external_id: str = None
) -> Dict:
    """Create a hosting transaction."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO hosting_transactions
                   (client_site_id, type, amount, currency, plan_id, period_months,
                    qr_code_url, payment_url, expires_at, external_id, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                   RETURNING *""",
                (client_site_id, type, amount, currency, plan_id, period_months,
                 qr_code_url, payment_url, expires_at, external_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def get_hosting_transaction(transaction_id: str) -> Optional[Dict]:
    """Get hosting transaction by ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM hosting_transactions WHERE id = %s",
                (transaction_id,)
            )
            return await cur.fetchone()


async def update_hosting_transaction(
    transaction_id: str,
    data: Dict
) -> Optional[Dict]:
    """Update hosting transaction."""
    allowed_fields = [
        'status', 'payment_method', 'external_id', 'verified_at',
        'completed_at', 'notes', 'metadata', 'qr_code_url', 'payment_url'
    ]

    updates = []
    params = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            params.append(data[field])

    if not updates:
        return await get_hosting_transaction(transaction_id)

    params.append(transaction_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE hosting_transactions
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def list_hosting_transactions(
    client_site_id: str = None,
    status: str = None,
    limit: int = 50
) -> List[Dict]:
    """List hosting transactions."""
    conditions = []
    params = []

    if client_site_id:
        conditions.append("client_site_id = %s")
        params.append(client_site_id)

    if status:
        conditions.append("status = %s")
        params.append(status)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT * FROM hosting_transactions
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT %s""",
                params
            )
            return await cur.fetchall()


# ==================== Auto-disable/Delete Functions ====================

async def get_sites_needing_payment_warning() -> List[Dict]:
    """Get sites that need payment warning (2 weeks before expiry)."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT cs.*, u.tg_id as manager_tg_id
                   FROM client_sites cs
                   JOIN users u ON u.id = cs.manager_id
                   WHERE cs.deploy_status = 'active'
                     AND cs.hosting_expires_at IS NOT NULL
                     AND cs.hosting_expires_at <= NOW() + INTERVAL '14 days'
                     AND cs.hosting_expires_at > NOW()
                     AND (cs.payment_warning_sent_at IS NULL
                          OR cs.payment_warning_sent_at < cs.hosting_expires_at - INTERVAL '13 days')
                   ORDER BY cs.hosting_expires_at ASC"""
            )
            return await cur.fetchall()


async def get_sites_to_auto_disable() -> List[Dict]:
    """Get sites that should be auto-disabled (2 weeks after expiry)."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT cs.*, u.tg_id as manager_tg_id
                   FROM client_sites cs
                   JOIN users u ON u.id = cs.manager_id
                   WHERE cs.deploy_status = 'active'
                     AND cs.hosting_expires_at IS NOT NULL
                     AND cs.hosting_expires_at < NOW() - INTERVAL '14 days'
                     AND cs.auto_disabled_at IS NULL"""
            )
            return await cur.fetchall()


async def get_sites_to_delete() -> List[Dict]:
    """Get sites scheduled for deletion (2 months after expiry)."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT cs.*, u.tg_id as manager_tg_id
                   FROM client_sites cs
                   JOIN users u ON u.id = cs.manager_id
                   WHERE cs.deploy_status IN ('active', 'stopped')
                     AND cs.hosting_expires_at IS NOT NULL
                     AND cs.hosting_expires_at < NOW() - INTERVAL '60 days'
                     AND (cs.scheduled_for_deletion_at IS NULL
                          OR cs.scheduled_for_deletion_at <= NOW())"""
            )
            return await cur.fetchall()


async def mark_payment_warning_sent(site_id: str):
    """Mark payment warning as sent."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE client_sites
                   SET payment_warning_sent_at = NOW()
                   WHERE id = %s""",
                (site_id,)
            )
            await conn.commit()


async def mark_site_auto_disabled(site_id: str):
    """Mark site as auto-disabled."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE client_sites
                   SET deploy_status = 'stopped',
                       auto_disabled_at = NOW()
                   WHERE id = %s""",
                (site_id,)
            )
            await conn.commit()


async def schedule_site_for_deletion(site_id: str):
    """Schedule site for deletion."""
    deletion_date = datetime.now(timezone.utc) + timedelta(days=7)  # Delete in 7 days

    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE client_sites
                   SET scheduled_for_deletion_at = %s
                   WHERE id = %s""",
                (deletion_date, site_id)
            )
            await conn.commit()


# ==================== Revisions (Правки) ====================

async def create_revision(
    site_id: str,
    manager_id: str,
    s3_folder: str = None,
    archive_s3_key: str = None,
    source: str = 'telegram_bot',
    client_id: str = None,
    request_id: str = None
) -> Dict:
    """Create a new revision (iteration of changes) for a site."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get next iteration number
            await cur.execute(
                "SELECT COALESCE(MAX(iteration), 0) + 1 as next_iter FROM revisions WHERE site_id = %s",
                (site_id,)
            )
            result = await cur.fetchone()
            iteration = result['next_iter']

            # Generate s3_folder if not provided
            if not s3_folder:
                s3_folder = f"sites/{site_id}/revisions/{iteration}/"

            await cur.execute(
                """INSERT INTO revisions
                   (site_id, request_id, iteration, s3_folder, archive_s3_key,
                    source, client_id, manager_id, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                   RETURNING *""",
                (site_id, request_id, iteration, s3_folder, archive_s3_key,
                 source, client_id, manager_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def get_revision(revision_id: str) -> Optional[Dict]:
    """Get revision by ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT r.*,
                          cs.company_name, cs.preview_url, cs.domain, cs.deploy_status,
                          u.first_name as manager_first_name, u.last_name as manager_last_name,
                          u.tg_id as manager_tg_id
                   FROM revisions r
                   JOIN client_sites cs ON cs.id = r.site_id
                   LEFT JOIN users u ON u.id = r.manager_id
                   WHERE r.id = %s""",
                (revision_id,)
            )
            return await cur.fetchone()


async def get_revision_by_n8n_job(job_id: str) -> Optional[Dict]:
    """Get revision by n8n job ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM revisions WHERE n8n_job_id = %s",
                (job_id,)
            )
            return await cur.fetchone()


async def get_active_revision_by_site(site_id: str) -> Optional[Dict]:
    """Get the most recent active (in_progress/processing) revision for a site."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT * FROM revisions
                   WHERE site_id = %s
                   AND status IN ('in_progress', 'processing', 'pending')
                   ORDER BY created_at DESC
                   LIMIT 1""",
                (site_id,)
            )
            return await cur.fetchone()


async def get_site_revisions(
    site_id: str,
    status: str = None,
    limit: int = 20,
    offset: int = 0
) -> List[Dict]:
    """List revisions for a site."""
    conditions = ["site_id = %s"]
    params = [site_id]

    if status:
        conditions.append("status = %s")
        params.append(status)

    where_clause = " AND ".join(conditions)
    params.extend([limit, offset])

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT r.*,
                           (SELECT COUNT(*) FROM revision_changes WHERE revision_id = r.id) as changes_count
                    FROM revisions r
                    WHERE {where_clause}
                    ORDER BY r.iteration DESC
                    LIMIT %s OFFSET %s""",
                params
            )
            return await cur.fetchall()


async def list_active_revisions(
    manager_id: str = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict]:
    """List active revisions (pending, in_progress, processing)."""
    conditions = ["r.status IN ('pending', 'in_progress', 'processing')"]
    params = []

    if manager_id:
        conditions.append("r.manager_id = %s")
        params.append(manager_id)

    where_clause = " AND ".join(conditions)
    params.extend([limit, offset])

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT r.*,
                           cs.company_name, cs.preview_url, cs.domain, cs.deploy_status,
                           u.first_name as manager_first_name, u.last_name as manager_last_name,
                           u.tg_id as manager_tg_id,
                           (SELECT COUNT(*) FROM revision_changes WHERE revision_id = r.id) as changes_count
                    FROM revisions r
                    JOIN client_sites cs ON cs.id = r.site_id
                    LEFT JOIN users u ON u.id = r.manager_id
                    WHERE {where_clause}
                    ORDER BY r.created_at DESC
                    LIMIT %s OFFSET %s""",
                params
            )
            return await cur.fetchall()


async def update_revision(revision_id: str, data: Dict) -> Optional[Dict]:
    """Update revision."""
    allowed_fields = [
        'status', 's3_folder', 'archive_s3_key', 'result_archive_s3_key',
        'n8n_job_id', 'n8n_webhook_url', 'n8n_sent_at', 'n8n_response_at',
        'error_message', 'error_details', 'completed_at'
    ]

    updates = []
    params = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            value = data[field]
            if field == 'error_details' and isinstance(value, dict):
                value = json.dumps(value)
            params.append(value)

    if not updates:
        return await get_revision(revision_id)

    params.append(revision_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE revisions
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def update_revision_status(
    revision_id: str,
    status: str,
    error_message: str = None,
    changed_by: str = None,
    change_source: str = 'system'
) -> Optional[Dict]:
    """Update revision status with history logging."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get current status
            await cur.execute(
                "SELECT status FROM revisions WHERE id = %s",
                (revision_id,)
            )
            current = await cur.fetchone()
            if not current:
                return None

            old_status = current['status']

            # Update status
            update_data = {'status': status}
            if error_message:
                update_data['error_message'] = error_message
            if status in ('completed', 'failed', 'cancelled'):
                update_data['completed_at'] = datetime.now(timezone.utc)

            # Build update query
            set_parts = ["status = %s"]
            params = [status]

            if error_message:
                set_parts.append("error_message = %s")
                params.append(error_message)

            if status in ('completed', 'failed', 'cancelled'):
                set_parts.append("completed_at = NOW()")

            params.append(revision_id)

            await cur.execute(
                f"UPDATE revisions SET {', '.join(set_parts)} WHERE id = %s RETURNING *",
                params
            )
            revision = await cur.fetchone()

            # Log status change
            await cur.execute(
                """INSERT INTO revision_history
                   (revision_id, old_status, new_status, changed_by, change_source)
                   VALUES (%s, %s, %s, %s, %s)""",
                (revision_id, old_status, status, changed_by, change_source)
            )

            await conn.commit()
            return revision


async def delete_revision(revision_id: str):
    """Delete a revision."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM revisions WHERE id = %s", (revision_id,))
            await conn.commit()


# ==================== Revision Changes (Отдельные правки) ====================

async def create_revision_change(
    revision_id: str,
    client_description: str,
    change_type: str = 'text_change',
    location_area: str = None,
    location_selector: str = None,
    location_description: str = None,
    old_value: str = None,
    new_value_suggestion: str = None,
    screenshot_s3_key: str = None,
    screenshot_comment: str = None,
    priority: str = 'normal',
    metadata: Dict = None
) -> Dict:
    """Create a revision change."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO revision_changes
                   (revision_id, change_type, location_area, location_selector,
                    location_description, client_description, old_value, new_value_suggestion,
                    screenshot_s3_key, screenshot_comment, priority, metadata)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING *""",
                (revision_id, change_type, location_area, location_selector,
                 location_description, client_description, old_value, new_value_suggestion,
                 screenshot_s3_key, screenshot_comment, priority,
                 json.dumps(metadata) if metadata else '{}')
            )
            await conn.commit()
            return await cur.fetchone()


async def get_revision_changes(revision_id: str) -> List[Dict]:
    """Get all changes for a revision."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT * FROM revision_changes
                   WHERE revision_id = %s
                   ORDER BY created_at ASC""",
                (revision_id,)
            )
            return await cur.fetchall()


async def update_revision_change(change_id: str, data: Dict) -> Optional[Dict]:
    """Update a revision change."""
    allowed_fields = [
        'status', 'ai_interpretation', 'ai_confidence', 'metadata'
    ]

    updates = []
    params = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            value = data[field]
            if field == 'metadata' and isinstance(value, dict):
                value = json.dumps(value)
            params.append(value)

    if not updates:
        return None

    params.append(change_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE revision_changes
                    SET {", ".join(updates)}
                    WHERE id = %s
                    RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def delete_revision_change(change_id: str):
    """Delete a revision change."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM revision_changes WHERE id = %s", (change_id,))
            await conn.commit()


# ==================== Revision History ====================

async def get_revision_history(revision_id: str) -> List[Dict]:
    """Get status history for a revision."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT rh.*, u.first_name, u.last_name
                   FROM revision_history rh
                   LEFT JOIN users u ON u.id = rh.changed_by
                   WHERE rh.revision_id = %s
                   ORDER BY rh.created_at ASC""",
                (revision_id,)
            )
            return await cur.fetchall()


# ==================== Revision Stats ====================

async def get_revision_stats() -> Dict:
    """Get overall revision statistics."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT
                   COUNT(*) as total_revisions,
                   COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                   COUNT(*) FILTER (WHERE status = 'in_progress') as in_progress_count,
                   COUNT(*) FILTER (WHERE status = 'processing') as processing_count,
                   COUNT(*) FILTER (WHERE status = 'completed') as completed_count,
                   COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                   COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE) as today_count,
                   AVG(EXTRACT(EPOCH FROM (completed_at - created_at))/3600)
                       FILTER (WHERE status = 'completed') as avg_completion_hours
                   FROM revisions"""
            )
            return await cur.fetchone()


async def get_site_revision_stats(site_id: str) -> Dict:
    """Get revision statistics for a specific site."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT
                   COUNT(*) as total_revisions,
                   MAX(iteration) as last_iteration,
                   COUNT(*) FILTER (WHERE status = 'completed') as completed_count,
                   COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                   MAX(completed_at) as last_completed_at,
                   (SELECT COUNT(*) FROM revision_changes rc
                    JOIN revisions r ON r.id = rc.revision_id
                    WHERE r.site_id = %s) as total_changes
                   FROM revisions
                   WHERE site_id = %s""",
                (site_id, site_id)
            )
            return await cur.fetchone()


# ==================== Service Categories ====================

async def list_service_categories(parent_id: str = None, active_only: bool = True) -> List[Dict]:
    """List service categories, optionally filtered by parent."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            conditions = []
            params = []

            if parent_id:
                conditions.append("parent_id = %s")
                params.append(parent_id)
            else:
                conditions.append("parent_id IS NULL")

            if active_only:
                conditions.append("is_active = TRUE")

            where_clause = " AND ".join(conditions)

            await cur.execute(
                f"""SELECT id, parent_id, name, description, icon, sort_order, is_active
                    FROM service_categories
                    WHERE {where_clause}
                    ORDER BY sort_order, name""",
                params
            )
            return await cur.fetchall()


async def get_service_category_tree() -> List[Dict]:
    """Get full service category tree."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """WITH RECURSIVE category_tree AS (
                    SELECT id, parent_id, name, description, icon, sort_order, 0 as level
                    FROM service_categories
                    WHERE parent_id IS NULL AND is_active = TRUE
                    UNION ALL
                    SELECT c.id, c.parent_id, c.name, c.description, c.icon, c.sort_order, ct.level + 1
                    FROM service_categories c
                    JOIN category_tree ct ON c.parent_id = ct.id
                    WHERE c.is_active = TRUE
                )
                SELECT * FROM category_tree ORDER BY level, sort_order, name"""
            )
            return await cur.fetchall()


async def create_service_category(
    name: str,
    parent_id: str = None,
    description: str = None,
    icon: str = None,
    sort_order: int = 0
) -> Dict:
    """Create a new service category."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO service_categories (name, parent_id, description, icon, sort_order)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING *""",
                (name, parent_id, description, icon, sort_order)
            )
            await conn.commit()
            return await cur.fetchone()


async def update_service_category(category_id: str, data: Dict) -> Optional[Dict]:
    """Update a service category."""
    allowed_fields = ['name', 'description', 'icon', 'sort_order', 'is_active', 'parent_id']
    updates = []
    params = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            params.append(data[field])

    if not updates:
        return None

    params.append(category_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE service_categories SET {", ".join(updates)}
                    WHERE id = %s RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def delete_service_category(category_id: str):
    """Delete a service category."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM service_categories WHERE id = %s", (category_id,))
            await conn.commit()


# ==================== Admin Groups ====================

async def create_admin_group(name: str, description: str = None, created_by: str = None) -> Dict:
    """Create a new admin group."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO admin_groups (name, description, created_by)
                   VALUES (%s, %s, %s)
                   RETURNING *""",
                (name, description, created_by)
            )
            await conn.commit()
            return await cur.fetchone()


async def list_admin_groups(active_only: bool = True, created_by: str = None) -> List[Dict]:
    """List admin groups. If created_by is provided, only show groups created by that admin."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            query = """SELECT ag.*,
                       (SELECT COUNT(*) FROM user_group_membership ugm WHERE ugm.group_id = ag.id) as member_count
                       FROM admin_groups ag"""
            conditions = []
            if active_only:
                conditions.append("ag.is_active = TRUE")
            if created_by:
                conditions.append("ag.created_by = %s")
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY ag.name"
            params = (created_by,) if created_by else None
            await cur.execute(query, params if params else None)
            return await cur.fetchall()


async def get_admin_group(group_id: str) -> Optional[Dict]:
    """Get admin group by ID with members."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM admin_groups WHERE id = %s",
                (group_id,)
            )
            group = await cur.fetchone()

            if group:
                await cur.execute(
                    """SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name,
                              u.role, ugm.role as group_role, ugm.created_at as joined_at
                       FROM user_group_membership ugm
                       JOIN users u ON u.id = ugm.user_id
                       WHERE ugm.group_id = %s
                       ORDER BY ugm.role DESC, u.first_name""",
                    (group_id,)
                )
                group['members'] = await cur.fetchall()

            return group


async def add_user_to_group(user_id: str, group_id: str, role: str = 'member', added_by: str = None) -> Dict:
    """Add user to an admin group."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO user_group_membership (user_id, group_id, role, added_by)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (user_id, group_id) DO UPDATE SET role = EXCLUDED.role
                   RETURNING *""",
                (user_id, group_id, role, added_by)
            )
            # Also update user's primary group
            await cur.execute(
                "UPDATE users SET admin_group_id = %s WHERE id = %s",
                (group_id, user_id)
            )
            await conn.commit()
            return await cur.fetchone()


async def remove_user_from_group(user_id: str, group_id: str):
    """Remove user from an admin group."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM user_group_membership WHERE user_id = %s AND group_id = %s",
                (user_id, group_id)
            )
            # Clear user's primary group if it was this one
            await cur.execute(
                "UPDATE users SET admin_group_id = NULL WHERE id = %s AND admin_group_id = %s",
                (user_id, group_id)
            )
            await conn.commit()


async def get_user_groups(user_id: str) -> List[Dict]:
    """Get all groups a user belongs to."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT ag.*, ugm.role as group_membership_role
                   FROM admin_groups ag
                   JOIN user_group_membership ugm ON ugm.group_id = ag.id
                   WHERE ugm.user_id = %s AND ag.is_active = TRUE
                   ORDER BY ag.name""",
                (user_id,)
            )
            return await cur.fetchall()


async def is_manager_accessible_by_admin(manager_id: str, admin_id: str) -> bool:
    """Check if a manager belongs to any group created by the admin."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT 1
                   FROM users u
                   JOIN user_group_membership ugm ON ugm.user_id = u.id
                   JOIN admin_groups ag ON ag.id = ugm.group_id
                   WHERE u.id = %s
                     AND ag.created_by = %s
                     AND ag.is_active = TRUE
                   LIMIT 1""",
                (manager_id, admin_id)
            )
            return await cur.fetchone() is not None


async def get_group_managers(group_id: str) -> List[Dict]:
    """Get all managers in a specific group."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name,
                          u.role, u.approval_status, u.is_blocked,
                          COUNT(r.id) as request_count
                   FROM users u
                   JOIN user_group_membership ugm ON ugm.user_id = u.id
                   LEFT JOIN projects p ON p.manager_id = u.id
                   LEFT JOIN requests r ON r.project_id = p.id
                   WHERE ugm.group_id = %s AND u.role = 'manager'
                   GROUP BY u.id
                   ORDER BY u.first_name""",
                (group_id,)
            )
            return await cur.fetchall()


async def list_managers_by_admin(admin_id: str) -> List[Dict]:
    """List managers that belong to groups created by this admin."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Get managers from groups created by this admin with group info
            await cur.execute(
                """SELECT DISTINCT u.id, u.tg_id, u.username, u.first_name, u.last_name,
                          u.full_name, u.phone, u.email, u.contact, u.role, u.approval_status,
                          u.is_blocked, u.created_at,
                          COUNT(r.id) as request_count,
                          ag.id as group_id, ag.name as group_name
                   FROM users u
                   JOIN user_group_membership ugm ON ugm.user_id = u.id
                   JOIN admin_groups ag ON ag.id = ugm.group_id
                   LEFT JOIN projects p ON p.manager_id = u.id
                   LEFT JOIN requests r ON r.project_id = p.id
                   WHERE ag.created_by = %s
                     AND u.role = 'manager'
                     AND u.approval_status = 'approved'
                     AND ag.is_active = TRUE
                   GROUP BY u.id, ag.id, ag.name
                   ORDER BY request_count DESC""",
                (admin_id,)
            )
            return await cur.fetchall()


# ==================== Anti-Nuke Protection ====================

async def get_anti_nuke_setting(key: str) -> Optional[str]:
    """Get an anti-nuke setting value."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT setting_value FROM anti_nuke_settings WHERE setting_key = %s",
                (key,)
            )
            result = await cur.fetchone()
            return result[0] if result else None


async def update_anti_nuke_setting(key: str, value: str, updated_by: str = None):
    """Update an anti-nuke setting."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE anti_nuke_settings
                   SET setting_value = %s, updated_by = %s, updated_at = NOW()
                   WHERE setting_key = %s""",
                (value, updated_by, key)
            )
            await conn.commit()


async def log_deletion(
    action_type: str,
    target_type: str,
    performed_by: str,
    target_id: str = None,
    target_ids: List[str] = None,
    reason: str = None,
    ip_address: str = None,
    user_agent: str = None,
    metadata: Dict = None
) -> Dict:
    """Log a deletion operation for audit purposes."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            target_count = 1 if target_id else len(target_ids) if target_ids else 0
            await cur.execute(
                """INSERT INTO deletion_audit_log
                   (action_type, target_type, target_id, target_ids, target_count,
                    performed_by, reason, ip_address, user_agent, metadata)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING *""",
                (action_type, target_type, target_id, target_ids, target_count,
                 performed_by, reason, ip_address, user_agent,
                 json.dumps(metadata) if metadata else '{}')
            )
            await conn.commit()
            return await cur.fetchone()


async def get_recent_deletions(user_id: str, seconds: int = 60) -> int:
    """Get count of recent deletions by a user (for rate limiting)."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT COALESCE(SUM(target_count), 0)
                   FROM deletion_audit_log
                   WHERE performed_by = %s
                   AND created_at > NOW() - INTERVAL '%s seconds'""",
                (user_id, seconds)
            )
            result = await cur.fetchone()
            return int(result[0]) if result else 0


async def get_managers_count() -> int:
    """Get total count of active managers."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM users WHERE role = 'manager' AND approval_status = 'approved'"
            )
            result = await cur.fetchone()
            return result[0] if result else 0


async def can_delete_manager(admin_id: str, manager_id: str) -> tuple:
    """Check if a manager can be deleted (anti-nuke protection)."""
    managers_count = await get_managers_count()
    min_managers = int(await get_anti_nuke_setting('min_managers_count') or '1')

    if managers_count <= min_managers:
        return False, f"Cannot delete: minimum {min_managers} manager(s) must remain"

    cooldown = int(await get_anti_nuke_setting('deletion_cooldown_seconds') or '30')
    recent_deletions = await get_recent_deletions(admin_id, cooldown)

    if recent_deletions > 0:
        return False, f"Please wait {cooldown} seconds between deletion operations"

    return True, None


async def can_mass_delete_requests(admin_id: str, count: int) -> tuple:
    """Check if bulk request deletion is allowed (anti-nuke protection)."""
    max_bulk = int(await get_anti_nuke_setting('max_bulk_delete_requests') or '10')

    if count > max_bulk:
        return False, f"Cannot delete more than {max_bulk} requests at once"

    cooldown = int(await get_anti_nuke_setting('deletion_cooldown_seconds') or '30')
    recent_deletions = await get_recent_deletions(admin_id, cooldown)

    if recent_deletions > 0:
        return False, f"Please wait {cooldown} seconds between deletion operations"

    return True, None


# ==================== Role-based Permission Checks ====================

async def get_user_role(user_id: str) -> Optional[str]:
    """Get user role by ID."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT role FROM users WHERE id = %s", (user_id,))
            result = await cur.fetchone()
            return result[0] if result else None


async def can_manage_user(manager_id: str, target_user_id: str) -> bool:
    """
    Check if manager can manage target user based on role hierarchy.
    Rules:
    - Owner can manage everyone (owner, director, supervisor, manager)
    - Director can manage (director, supervisor, manager) but not owner
    - Supervisor can manage only managers from their groups
    - Manager cannot manage anyone
    """
    manager_role = await get_user_role(manager_id)
    target_role = await get_user_role(target_user_id)

    if not manager_role or not target_role:
        return False

    # Owner can manage everyone
    if manager_role == 'owner':
        return True

    # Director can manage director, supervisor, manager (but not owner)
    if manager_role == 'director':
        return target_role in ('director', 'supervisor', 'manager')

    # Supervisor can manage only managers from their groups
    if manager_role == 'supervisor':
        if target_role != 'manager':
            return False
        return await is_manager_accessible_by_admin(target_user_id, manager_id)

    # Manager cannot manage anyone
    return False


async def can_manage_group(manager_id: str, group_id: str) -> bool:
    """
    Check if manager can manage a group.
    Rules:
    - Owner can manage all groups
    - Director can manage all groups
    - Supervisor can manage only groups they created
    - Manager cannot manage groups
    """
    manager_role = await get_user_role(manager_id)

    if not manager_role:
        return False

    # Owner and Director can manage all groups
    if manager_role in ('owner', 'director'):
        return True

    # Supervisor can manage only groups they created
    if manager_role == 'supervisor':
        group = await get_admin_group(group_id)
        if not group:
            return False
        return str(group.get('created_by')) == str(manager_id)

    # Manager cannot manage groups
    return False


async def can_assign_role(manager_id: str, target_role: str) -> bool:
    """
    Check if manager can assign a specific role to someone.
    Rules:
    - Owner can assign any role (owner, director, supervisor, manager)
    - Director can assign (director, supervisor, manager) but not owner
    - Supervisor can assign only manager role
    - Manager cannot assign roles
    """
    manager_role = await get_user_role(manager_id)

    if not manager_role:
        return False

    # Owner can assign any role
    if manager_role == 'owner':
        return target_role in ('owner', 'director', 'supervisor', 'manager')

    # Director can assign director, supervisor, manager (but not owner)
    if manager_role == 'director':
        return target_role in ('director', 'supervisor', 'manager')

    # Supervisor can assign only manager role
    if manager_role == 'supervisor':
        return target_role == 'manager'

    # Manager cannot assign roles
    return False


# ==================== Invite Codes ====================

import secrets
import string

def generate_invite_code(length: int = 8) -> str:
    """Generate a random invite code."""
    alphabet = string.ascii_uppercase + string.digits
    # Exclude confusing characters
    alphabet = alphabet.replace('O', '').replace('0', '').replace('I', '').replace('1', '').replace('L', '')
    return ''.join(secrets.choice(alphabet) for _ in range(length))


async def create_invite_code(
    created_by: str,
    group_id: str = None,
    name: str = None,
    max_uses: int = None,
    expires_at: datetime = None,
    auto_approve: bool = False,
    notes: str = None,
    target_role: str = 'manager'
) -> Dict:
    """Create a new invite code."""
    code = generate_invite_code()

    # Ensure code is unique
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check uniqueness and regenerate if needed
            for _ in range(5):
                await cur.execute("SELECT id FROM invite_codes WHERE code = %s", (code,))
                if not await cur.fetchone():
                    break
                code = generate_invite_code()

            await cur.execute(
                """INSERT INTO invite_codes
                   (code, group_id, created_by, name, max_uses, expires_at, auto_approve, notes, target_role)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING *""",
                (code, group_id, created_by, name, max_uses, expires_at, auto_approve, notes, target_role)
            )
            await conn.commit()
            return await cur.fetchone()


async def get_invite_code(code: str) -> Optional[Dict]:
    """Get invite code by code string."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT ic.*,
                          ag.name as group_name,
                          u.first_name as creator_first_name,
                          u.last_name as creator_last_name
                   FROM invite_codes ic
                   LEFT JOIN admin_groups ag ON ag.id = ic.group_id
                   LEFT JOIN users u ON u.id = ic.created_by
                   WHERE ic.code = %s""",
                (code.upper(),)
            )
            return await cur.fetchone()


async def get_invite_code_by_id(code_id: str) -> Optional[Dict]:
    """Get invite code by ID."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT ic.*,
                          ag.name as group_name,
                          u.first_name as creator_first_name,
                          u.last_name as creator_last_name
                   FROM invite_codes ic
                   LEFT JOIN admin_groups ag ON ag.id = ic.group_id
                   LEFT JOIN users u ON u.id = ic.created_by
                   WHERE ic.id = %s""",
                (code_id,)
            )
            return await cur.fetchone()


async def validate_invite_code(code: str) -> tuple:
    """
    Validate an invite code.
    Returns (is_valid, invite_code_data or error_message)
    """
    invite = await get_invite_code(code)

    if not invite:
        return False, "Неверный инвайт-код"

    if not invite['is_active']:
        return False, "Инвайт-код деактивирован"

    # Check expiration
    if invite['expires_at'] and invite['expires_at'] < datetime.now(timezone.utc):
        return False, "Инвайт-код истёк"

    # Check usage limit
    if invite['max_uses'] is not None and invite['uses_count'] >= invite['max_uses']:
        return False, "Инвайт-код исчерпан (достигнут лимит использований)"

    return True, invite


async def use_invite_code(code: str, user_id: str) -> Optional[Dict]:
    """
    Use an invite code for a user.
    Returns the invite code data if successful, None if failed.
    """
    is_valid, result = await validate_invite_code(code)

    if not is_valid:
        return None

    invite = result

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check if user already used this code
            await cur.execute(
                "SELECT id FROM invite_code_usage WHERE invite_code_id = %s AND user_id = %s",
                (invite['id'], user_id)
            )
            if await cur.fetchone():
                return None  # Already used by this user

            # Record usage
            await cur.execute(
                """INSERT INTO invite_code_usage (invite_code_id, user_id)
                   VALUES (%s, %s)""",
                (invite['id'], user_id)
            )

            # Increment usage count
            await cur.execute(
                "UPDATE invite_codes SET uses_count = uses_count + 1 WHERE id = %s",
                (invite['id'],)
            )

            # Update user with invite code reference
            await cur.execute(
                "UPDATE users SET registered_via_code = %s WHERE id = %s",
                (invite['id'], user_id)
            )

            # If code has a group, add user to that group
            if invite['group_id']:
                await cur.execute(
                    """INSERT INTO user_group_membership (user_id, group_id, role, added_by)
                       VALUES (%s, %s, 'member', %s)
                       ON CONFLICT (user_id, group_id) DO NOTHING""",
                    (user_id, invite['group_id'], invite['created_by'])
                )
                # Set as primary group
                await cur.execute(
                    "UPDATE users SET admin_group_id = %s WHERE id = %s",
                    (invite['group_id'], user_id)
                )

            # If auto_approve, approve the user
            if invite['auto_approve']:
                await cur.execute(
                    """UPDATE users SET approval_status = 'approved', approved_at = NOW()
                       WHERE id = %s AND approval_status = 'pending'""",
                    (user_id,)
                )

            await conn.commit()
            return invite


async def list_invite_codes(created_by: str = None, group_id: str = None, active_only: bool = True) -> List[Dict]:
    """List invite codes with optional filters."""
    conditions = []
    params = []

    if created_by:
        conditions.append("ic.created_by = %s")
        params.append(created_by)

    if group_id:
        conditions.append("ic.group_id = %s")
        params.append(group_id)

    if active_only:
        conditions.append("ic.is_active = TRUE")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""SELECT ic.*,
                           ag.name as group_name,
                           u.first_name as creator_first_name,
                           u.last_name as creator_last_name,
                           (SELECT COUNT(*) FROM invite_code_usage icu WHERE icu.invite_code_id = ic.id) as actual_uses
                    FROM invite_codes ic
                    LEFT JOIN admin_groups ag ON ag.id = ic.group_id
                    LEFT JOIN users u ON u.id = ic.created_by
                    WHERE {where_clause}
                    ORDER BY ic.created_at DESC""",
                params
            )
            return await cur.fetchall()


async def update_invite_code(code_id: str, data: Dict) -> Optional[Dict]:
    """Update an invite code."""
    allowed_fields = ['name', 'max_uses', 'expires_at', 'auto_approve', 'is_active', 'notes', 'group_id']
    updates = []
    params = []

    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            params.append(data[field])

    if not updates:
        return await get_invite_code_by_id(code_id)

    params.append(code_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""UPDATE invite_codes SET {", ".join(updates)}
                    WHERE id = %s RETURNING *""",
                params
            )
            await conn.commit()
            return await cur.fetchone()


async def delete_invite_code(code_id: str):
    """Delete an invite code."""
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM invite_codes WHERE id = %s", (code_id,))
            await conn.commit()


async def get_invite_code_usage(code_id: str) -> List[Dict]:
    """Get list of users who used an invite code."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name,
                          u.approval_status, u.is_blocked, icu.used_at
                   FROM invite_code_usage icu
                   JOIN users u ON u.id = icu.user_id
                   WHERE icu.invite_code_id = %s
                   ORDER BY icu.used_at DESC""",
                (code_id,)
            )
            return await cur.fetchall()


async def create_user_with_invite(
    tg_id: int,
    username: str,
    first_name: str,
    last_name: str,
    invite_code: str = None
) -> Dict:
    """Create user and optionally apply invite code with target role."""
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Check if invite code is valid
            auto_approve = False
            group_id = None
            invite_id = None
            invite_creator = None
            target_role = 'manager'  # Default role

            if invite_code:
                is_valid, result = await validate_invite_code(invite_code)
                if is_valid:
                    invite = result
                    auto_approve = invite['auto_approve']
                    group_id = invite['group_id']
                    invite_id = invite['id']
                    invite_creator = invite['created_by']
                    # Get target role from invite, default to 'manager' if not set
                    target_role = invite.get('target_role') or 'manager'

            # Determine approval status
            approval_status = 'approved' if auto_approve else 'pending'

            # Create user with the target role from invite code
            await cur.execute(
                """INSERT INTO users (tg_id, username, first_name, last_name, role,
                                     approval_status, registered_via_code, admin_group_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   RETURNING id, tg_id, username, first_name, last_name, contact, role,
                             approval_status, created_at, admin_group_id""",
                (tg_id, username, first_name, last_name, target_role, approval_status, invite_id, group_id)
            )
            user = await cur.fetchone()
            user_id = str(user['id'])

            # If invite code was used, record usage and add to group
            if invite_id:
                await cur.execute(
                    """INSERT INTO invite_code_usage (invite_code_id, user_id)
                       VALUES (%s, %s)""",
                    (invite_id, user_id)
                )
                await cur.execute(
                    "UPDATE invite_codes SET uses_count = uses_count + 1 WHERE id = %s",
                    (invite_id,)
                )

                if group_id:
                    await cur.execute(
                        """INSERT INTO user_group_membership (user_id, group_id, role, added_by)
                           VALUES (%s, %s, 'member', %s)
                           ON CONFLICT (user_id, group_id) DO NOTHING""",
                        (user_id, group_id, invite_creator)
                    )

            await conn.commit()
            return user
