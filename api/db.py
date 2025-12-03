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
        settings.DATABASE_URL,
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
                          approval_status, created_at, COALESCE(is_blocked, FALSE) as is_blocked
                   FROM users WHERE tg_id = %s""",
                (tg_id,)
            )
            return await cur.fetchone()


async def get_user_by_id(user_id: str) -> Optional[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, contact, role,
                          approval_status, created_at, COALESCE(is_blocked, FALSE) as is_blocked
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
            await cur.execute(
                """SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name, u.contact,
                          u.role, u.approval_status, u.created_at,
                          COALESCE(u.is_blocked, FALSE) as is_blocked,
                          COUNT(r.id) as request_count
                   FROM users u
                   LEFT JOIN projects p ON p.manager_id = u.id
                   LEFT JOIN requests r ON r.project_id = p.id
                   WHERE u.role = 'manager' AND u.approval_status = 'approved'
                   GROUP BY u.id
                   ORDER BY request_count DESC"""
            )
            return await cur.fetchall()


async def list_pending_registrations() -> List[Dict]:
    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """SELECT id, tg_id, username, first_name, last_name, created_at
                   FROM users WHERE approval_status = 'pending'
                   ORDER BY created_at DESC"""
            )
            return await cur.fetchall()


async def approve_user(user_id: str):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'approved',
                   approved_at = NOW() WHERE id = %s""",
                (user_id,)
            )
            await conn.commit()


async def reject_user(user_id: str, reason: str):
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """UPDATE users SET approval_status = 'rejected',
                   rejection_reason = %s WHERE id = %s""",
                (reason, user_id)
            )
            await conn.commit()


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
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
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
                    row['payload'] = row.pop('payload_json')
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
                          p.manager_id as user_id,
                          r.created_at
                   FROM requests r
                   JOIN projects p ON p.id = r.project_id
                   WHERE r.id = %s""",
                (request_id,)
            )
            row = await cur.fetchone()
            if row and row.get('payload_json'):
                row['payload'] = row.pop('payload_json')
            return row


async def create_request(user_id: str, company_name: str, client_name: str, payload: Dict) -> Dict:
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

            # Create the request
            await cur.execute(
                """INSERT INTO requests (project_id, status, payload_json)
                   VALUES (%s, 'draft', %s)
                   RETURNING id, status, payload_json, created_at""",
                (project_id, json.dumps(payload))
            )
            await conn.commit()
            row = await cur.fetchone()
            if row:
                row['company_name'] = company_name
                row['client_name'] = client_name
                if row.get('payload_json'):
                    row['payload'] = row.pop('payload_json')
            return row


async def update_request(request_id: str, data: Dict) -> Optional[Dict]:
    """Update request - only payload_json is updatable"""
    if 'payload' not in data:
        return await get_request(request_id)

    async with await get_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """UPDATE requests SET payload_json = %s
                    WHERE id = %s
                    RETURNING id, status, payload_json, created_at""",
                (json.dumps(data['payload']), request_id)
            )
            await conn.commit()
            row = await cur.fetchone()
            if row:
                payload = row.get('payload_json') or {}
                row['payload'] = payload
                row['company_name'] = payload.get('site', {}).get('company', '')
                row['client_name'] = payload.get('client', {}).get('name', '')
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

