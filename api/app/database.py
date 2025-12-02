"""
Database connection and utilities
"""
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
import psycopg
from psycopg.rows import dict_row

from app.config import settings

log = logging.getLogger("database")


def get_connection():
    """Get database connection"""
    return psycopg.connect(settings.DATABASE_URL, row_factory=dict_row)


@contextmanager
def get_db():
    """Database context manager"""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"Database error: {e}")
        raise
    finally:
        conn.close()


# ==================== User Queries ====================

def get_user_by_tgid(tg_id: int) -> Optional[Dict[str, Any]]:
    """Get user by Telegram ID"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT id, tg_id, username, first_name, last_name, contact, role,
                   approval_status, created_at
            FROM users WHERE tg_id = %s
            """,
            (tg_id,)
        )
        result = cur.fetchone()
        if result:
            result['is_blocked'] = False  # Default until migration applied
        return result


def get_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """Get user by ID"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT id, tg_id, username, first_name, last_name, contact, role,
                   approval_status, created_at
            FROM users WHERE id = %s
            """,
            (user_id,)
        )
        result = cur.fetchone()
        if result:
            result['is_blocked'] = False  # Default until migration applied
        return result


def create_user(
    tg_id: int,
    first_name: str,
    last_name: Optional[str] = None,
    username: Optional[str] = None,
    contact: Optional[str] = None,
    role: str = "manager"
) -> Optional[Dict[str, Any]]:
    """Create new user"""
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO users (tg_id, first_name, last_name, username, contact, role, approval_status)
            VALUES (%s, %s, %s, %s, %s, %s, 'pending')
            RETURNING id, tg_id, username, first_name, last_name, contact, role, approval_status, created_at
            """,
            (tg_id, first_name, last_name, username, contact, role)
        )
        return cur.fetchone()


def update_user(user_id: str, **kwargs) -> bool:
    """Update user fields"""
    if not kwargs:
        return False

    # Temporarily exclude is_blocked until migration is applied
    allowed_fields = ['first_name', 'last_name', 'username', 'contact', 'role', 'approval_status']

    fields = []
    values = []
    for key, value in kwargs.items():
        if key in allowed_fields:
            fields.append(f"{key} = %s")
            values.append(value)

    if not fields:
        return False

    values.append(user_id)

    with get_db() as conn:
        conn.execute(
            f"UPDATE users SET {', '.join(fields)} WHERE id = %s",
            values
        )
        return True


# ==================== Request Queries ====================

def get_requests_by_manager(
    manager_id: str,
    archived: bool = False,
    limit: int = 20,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """Get requests for a manager"""
    with get_db() as conn:
        if archived:
            status_filter = "status IN ('archived', 'closed', 'delivered', 'generated_ok')"
        else:
            status_filter = "status NOT IN ('archived')"

        cur = conn.execute(
            f"""
            SELECT id, manager_id, status,
                   payload->>'client'->>'name' as client_name,
                   payload->'site'->>'company' as company_name,
                   payload->'site'->>'business_type' as business_type,
                   result_url, created_at, updated_at
            FROM requests
            WHERE manager_id = %s AND {status_filter}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (manager_id, limit, offset)
        )
        return cur.fetchall()


def count_requests_by_manager(manager_id: str, archived: bool = False) -> int:
    """Count requests for a manager"""
    with get_db() as conn:
        if archived:
            status_filter = "status IN ('archived', 'closed', 'delivered', 'generated_ok')"
        else:
            status_filter = "status NOT IN ('archived')"

        cur = conn.execute(
            f"""
            SELECT COUNT(*) FROM requests
            WHERE manager_id = %s AND {status_filter}
            """,
            (manager_id,)
        )
        row = cur.fetchone()
        return row['count'] if row else 0


def get_request_by_id(request_id: str) -> Optional[Dict[str, Any]]:
    """Get request by ID"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT id, manager_id, status, payload, result_url, created_at, updated_at
            FROM requests WHERE id = %s
            """,
            (request_id,)
        )
        return cur.fetchone()


def create_request(manager_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Create new request"""
    import json
    with get_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO requests (manager_id, payload, status)
            VALUES (%s, %s, 'draft')
            RETURNING id, manager_id, status, payload, created_at
            """,
            (manager_id, json.dumps(payload))
        )
        return cur.fetchone()


def update_request(request_id: str, payload: Dict[str, Any] = None, status: str = None) -> bool:
    """Update request"""
    import json
    updates = []
    values = []

    if payload is not None:
        updates.append("payload = %s")
        values.append(json.dumps(payload))

    if status is not None:
        updates.append("status = %s")
        values.append(status)

    if not updates:
        return False

    updates.append("updated_at = NOW()")
    values.append(request_id)

    with get_db() as conn:
        conn.execute(
            f"UPDATE requests SET {', '.join(updates)} WHERE id = %s",
            values
        )
        return True


def delete_request(request_id: str) -> bool:
    """Delete request"""
    with get_db() as conn:
        conn.execute("DELETE FROM requests WHERE id = %s", (request_id,))
        return True


# ==================== Admin Queries ====================

def get_all_requests(limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
    """Get all requests (admin)"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT r.id, r.manager_id, r.status,
                   r.payload->'client'->>'name' as client_name,
                   r.payload->'site'->>'company' as company_name,
                   r.payload->'site'->>'business_type' as business_type,
                   r.result_url, r.created_at, r.updated_at,
                   u.first_name as manager_first_name, u.username as manager_username
            FROM requests r
            LEFT JOIN users u ON r.manager_id = u.id
            ORDER BY r.created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )
        return cur.fetchall()


def count_all_requests() -> int:
    """Count all requests"""
    with get_db() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM requests")
        row = cur.fetchone()
        return row['count'] if row else 0


def get_all_managers(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """Get all managers"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT u.id, u.tg_id, u.username, u.first_name, u.last_name, u.contact,
                   u.approval_status, u.created_at,
                   COUNT(r.id) as total_requests,
                   COUNT(r.id) FILTER (WHERE r.status IN ('generated_ok', 'delivered', 'closed')) as completed_requests
            FROM users u
            LEFT JOIN requests r ON u.id = r.manager_id
            WHERE u.role = 'manager'
            GROUP BY u.id
            ORDER BY u.created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset)
        )
        results = cur.fetchall()
        for r in results:
            r['is_blocked'] = False  # Default until migration applied
        return results


def get_pending_registrations() -> List[Dict[str, Any]]:
    """Get pending user registrations"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT id, tg_id, username, first_name, last_name, contact, created_at
            FROM users
            WHERE approval_status = 'pending' AND role = 'manager'
            ORDER BY created_at DESC
            """
        )
        return cur.fetchall()


def approve_user(user_id: str) -> bool:
    """Approve user registration"""
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET approval_status = 'approved' WHERE id = %s",
            (user_id,)
        )
        return True


def reject_user(user_id: str, reason: str = None) -> bool:
    """Reject user registration"""
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET approval_status = 'rejected', rejection_reason = %s WHERE id = %s",
            (reason, user_id)
        )
        return True


def get_stats() -> Dict[str, Any]:
    """Get overall statistics"""
    with get_db() as conn:
        # Total users
        cur = conn.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()['count']

        # Total managers
        cur = conn.execute("SELECT COUNT(*) FROM users WHERE role = 'manager'")
        total_managers = cur.fetchone()['count']

        # Total requests
        cur = conn.execute("SELECT COUNT(*) FROM requests")
        total_requests = cur.fetchone()['count']

        # Requests today
        cur = conn.execute(
            "SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE"
        )
        requests_today = cur.fetchone()['count']

        # Requests this week
        cur = conn.execute(
            "SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'"
        )
        requests_this_week = cur.fetchone()['count']

        # Requests this month
        cur = conn.execute(
            "SELECT COUNT(*) FROM requests WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'"
        )
        requests_this_month = cur.fetchone()['count']

        # Pending generation
        cur = conn.execute(
            "SELECT COUNT(*) FROM requests WHERE status IN ('queued', 'generating')"
        )
        pending_generation = cur.fetchone()['count']

        # Completed today
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM requests
            WHERE status IN ('generated_ok', 'delivered')
            AND updated_at >= CURRENT_DATE
            """
        )
        completed_today = cur.fetchone()['count']

        # By status
        cur = conn.execute(
            "SELECT status, COUNT(*) as count FROM requests GROUP BY status"
        )
        by_status = cur.fetchall()

        return {
            "total_users": total_users,
            "total_managers": total_managers,
            "total_requests": total_requests,
            "requests_today": requests_today,
            "requests_this_week": requests_this_week,
            "requests_this_month": requests_this_month,
            "pending_generation": pending_generation,
            "completed_today": completed_today,
            "by_status": by_status,
        }


def get_manager_stats(manager_id: str) -> Dict[str, Any]:
    """Get manager statistics"""
    with get_db() as conn:
        # Total requests
        cur = conn.execute(
            "SELECT COUNT(*) FROM requests WHERE manager_id = %s",
            (manager_id,)
        )
        total_requests = cur.fetchone()['count']

        # Completed
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM requests
            WHERE manager_id = %s AND status IN ('generated_ok', 'delivered', 'closed')
            """,
            (manager_id,)
        )
        completed_requests = cur.fetchone()['count']

        # Pending
        pending_requests = total_requests - completed_requests

        # This week
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM requests
            WHERE manager_id = %s AND created_at >= CURRENT_DATE - INTERVAL '7 days'
            """,
            (manager_id,)
        )
        this_week = cur.fetchone()['count']

        # Today
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM requests
            WHERE manager_id = %s AND created_at >= CURRENT_DATE
            """,
            (manager_id,)
        )
        today = cur.fetchone()['count']

        return {
            "total_requests": total_requests,
            "completed_requests": completed_requests,
            "pending_requests": pending_requests,
            "this_week": this_week,
            "today": today,
        }


# ==================== Search ====================

def search_requests(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Search requests by company name or client name"""
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT id, manager_id, status,
                   payload->'client'->>'name' as client_name,
                   payload->'site'->>'company' as company_name,
                   payload->'site'->>'business_type' as business_type,
                   result_url, created_at
            FROM requests
            WHERE
                payload->'site'->>'company' ILIKE %s OR
                payload->'client'->>'name' ILIKE %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (f"%{query}%", f"%{query}%", limit)
        )
        return cur.fetchall()

