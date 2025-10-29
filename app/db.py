import json, logging
from typing import Any, Dict, List, Optional
import psycopg
from psycopg.rows import dict_row

from app.config import DB_URL
import db_queries as Q  # твой файл с SQL

log = logging.getLogger("bot")

def get_db():
    return psycopg.connect(DB_URL, row_factory=dict_row, autocommit=True)

def init_db():
    log.info("Postgres mode; DB_URL is configured")

# --- Users/roles ---
def get_user_by_tgid(tg_id: int):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_USER_BY_TGID, (tg_id,))
        return cur.fetchone()

def get_user_by_id(uid: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_USER_BY_ID, (uid,))
        return cur.fetchone()

def create_user(tg_id: int, first_name: str, last_name: str, contact: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.CREATE_USER, (tg_id, first_name, last_name, contact))

def get_mode(tg_id: int) -> str:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_MODE, (tg_id,))
        row = cur.fetchone()
        return row["role"] if row and row.get("role") else "guest"

def set_mode(tg_id: int, mode: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.SET_MODE, (tg_id, mode))

# --- Requests listing ---
def list_manager_requests(tg_id: int, offset=0, limit=10):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.LIST_MANAGER_REQUESTS, (tg_id, limit, offset))
        return cur.fetchall()

def count_manager_requests(tg_id: int) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.COUNT_MANAGER_REQUESTS, (tg_id,))
        row = cur.fetchone()
        return row["n"] if row else 0

def list_all_requests(offset=0, limit=20):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.LIST_ALL_REQUESTS, (limit, offset))
        return cur.fetchall()

def count_all_requests() -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.COUNT_ALL_REQUESTS)
        row = cur.fetchone()
        return row["n"] if row else 0

def get_request(req_id: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_REQUEST, (req_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "manager_id": row["manager_id"],
            "client_name": row.get("client_name"),
            "client_company": row.get("client_company"),
            "client_contact": row.get("client_contact"),
            "status": row.get("status"),
            "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            "site_params_json": json.dumps(row.get("payload_json") or {}, ensure_ascii=False),
        }

def create_request_by_tgid(tg_id: int, payload: Dict[str, Any]) -> str:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.SELECT_USER_ID_BY_TGID, (tg_id,))
        u = cur.fetchone()
        if not u:
            raise RuntimeError("User not found in users")
        uid = u["id"]
        title = payload.get("site",{}).get("company") or payload.get("client",{}).get("company") or "Project"
        cur.execute(Q.INSERT_PROJECT_RETURNING_ID, (uid, title))
        pid = cur.fetchone()["id"]
        cur.execute(Q.INSERT_REQUEST_RETURNING_ID, (pid, json.dumps(payload, ensure_ascii=False)))
        rid = cur.fetchone()["id"]
        return str(rid)

def update_request_site_json(req_id: str, site_obj: Dict[str, Any]):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_SITE_JSON, (json.dumps(site_obj, ensure_ascii=False), req_id))

def delete_request(req_id: str, manager_id: Optional[str] = None) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        if manager_id is None:
            cur.execute(Q.DELETE_REQUEST_SIMPLE, (req_id,))
            return cur.rowcount > 0
        cur.execute(Q.DELETE_REQUEST_WITH_MANAGER, (req_id, manager_id))
        return cur.rowcount > 0
# --- Работа с payload_json активной заявки ---

def get_current_request_id_by_tgid(tg_id: int) -> Optional[str]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_LATEST_REQUEST_ID_BY_TGID, (tg_id,))
        row = cur.fetchone()
        return str(row["id"]) if row and row.get("id") else None

def get_request_payload(req_id: str) -> Dict[str, Any]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_REQUEST_PAYLOAD, (req_id,))
        row = cur.fetchone()
        return (row["payload_json"] if row and row.get("payload_json") else {}) or {}

def save_request_payload(req_id: str, payload: Dict[str, Any]) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_PAYLOAD, (json.dumps(payload, ensure_ascii=False), req_id))

def append_images_to_request(req_id: str, images: List[Dict[str, Any]]) -> None:
    """
    images — список объектов вида:
    {url, key, name, mime, width?, height?, alt?}
    """
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.APPEND_IMAGES_JSONB, (json.dumps(images, ensure_ascii=False), req_id))

# --- Admin helpers ---
def admin_counts():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_PANEL_USERS_COUNT); users_count = cur.fetchone()["n"]
        cur.execute(Q.ADMIN_PANEL_REQUESTS_COUNT); reqs_count = cur.fetchone()["n"]
    return users_count, reqs_count

def admin_users():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_USERS_SELECT)
        return cur.fetchall()

def admin_export_all():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_EXPORT_ALL_SELECT)
        return cur.fetchall()
