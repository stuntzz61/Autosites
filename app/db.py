# -*- coding: utf-8 -*-
import json
import logging
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.rows import dict_row
from app.utils import build_request_payload
from app.config import DB_URL
import db_queries as Q  # SQL-константы
from app.constants import (
    STATUS_DRAFT, STATUS_COLLECTING_INFO, STATUS_COLLECTING_PHOTOS,
    STATUS_READY_TO_GENERATE, STATUS_QUEUED,
)

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


# --- Requests listing (менеджер/админ) ---

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


# --- Helpers: users / projects ---

def _get_user_uuid_by_tgid(tgid: int) -> Optional[str]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.SELECT_USER_ID_BY_TGID, (tgid,))
        row = cur.fetchone()
        return row["id"] if row else None


def _get_or_create_project_for_manager(manager_id: str, title: str) -> str:
    """Ищем последний draft/active проект менеджера; иначе создаём новый."""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM projects
            WHERE manager_id = %s
              AND status IN ('draft','active')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (manager_id,),
        )
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(Q.INSERT_PROJECT_RETURNING_ID, (manager_id, title or "Новый проект"))
        return cur.fetchone()["id"]


# --- Current request pointer (current_request) ---

def set_current_request_id_by_tgid(tgid: int, req_id: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO current_request (tgid, request_id)
            VALUES (%s, %s)
            ON CONFLICT (tgid) DO UPDATE SET request_id = EXCLUDED.request_id
            """,
            (tgid, req_id),
        )


def get_current_request_id_by_tgid(tgid: int) -> Optional[str]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT request_id FROM current_request WHERE tgid = %s", (tgid,))
        row = cur.fetchone()
        if not row:
            return None
        req_id = row["request_id"]
        # валидация: указатель не должен указывать на удалённую
        cur.execute("SELECT 1 FROM requests WHERE id = %s::uuid", (req_id,))
        if cur.fetchone():
            return req_id
        # мусор — подчистим
        cur.execute("DELETE FROM current_request WHERE tgid = %s", (tgid,))
        return None


def clear_current_request_id_by_tgid(tgid: int) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM current_request WHERE tgid = %s", (tgid,))


# --- Requests CRUD / payload ---

def get_request(req_id: str):
    """Возвращает агрегат с manager_id и полями из payload_json (обратная совместимость)."""
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


def create_request_by_tgid(tgid: int, payload: dict, *, initial_status: str = "awaiting_photos") -> str:
    """
    Создаёт заявку:
      1) находим users.id по tg_id,
      2) находим/создаём projects.id для manager_id,
      3) вставляем requests(project_id uuid, payload_json jsonb, status),
      4) делаем её «текущей» в current_request.
    """
    manager_uuid = _get_user_uuid_by_tgid(tgid)
    if not manager_uuid:
        raise RuntimeError("Пользователь не зарегистрирован (нет users.tg_id)")

    site = (payload.get("site") or {})
    title = site.get("company") or "Проект"
    project_id = _get_or_create_project_for_manager(manager_uuid, title)

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            Q.INSERT_REQUEST,  # (project_id, payload_json, status) RETURNING id
            (project_id, json.dumps(payload, ensure_ascii=False), initial_status),
        )
        req_id = cur.fetchone()["id"]

    set_current_request_id_by_tgid(tgid, req_id)
    return req_id


def update_request_site_json(req_id: str, site_obj: Dict[str, Any]):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_SITE_JSON, (json.dumps(site_obj, ensure_ascii=False), req_id))


def get_request_payload(request_id: str) -> Dict[str, Any]:
    """
    Возвращает payload заявки как dict (client/site/…).
    """
    rec = get_request(request_id)
    if not rec:
        return {}
    return build_request_payload(rec)

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


def set_request_status(request_id: str, status: str) -> None:
    """
    Записывает статус в payload_json.site.meta.status через update_request_site_json.
    """
    rec = get_request(request_id)
    if not rec:
        return

    # Текущий payload и текущее site-содержимое
    payload = build_request_payload(rec)
    site = payload.get("site") or {}
    meta = site.get("meta") or {}
    meta["status"] = status
    site["meta"] = meta

    # Важно: update_request_site_json ДОЛЖЕН заменять ветку site целиком.
    update_request_site_json(request_id, site)
    
def delete_request(req_id: str, manager_id: Optional[str] = None) -> bool:
    """
    Жёстко удаляет заявку. current_request чистится автоматически (FK ON DELETE CASCADE).
    Если передан manager_id — удаление только если заявка принадлежит проекту этого менеджера.
    """
    with get_db() as conn, conn.cursor() as cur:
        if manager_id is None:
            cur.execute("DELETE FROM requests WHERE id = %s::uuid", (req_id,))
        else:
            cur.execute(
                Q.DELETE_REQUEST_WITH_MANAGER,
                (req_id, manager_id),
            )
        ok = cur.rowcount > 0
    return ok


# --- Admin helpers ---

def admin_counts():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_PANEL_USERS_COUNT)
        users_count = cur.fetchone()["n"]
        cur.execute(Q.ADMIN_PANEL_REQUESTS_COUNT)
        reqs_count = cur.fetchone()["n"]
    return users_count, reqs_count


def admin_users():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_USERS_SELECT)
        return cur.fetchall()


def admin_export_all():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_EXPORT_ALL_SELECT)
        return cur.fetchall()
