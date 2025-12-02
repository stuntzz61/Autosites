# -*- coding: utf-8 -*-
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
from app.utils import build_request_payload
from app.config import DB_URL
import db_queries as Q

log = logging.getLogger("bot")


def get_db():
    return psycopg.connect(DB_URL, row_factory=dict_row, autocommit=True)


def init_db():
    log.info("Postgres mode; DB_URL is configured")


# ==================== USERS / ROLES ====================

def get_user_by_tgid(tg_id: int):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_USER_BY_TGID, (tg_id,))
        return cur.fetchone()


def get_user_by_id(uid: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_USER_BY_ID, (uid,))
        return cur.fetchone()


def create_user(tg_id: int, first_name: str, last_name: str, contact: str) -> Optional[str]:
    """Создаёт пользователя со статусом pending"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.CREATE_USER, (tg_id, first_name, last_name, contact))
        row = cur.fetchone()
        return str(row["id"]) if row else None


def get_mode(tg_id: int) -> str:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_MODE, (tg_id,))
        row = cur.fetchone()
        return row["role"] if row and row.get("role") else "guest"


def set_mode(tg_id: int, mode: str):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.SET_MODE, (tg_id, mode))


# ==================== APPROVAL SYSTEM ====================

def is_user_approved(tg_id: int) -> bool:
    """Проверить, одобрен ли пользователь"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.IS_USER_APPROVED, (tg_id,))
            row = cur.fetchone()
            return row["is_approved"] if row else False
    except Exception:
        return False


def get_user_approval_status(tg_id: int) -> str:
    """Получить статус одобрения пользователя"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.GET_USER_APPROVAL_STATUS, (tg_id,))
            row = cur.fetchone()
            return row["approval_status"] if row else "unknown"
    except Exception:
        return "unknown"


def list_pending_registrations() -> List[dict]:
    """Список ожидающих одобрения"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.LIST_PENDING_REGISTRATIONS)
        return cur.fetchall()


def count_pending_registrations() -> int:
    """Количество ожидающих одобрения"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.COUNT_PENDING_REGISTRATIONS)
        row = cur.fetchone()
        return row["n"] if row else 0


def approve_user(user_id: str, approved_by_id: str) -> bool:
    """Одобрить регистрацию пользователя"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.APPROVE_USER, (approved_by_id, user_id))
            return cur.rowcount > 0
    except Exception:
        return False


def reject_user(user_id: str, approved_by_id: str, reason: str = None) -> bool:
    """Отклонить регистрацию пользователя"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.REJECT_USER, (reason, approved_by_id, user_id))
            return cur.rowcount > 0
    except Exception:
        return False


def delete_user(user_id: str) -> bool:
    """Удалить пользователя"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.DELETE_USER, (user_id,))
            return cur.rowcount > 0
    except Exception:
        return False


def update_user(user_id: str, first_name: str = None, last_name: str = None, contact: str = None) -> bool:
    """Обновить данные пользователя"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.UPDATE_USER, (first_name, last_name, contact, user_id))
            return cur.rowcount > 0
    except Exception:
        return False


# ==================== ADMIN NOTIFICATIONS ====================

def create_admin_notification(notification_type: str, title: str, message: str = None,
                              entity_type: str = None, entity_id: str = None) -> Optional[str]:
    """Создать уведомление для админа"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.CREATE_ADMIN_NOTIFICATION, (
                notification_type, title, message, entity_type, entity_id
            ))
            row = cur.fetchone()
            return str(row["id"]) if row else None
    except Exception:
        return None


def get_unread_notifications(limit: int = 20) -> List[dict]:
    """Получить непрочитанные уведомления"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_UNREAD_NOTIFICATIONS, (limit,))
        return cur.fetchall()


def count_unread_notifications() -> int:
    """Количество непрочитанных уведомлений"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.COUNT_UNREAD_NOTIFICATIONS)
        row = cur.fetchone()
        return row["n"] if row else 0


def mark_notification_read(notification_id: str) -> bool:
    """Отметить уведомление как прочитанное"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.MARK_NOTIFICATION_READ, (notification_id,))
            return True
    except Exception:
        return False


def mark_all_notifications_read() -> bool:
    """Отметить все уведомления как прочитанные"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.MARK_ALL_NOTIFICATIONS_READ)
            return True
    except Exception:
        return False


# ==================== SYSTEM SETTINGS ====================

def get_setting(key: str) -> Optional[str]:
    """Получить системную настройку"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_SETTING, (key,))
        row = cur.fetchone()
        return row["value"] if row else None


def set_setting(key: str, value: str) -> bool:
    """Установить системную настройку"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.SET_SETTING, (key, value))
            return True
    except Exception:
        return False


# ==================== MANAGER SETTINGS ====================

def is_manager_blocked(tg_id: int) -> bool:
    """Проверить, заблокирован ли менеджер"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.IS_MANAGER_BLOCKED, (tg_id,))
            row = cur.fetchone()
            return row["is_blocked"] if row else False
    except Exception:
        return False


def block_manager(user_id: str, blocked_by_id: str, reason: str = None):
    """Заблокировать менеджера"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPSERT_MANAGER_SETTINGS, (
            user_id, True, reason, datetime.now(), blocked_by_id, None
        ))


def unblock_manager(user_id: str):
    """Разблокировать менеджера"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPSERT_MANAGER_SETTINGS, (
            user_id, False, None, None, None, None
        ))


def get_manager_settings(user_id: str) -> Optional[dict]:
    """Получить настройки менеджера"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_MANAGER_SETTINGS, (user_id,))
        return cur.fetchone()


# ==================== REQUESTS LISTING ====================

def list_manager_requests(tg_id: int, offset=0, limit=10):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.LIST_MANAGER_REQUESTS, (tg_id, limit, offset))
        return cur.fetchall()


def list_manager_archive(tg_id: int, offset=0, limit=10):
    """Список архивных заявок менеджера"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.LIST_MANAGER_ARCHIVE, (tg_id, limit, offset))
        return cur.fetchall()


def count_manager_requests(tg_id: int) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.COUNT_MANAGER_REQUESTS, (tg_id,))
        row = cur.fetchone()
        return row["n"] if row else 0


def count_manager_archive(tg_id: int) -> int:
    """Количество архивных заявок"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.COUNT_MANAGER_ARCHIVE, (tg_id,))
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


# ==================== HELPERS ====================

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


# ==================== CURRENT REQUEST POINTER ====================

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
        cur.execute("SELECT 1 FROM requests WHERE id = %s::uuid", (req_id,))
        if cur.fetchone():
            return req_id
        cur.execute("DELETE FROM current_request WHERE tgid = %s", (tgid,))
        return None


def clear_current_request_id_by_tgid(tgid: int) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM current_request WHERE tgid = %s", (tgid,))


# ==================== REQUESTS CRUD ====================

def get_request(req_id: str):
    """Возвращает агрегат с manager_id и полями из payload_json"""
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
            "result_url": row.get("result_url"),
            "generation_started_at": row.get("generation_started_at"),
            "generation_completed_at": row.get("generation_completed_at"),
            "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            "site_params_json": json.dumps(row.get("payload_json") or {}, ensure_ascii=False),
        }


def create_request_by_tgid(tgid: int, payload: dict, *, initial_status: str = "awaiting_photos") -> str:
    """Создаёт заявку"""
    manager_uuid = _get_user_uuid_by_tgid(tgid)
    if not manager_uuid:
        raise RuntimeError("Пользователь не зарегистрирован")

    site = (payload.get("site") or {})
    title = site.get("company") or "Проект"
    project_id = _get_or_create_project_for_manager(manager_uuid, title)

    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            Q.INSERT_REQUEST,
            (project_id, json.dumps(payload, ensure_ascii=False), initial_status),
        )
        req_id = cur.fetchone()["id"]

    set_current_request_id_by_tgid(tgid, req_id)

    # Логируем создание
    log_activity(manager_uuid, "request_created", "request", req_id, {"company": title})

    return req_id


def update_request_site_json(req_id: str, site_obj: Dict[str, Any]):
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_SITE_JSON, (json.dumps(site_obj, ensure_ascii=False), req_id))


def get_request_payload(request_id: str) -> Dict[str, Any]:
    """Возвращает payload заявки как dict"""
    rec = get_request(request_id)
    if not rec:
        return {}
    return build_request_payload(rec)


def save_request_payload(req_id: str, payload: Dict[str, Any]) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_PAYLOAD, (json.dumps(payload, ensure_ascii=False), req_id))


def append_images_to_request(req_id: str, images: List[Dict[str, Any]]) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.APPEND_IMAGES_JSONB, (json.dumps(images, ensure_ascii=False), req_id))


# ==================== REQUEST STATUS ====================

def set_request_status(request_id: str, status: str) -> None:
    """Записывает статус в payload_json.site.meta.status"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_STATUS, (status, request_id))


def mark_generation_started(request_id: str) -> None:
    """Отметить начало генерации"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_GENERATION_STARTED, (request_id,))


def mark_generation_complete(request_id: str, result_url: str = None) -> None:
    """Отметить успешное завершение генерации"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_GENERATION_COMPLETE, (result_url, request_id))


def mark_generation_error(request_id: str, error_message: str = None) -> None:
    """Отметить ошибку генерации"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.UPDATE_REQUEST_GENERATION_ERROR, (error_message, request_id))


def archive_request(request_id: str, archived_by_id: str, reason: str = "completed") -> bool:
    """Архивировать заявку"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            # Сохраняем в архив
            cur.execute(Q.ARCHIVE_REQUEST, (reason, archived_by_id, request_id))
            # Обновляем статус
            cur.execute(Q.UPDATE_REQUEST_STATUS, ("archived", request_id))
            return True
    except Exception as e:
        log.exception("Failed to archive request")
        return False


def delete_request(req_id: str, manager_id: Optional[str] = None) -> bool:
    """Удаляет заявку"""
    with get_db() as conn, conn.cursor() as cur:
        if manager_id is None:
            cur.execute("DELETE FROM requests WHERE id = %s::uuid", (req_id,))
        else:
            cur.execute(Q.DELETE_REQUEST_WITH_MANAGER, (req_id, manager_id))
        ok = cur.rowcount > 0
    return ok


# ==================== ACTIVITY LOG ====================

def log_activity(user_id: str, action: str, entity_type: str = None,
                 entity_id: str = None, details: dict = None) -> None:
    """Записать действие в лог"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.INSERT_ACTIVITY_LOG, (
                user_id, action, entity_type, entity_id,
                json.dumps(details or {}, ensure_ascii=False)
            ))
    except Exception:
        pass  # Не ломаем основную логику из-за логов


def get_activity_log(offset: int = 0, limit: int = 50) -> List[dict]:
    """Получить лог активности"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_ACTIVITY_LOG, (limit, offset))
        return cur.fetchall()


def get_user_activity(user_id: str, limit: int = 20) -> List[dict]:
    """Получить активность пользователя"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_USER_ACTIVITY_LOG, (user_id, limit))
        return cur.fetchall()


# ==================== STATISTICS ====================

def get_overall_stats() -> dict:
    """Общая статистика"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.GET_OVERALL_STATS)
            row = cur.fetchone()
            return dict(row) if row else {}
    except Exception:
        # Fallback если функция не создана
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.ADMIN_PANEL_USERS_COUNT)
            users = cur.fetchone()["n"]
            cur.execute(Q.ADMIN_PANEL_REQUESTS_COUNT)
            requests = cur.fetchone()["n"]
            cur.execute(Q.ADMIN_PANEL_MANAGERS_COUNT)
            managers = cur.fetchone()["n"]
            return {
                "total_users": users,
                "total_managers": managers,
                "total_requests": requests,
            }


def get_manager_stats(manager_id: str) -> dict:
    """Статистика менеджера"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.GET_MANAGER_STATS, (manager_id,))
            row = cur.fetchone()
            return dict(row) if row else {}
    except Exception:
        return {}


def get_requests_by_status() -> List[dict]:
    """Заявки по статусам"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_REQUESTS_BY_STATUS)
        return cur.fetchall()


def get_manager_leaderboard(limit: int = 10) -> List[dict]:
    """Топ менеджеров"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_MANAGER_LEADERBOARD, (limit,))
        return cur.fetchall()


def get_requests_this_week() -> List[dict]:
    """Заявки за неделю по дням"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_REQUESTS_THIS_WEEK)
        return cur.fetchall()


# ==================== ADMIN ====================

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


def admin_managers():
    """Список менеджеров с их статистикой"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_MANAGERS_SELECT)
        return cur.fetchall()


def admin_export_all():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.ADMIN_EXPORT_ALL_SELECT)
        return cur.fetchall()


# ==================== BROADCAST ====================

def get_all_active_managers() -> List[dict]:
    """Список всех активных (одобренных и незаблокированных) менеджеров"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_ALL_ACTIVE_MANAGERS)
        return cur.fetchall()


def get_managers_by_ids(ids: List[str]) -> List[dict]:
    """Получить менеджеров по списку ID"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_MANAGERS_BY_IDS, (ids,))
        return cur.fetchall()


# ==================== SEARCH ====================

def search_requests(query: str, limit: int = 20) -> List[dict]:
    """Поиск заявок по названию/клиенту"""
    pattern = f"%{query}%"
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.SEARCH_REQUESTS, (pattern, pattern, pattern, pattern, limit))
        return cur.fetchall()


# ==================== MASS OPERATIONS ====================

def mass_archive_requests(ids: List[str]) -> int:
    """Массовое архивирование заявок"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.MASS_ARCHIVE_REQUESTS, (ids,))
            return cur.rowcount
    except Exception:
        return 0


def mass_delete_requests(ids: List[str]) -> int:
    """Массовое удаление заявок"""
    try:
        with get_db() as conn, conn.cursor() as cur:
            cur.execute(Q.MASS_DELETE_REQUESTS, (ids,))
            return cur.rowcount
    except Exception:
        return 0


# ==================== EXPORT ====================

def get_stats_for_export(days: int = 30) -> List[dict]:
    """Статистика по дням для экспорта"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_STATS_FOR_EXPORT.replace('%s', str(days)))
        return cur.fetchall()


def get_managers_stats_for_export() -> List[dict]:
    """Статистика менеджеров для экспорта"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(Q.GET_MANAGERS_STATS_FOR_EXPORT)
        return cur.fetchall()
