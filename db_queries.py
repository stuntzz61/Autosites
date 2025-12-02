# -*- coding: utf-8 -*-
import json

# ==================== USERS ====================

GET_USER_BY_TGID = "SELECT * FROM users WHERE tg_id=%s"
GET_USER_BY_ID = "SELECT * FROM users WHERE id=%s::uuid"

CREATE_USER = """
INSERT INTO users (tg_id, role, first_name, last_name, contact)
VALUES (%s, 'manager', %s, %s, %s)
ON CONFLICT (tg_id) DO UPDATE
SET first_name=EXCLUDED.first_name,
    last_name=EXCLUDED.last_name,
    contact=EXCLUDED.contact
"""

GET_MODE = "SELECT role FROM users WHERE tg_id=%s"
SET_MODE = """
INSERT INTO users (tg_id, role) VALUES (%s, %s)
ON CONFLICT (tg_id) DO UPDATE SET role=EXCLUDED.role
"""

SELECT_USER_ID_BY_TGID = "SELECT id FROM users WHERE tg_id=%s"

# ==================== MANAGER SETTINGS ====================

GET_MANAGER_SETTINGS = """
SELECT ms.*, u.first_name, u.last_name, u.contact, u.tg_id, u.created_at as user_created
FROM manager_settings ms
JOIN users u ON u.id = ms.user_id
WHERE ms.user_id = %s::uuid
"""

UPSERT_MANAGER_SETTINGS = """
INSERT INTO manager_settings (user_id, is_blocked, block_reason, blocked_at, blocked_by, notes)
VALUES (%s::uuid, %s, %s, %s, %s::uuid, %s)
ON CONFLICT (user_id) DO UPDATE SET
    is_blocked = EXCLUDED.is_blocked,
    block_reason = EXCLUDED.block_reason,
    blocked_at = EXCLUDED.blocked_at,
    blocked_by = EXCLUDED.blocked_by,
    notes = COALESCE(EXCLUDED.notes, manager_settings.notes)
"""

IS_MANAGER_BLOCKED = """
SELECT COALESCE(is_blocked, false) as is_blocked
FROM manager_settings
WHERE user_id = (SELECT id FROM users WHERE tg_id = %s)
"""

# ==================== REQUESTS LISTING ====================

LIST_MANAGER_REQUESTS = """
SELECT r.id,
       COALESCE(r.payload_json->'client'->>'name', '') AS client_name,
       COALESCE(r.payload_json->'site'->>'company', '') AS company_name,
       COALESCE(r.payload_json->'site'->>'business_type', '') AS business_type,
       COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) AS status,
       r.created_at,
       r.generation_completed_at,
       r.result_url
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
  AND COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) NOT IN ('archived', 'cancelled')
ORDER BY r.created_at DESC
LIMIT %s OFFSET %s
"""

LIST_MANAGER_ARCHIVE = """
SELECT r.id,
       COALESCE(r.payload_json->'client'->>'name', '') AS client_name,
       COALESCE(r.payload_json->'site'->>'company', '') AS company_name,
       COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) AS status,
       r.created_at,
       r.generation_completed_at
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
  AND COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) IN ('archived', 'closed', 'cancelled', 'delivered')
ORDER BY r.created_at DESC
LIMIT %s OFFSET %s
"""

COUNT_MANAGER_REQUESTS = """
SELECT COUNT(*) AS n
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
  AND COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) NOT IN ('archived', 'cancelled')
"""

COUNT_MANAGER_ARCHIVE = """
SELECT COUNT(*) AS n
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
  AND COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) IN ('archived', 'closed', 'cancelled', 'delivered')
"""

LIST_ALL_REQUESTS = """
SELECT r.id, p.manager_id,
       COALESCE(r.payload_json->'client'->>'name', '') AS client_name,
       COALESCE(r.payload_json->'site'->>'company', '') AS company_name,
       COALESCE(r.payload_json->'site'->>'business_type', '') AS business_type,
       COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) AS status,
       r.created_at,
       u.first_name as manager_first_name,
       u.last_name as manager_last_name
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
ORDER BY r.created_at DESC
LIMIT %s OFFSET %s
"""

COUNT_ALL_REQUESTS = "SELECT COUNT(*) AS n FROM requests"

# ==================== REQUEST AGGREGATE ====================

GET_REQUEST = """
SELECT r.*,
       p.manager_id,
       (r.payload_json->'client'->>'name')    AS client_name,
       (r.payload_json->'client'->>'company') AS client_company,
       (r.payload_json->'client'->>'contact') AS client_contact,
       (r.payload_json->'site')               AS site_json
FROM requests r
JOIN projects p ON p.id = r.project_id
WHERE r.id = %s::uuid
"""

# ==================== PROJECTS ====================

INSERT_PROJECT_RETURNING_ID = """
INSERT INTO projects (manager_id, title, status)
VALUES (%s, %s, 'draft')
RETURNING id
"""

# ==================== REQUESTS CRUD ====================

INSERT_REQUEST = """
INSERT INTO requests (project_id, payload_json, status)
VALUES (%s, %s::jsonb, %s)
RETURNING id
"""

UPDATE_REQUEST_SITE_JSON = """
UPDATE requests
SET payload_json = jsonb_set(payload_json, '{site}', %s::jsonb, true)
WHERE id = %s::uuid
"""

UPDATE_REQUEST_STATUS = """
UPDATE requests
SET payload_json = jsonb_set(
    COALESCE(payload_json, '{}'::jsonb),
    '{site,meta,status}',
    to_jsonb(%s::text),
    true
)
WHERE id = %s::uuid
"""

UPDATE_REQUEST_GENERATION_STARTED = """
UPDATE requests
SET generation_started_at = NOW(),
    payload_json = jsonb_set(
        COALESCE(payload_json, '{}'::jsonb),
        '{site,meta,status}',
        '"generating"'::jsonb,
        true
    )
WHERE id = %s::uuid
"""

UPDATE_REQUEST_GENERATION_COMPLETE = """
UPDATE requests
SET generation_completed_at = NOW(),
    result_url = %s,
    payload_json = jsonb_set(
        COALESCE(payload_json, '{}'::jsonb),
        '{site,meta,status}',
        '"generated_ok"'::jsonb,
        true
    )
WHERE id = %s::uuid
"""

UPDATE_REQUEST_GENERATION_ERROR = """
UPDATE requests
SET generation_completed_at = NOW(),
    error_message = %s,
    payload_json = jsonb_set(
        COALESCE(payload_json, '{}'::jsonb),
        '{site,meta,status}',
        '"generated_error"'::jsonb,
        true
    )
WHERE id = %s::uuid
"""

DELETE_REQUEST_SIMPLE = "DELETE FROM requests WHERE id=%s::uuid"

DELETE_REQUEST_WITH_MANAGER = """
DELETE FROM requests r USING projects p
WHERE r.id=%s::uuid AND r.project_id=p.id AND p.manager_id=%s::uuid
"""

# ==================== ACTIVITY LOG ====================

INSERT_ACTIVITY_LOG = """
INSERT INTO activity_log (user_id, action, entity_type, entity_id, details)
VALUES (%s::uuid, %s, %s, %s::uuid, %s::jsonb)
"""

GET_ACTIVITY_LOG = """
SELECT al.*, u.first_name, u.last_name, u.tg_id
FROM activity_log al
LEFT JOIN users u ON u.id = al.user_id
ORDER BY al.created_at DESC
LIMIT %s OFFSET %s
"""

GET_USER_ACTIVITY_LOG = """
SELECT al.*, u.first_name, u.last_name
FROM activity_log al
LEFT JOIN users u ON u.id = al.user_id
WHERE al.user_id = %s::uuid
ORDER BY al.created_at DESC
LIMIT %s
"""

# ==================== STATISTICS ====================

GET_OVERALL_STATS = "SELECT * FROM get_overall_stats()"

GET_MANAGER_STATS = "SELECT * FROM get_manager_stats(%s::uuid)"

GET_TOP_MANAGERS = """
SELECT * FROM top_managers
ORDER BY total_requests DESC
LIMIT %s
"""

GET_REQUESTS_BY_STATUS = """
SELECT
    COALESCE(payload_json->'site'->'meta'->>'status', status) as status,
    COUNT(*) as count
FROM requests
GROUP BY COALESCE(payload_json->'site'->'meta'->>'status', status)
ORDER BY count DESC
"""

GET_REQUESTS_TODAY = """
SELECT COUNT(*) as count
FROM requests
WHERE created_at >= CURRENT_DATE
"""

GET_REQUESTS_THIS_WEEK = """
SELECT
    DATE(created_at) as date,
    COUNT(*) as count
FROM requests
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(created_at)
ORDER BY date
"""

GET_REQUESTS_THIS_MONTH = """
SELECT
    DATE(created_at) as date,
    COUNT(*) as count
FROM requests
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(created_at)
ORDER BY date
"""

GET_MANAGER_LEADERBOARD = """
SELECT
    u.id,
    u.tg_id,
    u.first_name,
    u.last_name,
    COUNT(r.id) as total_requests,
    COUNT(r.id) FILTER (WHERE COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) = 'generated_ok') as completed,
    COUNT(r.id) FILTER (WHERE r.created_at >= CURRENT_DATE - INTERVAL '7 days') as this_week,
    COUNT(r.id) FILTER (WHERE r.created_at >= CURRENT_DATE) as today
FROM users u
LEFT JOIN projects p ON p.manager_id = u.id
LEFT JOIN requests r ON r.project_id = p.id
WHERE u.role = 'manager'
GROUP BY u.id, u.tg_id, u.first_name, u.last_name
ORDER BY total_requests DESC
LIMIT %s
"""

# ==================== ADMIN ====================

ADMIN_PANEL_USERS_COUNT = "SELECT COUNT(*) AS n FROM users"
ADMIN_PANEL_REQUESTS_COUNT = "SELECT COUNT(*) AS n FROM requests"
ADMIN_PANEL_MANAGERS_COUNT = "SELECT COUNT(*) AS n FROM users WHERE role = 'manager'"

ADMIN_USERS_SELECT = """
SELECT id, first_name, last_name, contact, created_at, tg_id, role
FROM users
ORDER BY created_at DESC
"""

ADMIN_MANAGERS_SELECT = """
SELECT
    u.id, u.first_name, u.last_name, u.contact, u.created_at, u.tg_id,
    COALESCE(ms.is_blocked, false) as is_blocked,
    ms.block_reason,
    COUNT(r.id) as total_requests,
    COUNT(r.id) FILTER (WHERE COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) = 'generated_ok') as completed_requests
FROM users u
LEFT JOIN manager_settings ms ON ms.user_id = u.id
LEFT JOIN projects p ON p.manager_id = u.id
LEFT JOIN requests r ON r.project_id = p.id
WHERE u.role = 'manager'
GROUP BY u.id, u.first_name, u.last_name, u.contact, u.created_at, u.tg_id, ms.is_blocked, ms.block_reason
ORDER BY total_requests DESC
"""

ADMIN_EXPORT_ALL_SELECT = """
SELECT r.id, p.manager_id,
       r.payload_json->'client' AS client,
       r.payload_json->'site'   AS site,
       r.status, r.created_at
FROM requests r
JOIN projects p ON p.id = r.project_id
ORDER BY r.created_at DESC
"""

# ==================== PAYLOAD ====================

GET_REQUEST_PAYLOAD = "SELECT payload_json FROM requests WHERE id = %s::uuid"

UPDATE_REQUEST_PAYLOAD = """
UPDATE requests
SET payload_json = %s::jsonb
WHERE id = %s::uuid
"""

APPEND_IMAGES_JSONB = """
UPDATE requests
SET payload_json =
  jsonb_set(
    jsonb_set(
      jsonb_set(
        payload_json,
        '{site}', COALESCE(payload_json->'site','{}')::jsonb,
        true
      ),
      '{site,assets}', COALESCE(payload_json->'site'->'assets','{}')::jsonb,
      true
    ),
    '{site,assets,images}',
    COALESCE(payload_json->'site'->'assets'->'images','[]'::jsonb) || %s::jsonb,
    true
  )
WHERE id = %s::uuid
"""

# ==================== ARCHIVE ====================

ARCHIVE_REQUEST = """
INSERT INTO request_archive (
    id, original_request_id, project_id, manager_id,
    payload_json, status, result_url, archive_reason,
    archived_by, original_created_at
)
SELECT
    gen_random_uuid(), r.id, r.project_id, p.manager_id,
    r.payload_json,
    COALESCE(r.payload_json->'site'->'meta'->>'status', r.status),
    r.result_url, %s, %s::uuid, r.created_at
FROM requests r
JOIN projects p ON p.id = r.project_id
WHERE r.id = %s::uuid
"""

GET_ARCHIVED_REQUESTS = """
SELECT * FROM request_archive
WHERE manager_id = %s::uuid
ORDER BY archived_at DESC
LIMIT %s OFFSET %s
"""

# ==================== HELPERS ====================

GET_LATEST_REQUEST_ID_BY_TGID = """
SELECT r.id
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
  AND COALESCE(r.payload_json->'site'->'meta'->>'status', r.status) IN ('draft','awaiting_photos','ready','queued','generating')
ORDER BY r.created_at DESC
LIMIT 1
"""

def as_json_str(obj):
    return json.dumps(obj, ensure_ascii=False)
