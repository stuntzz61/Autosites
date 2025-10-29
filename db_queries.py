# -*- coding: utf-8 -*-
import json

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

# --- Requests listing (привязка к manager_id) ---

LIST_MANAGER_REQUESTS = """
SELECT r.id,
       COALESCE(r.payload_json->'client'->>'name','Без имени') AS client_name,
       r.status, r.created_at
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
ORDER BY r.created_at DESC
LIMIT %s OFFSET %s
"""

COUNT_MANAGER_REQUESTS = """
SELECT COUNT(*) AS n
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
"""

LIST_ALL_REQUESTS = """
SELECT r.id, p.manager_id,
       COALESCE(r.payload_json->'client'->>'name','Без имени') AS client_name,
       r.status, r.created_at
FROM requests r
JOIN projects p ON p.id = r.project_id
ORDER BY r.created_at DESC
LIMIT %s OFFSET %s
"""

COUNT_ALL_REQUESTS = "SELECT COUNT(*) AS n FROM requests"

# --- Request aggregate ---

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

# --- Users / Projects ---

SELECT_USER_ID_BY_TGID = "SELECT id FROM users WHERE tg_id=%s"

INSERT_PROJECT_RETURNING_ID = """
INSERT INTO projects (manager_id, title, status)
VALUES (%s, %s, 'draft')
RETURNING id
"""

# --- Requests insert/update ---

# ВАЖНО: теперь 3 параметра и RETURNING id
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

DELETE_REQUEST_SIMPLE = "DELETE FROM requests WHERE id=%s::uuid"

DELETE_REQUEST_WITH_MANAGER = """
DELETE FROM requests r USING projects p
WHERE r.id=%s::uuid AND r.project_id=p.id AND p.manager_id=%s::uuid
"""

# --- Admin ---

ADMIN_PANEL_USERS_COUNT = "SELECT COUNT(*) AS n FROM users"
ADMIN_PANEL_REQUESTS_COUNT = "SELECT COUNT(*) AS n FROM requests"

ADMIN_USERS_SELECT = """
SELECT id, first_name, last_name, contact, created_at, tg_id, role
FROM users
ORDER BY created_at DESC
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

# Последняя «активная» заявка менеджера по tg_id (обновлён список статусов)
GET_LATEST_REQUEST_ID_BY_TGID = """
SELECT r.id
FROM requests r
JOIN projects p ON p.id = r.project_id
JOIN users u ON u.id = p.manager_id
WHERE u.tg_id = %s
  AND r.status IN ('draft','awaiting_photos','ready','queued','generating')
ORDER BY r.created_at DESC
LIMIT 1
"""

# Прочитать полный payload_json заявки
GET_REQUEST_PAYLOAD = "SELECT payload_json FROM requests WHERE id = %s::uuid"

# Полностью сохранить payload_json заявки
UPDATE_REQUEST_PAYLOAD = """
UPDATE requests
SET payload_json = %s::jsonb
WHERE id = %s::uuid
"""

# Атомарно дописать массив картинок в JSON
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

def as_json_str(obj):
    return json.dumps(obj, ensure_ascii=False)
