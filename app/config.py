import os
import logging
from dotenv import load_dotenv

load_dotenv()

def init_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("TG_BOT_TOKEN is required")

N8N_GEN_WEBHOOK = os.getenv("N8N_GEN_WEBHOOK", "").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")

# ID чатов администраторов для уведомлений (через запятую)
ADMIN_CHAT_IDS = os.getenv("ADMIN_CHAT_IDS", "").strip()

def get_admin_chat_ids() -> list:
    """Получить список ID чатов админов для уведомлений"""
    if not ADMIN_CHAT_IDS:
        return []
    try:
        return [int(x.strip()) for x in ADMIN_CHAT_IDS.split(",") if x.strip().isdigit()]
    except Exception:
        return []

def _build_db_url_from_env() -> str:
    user = os.getenv("DB_USER") or os.getenv("PG_USER") or os.getenv("POSTGRES_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("DB_HOST") or os.getenv("PG_HOST", "postgres")
    port = os.getenv("DB_PORT") or os.getenv("PG_PORT", "5432")
    db = os.getenv("DB_NAME") or os.getenv("PG_DB") or os.getenv("POSTGRES_DB") or "app_db"
    return f"postgresql://{user}:{password}@{host}:{port}/{db}" if user and password else ""

DB_URL = os.getenv("DB_URL", "").strip() or _build_db_url_from_env()
if not DB_URL:
    raise SystemExit("No Postgres connection info. Set DB_URL or PG_USER/PG_PASSWORD/etc.")
