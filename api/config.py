from pydantic_settings import BaseSettings
from typing import List, Optional
from functools import lru_cache


class Settings(BaseSettings):
    # API
    API_PORT: int = 8000
    DEBUG: bool = False

    # Database - supports both DATABASE_URL and DB_URL
    DATABASE_URL: Optional[str] = None
    DB_URL: Optional[str] = None

    # Telegram
    BOT_TOKEN: str = ""

    # Owner IDs (comma-separated list of Telegram IDs for Owner role)
    # Note: Owner role can only be set via config, not via UI
    OWNER_IDS: str = ""

    # Director IDs (comma-separated list of Telegram IDs for Director role)
    # Note: Used only for initial setup. After setup, directors are managed by owner via UI
    DIRECTOR_IDS: str = ""

    # Supervisor IDs (comma-separated list of Telegram IDs for Supervisor role)
    # Note: Used only for initial setup. After setup, supervisors are managed by owner/director via UI
    SUPERVISOR_IDS: str = ""

    # Legacy: Admin IDs (for backward compatibility, maps to supervisor)
    ADMIN_IDS: str = ""

    # S3
    S3_ENDPOINT: str = ""
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_BUCKET: str = "autosites"
    S3_REGION: str = "ru-central1"
    S3_PUBLIC_URL: str = ""

    # Admin password
    ADMIN_PASSWORD: str = "admin123"

    # n8n webhooks
    N8N_WEBHOOK_URL: str = ""  # Main webhook for site generation (standard)
    N8N_PREMIUM_WEBHOOK_URL: str = ""  # Premium webhook for advanced generation
    N8N_REVISIONS_WEBHOOK_URL: str = ""  # Webhook for revision processing

    # Bot webhook (for notifications)
    BOT_WEBHOOK_URL: str = ""

    # Deploy Node
    DEPLOY_NODE_URL: str = ""  # e.g. https://deploy.autosites.ru
    AUTO_DEPLOY_ENABLED: bool = True  # Auto-deploy after generation (default: enabled)

    # Preview Domain для автоматических поддоменов
    PREVIEW_DOMAIN: str = ""  # e.g. autosites.ru или wenlix.ru

    # Callback secret for secure webhooks
    DEPLOY_CALLBACK_SECRET: str = ""

    # API Public URL (for callbacks from n8n)
    API_PUBLIC_URL: str = ""  # e.g. https://api.autosites.ru

    # CMS Service Integration (для client-editor-ui)
    CMS_SERVICE_URL: str = ""  # e.g. http://cms-service:8090
    AUTH_SERVICE_URL: str = ""  # e.g. http://auth-service:8087
    AUTH_SERVICE_ADMIN_SECRET: str = ""  # Secret for internal API calls

    # Payment Settings
    PAYMENT_BASE_URL: str = ""  # URL for payment system (e.g. https://pay.example.com)
    PAYMENT_BANK_ACCOUNT: str = ""  # Bank account for SBP payments

    # Reviews Digger (self-hosted service for Yandex Maps reviews scraping)
    # URL of reviews-digger service (uses browser automation, no API keys needed)
    REVIEWS_DIGGER_URL: str = ""  # e.g. http://reviews-digger:8083

    # Legacy: Apify (deprecated, use REVIEWS_DIGGER_URL instead)
    APIFY_API_TOKEN: str = ""  # Get from https://console.apify.com/account/integrations

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    @property
    def database_url(self) -> str:
        """Get database URL from either DATABASE_URL or DB_URL"""
        return self.DATABASE_URL or self.DB_URL or ""

    @property
    def owner_tg_ids(self) -> List[int]:
        """Parse OWNER_IDS into list of integers"""
        if not self.OWNER_IDS:
            return []
        return [int(x.strip()) for x in self.OWNER_IDS.split(',') if x.strip().isdigit()]

    @property
    def director_tg_ids(self) -> List[int]:
        """Parse DIRECTOR_IDS into list of integers"""
        if not self.DIRECTOR_IDS:
            return []
        return [int(x.strip()) for x in self.DIRECTOR_IDS.split(',') if x.strip().isdigit()]

    @property
    def supervisor_tg_ids(self) -> List[int]:
        """Parse SUPERVISOR_IDS into list of integers"""
        ids = []
        if self.SUPERVISOR_IDS:
            ids.extend([int(x.strip()) for x in self.SUPERVISOR_IDS.split(',') if x.strip().isdigit()])
        # Legacy: also include ADMIN_IDS for backward compatibility
        if self.ADMIN_IDS:
            ids.extend([int(x.strip()) for x in self.ADMIN_IDS.split(',') if x.strip().isdigit()])
        return ids

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

