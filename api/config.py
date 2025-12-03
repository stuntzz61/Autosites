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

    # Admin IDs (comma-separated list of Telegram IDs)
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

    # n8n webhook
    N8N_WEBHOOK_URL: str = ""

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    @property
    def database_url(self) -> str:
        """Get database URL from either DATABASE_URL or DB_URL"""
        return self.DATABASE_URL or self.DB_URL or ""

    @property
    def admin_tg_ids(self) -> List[int]:
        """Parse ADMIN_IDS into list of integers"""
        if not self.ADMIN_IDS:
            return []
        return [int(x.strip()) for x in self.ADMIN_IDS.split(',') if x.strip().isdigit()]

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

