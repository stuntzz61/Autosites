"""
Application configuration
"""
from typing import List
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings"""

    # App
    DEBUG: bool = False
    SECRET_KEY: str = "your-secret-key-change-in-production"

    # Database
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/autosites"

    # Telegram
    BOT_TOKEN: str = ""

    # S3
    S3_ENDPOINT: str = ""
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_BUCKET: str = "autosites"
    S3_PUBLIC_URL: str = ""

    # N8N Webhook
    N8N_WEBHOOK_URL: str = ""

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    # Admin
    ADMIN_TG_IDS: List[int] = []

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

