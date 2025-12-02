from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # API
    API_PORT: int = 8000
    DEBUG: bool = False

    # Database
    DATABASE_URL: str

    # Telegram
    BOT_TOKEN: str

    # S3
    S3_ENDPOINT: str = ""
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_BUCKET: str = "autosites"
    S3_REGION: str = "us-east-1"

    # n8n webhook
    N8N_WEBHOOK_URL: str = ""

    # CORS
    CORS_ORIGINS: List[str] = ["*"]

    class Config:
        env_file = ".env"


settings = Settings()

