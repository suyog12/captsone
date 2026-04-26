from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Project root (two levels up from this file: backend/config.py -> backend -> root)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    """All runtime configuration in one object."""

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Postgres connection
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "recommendation_dashboard"
    postgres_user: str = "postgres"
    postgres_password: str = Field(default="", description="Set this in .env")
    postgres_schema: str = "recdash"

    # Parquet file paths (relative to project root)
    parquet_precomputed_dir: str = "data_clean/serving/precomputed"
    parquet_merged_file: str = "data_clean/serving/merged_dataset.parquet"

    # JWT settings (used when we add auth in Phase 4)
    jwt_secret_key: str = "change-me-in-production-please-this-is-only-for-local-dev"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 60 * 24  # 24 hours, capstone-friendly default

    # API settings
    api_title: str = "McKesson Recommendation Dashboard API"
    api_version: str = "0.1.0"
    api_debug: bool = True

    # CORS - which origins can call the API. Localhost ports cover Vite (5173)
    # and Create React App (3000) defaults. Add to this list if your frontend
    # ends up on a different port.
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]

    # Derived values
    @property
    def async_database_url(self) -> str:
        """asyncpg connection URL for FastAPI."""
        from urllib.parse import quote_plus
        pwd = quote_plus(self.postgres_password)
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{pwd}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def project_root(self) -> Path:
        return PROJECT_ROOT

    @property
    def precomputed_dir(self) -> Path:
        return PROJECT_ROOT / self.parquet_precomputed_dir

    @property
    def merged_file(self) -> Path:
        return PROJECT_ROOT / self.parquet_merged_file


@lru_cache
def get_settings() -> Settings:
    """
    Cached settings accessor. Cached so the .env file is parsed only once
    per process startup, not on every request.
    """
    return Settings()


# Module-level convenience: most code can `from backend.config import settings`.
settings = get_settings()
