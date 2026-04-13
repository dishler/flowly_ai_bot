from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="Flowly Meta Bot")
    environment: str = Field(default="dev")
    debug: bool = Field(default=True)

    meta_verify_token: str = Field(default="change-me")
    meta_app_secret: str = Field(default="")
    meta_page_access_token: str = Field(default="")
    meta_graph_api_version: str = Field(default="v21.0")
    meta_send_enabled: bool = Field(default=False)

    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-5-mini")
    openai_enabled: bool = Field(default=False)

    redis_enabled: bool = Field(default=False)
    redis_url: str = Field(default="redis://localhost:6379/0")
    redis_message_ttl_seconds: int = Field(default=60 * 60 * 24)
    redis_memory_ttl_seconds: int = Field(default=60 * 60 * 24 * 7)
    redis_booking_confirmation_ttl_seconds: int = Field(default=60 * 60)

    google_calendar_enabled: bool = Field(default=False)
    google_calendar_id: str = Field(default="")
    google_service_account_file: str = Field(default="")
    google_calendar_timezone: str = Field(default="Europe/Kyiv")

    default_timezone: str = Field(default="Europe/Kyiv")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()