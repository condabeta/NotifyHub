from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: Literal["local", "docker", "test"] = "local"
    log_level: str = "INFO"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "notifyhub"
    postgres_password: str = "notifyhub"
    postgres_db: str = "notifyhub"

    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "notifyhub"
    rabbitmq_password: str = "notifyhub"
    rabbitmq_vhost: str = "/"

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    idempotency_ttl_seconds: int = 60 * 60 * 24

    notifications_queue: str = "notifyhub.notifications"
    notifications_max_priority: int = 10

    worker_prefetch: int = Field(default=16, ge=1)
    worker_max_attempts: int = Field(default=5, ge=1)
    worker_retry_base_delay_ms: int = 500
    worker_retry_max_delay_ms: int = 30_000

    provider_transient_fail_rate: float = Field(default=0.1, ge=0.0, le=1.0)
    provider_permanent_fail_rate: float = Field(default=0.02, ge=0.0, le=1.0)
    provider_delivery_delay_ms: int = 200

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_sync_dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def rabbitmq_dsn(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
            f"@{self.rabbitmq_host}:{self.rabbitmq_port}{self.rabbitmq_vhost}"
        )

    @property
    def redis_dsn(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
