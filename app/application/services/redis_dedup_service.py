from typing import Optional

import redis

from app.core.config import get_settings


class RedisDedupService:
    def __init__(self, redis_client: Optional[redis.Redis]) -> None:
        self.redis_client = redis_client
        self.settings = get_settings()

    def is_duplicate(self, message_mid: str) -> bool:
        if self.redis_client is None:
            return False

        key = self._build_key(message_mid)
        return bool(self.redis_client.exists(key))

    def mark_processed(self, message_mid: str) -> None:
        if self.redis_client is None:
            return

        key = self._build_key(message_mid)
        self.redis_client.set(
            key,
            "1",
            ex=600,
        )

    @staticmethod
    def _build_key(message_mid: str) -> str:
        return f"processed_message:{message_mid}"
        