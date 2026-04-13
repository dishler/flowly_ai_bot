from typing import Optional

import redis

from app.core.config import get_settings


class RedisClientProvider:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: Optional[redis.Redis] = None

    def get_client(self) -> Optional[redis.Redis]:
        if not self.settings.redis_enabled:
            return None

        if self._client is not None:
            return self._client

        try:
            client = redis.Redis.from_url(
                self.settings.redis_url,
                decode_responses=True,
            )
            client.ping()
            self._client = client
            return self._client
        except redis.RedisError:
            return None
            