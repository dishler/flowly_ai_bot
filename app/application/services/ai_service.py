from typing import Any, Dict, List

from app.infrastructure.openai.client import OpenAIClient


class AIService:
    def __init__(self, openai_client: OpenAIClient) -> None:
        self.openai_client = openai_client

    def try_generate_reply(
        self,
        user_message: str,
        history: List[str],
    ) -> Dict[str, Any]:
        return self.openai_client.generate_reply(
            user_message=user_message,
            history=history,
        )