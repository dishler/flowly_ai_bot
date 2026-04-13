from typing import Optional

from pydantic import BaseModel


class NormalizedMessage(BaseModel):
    platform: str
    sender_id: str
    recipient_id: str
    message_mid: str
    user_message: str
    timestamp: Optional[int] = None