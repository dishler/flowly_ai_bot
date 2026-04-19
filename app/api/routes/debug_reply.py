from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.application.dto.normalized_message import NormalizedMessage

router = APIRouter(tags=["debug-reply"])


class DebugReplyRequest(BaseModel):
    sender_id: str
    message_text: str


@router.post("/debug/reply")
async def debug_reply(payload: DebugReplyRequest, request: Request):
    normalized_message = NormalizedMessage(
        platform="debug",
        sender_id=payload.sender_id,
        recipient_id="debug-local",
        message_mid="debug-local",
        user_message=payload.message_text,
    )
    reply_text = request.app.state.reply_service.generate_reply(normalized_message)
    return {"reply_text": reply_text}
