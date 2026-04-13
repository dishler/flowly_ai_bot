from typing import Any, Optional
import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from app.application.dto.normalized_message import NormalizedMessage
from app.core.config import settings

router = APIRouter(prefix="/webhooks", tags=["meta"])


@router.get("/meta", response_class=PlainTextResponse)
async def verify_meta_webhook(
    hub_mode: str = Query(..., alias="hub.mode"),
    hub_verify_token: str = Query(..., alias="hub.verify_token"),
    hub_challenge: str = Query(..., alias="hub.challenge"),
):
    if hub_mode != "subscribe":
        raise HTTPException(status_code=400, detail="Invalid hub mode")

    if hub_verify_token != settings.meta_verify_token:
        raise HTTPException(status_code=403, detail="Invalid verify token")

    return PlainTextResponse(content=hub_challenge, status_code=200)


def _parse_facebook(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    entries = payload.get("entry", [])
    if not entries:
        return None

    entry = entries[0]
    messaging = entry.get("messaging", [])
    if not messaging:
        return None

    event = messaging[0]
    message = event.get("message", {})
    text = message.get("text")
    mid = message.get("mid")

    sender_id = event.get("sender", {}).get("id")
    recipient_id = event.get("recipient", {}).get("id")
    timestamp = event.get("timestamp")

    if not text or not sender_id or not recipient_id or not mid:
        return None

    return NormalizedMessage(
        platform="facebook",
        sender_id=str(sender_id),
        recipient_id=str(recipient_id),
        message_mid=str(mid),
        user_message=str(text),
        timestamp=timestamp,
    )


def _parse_instagram_changes_style(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    entries = payload.get("entry", [])
    if not entries:
        return None

    entry = entries[0]
    changes = entry.get("changes", [])
    if not changes:
        return None

    change = changes[0]
    value = change.get("value", {})
    messages = value.get("messages", [])
    if not messages:
        return None

    message = messages[0]
    text = message.get("text", {}).get("body")
    mid = message.get("id")
    sender_id = message.get("from")
    recipient_id = value.get("metadata", {}).get(
        "phone_number_id", "instagram-recipient"
    )
    timestamp_raw = message.get("timestamp")

    if not text or not sender_id or not mid:
        return None

    timestamp = None
    if timestamp_raw is not None:
        try:
            timestamp = int(timestamp_raw)
        except (TypeError, ValueError):
            timestamp = None

    return NormalizedMessage(
        platform="instagram",
        sender_id=str(sender_id),
        recipient_id=str(recipient_id),
        message_mid=str(mid),
        user_message=str(text),
        timestamp=timestamp,
    )


def _parse_instagram_messaging_style(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    entries = payload.get("entry", [])
    if not entries:
        return None

    entry = entries[0]
    messaging = entry.get("messaging", [])
    if not messaging:
        return None

    event = messaging[0]
    message = event.get("message", {})
    text = message.get("text")
    mid = message.get("mid")

    sender_id = event.get("sender", {}).get("id")
    recipient_id = event.get("recipient", {}).get("id")
    timestamp = event.get("timestamp")

    if not text or not sender_id or not recipient_id or not mid:
        return None

    return NormalizedMessage(
        platform="instagram",
        sender_id=str(sender_id),
        recipient_id=str(recipient_id),
        message_mid=str(mid),
        user_message=str(text),
        timestamp=timestamp,
    )


def _parse_meta_payload(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    parsed = _parse_instagram_messaging_style(payload)
    if parsed is not None:
        return parsed

    parsed = _parse_instagram_changes_style(payload)
    if parsed is not None:
        return parsed

    parsed = _parse_facebook(payload)
    if parsed is not None:
        return parsed

    return None


@router.post("/meta")
async def receive_meta_webhook(request: Request):
    payload = await request.json()

    print("=== META WEBHOOK PAYLOAD START ===")
    print(json.dumps(payload, ensure_ascii=False))
    print("=== META WEBHOOK PAYLOAD END ===")

    message = _parse_meta_payload(payload)
    print(f"PARSED_MESSAGE={message}")

    if message is None:
        return {"status": "ignored"}

    result = request.app.state.message_processor.process(message)
    print(f"PROCESS_RESULT={json.dumps(result, ensure_ascii=False, default=str)}")

    return {
        "status": "processed",
        "platform": message.platform,
        "message_mid": message.message_mid,
        "outbound_result": result.get("outbound_result"),
    }
