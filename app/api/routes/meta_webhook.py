from __future__ import annotations

import json
import logging
from typing import Any, Optional

import httpx
from fastapi.encoders import jsonable_encoder
from fastapi import APIRouter, HTTPException, Query, Request

from app.application.dto.normalized_message import NormalizedMessage
from app.core.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()


def _safe_get(data: Any, *keys: Any) -> Any:
    """Safely walk nested dict/list structures."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int):
            if 0 <= key < len(current):
                current = current[key]
            else:
                return None
        else:
            return None
        if current is None:
            return None
    return current


def _extract_text(payload: dict[str, Any]) -> str:
    """Extract text from common Messenger / Instagram webhook shapes."""
    candidates = [
        _safe_get(payload, "entry", 0, "messaging", 0, "message", "text"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "text", "body"),
        _safe_get(payload, "message", "text"),
        _safe_get(payload, "text"),
    ]

    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def _extract_audio_url(payload: dict[str, Any]) -> Optional[str]:
    """
    Extract audio URL from likely Meta webhook shapes.

    Depending on the integration, audio may appear as:
    - attachments on Messenger
    - audio object in WhatsApp-like Meta payloads
    - nested media/url fields
    """
    candidates = [
        _safe_get(payload, "entry", 0, "messaging", 0, "message", "attachments", 0, "payload", "url"),
        _safe_get(payload, "entry", 0, "messaging", 0, "message", "attachments", 0, "url"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "audio", "url"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "voice", "url"),
        _safe_get(payload, "audio", "url"),
        _safe_get(payload, "voice", "url"),
        _safe_get(payload, "media", "url"),
    ]

    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


def _extract_audio_media_id(payload: dict[str, Any]) -> Optional[str]:
    candidates = [
        _safe_get(payload, "entry", 0, "messaging", 0, "message", "attachments", 0, "payload", "id"),
        _safe_get(payload, "entry", 0, "messaging", 0, "message", "attachments", 0, "target", "id"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "audio", "id"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "voice", "id"),
        _safe_get(payload, "audio", "id"),
        _safe_get(payload, "voice", "id"),
    ]

    for value in candidates:
        if value is not None and str(value).strip():
            return str(value).strip()

    return None


def get_media_url(media_id: str) -> str:
    if not media_id:
        return ""

    if not settings.meta_page_access_token:
        logger.warning("Cannot resolve media URL: META_PAGE_ACCESS_TOKEN is empty")
        return ""

    url = f"https://graph.facebook.com/{settings.meta_graph_api_version}/{media_id}"
    params = {
        "fields": "url",
        "access_token": settings.meta_page_access_token,
    }

    try:
        with httpx.Client(timeout=15.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        logger.exception("Failed to resolve media URL for media_id=%s: %s", media_id, exc)
        return ""

    resolved_url = data.get("url")
    if isinstance(resolved_url, str) and resolved_url.strip():
        return resolved_url.strip()

    logger.warning("Graph API did not return media url for media_id=%s", media_id)
    return ""


def _extract_sender_id(payload: dict[str, Any]) -> str:
    """Extract sender/user id from common Meta webhook shapes."""
    candidates = [
        _safe_get(payload, "entry", 0, "messaging", 0, "sender", "id"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "from"),
        _safe_get(payload, "sender_id"),
        _safe_get(payload, "from"),
    ]

    for value in candidates:
        if value is not None:
            return str(value)

    return "unknown"


def _extract_recipient_id(payload: dict[str, Any]) -> str:
    candidates = [
        _safe_get(payload, "entry", 0, "messaging", 0, "recipient", "id"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "metadata", "display_phone_number"),
        _safe_get(payload, "recipient_id"),
        _safe_get(payload, "to"),
    ]

    for value in candidates:
        if value is not None:
            return str(value)

    return ""


def _extract_message_mid(payload: dict[str, Any]) -> str:
    candidates = [
        _safe_get(payload, "entry", 0, "messaging", 0, "message", "mid"),
        _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0, "id"),
        _safe_get(payload, "message_mid"),
        _safe_get(payload, "id"),
    ]

    for value in candidates:
        if value is not None:
            return str(value)

    return ""


def _extract_platform(payload: dict[str, Any]) -> str:
    if _safe_get(payload, "entry", 0, "messaging", 0) is not None:
        return "facebook"

    if _safe_get(payload, "entry", 0, "changes", 0, "value", "messages", 0) is not None:
        return "instagram"

    return "facebook"


def _build_normalized_message(payload: dict[str, Any]) -> NormalizedMessage:
    user_message = _extract_text(payload)
    audio_url = _extract_audio_url(payload)
    if not audio_url:
        media_id = _extract_audio_media_id(payload)
        if media_id:
            audio_url = get_media_url(media_id)
            logger.info("Resolved audio URL from media_id=%s: %s", media_id, bool(audio_url))
    sender_id = _extract_sender_id(payload)
    recipient_id = _extract_recipient_id(payload)
    message_mid = _extract_message_mid(payload)
    platform = _extract_platform(payload)

    return NormalizedMessage(
        platform=platform,
        sender_id=sender_id,
        recipient_id=recipient_id,
        message_mid=message_mid,
        user_message=user_message,
        audio_url=audio_url,
    )


@router.get("/meta")
async def verify_meta_webhook(
    hub_mode: Optional[str] = Query(default=None, alias="hub.mode"),
    hub_verify_token: Optional[str] = Query(default=None, alias="hub.verify_token"),
    hub_challenge: Optional[str] = Query(default=None, alias="hub.challenge"),
    request: Request = None,
) -> str:
    """
    Meta webhook verification endpoint.
    Returns the challenge when the verify token matches.
    """
    verify_token = getattr(request.app.state, "meta_verify_token", None)

    if hub_mode == "subscribe" and hub_verify_token == verify_token and hub_challenge:
        return hub_challenge

    raise HTTPException(status_code=403, detail="Webhook verification failed")


@router.post("/meta")
async def receive_meta_webhook(request: Request) -> dict[str, Any]:
    """
    Main Meta webhook receiver.
    - extracts text
    - extracts audio_url
    - builds NormalizedMessage
    - passes message into async MessageProcessor
    """
    try:
        payload = await request.json()
    except Exception as exc:
        logger.exception("Invalid webhook JSON payload")
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {exc}") from exc

    logger.info("VOICE BUILD ACTIVE")
    logger.info("FULL META PAYLOAD: %s", json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    logger.info("Meta webhook payload received: %s", jsonable_encoder(payload))

    message_processor = getattr(request.app.state, "message_processor", None)
    if message_processor is None:
        raise HTTPException(status_code=500, detail="message_processor is not configured")

    try:
        message = _build_normalized_message(payload)
    except Exception as exc:
        logger.exception("Failed to build NormalizedMessage from payload")
        return {
            "status": "error",
            "reason": "failed_to_normalize_message",
            "detail": str(exc),
        }

    logger.info(
        "NormalizedMessage created: sender_id=%s platform=%s has_text=%s has_audio=%s",
        message.sender_id,
        message.platform,
        bool(message.user_message),
        bool(message.audio_url),
    )
    logger.info("audio_url resolved: %s", message.audio_url)

    if not message.user_message and not message.audio_url:
        return {
            "status": "ignored",
            "reason": "No text or audio found in payload",
        }

    logger.info("Entering message_processor for sender_id=%s", message.sender_id)
    try:
        result = await message_processor.process(message)
    except Exception as exc:
        logger.exception("message_processor.process failed for sender_id=%s", message.sender_id)
        return {
            "status": "error",
            "reason": "message_processing_failed",
            "detail": str(exc),
        }
    logger.info("message_processor.process completed for sender_id=%s", message.sender_id)
    if message.audio_url:
        logger.info("transcription result: %s", message.user_message)

    safe_result = jsonable_encoder(result)

    return {
        "status": "ok",
        "normalized_message": {
            "platform": message.platform,
            "sender_id": message.sender_id,
            "recipient_id": message.recipient_id,
            "message_mid": message.message_mid,
            "user_message": message.user_message,
            "audio_url": message.audio_url,
        },
        "result": safe_result,
    }