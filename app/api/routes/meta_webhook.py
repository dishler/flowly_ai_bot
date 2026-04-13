from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Request, Response, status
from fastapi.responses import JSONResponse, PlainTextResponse

from app.core.config import settings
from app.schemas.normalized_message import NormalizedMessage

router = APIRouter()
logger = logging.getLogger(__name__)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_text_from_message(message: dict[str, Any]) -> str:
    text = _safe_str(message.get("text"))
    if text:
        return text

    text_obj = message.get("text")
    if isinstance(text_obj, dict):
        body = _safe_str(text_obj.get("body"))
        if body:
            return body

    return ""


def _is_echo_or_self_message(event: dict[str, Any]) -> bool:
    message = event.get("message") or {}

    if message.get("is_echo") is True:
        return True

    sender_id = _safe_str(event.get("sender", {}).get("id"))
    recipient_id = _safe_str(event.get("recipient", {}).get("id"))

    if sender_id and recipient_id and sender_id == recipient_id:
        return True

    return False


def _parse_facebook(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    for entry in payload.get("entry", []):
        for event in entry.get("messaging", []):
            if _is_echo_or_self_message(event):
                continue

            message = event.get("message") or {}
            text = _extract_text_from_message(message)

            sender_id = event.get("sender", {}).get("id")
            recipient_id = event.get("recipient", {}).get("id")
            timestamp = event.get("timestamp")
            mid = message.get("mid")

            if not text or not sender_id or not recipient_id or not mid:
                continue

            return NormalizedMessage(
                platform="facebook",
                sender_id=str(sender_id),
                recipient_id=str(recipient_id),
                message_mid=str(mid),
                user_message=str(text),
                timestamp=timestamp,
            )

    return None


def _parse_instagram_changes_style(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    for entry in payload.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value") or {}
            messages = value.get("messages") or []

            if not messages:
                continue

            for msg in messages:
                text_obj = msg.get("text") or {}
                text = _safe_str(text_obj.get("body")) if isinstance(text_obj, dict) else _safe_str(msg.get("text"))

                sender_id = msg.get("from")
                recipient_id = (
                    value.get("metadata", {}).get("phone_number_id")
                    or value.get("metadata", {}).get("display_phone_number")
                    or value.get("id")
                )
                timestamp = msg.get("timestamp")
                mid = msg.get("id")

                if not text or not sender_id or not recipient_id or not mid:
                    continue

                return NormalizedMessage(
                    platform="instagram",
                    sender_id=str(sender_id),
                    recipient_id=str(recipient_id),
                    message_mid=str(mid),
                    user_message=str(text),
                    timestamp=timestamp,
                )

    return None


def _parse_instagram_messaging_style(payload: dict[str, Any]) -> Optional[NormalizedMessage]:
    for entry in payload.get("entry", []):
        for event in entry.get("messaging", []):
            if _is_echo_or_self_message(event):
                continue

            message = event.get("message") or {}
            text = _extract_text_from_message(message)

            sender_id = event.get("sender", {}).get("id")
            recipient_id = event.get("recipient", {}).get("id")
            timestamp = event.get("timestamp")
            mid = message.get("mid")

            if not text or not sender_id or not recipient_id or not mid:
                continue

            return NormalizedMessage(
                platform="instagram",
                sender_id=str(sender_id),
                recipient_id=str(recipient_id),
                message_mid=str(mid),
                user_message=str(text),
                timestamp=timestamp,
            )

    return None


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


@router.get("/meta", response_class=PlainTextResponse)
async def verify_meta_webhook(request: Request) -> PlainTextResponse:
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge", "")

    if mode == "subscribe" and token == settings.meta_verify_token:
        logger.info("Meta webhook verification succeeded")
        return PlainTextResponse(content=challenge, status_code=status.HTTP_200_OK)

    logger.warning("Meta webhook verification failed")
    return PlainTextResponse(content="Verification failed", status_code=status.HTTP_403_FORBIDDEN)


@router.post("/meta")
async def receive_meta_webhook(request: Request) -> Response:
    try:
        payload = await request.json()
    except Exception:
        logger.exception("Failed to parse Meta webhook JSON payload")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "bad_request", "detail": "Invalid JSON payload"},
        )

    message = _parse_meta_payload(payload)

    if message is None:
        logger.info(
            "Meta webhook ignored: unsupported or non-message event",
            extra={"object": payload.get("object")},
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"status": "ignored"},
        )

    logger.info(
        "Meta inbound message received",
        extra={
            "platform": message.platform,
            "sender_id": message.sender_id,
            "message_mid": message.message_mid,
        },
    )

    message_processor = getattr(request.app.state, "message_processor", None)
    if message_processor is None:
        logger.error("Message processor is not initialized on app.state")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": "Message processor unavailable"},
        )

    try:
        result = message_processor.process(message)
    except Exception:
        logger.exception("Message processor failed")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": "Processing failed"},
        )

    outbound_sent = False
    outbound_result = None

    if isinstance(result, dict):
        outbound_result = result.get("outbound_result")
        if isinstance(outbound_result, dict):
            outbound_sent = bool(outbound_result.get("sent"))

    logger.info(
        "Meta message processed",
        extra={
            "platform": message.platform,
            "sender_id": message.sender_id,
            "message_mid": message.message_mid,
            "outbound_sent": outbound_sent,
        },
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "status": "processed",
            "platform": message.platform,
            "message_mid": message.message_mid,
            "outbound_sent": outbound_sent,
            "outbound_result": outbound_result,
        },
    )
