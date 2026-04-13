from fastapi import APIRouter, HTTPException, Query, Request

from app.core.config import get_settings
from app.infrastructure.meta.filters import is_meaningful_message
from app.infrastructure.meta.parser import MetaPayloadParser

router = APIRouter(prefix="/webhooks/meta", tags=["meta-webhook"])


@router.get("")
async def verify_meta_webhook(
    hub_mode: str = Query(alias="hub.mode"),
    hub_verify_token: str = Query(alias="hub.verify_token"),
    hub_challenge: str = Query(alias="hub.challenge"),
) -> str:
    settings = get_settings()

    if hub_mode != "subscribe":
        raise HTTPException(status_code=400, detail="Invalid hub.mode")

    if hub_verify_token != settings.meta_verify_token:
        raise HTTPException(status_code=403, detail="Invalid verify token")

    return hub_challenge


@router.post("")
async def receive_meta_webhook(request: Request) -> dict:
    payload = await request.json()

    normalized = MetaPayloadParser.parse(payload)

    if normalized is None:
        return {
            "status": "ignored",
            "reason": "Unsupported or invalid payload",
        }

    if not is_meaningful_message(normalized):
        return {
            "status": "ignored",
            "reason": "Empty message",
        }

    dedup_service = request.app.state.dedup_service

    print("DEBUG message_mid:", normalized.message_mid)
    print("DEBUG is_duplicate_before:", dedup_service.is_duplicate(normalized.message_mid))

    if dedup_service.is_duplicate(normalized.message_mid):
        return {
            "status": "ignored",
            "reason": "Duplicate message",
            "message_mid": normalized.message_mid,
        }

    dedup_service.mark_processed(normalized.message_mid)

    print("DEBUG is_duplicate_after:", dedup_service.is_duplicate(normalized.message_mid))

    processor = request.app.state.message_processor
    result = processor.process(normalized)

    print("DEBUG processor result:", result)

    return {
        "status": "accepted",
        "normalized_message": normalized.model_dump(),
        "processor_result": result,
    }