from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

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


@router.post("/meta")
async def receive_meta_webhook(request: Request):
    payload = await request.json()
    # тут залиш свою поточну логіку обробки POST
    return {"status": "accepted"}