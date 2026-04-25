from __future__ import annotations

import os
from datetime import datetime

import pytest

os.environ.setdefault("DEBUG", "false")

from app.application.dto.normalized_message import NormalizedMessage
from app.application.services.booking_service import BookingService
from app.application.services.calendar_service import CalendarService
from app.application.services.intent_service import IntentService
from app.application.services.language_service import LanguageService
from app.application.services.memory_service import MemoryService
from app.application.services.message_processor import MessageProcessor
from app.application.services.reply_service import ReplyService


class DummyDedupService:
    def __init__(self) -> None:
        self.processed = set()

    def is_duplicate(self, message_mid: str) -> bool:
        return message_mid in self.processed

    def mark_processed(self, message_mid: str) -> None:
        self.processed.add(message_mid)


class DummyOutboundService:
    def __init__(self) -> None:
        self.sent = []

    def send_reply(self, platform: str, recipient_id: str, text: str) -> dict:
        self.sent.append(
            {
                "platform": platform,
                "recipient_id": recipient_id,
                "text": text,
            }
        )
        return {"sent": True}


class DummySpeechService:
    def __init__(self, transcript: str = "") -> None:
        self.transcript = transcript

    async def transcribe_audio(self, file_url: str) -> str:
        return self.transcript


@pytest.fixture
def processor_factory():
    def build(transcript: str = ""):
        memory_service = MemoryService()
        booking_service = BookingService(
            calendar_service=CalendarService(),
            language_service=LanguageService(),
        )
        reply_service = ReplyService(
            ai_service=None,
            memory_service=memory_service,
            knowledge_service=None,
        )
        outbound_service = DummyOutboundService()
        processor = MessageProcessor(
            memory_service=memory_service,
            reply_service=reply_service,
            outbound_service=outbound_service,
            dedup_service=DummyDedupService(),
            intent_service=IntentService(),
            booking_service=booking_service,
            speech_service=DummySpeechService(transcript),
        )
        return processor, booking_service

    return build


def _message(text: str = "", audio_url: str | None = None) -> NormalizedMessage:
    return NormalizedMessage(
        platform="instagram",
        sender_id="user-1",
        recipient_id="bot-1",
        message_mid="",
        user_message=text,
        audio_url=audio_url,
    )


def _mark_confirmed(booking_service: BookingService) -> None:
    booking_service._mark_booking_completed(
        "user-1",
        start_dt=datetime(2026, 4, 27, 12, 0),
        email="client@example.com",
        phone=None,
    )


@pytest.mark.parametrize(
    ("text", "expected_intent", "reply_part"),
    [
        (
            "а що саме буде на дзвінку?",
            "booking_call_explanation",
            "На дзвінку ми коротко розберемо ваш кейс",
        ),
        (
            "Давайте перенесемо на 15 в понеділок",
            "booking_reschedule",
            "15:00",
        ),
        (
            "коли є вільні слоти?",
            "booking_availability_question",
            "на який день вам зручно",
        ),
        (
            "Скасуйте дзвінок",
            "booking_cancel",
            "скасував ваш дзвінок",
        ),
    ],
)
async def test_confirmed_booking_text_flows(processor_factory, text, expected_intent, reply_part):
    processor, booking_service = processor_factory()
    _mark_confirmed(booking_service)

    result = await processor.process(_message(text=text))

    assert result["intent"] == expected_intent
    assert reply_part in result["reply_text"]
    assert result["routing_category"] != "safe_handoff"
    assert result["routing_category"] != "escalate_to_human"


@pytest.mark.parametrize(
    ("transcript", "expected_intent", "reply_part"),
    [
        (
            "а що саме буде на дзвінку?",
            "booking_call_explanation",
            "На дзвінку ми коротко розберемо ваш кейс",
        ),
        (
            "Давайте перенесемо на 15 в понеділок",
            "booking_reschedule",
            "15:00",
        ),
        (
            "коли є вільні слоти?",
            "booking_availability_question",
            "на який день вам зручно",
        ),
        (
            "Скасуйте дзвінок",
            "booking_cancel",
            "скасував ваш дзвінок",
        ),
    ],
)
async def test_confirmed_booking_voice_flows(processor_factory, transcript, expected_intent, reply_part):
    processor, booking_service = processor_factory(transcript)
    _mark_confirmed(booking_service)

    result = await processor.process(_message(audio_url="https://example.test/audio.ogg"))

    assert result["intent"] == expected_intent
    assert reply_part in result["reply_text"]
    assert result["routing_category"] != "safe_handoff"
    assert result["routing_category"] != "escalate_to_human"
