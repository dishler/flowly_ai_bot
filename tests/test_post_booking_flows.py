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


class RecordingCalendarService(CalendarService):
    def __init__(self) -> None:
        super().__init__()
        self.deleted_event_ids = []

    def delete_event(self, event_id: str) -> None:
        self.deleted_event_ids.append(event_id)


class FailingCalendarService(CalendarService):
    def delete_event(self, event_id: str) -> None:
        raise RuntimeError("delete failed")


@pytest.fixture
def processor_factory():
    def build(transcript: str = "", calendar_service: CalendarService | None = None):
        memory_service = MemoryService()
        booking_service = BookingService(
            calendar_service=calendar_service or CalendarService(),
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


def _mark_confirmed(booking_service: BookingService, event_id: str | None = None) -> None:
    booking_service._mark_booking_completed(
        "user-1",
        start_dt=datetime(2026, 4, 27, 12, 0),
        email="client@example.com",
        phone=None,
        calendar_event_id=event_id,
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
            "завтра о 12:00 або 15:00",
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
            "завтра о 12:00 або 15:00",
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


async def test_cancel_confirmed_booking_deletes_calendar_event(processor_factory):
    calendar_service = RecordingCalendarService()
    processor, booking_service = processor_factory(calendar_service=calendar_service)
    _mark_confirmed(booking_service, event_id="calendar-event-123")

    result = await processor.process(_message(text="Скасуйте дзвінок"))

    assert result["intent"] == "booking_cancel"
    assert result["reply_text"] == (
        "Добре, я скасував ваш дзвінок. Якщо буде актуально — можемо запланувати інший час."
    )
    assert calendar_service.deleted_event_ids == ["calendar-event-123"]
    assert not booking_service.has_confirmed_booking("user-1")


async def test_cancel_confirmed_booking_hands_off_when_calendar_delete_fails(processor_factory):
    processor, booking_service = processor_factory(calendar_service=FailingCalendarService())
    _mark_confirmed(booking_service, event_id="calendar-event-123")

    result = await processor.process(_message(text="Скасуйте дзвінок"))

    assert result["intent"] == "booking_cancel"
    assert result["booking_result"]["status"] == "cancel_handoff"
    assert result["reply_text"] == "Добре, я передам спеціалісту, щоб дзвінок скасували."
    assert booking_service.has_confirmed_booking("user-1")


async def test_availability_suggests_slots_and_day_followup(processor_factory):
    processor, _ = processor_factory()

    result = await processor.process(_message(text="коли є вільні слоти?"))

    assert result["intent"] == "booking_availability_question"
    assert "завтра о 12:00 або 15:00" in result["reply_text"]
    assert "післязавтра о 11:00 або 16:00" in result["reply_text"]

    result = await processor.process(_message(text="завтра"))

    assert result["intent"] == "booking_flow"
    assert "завтра можемо запропонувати о 12:00 або 15:00" in result["reply_text"]
    assert result["routing_category"] != "escalate_to_human"


async def test_availability_datetime_followup_asks_for_contact(processor_factory):
    processor, _ = processor_factory()
    await processor.process(_message(text="коли є вільні слоти?"))

    result = await processor.process(_message(text="завтра о 15"))

    assert result["intent"] == "booking_flow"
    assert "о 15:00 вільний" in result["reply_text"]
    assert "номер телефону або email" in result["reply_text"]


async def test_availability_time_only_followup_uses_context(processor_factory):
    processor, _ = processor_factory()
    await processor.process(_message(text="коли є вільні слоти?"))

    result = await processor.process(_message(text="15"))

    assert result["intent"] == "booking_flow"
    assert "о 15:00 вільний" in result["reply_text"]
    assert "номер телефону або email" in result["reply_text"]


async def test_empty_voice_transcription_uses_audio_retry_reply(processor_factory):
    processor, _ = processor_factory(transcript="")

    result = await processor.process(_message(audio_url="https://example.test/audio.ogg"))

    assert result["intent"] == "unrecognized_audio"
    assert result["reply_text"] == (
        "Не вдалося розпізнати аудіо. Напишіть, будь ласка, повідомлення текстом "
        "або надішліть голосове ще раз."
    )


@pytest.mark.parametrize(
    ("text", "reply"),
    [
        ("ок", "Дякую, зафіксував."),
        ("добре", "Добре, дякую."),
        ("давай", "Добре, підкажіть, будь ласка, що саме вам зручно обговорити?"),
        ("можливо потім", "Добре, без проблем. Якщо буде актуально — напишіть нам у будь-який момент."),
        ("я подумаю", "Звісно, подумайте. Якщо виникнуть питання — напишіть, я підкажу."),
        ("це не питання це пропозиція", "Дякую за повідомлення. Наш спеціаліст зв’яжеться з вами найближчим часом."),
    ],
)
async def test_short_contextual_replies_do_not_use_complex_fallback(processor_factory, text, reply):
    processor, _ = processor_factory()

    result = await processor.process(_message(text=text))

    assert result["reply_text"] == reply
    assert result["routing_category"] != "escalate_to_human"


async def test_business_details_after_price_prompt_call_not_slots(processor_factory):
    processor, _ = processor_factory()

    price_result = await processor.process(_message(text="Скільки коштує бот?"))
    detail_result = await processor.process(_message(text="у мене СТО, треба відповідати клієнтам"))

    assert "Можемо коротко обговорити ваш кейс на дзвінку" in price_result["reply_text"]
    assert "Можу зорієнтувати точніше під ваш кейс" not in price_result["reply_text"]
    assert detail_result["intent"] == "price_followup_case_details"
    assert detail_result["reply_text"] == (
        "Зрозумів, дякую за деталі. У вашому випадку краще коротко обговорити "
        "на дзвінку, щоб підібрати оптимальне рішення. Підкажіть, будь ласка, "
        "коли вам буде зручно?"
    )
    assert "Можемо запропонувати кілька варіантів" not in detail_result["reply_text"]
