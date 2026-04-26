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


class DummyConfiguredCalendarClient:
    def is_configured(self) -> bool:
        return True


class RecordingCreateCalendarService(CalendarService):
    def __init__(self) -> None:
        super().__init__(google_calendar_client=DummyConfiguredCalendarClient())
        self.created_descriptions = []
        self.created_events = []

    def check_specific_time_availability(self, start_dt, duration_minutes: int = 30) -> bool:
        return True

    def create_booking_event(
        self,
        start_dt,
        duration_minutes: int = 30,
        summary: str = "Consultation call",
        description: str = "",
        attendee_emails=None,
    ):
        self.created_descriptions.append(description)
        self.created_events.append(
            {
                "start_dt": start_dt,
                "duration_minutes": duration_minutes,
                "summary": summary,
                "description": description,
                "attendee_emails": attendee_emails or [],
            }
        )

        class CreatedEvent:
            event_id = "calendar-event-created"
            html_link = "https://calendar.example/event"
            status = "confirmed"

        return CreatedEvent()


class FailingCreateCalendarService(RecordingCreateCalendarService):
    def create_booking_event(
        self,
        start_dt,
        duration_minutes: int = 30,
        summary: str = "Consultation call",
        description: str = "",
        attendee_emails=None,
    ):
        raise RuntimeError("create failed")


class BusyThenAvailableCalendarService(CalendarService):
    def check_specific_time_availability(self, start_dt, duration_minutes: int = 30) -> bool:
        return not (start_dt.hour == 12 and start_dt.minute == 30)


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
    assert result["reply_text"] == "Добре, передам команді, щоб дзвінок скасували без зайвих дій з вашого боку."
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
    assert "ваше ім’я та номер телефону або email" in result["reply_text"]


async def test_availability_time_only_followup_uses_context(processor_factory):
    processor, _ = processor_factory()
    await processor.process(_message(text="коли є вільні слоти?"))

    result = await processor.process(_message(text="15"))

    assert result["intent"] == "booking_flow"
    assert "о 15:00 вільний" in result["reply_text"]
    assert "ваше ім’я та номер телефону або email" in result["reply_text"]


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
        (
            "можливо потім",
            "Без проблем 🙂 Якщо коротко — зазвичай це економить час на обробці заявок "
            "і допомагає не втрачати клієнтів.\n\n"
            "Якщо буде актуально — можемо швидко глянути ваш кейс і зрозуміти, "
            "чи є сенс впроваджувати.",
        ),
        (
            "я подумаю",
            "Без проблем 🙂 Якщо коротко — зазвичай це економить час на обробці заявок "
            "і допомагає не втрачати клієнтів.\n\n"
            "Якщо буде актуально — можемо швидко глянути ваш кейс і зрозуміти, "
            "чи є сенс впроваджувати.",
        ),
        ("це не питання це пропозиція", "Дякую, зафіксував. Передам це команді, щоб подивилися уважно."),
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

    assert "Вартість стартує від 200$" in price_result["reply_text"]
    assert "Можемо коротко обговорити ваш кейс на дзвінку" in price_result["reply_text"]
    assert "Можу зорієнтувати точніше під ваш кейс" not in price_result["reply_text"]
    assert detail_result["intent"] == "price_followup_case_details"
    assert detail_result["reply_text"] == (
        "Зрозумів, дякую за деталі. У вашому випадку це якраз можна "
        "автоматизувати — бот може приймати звернення, уточнювати деталі "
        "і записувати клієнтів у календар.\n\n"
        "Щоб підібрати оптимальне рішення під ваш процес, краще коротко "
        "обговорити це на дзвінку. Підкажіть, будь ласка, коли вам буде зручно?"
    )
    assert "Можемо запропонувати кілька варіантів" not in detail_result["reply_text"]


async def test_soft_followup_decline_does_not_start_booking(processor_factory):
    processor, _ = processor_factory()

    result = await processor.process(_message(text="не зараз"))

    assert result["reply_text"] == (
        "Без проблем 🙂 Якщо коротко — зазвичай це економить час на обробці заявок "
        "і допомагає не втрачати клієнтів.\n\n"
        "Якщо буде актуально — можемо швидко глянути ваш кейс і зрозуміти, "
        "чи є сенс впроваджувати."
    )
    assert result["routing_category"] == "answered_basic"
    assert result["booking_result"] is None


async def test_inactive_waiting_for_time_uses_normal_conversation(processor_factory):
    processor, booking_service = processor_factory()
    booking_service._save_pending_confirmation(
        "user-1",
        {
            "state": "WAITING_FOR_TIME",
            "language": "uk",
            "duration_minutes": 30,
            "summary": "Consultation call",
            "description": "Booked via Flowly Meta Bot",
        },
    )

    price_result = await processor.process(_message(text="вітаю, скільки коштує бот?"))

    assert price_result["intent"] == "price"
    assert "Вартість стартує від 200$" in price_result["reply_text"]
    assert "Підкажіть, будь ласка, точний день і час." not in price_result["reply_text"]
    assert booking_service.get_booking_state("user-1").value == "NONE"


async def test_price_question_skolko_ce_koshtue(processor_factory):
    processor, _ = processor_factory()

    result = await processor.process(_message(text="Скільки це коштує?"))

    assert result["intent"] == "price"
    assert "Вартість стартує від 200$" in result["reply_text"]
    assert "Можемо коротко обговорити ваш кейс на дзвінку" in result["reply_text"]


async def test_active_waiting_for_time_stays_in_booking_for_unrelated_question(processor_factory):
    processor, booking_service = processor_factory()

    start_result = await processor.process(_message(text="давайте дзвінок"))
    price_result = await processor.process(_message(text="вітаю, скільки коштує бот?"))

    assert start_result["booking_result"]["booking_state"] == "WAITING_FOR_TIME"
    assert price_result["intent"] == "booking_flow"
    assert price_result["booking_result"]["status"] == "booking_unrelated_question"
    assert "Для дзвінка підкажіть" in price_result["reply_text"]
    assert booking_service.get_booking_state("user-1").value == "WAITING_FOR_TIME"


async def test_waiting_for_time_still_accepts_normal_booking_time(processor_factory):
    processor, booking_service = processor_factory()

    await processor.process(_message(text="давайте дзвінок"))
    result = await processor.process(_message(text="завтра о 15"))

    assert result["intent"] == "booking_flow"
    assert "о 15:00 вільний" in result["reply_text"]
    assert "ваше ім’я та номер телефону або email" in result["reply_text"]
    assert booking_service._get_pending_confirmation("user-1")["is_active"] is True
    assert booking_service._get_pending_confirmation("user-1")["step"] == "WAITING_FOR_CONTACT"
    assert booking_service.get_booking_state("user-1").value == "WAITING_FOR_CONTACT"


async def test_waiting_for_time_1230_asks_for_name_and_contact(processor_factory):
    processor, booking_service = processor_factory()

    await processor.process(_message(text="давайте дзвінок"))
    result = await processor.process(_message(text="завтра 12:30"))

    assert result["intent"] == "booking_flow"
    assert "12:30 вільний" in result["reply_text"]
    assert "ваше ім’я та номер телефону або email" in result["reply_text"]
    assert booking_service.get_booking_state("user-1").value == "WAITING_FOR_CONTACT"


async def test_service_question_does_not_trigger_stale_waiting_for_contact(processor_factory):
    processor, booking_service = processor_factory()
    booking_service._save_pending_confirmation(
        "user-1",
        {
            "state": "WAITING_FOR_CONTACT",
            "language": "uk",
            "duration_minutes": 30,
            "summary": "Consultation call",
            "description": "Booked via Flowly Meta Bot",
            "source_channel": "instagram",
            "context_summary": "old stale context",
        },
    )

    result = await processor.process(_message(text="Привіт, що це у вас за сервіс?"))

    assert result["intent"] != "booking_flow"
    assert "Дякую, ім’я зафіксував" not in result["reply_text"]
    assert "залиште" not in result["reply_text"]
    assert booking_service.get_booking_state("user-1").value == "NONE"


async def test_active_waiting_for_contact_faq_stays_in_booking_state(processor_factory):
    processor, booking_service = processor_factory()

    await processor.process(_message(text="давайте дзвінок"))
    await processor.process(_message(text="завтра о 15"))
    result = await processor.process(_message(text="Привіт, що це у вас за сервіс?"))

    assert result["intent"] == "booking_flow"
    assert result["booking_result"]["status"] == "booking_unrelated_question"
    assert "Дякую, ім’я зафіксував" not in result["reply_text"]
    assert "Щоб продовжити підтвердження дзвінка" in result["reply_text"]
    assert booking_service.get_booking_state("user-1").value == "WAITING_FOR_CONTACT"


async def test_active_waiting_for_contact_still_accepts_contact(processor_factory):
    processor, _ = processor_factory()

    await processor.process(_message(text="давайте дзвінок"))
    await processor.process(_message(text="завтра о 15"))
    result = await processor.process(_message(text="Іван 0987121328"))

    assert result["intent"] == "booking_flow"
    assert result["booking_result"]["status"] in {"confirmed", "manual_followup"}


async def test_booking_collects_name_and_writes_calendar_description(processor_factory):
    calendar_service = RecordingCreateCalendarService()
    processor, _ = processor_factory(calendar_service=calendar_service)

    await processor.process(_message(text="Давайте дзвінок"))
    time_result = await processor.process(_message(text="Завтра о 12"))
    contact_result = await processor.process(_message(text="Іван 0987121328"))

    assert "ваше ім’я та номер телефону або email" in time_result["reply_text"]
    assert contact_result["booking_result"]["status"] == "confirmed"
    assert contact_result["booking_result"]["event_created"] is True
    assert contact_result["booking_result"]["event_id"] == "calendar-event-created"
    assert contact_result["booking_result"]["customer_name"] == "Іван"
    assert calendar_service.created_descriptions
    assert len(calendar_service.created_events) == 1
    description = calendar_service.created_descriptions[0]
    assert "Booked via Flowly Meta Bot" in description
    assert "Customer name: Іван" in description
    assert "Sender ID: user-1" in description
    assert "Source: instagram" in description
    assert "Context: Завтра о 12" in description
    assert "Contact: Phone: 0987121328" in description


async def test_booking_contact_only_asks_for_name(processor_factory):
    processor, booking_service = processor_factory()

    await processor.process(_message(text="Давайте дзвінок"))
    await processor.process(_message(text="Завтра о 12"))
    result = await processor.process(_message(text="0987121328"))

    assert result["booking_result"]["status"] == "waiting_for_name"
    assert result["reply_text"] == "Дякую. А підкажіть, будь ласка, ваше ім’я?"
    assert booking_service.get_booking_state("user-1").value == "WAITING_FOR_CONTACT"


async def test_booking_name_only_asks_for_contact(processor_factory):
    processor, booking_service = processor_factory()

    await processor.process(_message(text="Давайте дзвінок"))
    await processor.process(_message(text="Завтра о 12"))
    result = await processor.process(_message(text="Іван"))

    assert result["booking_result"]["status"] == "waiting_for_contact"
    assert result["reply_text"] == (
        "Дякую, ім’я зафіксував. А для підтвердження залиште, будь ласка, "
        "контактний номер або email."
    )
    assert booking_service.get_booking_state("user-1").value == "WAITING_FOR_CONTACT"


async def test_booking_with_contact_in_initial_message(processor_factory):
    processor, booking_service = processor_factory()

    result = await processor.process(_message(text="Давайте дзвінок завтра о 12 Іван 0991234567"))

    assert result["intent"] == "booking_request"
    assert result["booking_result"]["status"] == "manual_followup"
    assert result["booking_result"]["reply_text"] == (
        "Супер, зафіксували ваш запит 🙌 Ми зв’яжемося з вами, щоб підтвердити час"
    )
    assert "підтвердили дзвінок" not in result["booking_result"]["reply_text"]
    assert result["booking_result"]["event_created"] is False
    assert not booking_service.has_confirmed_booking("user-1")
    assert booking_service.get_booking_state("user-1").value == "NONE"


async def test_booking_with_contact_in_initial_message_creates_calendar_event_when_configured(processor_factory):
    calendar_service = RecordingCreateCalendarService()
    processor, booking_service = processor_factory(calendar_service=calendar_service)

    result = await processor.process(_message(text="Давайте дзвінок завтра о 12 Іван 0991234567"))

    assert result["intent"] == "booking_request"
    assert result["booking_result"]["status"] == "confirmed"
    assert result["booking_result"]["event_created"] is True
    assert result["booking_result"]["event_id"] == "calendar-event-created"
    assert "підтвердили дзвінок" in result["booking_result"]["reply_text"]
    assert booking_service.has_confirmed_booking("user-1")
    assert len(calendar_service.created_events) == 1


async def test_booking_manual_followup_when_calendar_not_configured_does_not_confirm(processor_factory):
    processor, booking_service = processor_factory()

    await processor.process(_message(text="Давайте дзвінок"))
    await processor.process(_message(text="Завтра о 12"))
    result = await processor.process(_message(text="Іван 0987121328"))

    assert result["booking_result"]["status"] == "manual_followup"
    assert result["booking_result"]["event_created"] is False
    assert result["booking_result"]["reply_text"] == (
        "Супер, зафіксували ваш запит 🙌 Ми зв’яжемося з вами, щоб підтвердити час"
    )
    assert "підтвердили дзвінок" not in result["booking_result"]["reply_text"]
    assert not booking_service.has_confirmed_booking("user-1")
    assert booking_service.get_booking_state("user-1").value == "NONE"


async def test_booking_manual_followup_when_calendar_create_fails(processor_factory):
    calendar_service = FailingCreateCalendarService()
    processor, booking_service = processor_factory(calendar_service=calendar_service)

    await processor.process(_message(text="Давайте дзвінок"))
    await processor.process(_message(text="Завтра о 12"))
    result = await processor.process(_message(text="Іван 0987121328"))

    assert result["booking_result"]["status"] == "manual_followup"
    assert result["booking_result"]["event_created"] is False
    assert result["booking_result"]["reply_text"] == (
        "Супер, зафіксували ваш запит 🙌 Ми зв’яжемося з вами, щоб підтвердити час"
    )
    assert not booking_service.has_confirmed_booking("user-1")


async def test_booking_slot_suggestion_and_confirmation(processor_factory):
    processor, booking_service = processor_factory(calendar_service=BusyThenAvailableCalendarService())

    # Request a busy slot
    result1 = await processor.process(_message(text="давай дзвінок завтра 12:30"))
    assert result1["booking_result"]["status"] == "slot_suggested"
    assert "Як щодо" in result1["booking_result"]["reply_text"]

    # Confirm with "ок"
    result2 = await processor.process(_message(text="ок"))
    assert result2["intent"] == "booking_flow"
    assert result2["booking_result"]["status"] == "waiting_for_contact"

    # Provide contact
    result3 = await processor.process(_message(text="Іван 0991234567"))
    assert result3["booking_result"]["status"] == "manual_followup"
    assert result3["booking_result"]["event_created"] is False
    assert "зафіксували ваш запит" in result3["booking_result"]["reply_text"]
    assert not booking_service.has_confirmed_booking("user-1")


async def test_service_question_for_whom(processor_factory):
    processor, _ = processor_factory()

    result = await processor.process(_message(text="А для кого це?"))

    assert result["intent"] == "service_description"
    assert result["routing_category"] == "answered_basic"
    assert "Для кого це?" not in result["reply_text"]
    assert "Так, це можна налаштувати" not in result["reply_text"]
    assert "сервісним бізнесам" in result["reply_text"]


async def test_service_question_what_does_bot_do(processor_factory):
    processor, _ = processor_factory()

    result = await processor.process(_message(text="Що конкретно робить бот?"))

    assert result["intent"] == "service_description"
    assert result["routing_category"] == "answered_basic"
    assert "Так, це можна налаштувати" not in result["reply_text"]
    assert ("працює" in result["reply_text"] or "AI-бот" in result["reply_text"] or "Як це" in result["reply_text"])


async def test_service_question_what_is_included(processor_factory):
    processor, _ = processor_factory()

    result = await processor.process(_message(text="Що входить у сервіс?"))

    assert result["intent"] == "service_description"
    assert result["routing_category"] == "answered_basic"
    assert "Так, це можна налаштувати" not in result["reply_text"]
    assert ("входить" in result["reply_text"] or "аудит" in result["reply_text"])
