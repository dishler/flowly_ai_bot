from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict
from zoneinfo import ZoneInfo

from app.application.services.calendar_service import CalendarService
from app.application.services.language_service import LanguageService
from app.core.config import settings


logger = logging.getLogger(__name__)


class BookingService:
    def __init__(
        self,
        calendar_service: CalendarService,
        language_service: LanguageService,
        booking_state_service=None,
    ) -> None:
        self.calendar_service = calendar_service
        self.language_service = language_service
        self.booking_state_service = booking_state_service
        self.timezone = ZoneInfo(settings.default_timezone)
        self.pending_confirmations: dict[str, dict[str, Any]] = {}

    def has_pending_confirmation(self, sender_id: str) -> bool:
        if self.booking_state_service is not None:
            return self.booking_state_service.has_pending_confirmation(sender_id)
        return sender_id in self.pending_confirmations

    def _save_pending_confirmation(self, sender_id: str, data: dict[str, Any]) -> None:
        if self.booking_state_service is not None:
            self.booking_state_service.save_pending_confirmation(sender_id, data)
            return
        self.pending_confirmations[sender_id] = data

    def _get_pending_confirmation(self, sender_id: str) -> dict[str, Any] | None:
        if self.booking_state_service is not None:
            return self.booking_state_service.get_pending_confirmation(sender_id)
        return self.pending_confirmations.get(sender_id)

    def _clear_pending_confirmation(self, sender_id: str) -> None:
        if self.booking_state_service is not None:
            self.booking_state_service.clear_pending_confirmation(sender_id)
            return
        self.pending_confirmations.pop(sender_id, None)

    def _detect_language(self, text: str) -> str:
        if re.search(r"[А-Яа-яІіЇїЄєҐґ]", text):
            return "uk"
        return "en"

    def _is_confirmation(self, text: str) -> bool:
        normalized = text.strip().lower()
        confirmations = {
            "yes", "y", "yeah", "yep", "sure", "ok", "okay", "confirm",
            "так", "та", "ага", "добре", "ок", "підтверджую", "підтвердити",
        }
        return normalized in confirmations

    def _is_rejection(self, text: str) -> bool:
        normalized = text.strip().lower()
        rejections = {
            "no", "nope", "not now", "cancel",
            "ні", "не", "скасувати", "не треба",
        }
        return normalized in rejections

    def _build_unclear_time_reply(self, language: str) -> str:
        if language == "uk":
            return "Підкажіть, будь ласка, точний день і час."
        return "Could you share the exact day and time, please?"

    def _build_unavailable_reply(self, language: str) -> str:
        slots = self.calendar_service.get_available_slots(language)
        if language == "uk":
            return f"На цей час слот уже зайнятий. Можу запропонувати: {', '.join(slots)}."
        return f"That time is already booked. I can offer: {', '.join(slots)}."

    def _build_available_reply(self, language: str, start_dt: datetime) -> str:
        if language == "uk":
            formatted = start_dt.strftime("%d.%m о %H:%M")
            return f"Схоже, цей слот вільний — {formatted}. Підтвердити бронювання?"
        formatted = start_dt.strftime("%d.%m at %H:%M")
        return f"That slot looks available — {formatted}. Should I confirm the booking?"

    def _build_confirmed_reply(self, language: str) -> str:
        if language == "uk":
            return "Готово, бронювання підтверджено."
        return "Done, your booking is confirmed."

    def _build_cancelled_reply(self, language: str) -> str:
        if language == "uk":
            return "Добре, не бронюю. Якщо хочете, можете надіслати інший час."
        return "Okay, I will not book it. You can send another time if you want."

    def _build_confirm_prompt_reply(self, language: str) -> str:
        if language == "uk":
            return "Напишіть, будь ласка, «так», щоб підтвердити, або надішліть інший час."
        return "Please reply with “yes” to confirm, or send another time."

    def _build_create_failed_reply(self, language: str) -> str:
        if language == "uk":
            return "Не вдалося створити бронювання в календарі. Можемо спробувати ще раз."
        return "I could not create the booking in the calendar. We can try again."

    def _serialize_pending_start_dt(self, value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=self.timezone)
        return value.isoformat()

    def _deserialize_pending_start_dt(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=self.timezone)
            return value.astimezone(self.timezone)

        if isinstance(value, str):
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=self.timezone)
            return parsed.astimezone(self.timezone)

        raise ValueError(f"Unsupported pending start_dt type: {type(value)!r}")

    def _parse_requested_datetime(self, text: str) -> datetime | None:
        now = datetime.now(self.timezone)
        normalized = text.strip().lower()

        match = re.search(r"(\d{4})-(\d{2})-(\d{2})[ ,T]+(\d{1,2}):(\d{2})", normalized)
        if match:
            year, month, day, hour, minute = map(int, match.groups())
            return datetime(year, month, day, hour, minute, tzinfo=self.timezone)

        match = re.search(r"(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?[ ,]+(\d{1,2}):(\d{2})", normalized)
        if match:
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3)) if match.group(3) else now.year
            hour = int(match.group(4))
            minute = int(match.group(5))
            return datetime(year, month, day, hour, minute, tzinfo=self.timezone)

        time_match = re.search(r"(\d{1,2}):(\d{2})", normalized)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))

            if "післязавтра" in normalized or "day after tomorrow" in normalized:
                base = now + timedelta(days=2)
                return base.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if "завтра" in normalized or "tomorrow" in normalized:
                base = now + timedelta(days=1)
                return base.replace(hour=hour, minute=minute, second=0, microsecond=0)

            if "сьогодні" in normalized or "today" in normalized:
                return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        hour_match = re.search(r"\b(\d{1,2})\b", normalized)
        if hour_match and (
            "завтра" in normalized
            or "tomorrow" in normalized
            or "післязавтра" in normalized
            or "day after tomorrow" in normalized
            or "сьогодні" in normalized
            or "today" in normalized
        ):
            hour = int(hour_match.group(1))
            minute = 0

            if 0 <= hour <= 23:
                if "післязавтра" in normalized or "day after tomorrow" in normalized:
                    base = now + timedelta(days=2)
                    return base.replace(hour=hour, minute=minute, second=0, microsecond=0)

                if "завтра" in normalized or "tomorrow" in normalized:
                    base = now + timedelta(days=1)
                    return base.replace(hour=hour, minute=minute, second=0, microsecond=0)

                if "сьогодні" in normalized or "today" in normalized:
                    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        return None

    def handle_booking_request(self, sender_id: str, message_text: str) -> Dict[str, Any]:
        language = self._detect_language(message_text)
        requested_dt = self._parse_requested_datetime(message_text)

        logger.info("booking request sender_id=%s text=%r parsed_dt=%s", sender_id, message_text, requested_dt)

        if requested_dt is None:
            return {
                "status": "needs_clarification",
                "reply_text": self._build_unclear_time_reply(language),
                "requires_confirmation": False,
                "start_dt": None,
            }

        # New booking request should replace any old pending confirmation.
        self._clear_pending_confirmation(sender_id)

        is_available = self.calendar_service.check_specific_time_availability(
            start_dt=requested_dt,
            duration_minutes=30,
        )

        logger.info(
            "booking availability sender_id=%s start_dt=%s is_available=%s",
            sender_id,
            requested_dt.isoformat(),
            is_available,
        )

        if not is_available:
            return {
                "status": "unavailable",
                "reply_text": self._build_unavailable_reply(language),
                "requires_confirmation": False,
                "start_dt": requested_dt.isoformat(),
            }

        self._save_pending_confirmation(
            sender_id,
            {
                "start_dt": self._serialize_pending_start_dt(requested_dt),
                "language": language,
                "duration_minutes": 30,
                "summary": "Consultation call",
                "description": f"Booked via Flowly Meta Bot. Sender ID: {sender_id}",
            },
        )

        return {
            "status": "available_pending_confirmation",
            "reply_text": self._build_available_reply(language, requested_dt),
            "requires_confirmation": True,
            "start_dt": requested_dt.isoformat(),
        }

    def handle_booking_confirmation(self, sender_id: str, message_text: str) -> Dict[str, Any] | None:
        pending = self._get_pending_confirmation(sender_id)
        if not pending:
            return None

        language = pending["language"]

        if self._is_rejection(message_text):
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "cancelled",
                "reply_text": self._build_cancelled_reply(language),
                "event_created": False,
            }

        if not self._is_confirmation(message_text):
            return {
                "status": "awaiting_confirmation",
                "reply_text": self._build_confirm_prompt_reply(language),
                "event_created": False,
            }

        try:
            start_dt = self._deserialize_pending_start_dt(pending["start_dt"])
        except Exception:
            logger.exception(
                "booking pending datetime deserialize failed sender_id=%s raw_start_dt=%r",
                sender_id,
                pending.get("start_dt"),
            )
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "create_failed",
                "reply_text": self._build_create_failed_reply(language),
                "event_created": False,
            }

        try:
            still_available = self.calendar_service.check_specific_time_availability(
                start_dt=start_dt,
                duration_minutes=pending["duration_minutes"],
            )
        except Exception:
            logger.exception(
                "booking availability recheck failed sender_id=%s start_dt=%s",
                sender_id,
                start_dt.isoformat(),
            )
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "create_failed",
                "reply_text": self._build_create_failed_reply(language),
                "event_created": False,
            }

        if not still_available:
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "unavailable",
                "reply_text": self._build_unavailable_reply(language),
                "event_created": False,
            }

        try:
            created = self.calendar_service.create_booking_event(
                start_dt=start_dt,
                duration_minutes=pending["duration_minutes"],
                summary=pending["summary"],
                description=pending["description"],
            )
        except Exception:
            logger.exception(
                "booking create_event failed sender_id=%s start_dt=%s pending=%r",
                sender_id,
                start_dt.isoformat(),
                pending,
            )
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "create_failed",
                "reply_text": self._build_create_failed_reply(language),
                "event_created": False,
            }

        self._clear_pending_confirmation(sender_id)

        return {
            "status": "confirmed",
            "reply_text": self._build_confirmed_reply(language),
            "event_created": True,
            "event_id": created.event_id,
            "event_link": created.html_link,
        }