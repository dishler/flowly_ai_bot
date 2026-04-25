from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict
from zoneinfo import ZoneInfo

from app.application.services.calendar_service import CalendarService
from app.application.services.language_service import LanguageService
from app.core.config import settings
from app.domain.enums import BookingState


logger = logging.getLogger(__name__)


class BookingService:
    EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)
    PHONE_RE = re.compile(r"(?:(?<=\D)|^)(\+?\d[\d\-\s\(\)]{8,}\d)(?=\D|$)")

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
        self.captured_contacts: dict[str, dict[str, Any]] = {}
        self.completed_bookings: dict[str, dict[str, Any]] = {}

    def has_pending_confirmation(self, sender_id: str) -> bool:
        if self.booking_state_service is not None:
            return self.booking_state_service.has_pending_confirmation(sender_id)
        return sender_id in self.pending_confirmations

    def get_booking_state(self, sender_id: str) -> BookingState:
        pending = self._get_pending_confirmation(sender_id)
        if not pending:
            return BookingState.NONE

        raw_state = pending.get("state")
        if raw_state:
            try:
                return BookingState(raw_state)
            except ValueError:
                logger.warning("Unknown booking state for sender_id=%s: %r", sender_id, raw_state)

        if pending.get("stage") == "awaiting_contact":
            return BookingState.WAITING_FOR_CONTACT

        return BookingState.WAITING_FOR_TIME

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

    def _save_booking_state(
        self,
        sender_id: str,
        *,
        state: BookingState,
        language: str,
        start_dt: datetime | None = None,
        duration_minutes: int = 30,
        summary: str = "Consultation call",
        description: str | None = None,
        contact_email: str | None = None,
        contact_phone: str | None = None,
        source_channel: str | None = None,
        context_summary: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "state": state.value,
            "language": language,
            "duration_minutes": duration_minutes,
            "summary": summary,
            "description": description or f"Booked via Flowly Meta Bot. Sender ID: {sender_id}",
            "contact_email": contact_email,
            "contact_phone": contact_phone,
            "source_channel": source_channel,
            "context_summary": context_summary,
        }
        if start_dt is not None:
            payload["start_dt"] = self._serialize_pending_start_dt(start_dt)
        logger.info(
            "Saving booking state sender_id=%s state=%s has_start_dt=%s has_email=%s has_phone=%s",
            sender_id,
            state.value,
            start_dt is not None,
            bool(contact_email),
            bool(contact_phone),
        )
        self._save_pending_confirmation(sender_id, payload)

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
        rejection_markers = [
            "скасуйте",
            "скасувати",
            "відмінити",
            "відмініть",
            "cancel",
            "not now",
        ]
        return normalized in rejections or any(marker in normalized for marker in rejection_markers)

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
            return (
                f"Супер, слот {formatted} вільний. "
                "Щоб підтвердити дзвінок, залиште, будь ласка, номер телефону або email."
            )
        formatted = start_dt.strftime("%d.%m at %H:%M")
        return (
            f"Great, the {formatted} slot is available. To confirm the call, please share your "
            "phone number or email."
        )

    def _build_confirmed_reply(self, language: str) -> str:
        if language == "uk":
            return "Дякую, дзвінок підтверджено. Ми зв’яжемося з вами у зазначений час."
        return "Thank you, your call is confirmed. We will contact you at the scheduled time."

    def _build_cancelled_reply(self, language: str) -> str:
        if language == "uk":
            return "Добре, не бронюю. Якщо хочете, можете надіслати інший час."
        return "Okay, I will not book it. You can send another time if you want."

    def _build_confirmed_cancelled_reply(self, language: str) -> str:
        if language == "uk":
            return "Добре, я скасував ваш дзвінок. Якщо буде актуально — можемо запланувати інший час."
        return "Okay, I cancelled your call. If it becomes relevant again, we can schedule another time."

    def _build_call_explanation_reply(self, language: str) -> str:
        if language == "uk":
            return "На дзвінку ми коротко розберемо ваш кейс, задачі і підкажемо, як бот може працювати саме у вас."
        return "On the call, we will briefly review your case and goals, then explain how the bot can work for your business."

    def _build_availability_question_reply(self, language: str) -> str:
        if language == "uk":
            return "Підкажіть, будь ласка, на який день вам зручно, і я запропоную доступні слоти."
        return "Please tell me which day works for you, and I will suggest available slots."

    def _build_confirm_prompt_reply(self, language: str) -> str:
        if language == "uk":
            return "Напишіть, будь ласка, «так», щоб підтвердити, або надішліть інший час."
        return "Please reply with “yes” to confirm, or send another time."

    def _build_contact_retry_reply(self, language: str) -> str:
        if language == "uk":
            return "Щоб підтвердити дзвінок, залиште, будь ласка, номер телефону або email."
        return (
            "To confirm the call, please share your phone number or email."
        )

    def _build_email_confirmed_reply(self, language: str) -> str:
        if language == "uk":
            return "Дякую, дзвінок підтверджено. Ми зв’яжемося з вами у зазначений час."
        return "Thank you, your call is confirmed. We will contact you at the scheduled time."

    def _build_phone_handoff_reply(self, language: str) -> str:
        return self._build_confirmed_reply(language)

    def _build_both_contacts_confirmed_reply(self, language: str) -> str:
        return self._build_confirmed_reply(language)

    def _build_create_failed_reply(self, language: str) -> str:
        return self._build_confirmed_reply(language)

    def _normalize_phone(self, raw_phone: str) -> str:
        compact = re.sub(r"[^\d+]", "", raw_phone.strip())
        if compact.startswith("++"):
            compact = compact[1:]
        return compact

    def _extract_contact_details(self, text: str) -> Dict[str, Any]:
        emails = []
        seen_emails = set()
        for match in self.EMAIL_RE.findall(text):
            email = match.strip().lower()
            if email and email not in seen_emails:
                seen_emails.add(email)
                emails.append(email)

        phones = []
        seen_phones = set()
        for raw_phone in self.PHONE_RE.findall(text):
            phone = self._normalize_phone(raw_phone)
            digits_only = re.sub(r"\D", "", phone)
            if len(digits_only) < 9:
                continue
            if phone and phone not in seen_phones:
                seen_phones.add(phone)
                phones.append(phone)

        primary_email = emails[0] if emails else None
        primary_phone = phones[0] if phones else None

        return {
            "email": primary_email,
            "phone": primary_phone,
            "emails": emails,
            "phones": phones,
            "has_email": bool(primary_email),
            "has_phone": bool(primary_phone),
        }

    def _save_captured_contact(
        self,
        sender_id: str,
        *,
        email: str | None,
        phone: str | None,
        start_dt: datetime | None = None,
    ) -> None:
        self.captured_contacts[sender_id] = {
            "email": email,
            "phone": phone,
            "start_dt": self._serialize_pending_start_dt(start_dt) if start_dt else None,
        }

    def has_confirmed_booking(self, sender_id: str) -> bool:
        return sender_id in self.completed_bookings

    def get_call_explanation_reply(self, language: str) -> str:
        return self._build_call_explanation_reply(language)

    def get_availability_question_reply(self, language: str) -> str:
        return self._build_availability_question_reply(language)

    def cancel_confirmed_booking(self, sender_id: str, message_text: str) -> Dict[str, Any]:
        language = self._detect_language(message_text)
        self.completed_bookings.pop(sender_id, None)
        self._clear_pending_confirmation(sender_id)
        return {
            "status": "cancelled",
            "reply_text": self._build_confirmed_cancelled_reply(language),
            "booking_state": BookingState.NONE.value,
        }

    def _mark_booking_completed(
        self,
        sender_id: str,
        *,
        start_dt: datetime | None,
        email: str | None,
        phone: str | None,
    ) -> None:
        self.completed_bookings[sender_id] = {
            "start_dt": self._serialize_pending_start_dt(start_dt) if start_dt else None,
            "email": email,
            "phone": phone,
        }

    def get_reschedule_reply(self, language: str) -> str:
        if language == "en":
            return "You already have a confirmed call. If you want, I can help you move it to a different time."
        return "У вас уже є підтверджений дзвінок. Якщо хочете, можу допомогти перенести його на інший час."

    def get_reschedule_prompt_reply(self, language: str) -> str:
        if language == "en":
            return "Yes, of course. Please tell me what day and time would work better for rescheduling the call."
        return "Так, звісно. Підкажіть, будь ласка, на який день і час вам буде зручно перенести дзвінок?"

    def handle_reschedule_request(self, sender_id: str, message_text: str) -> Dict[str, Any]:
        language = self._detect_language(message_text)
        requested_dt = self._parse_requested_datetime(message_text)

        if requested_dt is None:
            return {
                "status": "reschedule_prompt",
                "reply_text": self.get_reschedule_prompt_reply(language),
                "booking_state": BookingState.NONE.value,
            }

        self.completed_bookings[sender_id] = {
            **self.completed_bookings.get(sender_id, {}),
            "start_dt": self._serialize_pending_start_dt(requested_dt),
        }

        if language == "en":
            formatted = requested_dt.strftime("%d.%m at %H:%M")
            reply_text = f"Okay, we can move it. New time: {formatted}. We will contact you at the scheduled time."
        else:
            formatted = requested_dt.strftime("%d.%m о %H:%M")
            reply_text = f"Добре, можемо перенести. Новий час: {formatted}. Ми зв’яжемося з вами у зазначений час."

        return {
            "status": "rescheduled",
            "reply_text": reply_text,
            "booking_state": BookingState.NONE.value,
            "start_dt": requested_dt.isoformat(),
        }

    def _build_manual_followup_result(
        self,
        *,
        sender_id: str,
        language: str,
        start_dt: datetime | None,
        email: str | None,
        phone: str | None,
    ) -> Dict[str, Any]:
        self._save_captured_contact(
            sender_id,
            email=email,
            phone=phone,
            start_dt=start_dt,
        )
        self._mark_booking_completed(
            sender_id,
            start_dt=start_dt,
            email=email,
            phone=phone,
        )
        self._clear_pending_confirmation(sender_id)
        return {
            "status": "manual_followup",
            "reply_text": self._build_create_failed_reply(language),
            "event_created": False,
            "booking_state": BookingState.NONE.value,
            "contact_email": email,
            "contact_phone": phone,
        }

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
        weekday_map = {
            "monday": 0,
            "понеділок": 0,
            "понеділка": 0,
            "вівторок": 1,
            "вівторка": 1,
            "tuesday": 1,
            "середа": 2,
            "середу": 2,
            "wednesday": 2,
            "четвер": 3,
            "четверг": 3,
            "thursday": 3,
            "п'ятниц": 4,
            "п’ятниц": 4,
            "friday": 4,
        }
        matched_weekday = None
        for marker, weekday in weekday_map.items():
            if marker in normalized:
                matched_weekday = weekday
                break

        if hour_match and matched_weekday is not None:
            hour = int(hour_match.group(1))
            if 0 <= hour <= 23:
                days_ahead = (matched_weekday - now.weekday()) % 7
                if days_ahead == 0:
                    days_ahead = 7
                base = now + timedelta(days=days_ahead)
                return base.replace(hour=hour, minute=0, second=0, microsecond=0)

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
        return self.start_booking_flow(sender_id=sender_id, message_text=message_text)

    def start_booking_flow(
        self,
        sender_id: str,
        message_text: str,
        source_channel: str | None = None,
    ) -> Dict[str, Any]:
        language = self._detect_language(message_text)
        requested_dt = self._parse_requested_datetime(message_text)

        logger.info("Entered start_booking_flow sender_id=%s", sender_id)
        logger.info("booking request sender_id=%s text=%r parsed_dt=%s", sender_id, message_text, requested_dt)

        if requested_dt is None:
            self._save_booking_state(
                sender_id,
                state=BookingState.WAITING_FOR_TIME,
                language=language,
                source_channel=source_channel,
                context_summary=message_text[:280],
            )
            return {
                "status": "waiting_for_time",
                "reply_text": self._build_unclear_time_reply(language),
                "requires_confirmation": False,
                "booking_state": BookingState.WAITING_FOR_TIME.value,
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

        self._save_booking_state(
            sender_id,
            state=BookingState.WAITING_FOR_CONTACT,
            language=language,
            start_dt=requested_dt,
            source_channel=source_channel,
            context_summary=message_text[:280],
        )

        return {
            "status": "waiting_for_contact",
            "reply_text": self._build_available_reply(language, requested_dt),
            "requires_confirmation": False,
            "requires_contact": True,
            "booking_state": BookingState.WAITING_FOR_CONTACT.value,
            "start_dt": requested_dt.isoformat(),
        }

    def handle_booking_confirmation(self, sender_id: str, message_text: str) -> Dict[str, Any] | None:
        return self.process_booking_message(sender_id=sender_id, message_text=message_text)

    def process_booking_message(
        self,
        sender_id: str,
        message_text: str,
        source_channel: str | None = None,
    ) -> Dict[str, Any] | None:
        pending = self._get_pending_confirmation(sender_id)
        if not pending:
            return None

        logger.info("Entered process_booking_message sender_id=%s", sender_id)
        language = pending["language"]
        state = self.get_booking_state(sender_id)
        logger.info("Booking state: %s", state.value)

        if self._is_rejection(message_text):
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "cancelled",
                "reply_text": self._build_cancelled_reply(language),
                "event_created": False,
                "booking_state": BookingState.NONE.value,
            }

        if state == BookingState.WAITING_FOR_TIME:
            return self.start_booking_flow(
                sender_id=sender_id,
                message_text=message_text,
                source_channel=source_channel or pending.get("source_channel"),
            )

        if state == BookingState.WAITING_FOR_CONTACT:
            contact_details = self._extract_contact_details(message_text)
            logger.info(
                "booking contact received sender_id=%s has_email=%s has_phone=%s",
                sender_id,
                contact_details["has_email"],
                contact_details["has_phone"],
            )

            if not contact_details["has_email"] and not contact_details["has_phone"]:
                return {
                    "status": "waiting_for_contact",
                    "reply_text": self._build_contact_retry_reply(language),
                    "event_created": False,
                    "requires_contact": True,
                    "booking_state": BookingState.WAITING_FOR_CONTACT.value,
                }

            pending["contact_email"] = contact_details["email"]
            pending["contact_phone"] = contact_details["phone"]
            pending["state"] = BookingState.CONFIRMATION.value

        elif state == BookingState.CONFIRMATION:
            pass
        else:
            return None

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
                "booking_state": BookingState.NONE.value,
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
                "booking_state": BookingState.NONE.value,
            }

        if not still_available:
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "unavailable",
                "reply_text": self._build_unavailable_reply(language),
                "event_created": False,
                "booking_state": BookingState.NONE.value,
            }

        if not self.calendar_service.google_calendar_client:
            return self._build_manual_followup_result(
                sender_id=sender_id,
                language=language,
                start_dt=start_dt,
                email=pending.get("contact_email"),
                phone=pending.get("contact_phone"),
            )

        if not self.calendar_service.google_calendar_client.is_configured():
            logger.warning(
                "Google Calendar is not configured; switching to manual follow-up sender_id=%s",
                sender_id,
            )
            return self._build_manual_followup_result(
                sender_id=sender_id,
                language=language,
                start_dt=start_dt,
                email=pending.get("contact_email"),
                phone=pending.get("contact_phone"),
            )

        try:
            description_parts = [pending["description"], f"Sender ID: {sender_id}"]
            if pending.get("source_channel"):
                description_parts.append(f"Source: {pending['source_channel']}")
            if pending.get("context_summary"):
                description_parts.append(f"Context: {pending['context_summary']}")
            contact_parts = []
            if pending.get("contact_email"):
                contact_parts.append(f"Email: {pending['contact_email']}")
            if pending.get("contact_phone"):
                contact_parts.append(f"Phone: {pending['contact_phone']}")
            if contact_parts:
                description_parts.append("Contact: " + " | ".join(contact_parts))
            description = "\n".join(description_parts)

            created = self.calendar_service.create_booking_event(
                start_dt=start_dt,
                duration_minutes=pending["duration_minutes"],
                summary=pending["summary"],
                description=description,
                attendee_emails=[],
            )
            logger.info("Calendar event created")
        except Exception:
            logger.exception("Booking creation failed")
            logger.exception(
                "booking create_event failed sender_id=%s start_dt=%s pending=%r",
                sender_id,
                start_dt.isoformat(),
                pending,
            )
            return self._build_manual_followup_result(
                sender_id=sender_id,
                language=language,
                start_dt=start_dt,
                email=pending.get("contact_email"),
                phone=pending.get("contact_phone"),
            )

        self._save_captured_contact(
            sender_id,
            email=pending.get("contact_email"),
            phone=pending.get("contact_phone"),
            start_dt=start_dt,
        )
        self._mark_booking_completed(
            sender_id,
            start_dt=start_dt,
            email=pending.get("contact_email"),
            phone=pending.get("contact_phone"),
        )
        self._clear_pending_confirmation(sender_id)

        has_email = bool(pending.get("contact_email"))
        has_phone = bool(pending.get("contact_phone"))
        if has_email and has_phone:
            reply_text = self._build_both_contacts_confirmed_reply(language)
        elif has_email:
            reply_text = self._build_email_confirmed_reply(language)
        else:
            reply_text = self._build_confirmed_reply(language)

        return {
            "status": "confirmed",
            "reply_text": reply_text,
            "event_created": True,
            "booking_state": BookingState.NONE.value,
            "event_id": created.event_id,
            "event_link": created.html_link,
            "contact_email": pending.get("contact_email"),
            "contact_phone": pending.get("contact_phone"),
        }
