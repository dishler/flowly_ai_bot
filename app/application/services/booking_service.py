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
            return (
                f"Супер, слот {formatted} вільний. "
                "Щоб підтвердити дзвінок, залиште, будь ласка, номер телефону або email. "
                "Якщо зручніше, можемо надіслати запрошення в календар."
            )
        formatted = start_dt.strftime("%d.%m at %H:%M")
        return (
            f"Great, the {formatted} slot is available. To confirm the call, please share your "
            "phone number or email. If it's easier, we can send a calendar invite."
        )

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

    def _build_contact_retry_reply(self, language: str) -> str:
        if language == "uk":
            return (
                "Щоб підтвердити дзвінок, потрібен номер телефону або email. "
                "Надішліть, будь ласка, один із цих контактів."
            )
        return (
            "To confirm the call, I need either your phone number or email. "
            "Please send one of those contact details."
        )

    def _build_email_confirmed_reply(self, language: str) -> str:
        if language == "uk":
            return "Дякую, дзвінок підтверджено. Запрошення надіслано на ваш email."
        return "Thank you, your call is confirmed. The invite has been sent to your email."

    def _build_phone_handoff_reply(self, language: str) -> str:
        if language == "uk":
            return "Дякую. Передам ваш контакт спеціалісту, і він зв’яжеться з вами найближчим часом щодо дзвінка."
        return "Thank you. I will pass your contact to our specialist, and they will reach out shortly regarding the call."

    def _build_both_contacts_confirmed_reply(self, language: str) -> str:
        if language == "uk":
            return "Дякую, дзвінок підтверджено. Запрошення надіслано на ваш email, а контакт передано спеціалісту."
        return "Thank you, your call is confirmed. The invite has been sent to your email, and your contact has been passed to our specialist."

    def _build_create_failed_reply(self, language: str) -> str:
        if language == "uk":
            return "Не вдалося створити бронювання в календарі. Можемо спробувати ще раз."
        return "I could not create the booking in the calendar. We can try again."

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
                "stage": "awaiting_contact",
                "start_dt": self._serialize_pending_start_dt(requested_dt),
                "language": language,
                "duration_minutes": 30,
                "summary": "Consultation call",
                "description": f"Booked via Flowly Meta Bot. Sender ID: {sender_id}",
            },
        )

        return {
            "status": "awaiting_contact",
            "reply_text": self._build_available_reply(language, requested_dt),
            "requires_confirmation": False,
            "requires_contact": True,
            "start_dt": requested_dt.isoformat(),
        }

    def handle_booking_confirmation(self, sender_id: str, message_text: str) -> Dict[str, Any] | None:
        pending = self._get_pending_confirmation(sender_id)
        if not pending:
            return None

        language = pending["language"]
        stage = pending.get("stage", "awaiting_confirmation")

        if self._is_rejection(message_text):
            self._clear_pending_confirmation(sender_id)
            return {
                "status": "cancelled",
                "reply_text": self._build_cancelled_reply(language),
                "event_created": False,
            }

        if stage == "awaiting_contact":
            contact_details = self._extract_contact_details(message_text)
            logger.info(
                "booking contact received sender_id=%s has_email=%s has_phone=%s",
                sender_id,
                contact_details["has_email"],
                contact_details["has_phone"],
            )

            if not contact_details["has_email"] and not contact_details["has_phone"]:
                requested_dt = self._parse_requested_datetime(message_text)
                if requested_dt is not None:
                    return self.handle_booking_request(sender_id=sender_id, message_text=message_text)

            if not contact_details["has_email"] and not contact_details["has_phone"]:
                return {
                    "status": "awaiting_contact",
                    "reply_text": self._build_contact_retry_reply(language),
                    "event_created": False,
                    "requires_contact": True,
                }

            pending["contact_email"] = contact_details["email"]
            pending["contact_phone"] = contact_details["phone"]

            if contact_details["has_phone"] and not contact_details["has_email"]:
                try:
                    start_dt = self._deserialize_pending_start_dt(pending["start_dt"])
                except Exception:
                    start_dt = None
                self._save_captured_contact(
                    sender_id,
                    email=None,
                    phone=contact_details["phone"],
                    start_dt=start_dt,
                )
                self._clear_pending_confirmation(sender_id)
                return {
                    "status": "contact_shared_phone",
                    "reply_text": self._build_phone_handoff_reply(language),
                    "event_created": False,
                    "requires_contact": False,
                    "contact_phone": contact_details["phone"],
                }

            attendee_emails = [contact_details["email"]] if contact_details["email"] else []
        else:
            if not self._is_confirmation(message_text):
                return {
                    "status": "awaiting_confirmation",
                    "reply_text": self._build_confirm_prompt_reply(language),
                    "event_created": False,
                }

            attendee_emails = []

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
            description = pending["description"]
            if pending.get("contact_email") or pending.get("contact_phone"):
                contact_parts = []
                if pending.get("contact_email"):
                    contact_parts.append(f"Email: {pending['contact_email']}")
                if pending.get("contact_phone"):
                    contact_parts.append(f"Phone: {pending['contact_phone']}")
                description = f"{description}\nContact: {' | '.join(contact_parts)}"

            created = self.calendar_service.create_booking_event(
                start_dt=start_dt,
                duration_minutes=pending["duration_minutes"],
                summary=pending["summary"],
                description=description,
                attendee_emails=attendee_emails,
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

        self._save_captured_contact(
            sender_id,
            email=pending.get("contact_email"),
            phone=pending.get("contact_phone"),
            start_dt=start_dt,
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
            "event_id": created.event_id,
            "event_link": created.html_link,
            "contact_email": pending.get("contact_email"),
            "contact_phone": pending.get("contact_phone"),
        }
