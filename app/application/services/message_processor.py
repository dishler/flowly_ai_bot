import logging
from typing import Any, Dict
import re

from app.application.dto.normalized_message import NormalizedMessage
from app.application.services.booking_service import BookingService
from app.application.services.intent_service import IntentService
from app.application.services.memory_service import MemoryService
from app.application.services.outbound_service import OutboundService
from app.application.services.reply_service import ReplyService
from app.application.services.speech_service import SpeechService
from app.domain.enums import BookingState, IntentType

logger = logging.getLogger(__name__)

_STANDARD_SALES_INTENTS = frozenset(
    {
        IntentType.PRICE,
        IntentType.CHANNELS,
        IntentType.SERVICE_DESCRIPTION,
        IntentType.BOOKING_REQUEST,
        IntentType.CONSULTATION_INTEREST,
    }
)


class MessageProcessor:
    def __init__(
        self,
        memory_service: MemoryService,
        reply_service: ReplyService,
        outbound_service: OutboundService,
        dedup_service: Any,
        intent_service: IntentService,
        booking_service: BookingService,
        speech_service: SpeechService,
    ) -> None:
        self.memory_service = memory_service
        self.reply_service = reply_service
        self.outbound_service = outbound_service
        self.dedup_service = dedup_service
        self.intent_service = intent_service
        self.booking_service = booking_service
        self.speech_service = speech_service

    def _looks_like_booking_message(self, text: str) -> bool:
        normalized = text.strip().lower()

        consultation_words = [
            "consultation",
            "call",
            "quick call",
            "дзвінок",
            "консультація",
            "созвон",
        ]

        date_words = [
            "today",
            "tomorrow",
            "day after tomorrow",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "сьогодні",
            "завтра",
            "післязавтра",
            "понеділок",
            "вівторок",
            "середа",
            "четвер",
            "п'ятниц",
            "п’ятниц",
        ]

        has_consultation = any(word in normalized for word in consultation_words)
        has_date_word = any(word in normalized for word in date_words)
        has_time = bool(
            re.search(r"\b([01]?\d|2[0-3]):[0-5]\d\b", normalized)
            or re.search(r"\b(10|11|12|13|14|15|16|17|18|19|20|21|22|23)\b", normalized)
            or re.search(r"\b(о|на)\s*(10|11|12|13|14|15|16|17|18|19|20|21|22|23)\b", normalized)
        )

        if has_consultation and (has_date_word or has_time):
            return True

        return False

    def _looks_like_datetime_only_message(self, text: str) -> bool:
        normalized = text.strip().lower()
        date_words = [
            "today",
            "tomorrow",
            "day after tomorrow",
            "сьогодні",
            "завтра",
            "післязавтра",
        ]
        has_date_word = any(word in normalized for word in date_words)
        has_time = bool(
            re.search(r"\b([01]?\d|2[0-3]):[0-5]\d\b", normalized)
            or re.search(r"\b(10|11|12|13|14|15|16|17|18|19|20|21|22|23)\b", normalized)
            or re.search(r"\b(о|на|at)\s*(10|11|12|13|14|15|16|17|18|19|20|21|22|23)\b", normalized)
        )
        return has_date_word and has_time

    def _looks_like_reschedule_request(self, text: str) -> bool:
        normalized = text.strip().lower()
        markers = [
            "перенести",
            "перенес",
            "змінити час",
            "змінити дату",
            "інший час",
            "іншу дату",
            "reschedule",
            "move the call",
            "change the time",
            "change the date",
            "book again",
            "записати знову",
        ]
        return any(marker in normalized for marker in markers)

    async def _resolve_message_text(self, message: NormalizedMessage) -> str:
        user_message = (getattr(message, "user_message", "") or "").strip()
        audio_url = (getattr(message, "audio_url", "") or "").strip()

        if user_message:
            return user_message

        if audio_url:
            transcribed_text = await self.speech_service.transcribe_audio(audio_url)
            return transcribed_text.strip()

        return ""

    def _is_first_assistant_reply(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        return not any(item.startswith("assistant:") for item in history)

    def _has_greeting_prefix(self, reply_text: str, language: str) -> bool:
        normalized = reply_text.lstrip()
        if language == "en":
            return normalized.startswith("Hi!") or normalized.startswith("Hello!")
        return normalized.startswith("Привіт!")

    def _prepend_first_greeting_if_needed(self, sender_id: str, user_text: str, reply_text: str) -> str:
        if not reply_text.strip():
            return reply_text
        if not self._is_first_assistant_reply(sender_id):
            return reply_text

        language = self.reply_service.detect_user_language(user_text)
        if language == "en":
            prefix = "Hi! "
        else:
            prefix = "Привіт! "

        if self._has_greeting_prefix(reply_text, language):
            return reply_text
        return f"{prefix}{reply_text}"

    def _finalize_general_reply_text(self, sender_id: str, user_text: str, reply_text: str) -> str:
        finalized = self._prepend_first_greeting_if_needed(
            sender_id=sender_id,
            user_text=user_text,
            reply_text=reply_text,
        )
        return finalized

    async def process(self, message: NormalizedMessage) -> Dict[str, Any]:
        message_mid = (getattr(message, "message_mid", "") or "").strip()
        if message_mid:
            if self.dedup_service.is_duplicate(message_mid):
                logger.info("Duplicate message skipped: %s", message_mid)
                return {
                    "intent": "duplicate_skipped",
                    "reply_text": "",
                    "history": self.memory_service.get_history(message.sender_id),
                    "booking_result": None,
                    "outbound_result": None,
                }
            self.dedup_service.mark_processed(message_mid)

        resolved_text = await self._resolve_message_text(message)

        if not resolved_text:
            reply_text = "Не зовсім розібрав голосове повідомлення. Можете коротко написати текстом?"

            outbound_result = self.outbound_service.send_reply(
                platform=message.platform,
                recipient_id=message.sender_id,
                text=reply_text,
            )

            return {
                "intent": "unrecognized_audio",
                "reply_text": reply_text,
                "history": self.memory_service.get_history(message.sender_id),
                "booking_result": None,
                "outbound_result": outbound_result,
            }

        message.user_message = resolved_text

        self.memory_service.add_user_message(message.sender_id, message.user_message)

        booking_result = None
        reply_text = ""
        routing_category = "answered_basic"
        booking_state = self.booking_service.get_booking_state(message.sender_id)
        logger.info("Booking state: %s", booking_state.value)

        if booking_state != BookingState.NONE:
            booking_result = self.booking_service.process_booking_message(
                sender_id=message.sender_id,
                message_text=message.user_message,
                source_channel=message.platform,
            )
            intent_value = "booking_flow"
            if booking_result is not None:
                logger.info("Booking result used")
                reply_text = booking_result["reply_text"]
                routing_category = "consultation_cta"
            else:
                reply_text = self.reply_service.generate_reply(message, intent=IntentType.BOOKING_REQUEST)
            logger.info("Reply before guard: %s", reply_text)
            reply_text = self.reply_service.enforce_response_policy(
                reply_text=reply_text,
                user_text=message.user_message,
                intent=IntentType.BOOKING_REQUEST,
            )
            logger.info("Reply after guard: %s", reply_text)

            self.memory_service.add_assistant_message(message.sender_id, reply_text)

            outbound_result = self.outbound_service.send_reply(
                platform=message.platform,
                recipient_id=message.sender_id,
                text=reply_text,
            )

            return {
                "intent": intent_value,
                "routing_category": routing_category,
                "reply_text": reply_text,
                "history": self.memory_service.get_history(message.sender_id),
                "booking_result": booking_result,
                "outbound_result": outbound_result,
            }

        if (
            booking_state == BookingState.NONE
            and self.booking_service.has_confirmed_booking(message.sender_id)
            and self._looks_like_reschedule_request(message.user_message)
        ):
            booking_result = self.booking_service.handle_reschedule_request(
                sender_id=message.sender_id,
                message_text=message.user_message,
            )
            logger.info("Booking result used")
            reply_text = self.reply_service.enforce_response_policy(
                reply_text=booking_result["reply_text"],
                user_text=message.user_message,
                intent=IntentType.BOOKING_REQUEST,
            )
            self.memory_service.add_assistant_message(message.sender_id, reply_text)
            outbound_result = self.outbound_service.send_reply(
                platform=message.platform,
                recipient_id=message.sender_id,
                text=reply_text,
            )
            return {
                "intent": "booking_reschedule",
                "routing_category": "consultation_cta",
                "reply_text": reply_text,
                "history": self.memory_service.get_history(message.sender_id),
                "booking_result": booking_result,
                "outbound_result": outbound_result,
            }

        if (
            booking_state == BookingState.NONE
            and self.booking_service.has_confirmed_booking(message.sender_id)
            and self._looks_like_datetime_only_message(message.user_message)
            and not self._looks_like_reschedule_request(message.user_message)
        ):
            reply_text = self.booking_service.get_reschedule_reply(
                self.reply_service.detect_user_language(message.user_message)
            )
            reply_text = self.reply_service.enforce_response_policy(
                reply_text=reply_text,
                user_text=message.user_message,
                intent=IntentType.BOOKING_REQUEST,
            )
            self.memory_service.add_assistant_message(message.sender_id, reply_text)
            outbound_result = self.outbound_service.send_reply(
                platform=message.platform,
                recipient_id=message.sender_id,
                text=reply_text,
            )
            return {
                "intent": "post_booking_reschedule_prompt",
                "routing_category": "consultation_cta",
                "reply_text": reply_text,
                "history": self.memory_service.get_history(message.sender_id),
                "booking_result": None,
                "outbound_result": outbound_result,
            }

        intent = self.intent_service.detect_intent(message.user_message)
        intent_value = intent.value
        logger.info("Intent detected: %s", intent)
        history = self.memory_service.get_history(message.sender_id)
        question_level, question_reason = self.reply_service.classify_question_level(
            user_text=message.user_message,
            intent=intent,
            history=history,
        )
        logger.info("Question level: %s", question_level)
        logger.debug("Question level reason: %s", question_reason)

        force_booking = self._looks_like_booking_message(message.user_message)

        if force_booking and intent != IntentType.BOOKING_REQUEST:
            intent = IntentType.BOOKING_REQUEST
            intent_value = intent.value
            question_level = "mid"
            question_reason = "forced_booking_pattern"
            logger.info("Detected booking intent via forced booking pattern")
            logger.info("Question level: %s", question_level)
            logger.debug("Question level reason: %s", question_reason)

        if intent == IntentType.BOOKING_REQUEST:
            logger.info("Detected booking intent: %s", intent.value)
            logger.info("Calling start_booking_flow for sender_id=%s", message.sender_id)
            booking_result = self.booking_service.start_booking_flow(
                sender_id=message.sender_id,
                message_text=message.user_message,
                source_channel=message.platform,
            )
            logger.info("Booking result used")
            reply_text = booking_result["reply_text"]
            routing_category = "consultation_cta"

        elif question_level == "complex":
            logger.info("Escalation triggered: %s", question_reason)
            language = self.reply_service.detect_user_language(message.user_message)
            reply_text = self.reply_service.get_contextual_complex_reply(
                message.user_message,
                language,
            )
            routing_category = "escalate_to_human"

        elif question_level == "unclear":
            language = self.reply_service.detect_user_language(message.user_message)
            reply_text = self.reply_service.get_safe_fallback_reply(language)
            routing_category = "safe_handoff"

        elif intent not in _STANDARD_SALES_INTENTS:
            reply_text = self.reply_service.generate_reply(message, intent=intent)
            if question_level == "mid":
                routing_category = "consultation_cta"
            else:
                routing_category = "answered_basic"

        else:
            reply_text = self.reply_service.generate_reply(message, intent=intent)
            if question_level == "basic":
                routing_category = "answered_basic"
            else:
                routing_category = "consultation_cta"

        logger.info("Reply before guard: %s", reply_text)
        if booking_result is not None:
            logger.info("Booking result used")
            reply_text = booking_result["reply_text"]
        reply_text = self.reply_service.enforce_response_policy(
            reply_text=reply_text,
            user_text=message.user_message,
            intent=intent,
        )
        if booking_result is None and routing_category != "safe_handoff":
            reply_text = self._finalize_general_reply_text(
                sender_id=message.sender_id,
                user_text=message.user_message,
                reply_text=reply_text,
            )
        logger.info("Reply after guard: %s", reply_text)

        self.memory_service.add_assistant_message(message.sender_id, reply_text)

        outbound_result = self.outbound_service.send_reply(
            platform=message.platform,
            recipient_id=message.sender_id,
            text=reply_text,
        )

        return {
            "intent": intent_value,
            "routing_category": routing_category,
            "reply_text": reply_text,
            "history": self.memory_service.get_history(message.sender_id),
            "booking_result": booking_result,
            "outbound_result": outbound_result,
        }
