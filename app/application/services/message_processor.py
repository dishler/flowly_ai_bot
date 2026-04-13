from typing import Any, Dict

from app.application.dto.normalized_message import NormalizedMessage
from app.application.services.booking_service import BookingService
from app.application.services.intent_service import IntentService
from app.application.services.memory_service import MemoryService
from app.application.services.outbound_service import OutboundService
from app.application.services.reply_service import ReplyService
from app.domain.enums import IntentType


class MessageProcessor:
    def __init__(
        self,
        memory_service: MemoryService,
        reply_service: ReplyService,
        outbound_service: OutboundService,
        intent_service: IntentService,
        booking_service: BookingService,
    ) -> None:
        self.memory_service = memory_service
        self.reply_service = reply_service
        self.outbound_service = outbound_service
        self.intent_service = intent_service
        self.booking_service = booking_service

    def process(self, message: NormalizedMessage) -> Dict[str, Any]:
        self.memory_service.add_user_message(message.sender_id, message.user_message)

        booking_result = None
        reply_text = ""
        intent = self.intent_service.detect_intent(message.user_message)
        intent_value = intent.value

        has_pending_confirmation = self.booking_service.has_pending_confirmation(message.sender_id)

        # 1. If user sends a NEW booking request while pending confirmation exists,
        # treat it as a fresh booking request instead of forcing confirmation flow.
        if intent == IntentType.BOOKING_REQUEST:
            booking_result = self.booking_service.handle_booking_request(
                sender_id=message.sender_id,
                message_text=message.user_message,
            )
            reply_text = booking_result["reply_text"]

        # 2. If there is pending confirmation and this is not a new booking request,
        # try confirmation / rejection flow.
        elif has_pending_confirmation:
            booking_result = self.booking_service.handle_booking_confirmation(
                sender_id=message.sender_id,
                message_text=message.user_message,
            )
            intent_value = "booking_confirmation"

            if booking_result is not None:
                reply_text = booking_result["reply_text"]
            else:
                reply_text = self.reply_service.generate_reply(message)

        # 3. Normal non-booking flow.
        else:
            reply_text = self.reply_service.generate_reply(message)

        self.memory_service.add_assistant_message(message.sender_id, reply_text)

        outbound_result = self.outbound_service.send_reply(
            platform=message.platform,
            recipient_id=message.sender_id,
            text=reply_text,
        )

        return {
            "intent": intent_value,
            "reply_text": reply_text,
            "history": self.memory_service.get_history(message.sender_id),
            "booking_result": booking_result,
            "outbound_result": outbound_result,
        }