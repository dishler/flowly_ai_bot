from app.domain.enums import IntentType


class IntentService:
    def detect_intent(self, text: str) -> IntentType:
        normalized = text.strip().lower()

        price_markers = [
            "price",
            "pricing",
            "cost",
            "how much",
            "ціна",
            "скільки",
            "вартість",
            "бюджет",
        ]

        booking_markers = [
            "book",
            "booking",
            "available time",
            "available slot",
            "schedule a call",
            "book a call",
            "set up a call",
            "calendar",
            "slot",
            "appointment",
            "do you have time",
            "what time works",
            "what time do you have",
            "are you free",
            "tomorrow at",
            "today at",
            "monday at",
            "tuesday at",
            "wednesday at",
            "thursday at",
            "friday at",
            "зустріч",
            "забронювати",
            "запис",
            "коли вам зручно",
            "коли можна",
            "слот",
            "час для дзвінка",
            "завтра о",
            "сьогодні о",
        ]

        consultation_markers = [
            "consultation",
            "call",
            "quick call",
            "discuss",
            "let's talk",
            "can we talk",
            "дзвінок",
            "консультація",
            "обговорити",
            "созвон",
        ]

        time_markers = [
            "tomorrow",
            "today",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "11:",
            "12:",
            "13:",
            "14:",
            "15:",
            "16:",
            "17:",
            "завтра",
            "сьогодні",
            "понеділок",
            "вівторок",
            "середа",
            "четвер",
            "п’ятниц",
        ]

        has_booking_marker = any(marker in normalized for marker in booking_markers)
        has_consultation_marker = any(marker in normalized for marker in consultation_markers)
        has_time_marker = any(marker in normalized for marker in time_markers)

        if has_booking_marker:
            return IntentType.BOOKING_REQUEST

        if has_consultation_marker and has_time_marker:
            return IntentType.BOOKING_REQUEST

        if has_consultation_marker:
            return IntentType.CONSULTATION_INTEREST

        if any(marker in normalized for marker in price_markers):
            return IntentType.PRICE

        return IntentType.GENERAL_QUESTION