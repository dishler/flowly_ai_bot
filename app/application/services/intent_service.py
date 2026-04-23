import logging

from app.domain.enums import IntentType

logger = logging.getLogger(__name__)


class IntentService:
    def detect_intent(self, text: str) -> IntentType:
        normalized = text.strip().lower()

        price_markers = [
            "ціна",
            "вартість",
            "скільки коштує",
            "скільки буде",
            "бюджет",
            "цена",
            "стоимость",
            "сколько стоит",
            "сколько будет",
            "price",
            "pricing",
            "cost",
            "how much",
        ]

        channel_markers = [
            "instagram",
            "інстаграм",
            "инстаграм",
            "facebook",
            "фейсбук",
            "whatsapp",
            "telegram",
            "телеграм",
            "канали",
            "каналы",
            "channels",
            "тільки з instagram",
            "только с instagram",
            "only with instagram",
            "only instagram",
            "канал",
            "канали",
        ]

        service_markers = [
            "чим займаєтесь",
            "чим ви займаєтесь",
            "що ви робите",
            "що робите",
            "що входить",
            "що входить у сервіс",
            "для стоматології",
            "для клініки",
            "що включено",
            "что входит",
            "что вы делаете",
            "что входит в сервис",
            "для стоматологии",
            "для клиники",
            "что включено",
            "what do you do",
            "what is included",
            "what's included",
            "what’s included",
            "for dental clinic",
            "for dentistry",
            "what does the service include",
        ]

        booking_markers = [
            "консультація",
            "дзвінок",
            "запис",
            "зустріч",
            "консультация",
            "звонок",
            "запись",
            "встреча",
            "consultation",
            "call",
            "booking",
            "meeting",
        ]

        # Priority: PRICE > CHANNELS > SERVICE_DESCRIPTION > BOOKING > FALLBACK
        if any(marker in normalized for marker in price_markers):
            intent = IntentType.PRICE
        elif any(marker in normalized for marker in channel_markers):
            intent = IntentType.CHANNELS
        elif any(marker in normalized for marker in service_markers):
            intent = IntentType.SERVICE_DESCRIPTION
        elif any(marker in normalized for marker in booking_markers):
            intent = IntentType.BOOKING_REQUEST
        else:
            intent = IntentType.GENERAL_QUESTION

        logger.info("Intent detected: %s | text=%s", intent, text)
        return intent
