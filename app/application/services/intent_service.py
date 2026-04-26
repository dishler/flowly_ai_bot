import logging
import re

from app.domain.enums import IntentType

logger = logging.getLogger(__name__)


class IntentService:
    def _contains_any(self, normalized: str, markers: list[str]) -> bool:
        return any(marker in normalized for marker in markers)

    def _contains_price_marker(self, normalized: str, markers: list[str]) -> bool:
        for marker in markers:
            if re.search(rf"(?<![A-Za-zА-Яа-яІіЇїЄєҐґ]){re.escape(marker)}(?![A-Za-zА-Яа-яІіЇїЄєҐґ])", normalized):
                return True
        return False

    def _normalize_for_matching(self, text: str) -> str:
        normalized = " ".join(text.strip().lower().split())
        return re.sub(r"([аеєиіїоуюя])\1+", r"\1", normalized)

    def detect_intent(self, text: str) -> IntentType:
        normalized = self._normalize_for_matching(text)

        price_markers = [
            "ціна",
            "ціні",
            "що по ціні",
            "вартість",
            "скільки коштує",
            "скільки це коштує",
            "яка ціна",
            "скільки коштує бот",
            "прайс",
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
            "що це",
            "що це у вас",
            "що це за сервіс",
            "за сервіс",
            "чим займаєтесь",
            "чим ви займаєтесь",
            "що ви робите",
            "що робите",
            "що ви пропонуєте",
            "що пропонуєте",
            "що можете запропонувати",
            "яка у вас пропозиція",
            "що робить ваш бот",
            "що робить бот",
            "що вміє бот",
            "як працює бот",
            "що входить",
            "що входить у сервіс",
            "для стоматології",
            "для клініки",
            "що включено",
            "а для кого",
            "для кого це",
            "для кого цей",
            "кому це підходить",
            "для кого підходить",
            "що конкретно робить",
            "як це працює",
            "як ви працюєте",
            "які функції",
            "розкажіть про",
            "розкажіть детальніше",
            "пояснити ваш сервіс",
            "поясніть ваш сервіс",
            "поясніть сервіс",
            "можете коротко пояснити",
            "хочу зрозуміти що за бот",
            "що за бот",
            "що саме ви робите",
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

        industry_markers = [
            "з якими напрямками працюєте",
            "з якими бізнесами працюєте",
            "для яких сфер",
            "для яких ніш",
            "кому це підходить",
            "для кого це",
            "які напрями",
            "які напрямки",
            "які ніші",
            "кому підходить бот",
            "з якими напрямами",
        ]

        use_case_markers = [
            "а є якісь кейси",
            "є якісь кейси",
            "є кейси",
            "кейси",
            "покажіть кейси",
            "покажи кейси",
            "є приклади впроваджень",
            "приклади впроваджень",
            "які є приклади",
            "приклади",
            "для кого це працює",
            "use cases",
            "case studies",
            "examples",
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

        interest_markers = [
            "цікаво",
            "звучить цікаво",
            "ок цікаво",
            "ок, цікаво",
            "гаразд цікаво",
            "можливо цікаво",
            "хм цікаво",
            "хм, цікаво",
        ]

        rejection_markers = [
            "ні",
            "не цікаво",
            "не треба",
            "ні дякую",
            "ні, дякую",
            "поки ні",
            "не актуально",
        ]

        frustrated_markers = [
            "ти дебіл",
            "що за хуйня",
            "що за херня",
            "шо за херня",
            "ідіот",
            "идиот",
            "дебіл",
            "дебил",
            "хуйня",
            "херня",
        ]

        # Priority: REJECTION > FRUSTRATED > PRICE > CHANNELS > INDUSTRIES > SERVICE_DESCRIPTION > USE_CASES > INTEREST > BOOKING > FALLBACK
        if normalized in rejection_markers:
            intent = IntentType.REJECTION
        elif self._contains_any(normalized, frustrated_markers):
            intent = IntentType.FRUSTRATED
        elif self._contains_price_marker(normalized, price_markers):
            intent = IntentType.PRICE
        elif self._contains_any(normalized, channel_markers):
            intent = IntentType.CHANNELS
        elif self._contains_any(normalized, industry_markers):
            intent = IntentType.INDUSTRIES
        elif self._contains_any(normalized, service_markers):
            intent = IntentType.SERVICE_DESCRIPTION
        elif self._contains_any(normalized, use_case_markers):
            intent = IntentType.USE_CASES
        elif self._contains_any(normalized, interest_markers):
            intent = IntentType.INTEREST_SIGNAL
        elif self._contains_any(normalized, booking_markers):
            intent = IntentType.BOOKING_REQUEST
        else:
            intent = IntentType.GENERAL_QUESTION

        logger.info("Intent detected: %s | text=%s", intent, text)
        return intent
