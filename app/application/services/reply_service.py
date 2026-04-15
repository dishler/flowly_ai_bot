from app.application.dto.normalized_message import NormalizedMessage
from app.application.services.ai_service import AIService
from app.application.services.knowledge_service import KnowledgeService
from app.application.services.memory_service import MemoryService


class ReplyService:
    def __init__(
        self,
        ai_service: AIService,
        memory_service: MemoryService,
        knowledge_service: KnowledgeService,
    ) -> None:
        self.ai_service = ai_service
        self.memory_service = memory_service
        self.knowledge_service = knowledge_service

    def _detect_language(self, text: str) -> str:
        for ch in text:
            if ch in "АБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЬЮЯабвгґдеєжзиіїйклмнопрстуфхцчшщьюя":
                return "uk"
        return "en"

    def _get_pricing_reply(self, language: str) -> str:
        pricing = self.knowledge_service.get_pricing()
        starting_from = pricing.get("starting_from_usd", 300)

        if language == "uk":
            return (
                f"Вартість стартує від {starting_from}$, але точна ціна залежить від формату бізнесу, "
                f"кількості послуг, каналів звернень і складності налаштування. "
                f"Можемо запропонувати коротку безкоштовну консультацію, щоб підібрати оптимальний варіант."
            )

        return (
            f"Pricing starts from ${starting_from}, but the exact cost depends on your business format, "
            f"number of services, inquiry channels, and setup complexity. "
            f"We can offer a short free consultation to suggest the best option."
        )

    def _get_service_reply(self, language: str) -> str:
        service = self.knowledge_service.get_service_by_id("ai_dm_bot")
        if not service:
            if language == "uk":
                return "Ми допомагаємо автоматизувати обробку вхідних звернень і запис клієнтів."
            return "We help automate inbound communication and client booking."

        short_description = service.get("short_description", "")
        typical_result = service.get("typical_result", [])

        if language == "uk":
            result_part = ""
            if typical_result:
                result_part = f" Зазвичай це дає: {', '.join(typical_result[:3])}."
            return f"{short_description}{result_part}".strip()

        result_part = ""
        if typical_result:
            result_part = f" Typical outcomes include: {', '.join(typical_result[:3])}."
        return f"{short_description}{result_part}".strip()

    def _get_channel_reply(self, text: str, language: str) -> str | None:
        normalized = text.lower()

        if "instagram" in normalized and "facebook" in normalized:
            if language == "uk":
                return "Так, можемо допомогти і з Instagram, і з Facebook."
            return "Yes, we can help with both Instagram and Facebook."

        if "instagram" in normalized:
            if language == "uk":
                return "Так, можемо допомогти з Instagram."
            return "Yes, we can help with Instagram."

        if "facebook" in normalized:
            if language == "uk":
                return "Так, можемо допомогти з Facebook."
            return "Yes, we can help with Facebook."

        return None

    def generate_reply(self, message: NormalizedMessage) -> str:
        history = self.memory_service.get_history(message.sender_id)
        text = message.user_message.strip()
        normalized = text.lower()
        language = self._detect_language(text)

        faq_answer = self.knowledge_service.find_faq_answer(text, language=language)
        if faq_answer:
            return faq_answer

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
        if any(marker in normalized for marker in price_markers):
            return self._get_pricing_reply(language)

        service_markers = [
            "що це",
            "що ви робите",
            "як це працює",
            "що входить",
            "what is this",
            "what do you do",
            "how does it work",
            "what's included",
            "що за сервіс",
            "розкажіть про сервіс",
        ]
        if any(marker in normalized for marker in service_markers):
            return self._get_service_reply(language)

        channel_reply = self._get_channel_reply(normalized, language)
        if channel_reply:
            return channel_reply

        consultation_markers = [
            "call",
            "consultation",
            "book",
            "booking",
            "дзвінок",
            "консультац",
            "зідзвон",
            "созвон",
        ]
        if any(marker in normalized for marker in consultation_markers):
            if language == "uk":
                return "Можу запропонувати коротку безкоштовну консультацію. Напишіть, будь ласка, зручний день і час."
            return "I can offer a short free consultation. Please send a convenient day and time."

        ai_result = self.ai_service.try_generate_reply(
            user_message=message.user_message,
            history=history,
        )

        ai_reply_text = ai_result.get("reply_text")
        if ai_reply_text:
            return str(ai_reply_text)

        if language == "uk":
            return "Дякую. Можете коротко описати вашу задачу?"
        return "Thanks. Could you briefly describe your request?"