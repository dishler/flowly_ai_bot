from typing import Any, Dict, List, Optional

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

    def _normalize(self, text: str) -> str:
        return " ".join(text.lower().strip().split())

    def _contains_any(self, text: str, markers: List[str]) -> bool:
        return any(marker in text for marker in markers)

    def _get_pricing_reply(self, language: str) -> str:
        pricing = self.knowledge_service.get_pricing() or {}
        starting_from = pricing.get("starting_from_usd", 300)

        if language == "uk":
            return (
                f"Вартість стартує від {starting_from}$, але точна ціна залежить від формату бізнесу, "
                f"кількості послуг, каналів звернень, обсягу автоматизації та складності налаштування. "
                f"Можемо запропонувати коротку безкоштовну консультацію, щоб зрозуміти ваш кейс і "
                f"запропонувати оптимальний варіант."
            )

        return (
            f"Pricing starts from ${starting_from}, but the exact cost depends on your business format, "
            f"number of services, inquiry channels, automation scope, and setup complexity. "
            f"We can offer a short free consultation to understand your case and suggest the best option."
        )

    def _get_channel_reply(self, text: str, language: str) -> Optional[str]:
        normalized = self._normalize(text)

        has_instagram = "instagram" in normalized
        has_facebook = "facebook" in normalized

        if has_instagram and has_facebook:
            if language == "uk":
                return "Так, можемо допомогти і з Instagram, і з Facebook."
            return "Yes, we can help with both Instagram and Facebook."

        if has_instagram:
            if language == "uk":
                return "Так, можемо допомогти з Instagram."
            return "Yes, we can help with Instagram."

        if has_facebook:
            if language == "uk":
                return "Так, можемо допомогти з Facebook."
            return "Yes, we can help with Facebook."

        return None

    def _get_consultation_reply(self, language: str) -> str:
        consultation = self.knowledge_service.get_consultation() or {}
        duration = consultation.get("duration_minutes", 30)

        if language == "uk":
            return (
                f"Можемо запропонувати коротку безкоштовну консультацію на {duration} хвилин, "
                f"щоб зрозуміти ваш запит і подивитися, чи підійде вам Flowly. "
                f"Напишіть, будь ласка, зручний день і час."
            )

        return (
            f"We can offer a short free {duration}-minute consultation to understand your request "
            f"and see whether Flowly is a good fit for your business. "
            f"Please send a convenient day and time."
        )

    def _is_price_query(self, normalized: str) -> bool:
        price_markers = [
            "price",
            "pricing",
            "cost",
            "how much",
            "how much does it cost",
            "what does it cost",
            "ціна",
            "скільки",
            "вартість",
            "бюджет",
            "скільки коштує",
        ]
        return self._contains_any(normalized, price_markers)

    def _is_service_query(self, normalized: str) -> bool:
        service_markers = [
            "що це",
            "що ви робите",
            "як це працює",
            "що входить",
            "що входить у сервіс",
            "що входить в сервіс",
            "що за сервіс",
            "розкажіть про сервіс",
            "розкажіть детальніше",
            "для кого це",
            "кому це підходить",
            "як ви працюєте",
            "що саме ви робите",
            "what is this",
            "what do you do",
            "how does it work",
            "what's included",
            "what is included",
            "what does it include",
            "tell me about the service",
            "tell me more about the service",
            "who is it for",
            "how do you work",
            "what exactly do you do",
        ]
        return self._contains_any(normalized, service_markers)

    def _is_consultation_query(self, normalized: str) -> bool:
        consultation_markers = [
            "call",
            "consultation",
            "book",
            "booking",
            "schedule",
            "meeting",
            "дзвінок",
            "консультац",
            "зідзвон",
            "созвон",
            "зустріч",
            "забронювати",
            "запис",
        ]
        return self._contains_any(normalized, consultation_markers)

    def _build_service_grounding_context(self, language: str) -> Dict[str, Any]:
        company = self.knowledge_service.get_company() or {}
        service = self.knowledge_service.get_service_by_id("ai_dm_bot") or {}
        pricing = self.knowledge_service.get_pricing() or {}
        consultation = self.knowledge_service.get_consultation() or {}
        constraints = self.knowledge_service.get_constraints() or {}

        return {
            "language": language,
            "company": {
                "name": company.get("name"),
                "short_description": company.get("short_description"),
                "tone": company.get("tone"),
                "languages": company.get("languages", []),
            },
            "service": {
                "id": service.get("id"),
                "name": service.get("name"),
                "short_description": service.get("short_description"),
                "for_whom": service.get("for_whom", []),
                "solves": service.get("solves", []),
                "includes": service.get("includes", []),
                "does_not_include": service.get("does_not_include", []),
                "typical_result": service.get("typical_result", []),
            },
            "pricing": {
                "starting_from_usd": pricing.get("starting_from_usd"),
                "pricing_note": pricing.get("pricing_note"),
                "what_affects_price": pricing.get("what_affects_price", []),
                "how_to_answer_price_questions": pricing.get("how_to_answer_price_questions"),
            },
            "consultation": {
                "duration_minutes": consultation.get("duration_minutes"),
                "goal": consultation.get("goal"),
                "cta_soft": consultation.get("cta_soft"),
            },
            "constraints": constraints,
        }

    def _get_service_system_instruction(self, language: str) -> str:
        if language == "uk":
            return (
                "Ти AI-асистент компанії Flowly. "
                "Відповідай ТІЛЬКИ на основі переданого knowledge context. "
                "Нічого не вигадуй. "
                "Відповідай коротко, природно, спокійно, по суті та мовою користувача. "
                "Пояснюй, що це за сервіс, як він працює, що входить, для кого він підходить і який типовий результат. "
                "Не називай фіксовану ціну для всіх, якщо користувач питає не прямо про ціну. "
                "Не обіцяй гарантовані результати, продажі чи інтеграції, які не підтверджені. "
                "За потреби м’яко запропонуй коротку безкоштовну консультацію."
            )

        return (
            "You are an AI assistant for Flowly. "
            "Answer ONLY using the provided knowledge context. "
            "Do not invent anything. "
            "Reply briefly, naturally, calmly, practically, and in the user's language. "
            "Explain what the service is, how it works, what is included, who it is for, and the typical result. "
            "Do not present a fixed universal price unless the user directly asks about pricing. "
            "Do not promise guaranteed results, sales, or unsupported integrations. "
            "If relevant, softly suggest a short free consultation."
        )

    def _get_service_fallback_reply(self, language: str) -> str:
        service = self.knowledge_service.get_service_by_id("ai_dm_bot") or {}
        short_description = service.get("short_description", "")
        includes = service.get("includes", [])
        typical_result = service.get("typical_result", [])
        for_whom = service.get("for_whom", [])

        if language == "uk":
            parts: List[str] = []

            if short_description:
                parts.append(short_description)
            else:
                parts.append(
                    "Це AI-асистент для Instagram і Facebook DM, який допомагає автоматизувати "
                    "обробку вхідних звернень і вести клієнта до запису."
                )

            if includes:
                parts.append("Зазвичай у сервіс входить: " + ", ".join(includes[:5]) + ".")

            if for_whom:
                parts.append("Найкраще підходить для: " + ", ".join(for_whom[:3]) + ".")

            if typical_result:
                parts.append("Типовий результат: " + ", ".join(typical_result[:3]) + ".")

            parts.append("Якщо хочете, можемо коротко подивитися ваш кейс на безкоштовній консультації.")

            return " ".join(parts)

        parts = []

        if short_description:
            parts.append(short_description)
        else:
            parts.append(
                "It is an AI assistant for Instagram and Facebook DMs that helps automate "
                "inbound communication and guide clients toward booking."
            )

        if includes:
            parts.append("It usually includes: " + ", ".join(includes[:5]) + ".")

        if for_whom:
            parts.append("It is best suited for: " + ", ".join(for_whom[:3]) + ".")

        if typical_result:
            parts.append("Typical results include: " + ", ".join(typical_result[:3]) + ".")

        parts.append("If you want, we can take a quick look at your case during a free consultation.")

        return " ".join(parts)

    def _generate_service_ai_reply(
        self,
        user_message: str,
        history: List[Dict[str, Any]],
        language: str,
    ) -> str:
        grounding_context = self._build_service_grounding_context(language=language)
        system_instruction = self._get_service_system_instruction(language=language)

        try:
            ai_result = self.ai_service.try_generate_reply(
                user_message=user_message,
                history=history,
                grounding_context=grounding_context,
                system_instruction=system_instruction,
            )
        except TypeError:
            # Fallback for the current AIService signature if it still only accepts
            # user_message and history.
            ai_result = self.ai_service.try_generate_reply(
                user_message=user_message,
                history=history,
            )

        ai_reply_text = ai_result.get("reply_text") if isinstance(ai_result, dict) else None
        if ai_reply_text:
            return str(ai_reply_text)

        return self._get_service_fallback_reply(language)

    def generate_reply(self, message: NormalizedMessage) -> str:
        history = self.memory_service.get_history(message.sender_id)
        text = message.user_message.strip()
        normalized = self._normalize(text)
        language = self._detect_language(text)

        faq_answer = self.knowledge_service.find_faq_answer(text, language=language)
        if faq_answer:
            return faq_answer

        if self._is_price_query(normalized):
            return self._get_pricing_reply(language)

        if self._is_service_query(normalized):
            return self._generate_service_ai_reply(
                user_message=message.user_message,
                history=history,
                language=language,
            )

        channel_reply = self._get_channel_reply(text, language)
        if channel_reply:
            return channel_reply

        if self._is_consultation_query(normalized):
            return self._get_consultation_reply(language)

        try:
            ai_result = self.ai_service.try_generate_reply(
                user_message=message.user_message,
                history=history,
            )
        except TypeError:
            ai_result = {"reply_text": None}

        ai_reply_text = ai_result.get("reply_text") if isinstance(ai_result, dict) else None
        if ai_reply_text:
            return str(ai_reply_text)

        if language == "uk":
            return "Дякую. Можете коротко описати вашу задачу?"
        return "Thanks. Could you briefly describe your request?"
        