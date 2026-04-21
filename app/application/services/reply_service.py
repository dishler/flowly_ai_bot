import logging
import re
from typing import Any, Dict, List, Optional

from app.application.dto.normalized_message import NormalizedMessage
from app.application.services.ai_service import AIService
from app.application.services.knowledge_service import KnowledgeService
from app.application.services.memory_service import MemoryService
from app.domain.enums import IntentType

logger = logging.getLogger(__name__)


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
        has_cyrillic = bool(re.search(r"[А-Яа-яЁёІіЇїЄєҐґ]", text))
        has_latin = bool(re.search(r"[A-Za-z]", text))
        if has_latin and not has_cyrillic:
            return "en"
        if has_cyrillic:
            return "uk"
        return "en"

    def _contains_russian(self, text: str) -> bool:
        lowered = text.lower()
        if any(ch in lowered for ch in ("ё", "ъ", "ы", "э")):
            return True
        russian_markers = [
            "что",
            "это",
            "работает",
            "стоит",
            "входит",
            "только",
            "можем",
            "давайте",
        ]
        return any(marker in lowered for marker in russian_markers)

    def _fallback_for_intent(self, intent: IntentType, language: str) -> str:
        if language == "en":
            if intent == IntentType.PRICE:
                return "Pricing starts from $300 and depends on your case. Want a quick estimate for your workflow?"
            if intent == IntentType.CHANNELS:
                return "We work with Instagram, Facebook, WhatsApp, and Telegram. Which channel is your priority?"
            if intent == IntentType.BOOKING_REQUEST:
                return "Sure, we can arrange a short call. What day and time works best for you?"
            return "Flowly automates replies, lead qualification, and booking in messengers. Want a quick example for your business?"

        if intent == IntentType.PRICE:
            return "Вартість стартує від 300$, але залежить від задач. Хочете, зорієнтую по бюджету під ваш кейс?"
        if intent == IntentType.CHANNELS:
            return "Працюємо з Instagram, Facebook, WhatsApp і Telegram. Який канал для вас пріоритетний?"
        if intent == IntentType.BOOKING_REQUEST:
            return "Так, можемо узгодити короткий дзвінок. Який день і час вам зручні?"
        return "Flowly автоматизує відповіді, кваліфікацію лідів і запис у месенджерах. Хочете короткий приклад під ваш бізнес?"

    def enforce_response_policy(self, reply_text: str, user_text: str, intent: IntentType) -> str:
        language = self._detect_language(user_text)
        if self._contains_russian(reply_text):
            logger.warning("Russian output detected, applying hard language guard")
            return self._fallback_for_intent(intent, language if language == "en" else "uk")
        return reply_text

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
                "Ти AI-асистент компанії Flowly.\n\n"
                "Відповідай ТІЛЬКИ українською мовою, без змішування з іншими мовами.\n\n"
                "Використовуй лише факти з knowledge context, але НЕ копіюй його і НЕ переказуй як список. "
                "Твоя задача — перетворити ці факти у живу, коротку відповідь.\n\n"
                "Стиль:\n"
                "як реальна переписка в Instagram — просто, природно, без канцеляриту і без "
                "“презентаційного” тону.\n\n"
                "Правила:\n"
                "- максимум 3–4 короткі речення\n"
                "- без списків і довгих переліків\n"
                "- не пояснюй все одразу, відповідай тільки на те, що запитали\n"
                "- не повторюй однакову структуру в кожній відповіді\n\n"
                "Адаптація:\n"
                "- якщо питають “що це” — коротко поясни суть\n"
                "- якщо “як працює” — поясни простими словами процес\n"
                "- якщо “для кого” — скажи кому це реально підходить\n\n"
                "Заборонено:\n"
                "- вигадувати\n"
                "- копіювати KB\n"
                "- писати як сайт або презентація\n\n"
                "В кінці (опціонально):\n"
                "додай одну коротку, природну фразу типу:\n"
                "“можемо коротко глянути ваш кейс і підказати, як це буде працювати у вас”"
            )

        return (
            "You are an AI assistant for Flowly.\n\n"
            "Always use one language only, with no language mixing.\n\n"
            "Use only facts from the knowledge context, but do not copy or dump it like a knowledge base. "
            "Turn those facts into a short, natural reply.\n\n"
            "Style:\n"
            "conversational Instagram DM tone, simple and human.\n\n"
            "Rules:\n"
            "- maximum 3-4 short sentences\n"
            "- no bullet points or list-style dumping in the actual reply\n"
            "- answer only what the user asked\n"
            "- avoid repeating the same structure every time\n\n"
            "Adaptation:\n"
            "- for what-is questions, explain the core idea briefly\n"
            "- for how-it-works questions, explain the process in simple words\n"
            "- for for-whom questions, explain realistic fit\n\n"
            "Optional ending:\n"
            "you may add one short, natural soft CTA."
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

        if isinstance(ai_result, dict):
            logger.debug(
                "ReplyService service-query ai_result: used_ai=%s reason=%s has_reply_text=%s",
                ai_result.get("used_ai"),
                ai_result.get("reason"),
                bool(ai_result.get("reply_text")),
            )

        ai_reply_text = ai_result.get("reply_text") if isinstance(ai_result, dict) else None
        if ai_reply_text:
            logger.debug("ReplyService service-query path: OpenAI reply_text returned")
            return str(ai_reply_text)

        if isinstance(ai_result, dict):
            logger.debug(
                "ReplyService service-query fallback used: used_ai=%s reason=%s",
                ai_result.get("used_ai"),
                ai_result.get("reason"),
            )
        return self._get_service_fallback_reply(language)

    def generate_reply(self, message: NormalizedMessage, intent: Optional[IntentType] = None) -> str:
        history = self.memory_service.get_history(message.sender_id)
        text = message.user_message.strip()
        language = self._detect_language(text)
        resolved_intent = intent or IntentType.GENERAL_QUESTION

        if resolved_intent == IntentType.PRICE:
            return self._get_pricing_reply(language)

        if resolved_intent == IntentType.CHANNELS:
            channel_reply = self._get_channel_reply(text, language)
            if channel_reply:
                return channel_reply
            return self._fallback_for_intent(IntentType.CHANNELS, language)

        if resolved_intent == IntentType.SERVICE_DESCRIPTION:
            reply = self._generate_service_ai_reply(
                user_message=message.user_message,
                history=history,
                language=language,
            )
            return reply

        if resolved_intent in {IntentType.CONSULTATION_INTEREST, IntentType.BOOKING_REQUEST}:
            return self._get_consultation_reply(language)

        # Unknown intent fallback only.
        return self._fallback_for_intent(IntentType.SERVICE_DESCRIPTION, language)
        