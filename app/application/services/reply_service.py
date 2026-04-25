import logging
import re
from typing import Any, Dict, List, Optional, Tuple

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
        return any(re.search(rf"\b{re.escape(marker)}\b", lowered) for marker in russian_markers)

    def _fallback_for_intent(self, intent: IntentType, language: str) -> str:
        if language == "en":
            if intent == IntentType.PRICE:
                return "Pricing starts from $200, depending on the scope. We can briefly discuss your case on a call, or I can pass the request to a specialist."
            if intent == IntentType.CHANNELS:
                return "We work not only with Instagram, but also with Facebook, WhatsApp, and Telegram. If you want, I can suggest which channel would fit your case best."
            if intent == IntentType.SERVICE_DESCRIPTION:
                return self._get_service_description_fallback_reply(language)
            if intent in {IntentType.BOOKING_REQUEST, IntentType.CONSULTATION_INTEREST}:
                return "We can quickly review your request and suggest the best implementation format. Would it be convenient if our specialist contacts you?"
            return "We can set up automated replies, lead qualification, booking, and reminders in messengers. If you want, our specialist can briefly explain how this would look for your case."

        if intent == IntentType.PRICE:
            return "Вартість стартує від 200$, але залежить від задач. Можемо коротко обговорити ваш кейс на дзвінку або я передам запит спеціалісту."
        if intent == IntentType.CHANNELS:
            return "Працюємо не лише з Instagram, а й з Facebook, WhatsApp і Telegram. Можу підказати, що краще підійде саме вам, або наш спеціаліст може коротко проконсультувати."
        if intent == IntentType.SERVICE_DESCRIPTION:
            return self._get_service_description_fallback_reply(language)
        if intent in {IntentType.BOOKING_REQUEST, IntentType.CONSULTATION_INTEREST}:
            return "Можемо коротко обговорити ваш запит і підказати найкращий формат реалізації. Зручно, щоб із вами зв’язався наш спеціаліст?"
        return "Можемо налаштувати автоматичні відповіді, кваліфікацію звернень, запис і нагадування в месенджерах. Якщо хочете, наш спеціаліст може коротко підказати, як це виглядатиме саме для вашого кейсу."

    def get_escalation_reply(self, language: str) -> str:
        if language == "en":
            return "This is a more case-specific question. To give you an accurate answer, one of our specialists can follow up with you shortly."
        return "Це вже більш індивідуальне питання. Щоб дати точну відповідь, можу запропонувати вам короткий дзвінок з нашим спеціалістом. Що скажете?"

    def _is_complex_query(self, normalized: str) -> bool:
        complex_markers = [
            "crm",
            "api",
            "інтеграц",
            "integration",
            "custom",
            "кастом",
            "філі",
            "branches",
            "branch",
            "enterprise",
            "логік",
            "logic",
            "кілька",
            "multiple",
            "system",
            "workflow",
        ]
        return self._contains_any(normalized, complex_markers)

    def get_contextual_complex_reply(self, user_text: str, language: str) -> str:
        normalized = self._normalize(user_text)
        if self._is_complex_query(normalized):
            if language == "en":
                return (
                    "Cases like this usually depend on your workflow logic, CRM, integrations, "
                    "and the number of branches. To give you an accurate answer, I can suggest a short call with our specialist. What do you think?"
                )
            return (
                "Такі кейси вже залежать від вашої логіки роботи, CRM і кількості філій. "
                "Щоб дати точну відповідь, можу запропонувати вам короткий дзвінок з нашим спеціалістом. Що скажете?"
            )
        return self.get_escalation_reply(language)

    def get_safe_fallback_reply(self, language: str) -> str:
        if language == "en":
            return "Thank you for your message. Our specialist will contact you shortly."
        return "Дякую за повідомлення. Наш спеціаліст зв’яжеться з вами найближчим часом."

    def detect_user_language(self, text: str) -> str:
        return self._detect_language(text)

    def evaluate_escalation(self, user_text: str, history: List[str]) -> Tuple[bool, str]:
        """
        Escalate only for non-standard / complex cases. Standard FAQ intents are handled
        elsewhere and must not hit this path.
        """
        normalized = self._normalize(user_text)

        integration_markers = [
            "api",
            "webhook",
            "sdk",
            "endpoint",
            "інтеграція",
            "integration",
            "crm",
        ]
        technical_markers = [
            "техніч",
            "technical",
        ]
        legal_markers = [
            "contract",
            "legal",
            "гаранті",
            "догов",
            "sla",
            "угод",
        ]
        enterprise_markers = [
            "кастом",
            "custom",
            "філій",
            "філія",
            "філіями",
            "ip-телефон",
            "ip телефон",
            "внутрішн",
            "внутрішня система",
            "мережа клінік",
            "мережу клінік",
            "мережею клінік",
            "складна логіка",
            "branches",
            "multi-location",
            "franchise",
            "кілька філій",
            "5 філій",
        ]

        if any(marker in normalized for marker in integration_markers):
            return True, "integration_or_technical_stack"
        if any(marker in normalized for marker in technical_markers):
            return True, "technical_question"
        if any(marker in normalized for marker in legal_markers):
            return True, "legal_or_contract"
        if any(marker in normalized for marker in enterprise_markers):
            return True, "enterprise_or_custom_setup"

        question_marks = user_text.count("?")
        very_long = len(user_text) > 420
        multipart_heavy = question_marks >= 2 and len(user_text) > 140

        if very_long:
            return True, "very_long_message"
        if multipart_heavy:
            return True, "multipart_question"

        user_turns = sum(1 for line in history if line.startswith("user:"))
        if user_turns >= 4:
            return True, "many_turns_unresolved"

        return False, ""

    def should_escalate(self, user_text: str, history: List[str]) -> bool:
        should, _ = self.evaluate_escalation(user_text, history)
        return should

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

    def _get_faq_answer(self, question_uk: str, language: str) -> Optional[str]:
        faq_items = self.knowledge_service.get_all_faq() or []
        for item in faq_items:
            if item.get("question") == question_uk:
                key = "answer_en" if language == "en" else "answer_uk"
                answer = item.get(key)
                if answer:
                    return str(answer)
        return None

    def _get_pricing_reply(self, language: str) -> str:
        if language == "uk":
            return (
                "Вартість стартує від 200$, але залежить від задач, каналів і складності "
                "налаштування. Можемо коротко обговорити ваш кейс на дзвінку або я передам запит спеціалісту."
            )

        faq_answer = self._get_faq_answer("Скільки це коштує?", language)
        if faq_answer:
            return faq_answer
        return self._fallback_for_intent(IntentType.PRICE, language)

    def _get_channel_reply(self, text: str, language: str) -> Optional[str]:
        _ = text
        faq_answer = self._get_faq_answer("З якими каналами ви працюєте?", language)
        if faq_answer:
            return faq_answer
        return self._fallback_for_intent(IntentType.CHANNELS, language)

    def _get_consultation_reply(self, language: str) -> str:
        return self._fallback_for_intent(IntentType.BOOKING_REQUEST, language)

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
            "чим займаєтесь",
            "чим ви займаєтесь",
            "що ви робите",
            "що робите",
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

    def _is_greeting(self, normalized: str) -> bool:
        greeting_markers = [
            "привіт",
            "доброго дня",
            "добрий день",
            "добрий вечір",
            "вітаю",
            "hello",
            "hi",
            "hey",
            "good morning",
            "good afternoon",
            "good evening",
        ]
        return self._contains_any(normalized, greeting_markers)

    def _is_mid_level_query(self, normalized: str) -> bool:
        mid_level_markers = [
            "як це працює",
            "як працює",
            "як швидко запуск",
            "як швидко можна запустити",
            "чи підійде",
            "чи підходить",
            "для клініки",
            "для стоматології",
            "для салону",
            "для бізнесу",
            "for clinic",
            "for dental clinic",
            "is it suitable",
            "how does it work",
            "how fast",
            "how quickly can you launch",
            "how long does launch take",
        ]
        return self._contains_any(normalized, mid_level_markers)

    def _looks_like_question(self, normalized: str, original_text: str) -> bool:
        if "?" in original_text:
            return True

        question_markers = [
            "що",
            "як",
            "чи",
            "скільки",
            "коли",
            "why",
            "what",
            "how",
            "when",
            "can",
            "could",
            "would",
            "do you",
            "does it",
            "is it",
            "are you",
        ]
        return self._contains_any(normalized, question_markers)

    def classify_question_level(
        self,
        user_text: str,
        intent: IntentType,
        history: List[str],
    ) -> Tuple[str, str]:
        normalized = self._normalize(user_text)

        basic_intents = {
            IntentType.PRICE,
            IntentType.CHANNELS,
            IntentType.SERVICE_DESCRIPTION,
        }
        if intent in basic_intents:
            return "basic", intent.value
        if self._is_greeting(normalized):
            return "basic", "greeting"
        if intent in {IntentType.BOOKING_REQUEST, IntentType.CONSULTATION_INTEREST}:
            return "mid", intent.value
        if self._is_complex_query(normalized):
            return "complex", "complex_keywords_detected"

        should_escalate, reason = self.evaluate_escalation(user_text, history)
        if should_escalate:
            return "complex", reason

        if self._is_service_query(normalized) or self._is_mid_level_query(normalized):
            return "mid", "known_product_question"

        if not self._looks_like_question(normalized, user_text):
            return "unclear", "not_clearly_a_question"

        return "mid", "general_non_complex"

    def _get_greeting_reply(self, language: str) -> str:
        if language == "en":
            return (
                "Hello! We set up AI bots for Instagram, Facebook, WhatsApp, and Telegram "
                "so businesses can reply to clients 24/7 and guide them toward booking. "
                "If you want, I can briefly explain how this could work in your case."
            )
        return (
            "Привіт! Ми налаштовуємо AI-ботів для Instagram, Facebook, WhatsApp і Telegram, "
            "щоб бізнес відповідав клієнтам 24/7 і доводив їх до запису. Хочете, коротко "
            "підкажу, як це може працювати у вашому випадку?"
        )

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

    def _is_service_includes_query(self, normalized: str) -> bool:
        includes_markers = [
            "що входить",
            "що входить у сервіс",
            "що входить в сервіс",
            "що включено",
            "what is included",
            "what's included",
            "what does the service include",
        ]
        return self._contains_any(normalized, includes_markers)

    def _get_service_description_fallback_reply(
        self,
        language: str,
        user_text: Optional[str] = None,
    ) -> str:
        normalized = self._normalize(user_text or "")
        if normalized and self._is_service_includes_query(normalized):
            faq_answer = self._get_faq_answer("Що входить у сервіс?", language)
            if faq_answer:
                return faq_answer

        service = self.knowledge_service.get_service_by_id("ai_dm_bot") or {}
        short_description = service.get("short_description", "")
        includes = service.get("includes", [])

        if language == "uk":
            parts: List[str] = []
            if short_description:
                parts.append(short_description)
            else:
                parts.append(
                    "Ми налаштовуємо AI-бота, який відповідає на типові звернення, допомагає кваліфікувати заявки та веде клієнта до запису."
                )
            if includes and normalized and self._is_service_includes_query(normalized):
                parts.append("У сервіс зазвичай входить " + ", ".join(includes[:4]) + ".")
            return " ".join(parts)

        parts = []
        if short_description:
            parts.append(short_description)
        else:
            parts.append(
                "We set up an AI bot that handles common inbound questions, helps qualify leads, and guides clients toward booking."
            )
        if includes and normalized and self._is_service_includes_query(normalized):
            parts.append("The service usually includes " + ", ".join(includes[:4]) + ".")
        return " ".join(parts)

    def _get_niche_fit_reply(self, normalized: str, language: str) -> Optional[str]:
        niche_markers = {
            "dentistry": [
                "стоматолог",
                "стоматологія",
                "стоматологии",
                "dental",
                "dentistry",
            ],
            "clinic": [
                "клінік",
                "клиник",
                "clinic",
            ],
            "auto_service": [
                "автосерв",
                "сто",
                "car service",
                "auto service",
                "repair shop",
            ],
            "beauty_salon": [
                "салон краси",
                "б'юті",
                "бюті",
                "beauty salon",
                "beauty studio",
            ],
        }

        matched_dentistry = self._contains_any(normalized, niche_markers["dentistry"])
        matched_clinic = self._contains_any(normalized, niche_markers["clinic"])
        matched_auto_service = self._contains_any(normalized, niche_markers["auto_service"])
        matched_beauty_salon = self._contains_any(normalized, niche_markers["beauty_salon"])
        if not matched_dentistry and not matched_clinic and not matched_auto_service and not matched_beauty_salon:
            return None

        if language == "en":
            if matched_dentistry:
                return (
                    "Yes, it is a good fit for dental practices. The bot can help with booking, "
                    "answer common patient questions, and send visit reminders."
                )
            if matched_clinic:
                return (
                    "Yes, it is a good fit for clinics. The bot can help with booking, answer common "
                    "questions, and remind clients about upcoming visits."
                )
            if matched_auto_service:
                return (
                    "Yes, it can be a good fit for a car service. The bot can answer common questions, "
                    "help with booking, and pass requests to your specialist."
                )
            return (
                "Yes, it can work well for a beauty salon. The bot can help with booking, answer "
                "common questions, and remind clients about upcoming visits."
            )

        if matched_auto_service:
            return (
                "Так, для автосервісу це може добре підійти — бот може відповідати на типові "
                "питання, допомагати з записом і передавати заявки спеціалісту. Можу коротко "
                "підказати, як це виглядало б саме для вашого сервісу."
            )

        if matched_dentistry:
            return (
                "Так, добре підходить для стоматологій — бот може допомагати з записом, "
                "відповідати на типові питання і нагадувати про візити."
            )
        if matched_auto_service:
            return (
                "Так, для автосервісу це може добре підійти — бот може відповідати на типові "
                "питання, допомагати з записом і передавати заявки спеціалісту. Можу коротко "
                "підказати, як це виглядало б саме для вашого сервісу."
            )
        if matched_beauty_salon:
            return (
                "Так, для салону краси це може добре підійти — бот може допомагати з записом, "
                "відповідати на типові питання і нагадувати про візити. Можу коротко підказати, "
                "як це працювало б саме у вашому випадку."
            )
        if matched_clinic:
            return (
            "Так, добре підходить для клінік — бот може допомагати з записом, відповідати на "
            "типові питання і нагадувати про візити."
            )
        return None

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
        text = message.user_message.strip()
        language = self._detect_language(text)
        resolved_intent = intent or IntentType.GENERAL_QUESTION
        normalized = self._normalize(text)

        if resolved_intent == IntentType.PRICE:
            return self._get_pricing_reply(language)

        if resolved_intent == IntentType.CHANNELS:
            channel_reply = self._get_channel_reply(text, language)
            if channel_reply:
                return channel_reply
            return self._fallback_for_intent(IntentType.CHANNELS, language)

        if resolved_intent == IntentType.SERVICE_DESCRIPTION:
            return self._get_service_description_fallback_reply(language, text)

        if resolved_intent in {IntentType.CONSULTATION_INTEREST, IntentType.BOOKING_REQUEST}:
            return self._get_consultation_reply(language)

        if self._is_greeting(normalized):
            return self._get_greeting_reply(language)

        niche_fit_reply = self._get_niche_fit_reply(normalized, language)
        if niche_fit_reply:
            return niche_fit_reply

        if self._is_service_query(normalized) or self._is_mid_level_query(normalized):
            history = self.memory_service.get_history(message.sender_id)
            return self._generate_service_ai_reply(
                user_message=text,
                history=history,
                language=language,
            )

        # Unknown intent fallback only.
        return self._fallback_for_intent(IntentType.SERVICE_DESCRIPTION, language)
        
