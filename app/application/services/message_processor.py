from __future__ import annotations

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
        IntentType.INDUSTRIES,
        IntentType.USE_CASES,
        IntentType.INTEREST_SIGNAL,
        IntentType.REJECTION,
        IntentType.FRUSTRATED,
        IntentType.HESITATION,
        IntentType.BUYING_SIGNAL,
        IntentType.START_REQUIREMENTS,
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

    def _normalize_for_booking_keywords(self, text: str) -> str:
        normalized = " ".join(text.strip().lower().split())
        return re.sub(r"([а-яіїєґ])\1+", r"\1", normalized)

    def _contains_booking_keyword(self, normalized: str, keyword: str) -> bool:
        if keyword == "кол":
            return bool(
                re.search(
                    r"(?<![A-Za-zА-Яа-яІіЇїЄєҐґ])кол(?![A-Za-zА-Яа-яІіЇїЄєҐґ])",
                    normalized,
                )
            )
        return keyword in normalized

    def _contains_booking_action_word(self, normalized: str, word: str) -> bool:
        if word in {"ок", "ok", "go"}:
            return bool(
                re.search(
                    rf"(?<![A-Za-zА-Яа-яІіЇїЄєҐґ]){re.escape(word)}(?![A-Za-zА-Яа-яІіЇїЄєҐґ])",
                    normalized,
                )
            )
        return word in normalized

    def _looks_like_booking_message(self, text: str) -> bool:
        normalized = text.strip().lower()
        keyword_normalized = self._normalize_for_booking_keywords(text)
        no_call_markers = [
            "без дзвінка",
            "без дзвонка",
            "не хочу дзвінок",
            "не хочу дзвонок",
            "не хочу консультацію",
            "пояснити без",
            "в тексті",
            "текстом",
            "поясни тут",
            "поясніть тут",
            "давайте тут",
        ]
        if any(marker in keyword_normalized for marker in no_call_markers):
            return False

        consultation_words = [
            "consultation",
            "call",
            "quick call",
            "дзвінок",
            "дзвін",
            "консультац",
            "обговорити",
            "созвон",
            "зідзвон",
            "зідзвонитися",
            "зідзвонитись",
            "кол",
        ]

        call_request_words = [
            "давай",
            "давайте",
            "хочу",
            "можна",
            "можемо",
            "ок",
            "окей",
            "потрібен",
            "потрібно",
            "запишіть",
            "записати",
            "забронювати",
            "брон",
            "плануємо",
            "обговоримо",
            "go",
            "yes",
            "ok",
            "okay",
            "lets",
            "let's",
            "want",
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

        has_consultation = any(
            self._contains_booking_keyword(normalized, word)
            or self._contains_booking_keyword(keyword_normalized, word)
            for word in consultation_words
        )
        has_call_request = any(
            self._contains_booking_action_word(keyword_normalized, word)
            for word in call_request_words
        )
        has_date_word = any(word in normalized or word in keyword_normalized for word in date_words)
        has_time = bool(
            re.search(r"\b([01]?\d|2[0-3]):[0-5]\d\b", normalized)
            or re.search(r"\b(10|11|12|13|14|15|16|17|18|19|20|21|22|23)\b", normalized)
            or re.search(r"\b(о|на)\s*(10|11|12|13|14|15|16|17|18|19|20|21|22|23)\b", normalized)
        )

        if has_consultation and (has_date_word or has_time):
            return True

        if has_consultation and has_call_request:
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

    def _looks_like_cancel_request(self, text: str) -> bool:
        normalized = text.strip().lower()
        markers = [
            "скасуйте",
            "скасувати",
            "відмінити",
            "відмініть",
            "не зможу",
            "cancel",
            "cancel the call",
            "cancel my call",
        ]
        return any(marker in normalized for marker in markers)

    def _looks_like_availability_question(self, text: str) -> bool:
        normalized = text.strip().lower()
        markers = [
            "вільні слоти",
            "вільний слот",
            "вільний час",
            "вільні години",
            "доступні слоти",
            "які слоти",
            "який є вільний час",
            "які є варіанти",
            "варіанти по часу",
            "коли є",
            "коли можна",
            "коли можна зідзвонитись",
            "коли можна зідзвонитися",
            "коли ви вільні",
            "коли вільно",
            "коли вільні",
            "вільно",
            "в який час",
            "available slots",
            "free slots",
            "free time",
            "what slots",
            "what time options",
            "when are you available",
        ]
        return any(marker in normalized for marker in markers)

    def _looks_like_call_explanation_question(self, text: str) -> bool:
        normalized = text.strip().lower()
        markers = [
            "що саме буде",
            "що буде на дзвінку",
            "що на дзвінку",
            "про що дзвінок",
            "про що буде дзвінок",
            "що буде на консультації",
            "what will be on the call",
            "what is the call about",
            "what happens on the call",
        ]
        return any(marker in normalized for marker in markers)

    def _normalize_for_conversation_matching(self, text: str) -> str:
        normalized = " ".join(text.strip().lower().split())
        return re.sub(r"([аеєиіїоуюя])\1+", r"\1", normalized)

    def _looks_like_language_request(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        markers = [
            "можно на русском",
            "можете на русском",
            "на русском",
            "російською",
            "по русски",
            "по-русски",
        ]
        return any(marker in normalized for marker in markers)

    def _looks_like_noise_only_message(self, text: str) -> bool:
        stripped = text.strip()
        if not stripped:
            return True
        return not bool(re.search(r"[A-Za-zА-Яа-яЁёІіЇїЄєҐґ0-9]", stripped))

    def _looks_like_product_question_during_booking(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        product_markers = [
            "що ви пропонуєте",
            "що ви робите",
            "що робите",
            "що робить",
            "як це працює",
            "для кого",
            "кому підходить",
            "з якими напрям",
            "з якими бізнес",
            "скільки коштує",
            "ціна",
            "вартість",
            "кейси",
            "приклади",
            "канали",
            "бот",
            "інстаграм",
            "телеграм",
            "instagram",
            "facebook",
            "whatsapp",
            "viber",
            "вайбер",
            "telegram",
            "вночі",
            "ніч",
            "впроваджен",
            "запуск",
            "як довго",
            "скільки часу",
            "термін",
            "гаранті",
            "crm",
            "інтеграц",
            "заявки менеджеру",
            "передає заявки",
            "передавати заявки",
            "що бот буде питати",
            "що буде питати",
            "ціни на ремонт",
            "рахувати ремонт",
            "це дорого",
            "дорого",
            "сто",
            "автосерв",
            "салон",
            "салон краси",
            "стомат",
            "клінік",
            "записує",
            "записувати",
            "записуєте",
            "записує до",
            "запис до",
            "на прийом",
            "прийом",
            "майстр",
            "календар",
            "пацієнт",
            "відповідати пацієнтам",
        ]
        return any(marker in normalized for marker in product_markers)

    def _looks_like_capability_question(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        if "нестандарт" in normalized:
            return False
        if "тільки" in normalized and any(
            marker in normalized
            for marker in ["instagram", "інстаграм", "інсті", "інсту", "facebook", "фейсбук", "telegram", "телеграм", "whatsapp", "viber", "вайбер"]
        ):
            return False
        has_question_shape = (
            "?" in text
            or any(
                marker in normalized
                for marker in [
                    "може",
                    "можна",
                    "чи",
                    "а якщо",
                    "що буде",
                    "як буде",
                    "якщо",
                    "буде",
                    "вміє",
                    "працює",
                    "працювати",
                    "підключити",
                    "підключається",
                    "передає",
                ]
            )
        )
        capability_markers = [
            "instagram",
            "інстаграм",
            "інсті",
            "інсту",
            "facebook",
            "фейсбук",
            "telegram",
            "телеграм",
            "телега",
            "телезі",
            "whatsapp",
            "ватсап",
            "вотсап",
            "viber",
            "вайбер",
            "записує",
            "записувати",
            "записуєте",
            "запис до",
            "запис на",
            "до майстра",
            "майстр",
            "календар",
            "прийом",
            "пацієнт",
            "відповідати пацієнтам",
            "відповідати вночі",
            "відповідати в інстаграм",
            "заявки менеджеру",
            "заявки менеджер",
            "передає заявки",
            "передавати заявки",
            "передає менеджеру",
            "передавати менеджеру",
            "crm",
            "сrm",
            "інтеграц",
            "питати в клієнта",
            "що бот буде питати",
            "що буде питати",
            "рахувати ціну",
            "рахувати ремонт",
            "ціни на ремонт",
            "точну ціну",
            "пише незрозуміло",
            "пише криво",
            "незрозуміло",
            "вночі",
            "уночі",
            "24/7",
        ]
        explicit_booking_markers = [
            "хочу записатися",
            "хочу записатись",
            "запишіть мене",
            "забронювати час",
            "давайте дзвінок",
            "давайте кол",
            "хочу консультацію",
            "можемо зідзвон",
        ]
        if any(marker in normalized for marker in explicit_booking_markers):
            return False
        return has_question_shape and any(marker in normalized for marker in capability_markers)

    def _capability_cta_mode(self, text: str) -> str:
        normalized = self._normalize_for_conversation_matching(text)
        soft_markers = [
            "crm",
            "сrm",
            "інтеграц",
            "календар",
            "заявки менеджеру",
            "заявки менеджер",
            "передає заявки",
            "передавати заявки",
            "передає менеджеру",
            "передавати менеджеру",
            "вночі",
            "уночі",
            "24/7",
            "пише криво",
            "пише незрозуміло",
            "незрозуміло",
            "рахувати ціну",
            "рахувати ремонт",
            "ціни на ремонт",
            "точну ціну",
            "у нас",
            "в нас",
            "наш",
            "наші",
        ]
        return "soft_cta" if any(marker in normalized for marker in soft_markers) else "no_cta"

    def _append_capability_cta(self, reply_text: str, cta_mode: str) -> str:
        if cta_mode != "soft_cta":
            return reply_text
        if "CRM" in reply_text:
            cta = "Можемо прикинути під ваш кейс, якщо напишете, яку CRM використовуєте."
        elif "календар" in reply_text.lower():
            cta = "Можемо розкласти під ваш процес, щоб не ламати ваші правила запису."
        elif "Telegram" in reply_text:
            cta = "Можу підказати під вашу ситуацію, якщо напишете, де зараз найбільше звернень."
        elif "Viber" in reply_text:
            cta = "Можемо глянути, як це у вас виглядатиме, якщо Viber справді дає заявки."
        elif "вночі" in reply_text.lower() or "нечітко" in reply_text.lower():
            cta = "Можемо глянути, як це у вас виглядатиме в реальних діалогах."
        else:
            cta = "Можемо прикинути під ваш кейс і зрозуміти, що варто автоматизувати першим."
        return (
            f"{reply_text} {cta}"
        )

    def _recent_user_question_count(self, sender_id: str) -> int:
        question_markers = [
            "?",
            "що",
            "як",
            "чи",
            "скільки",
            "може",
            "можна",
            "працює",
            "підключити",
            "вартість",
            "ціна",
        ]
        count = 0
        for item in reversed(self.memory_service.get_history(sender_id)[-8:]):
            if item.startswith("assistant:"):
                continue
            text = item.removeprefix("user:").strip().lower()
            if any(marker in text for marker in question_markers):
                count += 1
        return count

    def _get_capability_question_reply(self, text: str) -> tuple[str, str]:
        normalized = self._normalize_for_conversation_matching(text)
        cta_mode = self._capability_cta_mode(text)
        has_niche = any(
            marker in normalized
            for marker in ["сто", "автосерв", "салон", "стомат", "клінік", "майстр", "пацієнт"]
        )

        if any(marker in normalized for marker in ["питати в клієнта", "що бот буде питати", "що буде питати"]):
            reply_text = (
                "Зазвичай бот уточнює ім’я, телефон або email, послугу чи запит, бажаний час "
                "і важливі деталі по ситуації."
            )
            if has_niche:
                reply_text += " Деталі можна адаптувати під вашу нішу: авто, майстра, філію, напрям консультації чи інший процес."
            return self._append_capability_cta(reply_text, cta_mode), cta_mode

        if any(
            marker in normalized
            for marker in [
                "instagram",
                "інстаграм",
                "інсті",
                "інсту",
                "facebook",
                "фейсбук",
                "telegram",
                "телеграм",
                "телега",
                "телезі",
                "whatsapp",
                "ватсап",
                "вотсап",
                "viber",
                "вайбер",
            ]
        ):
            if any(marker in normalized for marker in ["viber", "вайбер"]):
                reply_text = "Так, Viber можна розглядати як канал для бота, якщо там є ваші звернення."
            elif any(marker in normalized for marker in ["telegram", "телеграм", "телега", "телезі"]):
                reply_text = "Так, у Telegram бот може відповідати на повідомлення, збирати заявки й передавати їх команді."
            elif any(marker in normalized for marker in ["whatsapp", "ватсап", "вотсап"]):
                reply_text = "Так, WhatsApp можна підключити для відповідей, збору заявок і передачі звернень менеджеру."
            elif any(marker in normalized for marker in ["facebook", "фейсбук"]):
                reply_text = "Так, Facebook DM підтримуємо: бот може відповідати клієнтам і збирати заявки з переписки."
            else:
                reply_text = "Так, в Instagram бот може відповідати в DM, уточнювати запит і передавати заявку менеджеру."
            return self._append_capability_cta(reply_text, cta_mode), cta_mode

        if any(marker in normalized for marker in ["календар", "calendar"]):
            reply_text = (
                "Так, календар можна підключити. Бот може збирати потрібні дані, перевіряти "
                "доступність або передавати заявку адміністратору — залежить від того, як у вас "
                "зараз ведеться запис. Який календар або система запису у вас зараз?"
            )
            return self._append_capability_cta(reply_text, "soft_cta"), "soft_cta"

        if any(marker in normalized for marker in ["crm", "сrm", "інтеграц"]):
            reply_text = (
                "CRM можна підключити, якщо у неї є API, інтеграція або зрозумілий спосіб "
                "передачі заявок. Тут залежить від вашого процесу і конкретної CRM."
            )
            return self._append_capability_cta(reply_text, "soft_cta"), "soft_cta"

        if any(
            marker in normalized
            for marker in [
                "заявки менеджеру",
                "заявки менеджер",
                "передає заявки",
                "передавати заявки",
                "передає менеджеру",
                "передавати менеджеру",
            ]
        ):
            reply_text = (
                "Так, бот може передавати заявки менеджеру: зібрати контакт, запит, канал "
                "і потрібні деталі, щоб людина отримала вже структуроване звернення."
            )
            return self._append_capability_cta(reply_text, "soft_cta"), "soft_cta"

        if any(marker in normalized for marker in ["пацієнт", "стомат", "клінік", "прийом"]):
            reply_text = (
                "Так, може відповідати на типові питання, уточнювати запит, бажаний час "
                "і контакт та передавати заявку команді. Медичні діагнози або точні "
                "призначення бот не вигадує."
            )
            if any(marker in normalized for marker in ["пацієнт", "стомат", "клінік", "прийом"]):
                reply_text = (
                    "Так, може відповідати пацієнтам на типові питання, уточнювати послугу, "
                    "бажаний час і контакт та передавати заявку адміністратору. Медичні "
                    "діагнози або точні призначення бот не вигадує."
                )
            return self._append_capability_cta(reply_text, cta_mode), cta_mode

        if any(marker in normalized for marker in ["рахувати ціну", "рахувати ремонт", "ціни на ремонт", "точну ціну"]):
            reply_text = (
                "Може дати орієнтир за вашими правилами, але Точну вартість ремонту не має "
                "вигадувати. Якщо потрібна точна оцінка, бот збере деталі по авто і передасть "
                "заявку менеджеру. Які звернення у вас найчастіші?"
            )
            return self._append_capability_cta(reply_text, "soft_cta"), "soft_cta"

        if any(marker in normalized for marker in ["незрозуміло", "пише криво", "пише незрозуміло"]):
            reply_text = (
                "Якщо клієнт пише нечітко, бот може нормально перепитати: яку послугу потрібно, "
                "на який день, який контакт і що саме сталося. Якщо запит складний — краще "
                "передати його людині, а не вигадувати відповідь."
            )
            return self._append_capability_cta(reply_text, "soft_cta"), "soft_cta"

        if any(marker in normalized for marker in ["вночі", "уночі", "24/7"]):
            reply_text = (
                "Так, у цьому якраз є сенс: бот може відповідати вночі, збирати заявку і контакт, "
                "а команда вже обробить її в робочий час. Це допомагає не втрачати людей, які "
                "пишуть після закриття."
            )
            return self._append_capability_cta(reply_text, "soft_cta"), "soft_cta"

        if any(marker in normalized for marker in ["майстр", "записує", "записувати", "запис до", "запис на"]):
            if has_niche:
                reply_text = (
                    "Так, бот може допомагати із записом: уточнювати послугу, бажаний час, "
                    "майстра або філію і передавати заявку менеджеру чи в календар."
                )
            else:
                reply_text = (
                    "Так, бот може допомагати із записом: уточнювати потрібну послугу, бажаний "
                    "час, контакт і передавати заявку команді або в календар."
                )
            return self._append_capability_cta(reply_text, cta_mode), cta_mode

        return (
            "Тут залежить від вашого процесу. Можемо коротко розібрати це на консультації "
            "й зрозуміти, який сценарій буде найкращий.",
            "soft_cta",
        )

    def _build_capability_question_result(self, message: NormalizedMessage) -> Dict[str, Any]:
        reply_text, cta_mode = self._get_capability_question_reply(message.user_message)
        if (
            cta_mode == "no_cta"
            and self._recent_user_question_count(message.sender_id) >= 2
            and not self._has_recent_soft_call_cta(message.sender_id)
        ):
            cta_mode = "soft_cta"
            reply_text = self._append_capability_cta(reply_text, cta_mode)
        routing_category = {
            "no_cta": "answered_basic",
            "soft_cta": "consultation_soft_cta",
            "booking_cta": "consultation_cta",
        }.get(cta_mode, "answered_basic")
        return self._build_direct_reply_result(
            message=message,
            reply_text=reply_text,
            intent_value="capability_question",
            routing_category=routing_category,
            intent_for_policy=IntentType.GENERAL_QUESTION,
        )

    def _looks_like_buying_signal(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        bot_markers = [
            "потрібен бот",
            "потрібний бот",
            "треба бот",
            "хочу бот",
            "може спробуємо",
            "окей спробуємо",
            "ок спробуємо",
            "давайте спробуємо",
            "давай спробуємо",
            "ок спробуєм",
            "окей спробуєм",
            "спробуєм",
            "спробуємо",
            "звучить норм",
            "цікаве впровадження бота",
            "цікавить впровадження бота",
            "цікавить бот",
            "бот для інстаграм",
            "бот в інстаграм",
            "бот для instagram",
            "бот в instagram",
            "бот для телеграм",
            "бот в телеграм",
            "бот для telegram",
            "бот в telegram",
            "бот для телеги",
            "бот в телезі",
        ]
        return any(marker in normalized for marker in bot_markers)

    def _looks_like_hesitation(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        exact = {"може", "подумаю", "я подумаю"}
        markers = [
            "ну не знаю",
            "не знаю чи треба",
            "не впевнений",
            "не впевнена",
            "я поки думаю",
        ]
        return normalized in exact or any(marker in normalized for marker in markers)

    def _looks_like_price_objection(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        markers = ["це дорого", "дорого", "дорогувато", "задорого"]
        return any(marker in normalized for marker in markers)

    def _looks_like_skepticism(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        markers = [
            "черговий чатбот",
            "звичайний чатбот",
            "просто чатбот",
            "шаблони продаєте",
            "шаблон продаєте",
            "просто шаблони",
            "бот буде тупити",
            "буде тупити",
            "бот тупитиме",
            "клієнти бачили робота",
            "бачили робота",
            "робот буде відповідати",
            "чим ви кращі",
            "чим ви кращ",
            "чим кращі",
            "чим краще",
        ]
        return any(marker in normalized for marker in markers)

    def _get_skepticism_reply(self, text: str) -> str:
        normalized = self._normalize_for_conversation_matching(text)
        if any(marker in normalized for marker in ["шаблон", "шаблони"]):
            return (
                "Ні, ідея не в тому, щоб продати набір шаблонів. Ми збираємо сценарій під ваші "
                "типові звернення, тон спілкування і правила передачі заявок."
            )
        if any(marker in normalized for marker in ["тупить", "тупити", "тупитиме"]):
            return (
                "Це реальний ризик, тому бот не має вигадувати відповіді. Для складних або "
                "нечітких запитів він краще уточнює деталі або передає діалог людині."
            )
        if "робот" in normalized:
            return (
                "Розумію. Нормальний бот має звучати просто й корисно, а не як автомат із заготовками. "
                "Плюс складні звернення можна одразу переводити на менеджера."
            )
        if "чим" in normalized and ("кращ" in normalized or "краще" in normalized):
            return (
                "Наша різниця не в красивій обгортці, а в сценарії під ваш процес: що бот має "
                "питати, коли передавати менеджеру і як не губити заявки. Тобто робимо не просто "
                "відповіді, а нормальну логіку продажу в месенджерах."
            )
        return (
            "Різниця в тому, що це не просто кнопковий чатбот. Ми робимо AI-асистента під ваш процес: "
            "він розуміє запит, уточнює деталі й передає нормальну заявку менеджеру."
        )

    def _wants_more_info_before_booking(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        markers = [
            "спочатку",
            "спершу",
            "перед тим",
            "ще питання",
            "є питання",
            "хочу зрозуміти",
            "цікавить спочатку",
            "хай",
            "привіт",
            "як це",
            "як працюват",
            "чи підійде",
            "для нас",
            "?",
        ]
        return any(marker in normalized for marker in markers)

    def _has_recent_interest_qualification(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if "актуально розглядаєте впровадження" in normalized:
                return True
        return False

    def _get_interest_acceptance_followup_reply(self) -> str:
        return "Супер. Для якого бізнесу розглядаєте бота і що хочете автоматизувати в першу чергу?"

    def _looks_like_booking_pause_or_postpone(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        markers = [
            "пізніше",
            "потім",
            "не зараз",
            "ще не знаю",
            "напишу пізніше",
            "пізніше напишу",
            "я подумаю",
            "давайте пізніше",
        ]
        return any(marker in normalized for marker in markers)

    def _build_booking_product_question_reply(
        self,
        *,
        message: NormalizedMessage,
        booking_state: BookingState,
        include_booking_prompt: bool = True,
    ) -> str:
        normalized = self._normalize_for_conversation_matching(message.user_message)
        intent = self.intent_service.detect_intent(message.user_message)
        if any(marker in normalized for marker in ["впроваджен", "запуск", "як довго", "скільки часу", "термін"]):
            intent = IntentType.GENERAL_QUESTION

        reply_text = self.reply_service.generate_reply(message, intent=intent)
        if not include_booking_prompt:
            return reply_text

        if booking_state == BookingState.WAITING_FOR_CONTACT:
            reply_text = (
                f"{reply_text}\n\n"
                "А для підтвердження дзвінка залиште, будь ласка, ваше ім’я та номер телефону або email."
            )
        elif booking_state == BookingState.WAITING_FOR_TIME:
            reply_text = (
                f"{reply_text}\n\n"
                "Для дзвінка підкажіть, будь ласка, який день і час вам зручний."
            )
        return reply_text

    def _build_booking_reply_result(
        self,
        *,
        message: NormalizedMessage,
        reply_text: str,
        intent_value: str,
        booking_result: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        reply_text = self.reply_service.enforce_response_policy(
            reply_text=reply_text,
            user_text=message.user_message,
            intent=IntentType.BOOKING_REQUEST,
        )
        reply_text = self._avoid_exact_repeat(message.sender_id, reply_text)
        self.memory_service.add_assistant_message(message.sender_id, reply_text)
        outbound_result = self.outbound_service.send_reply(
            platform=message.platform,
            recipient_id=message.sender_id,
            text=reply_text,
        )
        return {
            "intent": intent_value,
            "routing_category": "consultation_cta",
            "reply_text": reply_text,
            "history": self.memory_service.get_history(message.sender_id),
            "booking_result": booking_result,
            "outbound_result": outbound_result,
        }

    def _build_direct_reply_result(
        self,
        *,
        message: NormalizedMessage,
        reply_text: str,
        intent_value: str,
        routing_category: str = "answered_basic",
        intent_for_policy: IntentType = IntentType.GENERAL_QUESTION,
    ) -> Dict[str, Any]:
        reply_text = self.reply_service.enforce_response_policy(
            reply_text=reply_text,
            user_text=message.user_message,
            intent=intent_for_policy,
        )
        reply_text = self._avoid_exact_repeat(message.sender_id, reply_text)
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
            "booking_result": None,
            "outbound_result": outbound_result,
        }

    def _get_contextual_short_reply(self, text: str) -> str | None:
        normalized = " ".join(text.strip().lower().split())
        normalized = re.sub(r"[.!?…]+$", "", normalized).strip()
        soft_followup_reply = (
            "Без проблем 🙂 Якщо коротко — зазвичай це економить час на обробці заявок "
            "і допомагає не втрачати клієнтів.\n\n"
            "Якщо буде актуально — можемо швидко глянути ваш кейс і зрозуміти, "
            "чи є сенс впроваджувати."
        )
        replies = {
            "ок": "Дякую, зафіксував.",
            "окей": "Дякую, зафіксував.",
            "добре": "Добре, дякую.",
            "давай": "Добре, підкажіть, будь ласка, що саме вам зручно обговорити?",
            "давайте": "Добре, тоді почнемо з контексту: який у вас бізнес і де зараз найбільше звернень?",
            "так давай": "Добре, тоді почнемо з контексту: який у вас бізнес і де зараз найбільше звернень?",
            "так давайте": "Добре, тоді почнемо з контексту: який у вас бізнес і де зараз найбільше звернень?",
            "ага": "Зрозумів. Можу коротко зорієнтувати по суті: як працює бот, для яких бізнесів підходить або скільки коштує.",
            "актуально": "Супер. Для якого бізнесу розглядаєте бота і що хочете автоматизувати в першу чергу?",
            "можливо": "Ок, можна рухатись без поспіху. Найпростіше почати з контексту: який у вас бізнес і де зараз найбільше звернень?",
            "ну може": "Ок, без тиску. Можемо спершу коротко зрозуміти ваш процес і підказати, чи є сенс автоматизувати саме зараз.",
            "ну це таке": "Розумію, звучить поки не дуже переконливо. Можу пояснити простіше на прикладі вашого бізнесу, якщо напишете сферу.",
            "не впевнений": "Нормально, тут не треба вирішувати з першого повідомлення. Можемо спершу зрозуміти, чи є у вас достатньо повторюваних звернень, які реально варто автоматизувати.",
            "не впевнена": "Нормально, тут не треба вирішувати з першого повідомлення. Можемо спершу зрозуміти, чи є у вас достатньо повторюваних звернень, які реально варто автоматизувати.",
            "не знаю чи треба": "Тоді найпростіше перевірити від потреби: якщо клієнти часто пишуть одне й те саме або заявки губляться, бот може мати сенс. Де зараз найбільше ручної переписки?",
            "не знаю": "Ок, без поспіху. Можемо почати з простого: де зараз найбільше повідомлень від клієнтів і що найчастіше питають?",
            "поясни простіше": "По суті: бот відповідає замість менеджера і не дає губити заявки. Він може прийняти звернення, уточнити деталі й передати людині вже готовий запит.",
            "поясніть простіше": "По суті: бот відповідає замість менеджера і не дає губити заявки. Він може прийняти звернення, уточнити деталі й передати людині вже готовий запит.",
            "можливо потім": soft_followup_reply,
            "можливо пізніше": soft_followup_reply,
            "не зараз": soft_followup_reply,
            "я подумаю": soft_followup_reply,
            "я поки думаю": "Без проблем. Якщо будете повертатися до теми, можна почати з простого питання: де зараз найбільше ручних відповідей і що саме хочете автоматизувати.",
            "це не питання це пропозиція": "Дякую, зафіксував. Передам це команді, щоб подивилися уважно.",
        }
        return replies.get(normalized)

    def _get_last_assistant_message(self, sender_id: str) -> str | None:
        for item in reversed(self.memory_service.get_history(sender_id)):
            if item.startswith("assistant:"):
                return item.removeprefix("assistant:").strip()
        return None

    def _avoid_exact_repeat(self, sender_id: str, reply_text: str) -> str:
        last_reply = self._get_last_assistant_message(sender_id)
        if last_reply != reply_text:
            return reply_text

        if reply_text == "Підкажіть, будь ласка, точний день і час.":
            return "Так, давайте. Напишіть, будь ласка, конкретний день і час, наприклад завтра о 12:30."

        if reply_text.startswith("Супер, тоді бронюємо") and "Залиште, будь ласка" in reply_text:
            return "Так, цей варіант підходить. Залиште, будь ласка, ваше ім’я та номер телефону або email."

        if reply_text.startswith("Вартість стартує від 200$"):
            return (
                "Так, стартова вартість від 200$. Точніше залежить від каналів, сценарію "
                "і потрібних інтеграцій, тому фінальну суму краще рахувати під конкретний кейс."
            )

        if reply_text == "Добре, зрозумів.":
            return "Все ок, не турбую."

        if reply_text.startswith("Розумію, відповідь була не зовсім по суті."):
            return (
                "Бачу, попередня відповідь не зайшла. Можу коротко відповісти по суті: "
                "що робить бот, для яких сфер підходить або скільки коштує."
            )

        if reply_text.startswith("Можете трохи уточнити") or reply_text.startswith("Хочу правильно зрозуміти"):
            return (
                "Не зовсім зрозумів повідомлення. Напишіть, будь ласка, одним реченням: "
                "цікавить ціна, канали, сфера бізнесу чи запис на дзвінок?"
            )

        if reply_text.startswith("Можемо показати типові use cases"):
            return (
                "Так, приклади тут радше типові сценарії, без вигаданих реальних кейсів. "
                "Для СТО бот приймає звернення й уточнює проблему з авто, для стоматології "
                "допомагає із записом, для салону веде клієнта до майстра, а для освіти "
                "кваліфікує заявку перед консультацією."
            )

        return reply_text

    def _has_recent_interest_signal_reply(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if "зручно буде на дзвінок" in normalized:
                return True
        return False

    def _has_recent_soft_call_cta(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        markers = [
            "зручно буде на дзвінок",
            "якщо вам ок",
            "якщо вам ок.",
            "коротко обговорити на дзвінку",
            "розглянути ваш кейс на дзвінку",
            "на дзвінку і поспілкуватися",
            "зі спеціалістом на дзвінку",
            "розібрати ваш процес зі спеціалістом",
            "коротко розібрати ваш процес",
            "підказати, як це краще автоматизувати",
        ]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if any(marker in normalized for marker in markers):
                return True
        return False

    def _has_recent_rejection_reply(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if "якщо пізніше буде актуально" in normalized or "добре, зрозумів" in normalized:
                return True
        return False

    def _looks_like_interest_booking_acceptance(self, text: str) -> bool:
        normalized = " ".join(text.strip().lower().split())
        normalized = re.sub(r"[.!?…]+$", "", normalized).strip()
        normalized = re.sub(r"([аеєиіїоуюя])\1+", r"\1", normalized)
        normalized = normalized.replace(",", " ")
        normalized = " ".join(normalized.split())
        acceptances = {
            "так",
            "так давай",
            "так давайте",
            "так актуально",
            "так підкажи",
            "так ок",
            "так окк",
            "так окей",
            "давай",
            "давайте",
            "ок",
            "окк",
            "окей",
            "ну ок",
            "ну окей",
            "добре",
            "можна",
            "yes",
            "ok",
            "okay",
            "sure",
        }
        if normalized in acceptances:
            return True
        if "сценар" in normalized:
            acceptance_markers = ["так", "давай", "давайте", "ок", "окей", "ну ок", "добре"]
            return any(marker in normalized for marker in acceptance_markers)
        return False

    def _looks_like_more_details_request(self, text: str) -> bool:
        normalized = self._normalize_for_conversation_matching(text)
        markers = [
            "більше деталей",
            "детальніше",
            "більше детально",
            "просто пояснити",
            "пояснити без дзвінка",
            "без дзвінка",
            "без дзвонка",
            "не хочу дзвінок",
            "не хочу дзвонок",
            "не хочу консультацію",
            "в тексті",
            "текстом",
            "поясни тут",
            "поясніть тут",
            "давайте тут",
            "спочатку хочу більше",
            "спершу зрозуміти",
            "спочатку зрозуміти",
            "хочу спершу зрозуміти",
            "хочу спочатку зрозуміти",
        ]
        return any(marker in normalized for marker in markers)

    def _get_more_details_reply(self) -> str:
        return (
            "Так, звісно, можна без дзвінка. Якщо коротко: бот бере типові повідомлення "
            "в месенджерах, відповідає клієнтам, уточнює потрібні деталі й передає вже "
            "готову заявку або веде до запису. Щоб пояснити точніше, який у вас бізнес "
            "і де зараз найбільше звернень: Instagram, Telegram, WhatsApp чи Viber?"
        )

    def _has_recent_intro_offer(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if "коротко підкажу, як це може працювати" in normalized:
                return True
        return False

    def _has_recent_niche_reply(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        markers = [
            "для автосервісу",
            "для салону краси",
            "для стоматологій",
            "для клінік",
        ]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if any(marker in normalized for marker in markers):
                return True
        return False

    def _has_recent_price_reply(self, sender_id: str) -> bool:
        history = self.memory_service.get_history(sender_id)
        previous_items = history[:-1]
        for item in reversed(previous_items[-4:]):
            if not item.startswith("assistant:"):
                continue
            normalized = item.lower()
            if (
                "вартість стартує" in normalized
                or "старт від 200" in normalized
                or "pricing starts" in normalized
            ):
                return True
        return False

    def _looks_like_business_details(self, text: str) -> bool:
        normalized = " ".join(text.strip().lower().split())
        detail_markers = [
            "у мене",
            "маю",
            "наш бізнес",
            "потрібно",
            "треба",
            "хочу",
            "задача",
            "задачі",
            "клієнт",
            "заявк",
            "запит",
            "пиш",
            "пишут",
            "пишуть",
            "інсту",
            "інста",
            "instagram",
            "багато",
            "послуг",
            "менеджер",
            "команд",
            "сто",
            "автосерв",
            "салон",
            "клінік",
            "стомат",
            "магазин",
            "ресторан",
            "студія",
            "школа",
            "курс",
            "нерухом",
        ]
        return any(marker in normalized for marker in detail_markers)

    def _get_business_context_reply(self, text: str) -> str:
        normalized = self._normalize_for_conversation_matching(text)
        if any(marker in normalized for marker in ["інсту", "інста", "instagram"]) and any(
            marker in normalized for marker in ["багато", "пиш", "пишут", "пишуть"]
        ):
            return (
                "Зрозумів. Якщо багато звернень саме в Instagram, бот може забрати першу лінію: "
                "відповідати на типові питання, уточнювати запит і збирати контакт або бажаний час. "
                "А що найчастіше питають клієнти: ціни, запис, послуги чи статус заявки?"
            )
        return (
            "Зрозумів контекст. Тоді бот може закривати типові звернення і передавати менеджеру "
            "вже більш теплу заявку. Що саме зараз забирає найбільше часу у переписках?"
        )

    def _looks_like_after_hours_question(self, text: str) -> bool:
        normalized = " ".join(text.strip().lower().split())
        markers = [
            "вночі",
            "уночі",
            "ночі",
            "після робочого часу",
            "поза робочим часом",
            "неробочий час",
            "коли ми не працюємо",
            "24/7",
        ]
        return any(marker in normalized for marker in markers)

    def _get_niche_fit_reply(self, text: str) -> str | None:
        language = self.reply_service.detect_user_language(text)
        return self.reply_service.get_niche_fit_reply(text, language)

    def _get_price_followup_case_reply(self, text: str) -> str:
        language = self.reply_service.detect_user_language(text)
        if language == "uk":
            return (
                "Зрозумів, дякую за деталі. У вашому випадку це якраз можна "
                "автоматизувати — бот може приймати звернення, уточнювати деталі "
                "і записувати клієнтів у календар.\n\n"
                "Щоб підібрати оптимальне рішення під ваш процес, краще коротко "
                "обговорити це на дзвінку. Підкажіть, будь ласка, коли вам буде зручно?"
            )
        return (
            "Зрозумів, дякую за деталі. У вашому випадку це якраз можна автоматизувати — бот може "
            "приймати звернення, уточнювати деталі і записувати клієнтів у календар.\n\n"
            "Щоб підібрати оптимальне рішення під ваш процес, краще коротко обговорити це на дзвінку, "
            "якщо вам ок. Коли вам було б зручно?"
        )

    def _handle_confirmed_booking_message(self, message: NormalizedMessage) -> Dict[str, Any] | None:
        language = self.reply_service.detect_user_language(message.user_message)

        if self._looks_like_reschedule_request(message.user_message):
            booking_result = self.booking_service.handle_reschedule_request(
                sender_id=message.sender_id,
                message_text=message.user_message,
            )
            return self._build_booking_reply_result(
                message=message,
                reply_text=booking_result["reply_text"],
                intent_value="booking_reschedule",
                booking_result=booking_result,
            )

        if self._looks_like_cancel_request(message.user_message):
            booking_result = self.booking_service.cancel_confirmed_booking(
                sender_id=message.sender_id,
                message_text=message.user_message,
            )
            return self._build_booking_reply_result(
                message=message,
                reply_text=booking_result["reply_text"],
                intent_value="booking_cancel",
                booking_result=booking_result,
            )

        if self._looks_like_availability_question(message.user_message):
            reply_text = self.booking_service.get_availability_question_reply(language)
            return self._build_booking_reply_result(
                message=message,
                reply_text=reply_text,
                intent_value="booking_availability_question",
            )

        if self._looks_like_call_explanation_question(message.user_message):
            reply_text = self.booking_service.get_call_explanation_reply(language)
            return self._build_booking_reply_result(
                message=message,
                reply_text=reply_text,
                intent_value="booking_call_explanation",
            )

        if self._looks_like_datetime_only_message(message.user_message):
            booking_result = self.booking_service.handle_reschedule_request(
                sender_id=message.sender_id,
                message_text=message.user_message,
            )
            return self._build_booking_reply_result(
                message=message,
                reply_text=booking_result["reply_text"],
                intent_value="booking_reschedule",
                booking_result=booking_result,
            )

        if self.booking_service.looks_like_booking_status_question(message.user_message):
            reply_text = self.booking_service.get_confirmed_booking_status_reply(
                message.sender_id,
                language,
            )
            return self._build_booking_reply_result(
                message=message,
                reply_text=reply_text,
                intent_value="booking_status_confirmed",
            )

        return None

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

    def _looks_like_greeting_text(self, user_text: str) -> bool:
        normalized = " ".join(user_text.strip().lower().split())
        greeting_markers = [
            "привіт",
            "вітаю",
            "доброго дня",
            "добрий день",
            "добрий вечір",
            "хай",
            "hello",
            "hi",
            "hey",
        ]
        return any(marker in normalized for marker in greeting_markers)

    def _prepend_first_greeting_if_needed(
        self,
        sender_id: str,
        user_text: str,
        reply_text: str,
        intent: IntentType,
    ) -> str:
        if not reply_text.strip():
            return reply_text
        if not self._is_first_assistant_reply(sender_id):
            return reply_text
        if intent != IntentType.SERVICE_DESCRIPTION and not self._looks_like_greeting_text(user_text):
            return reply_text

        language = self.reply_service.detect_user_language(user_text)
        prefix = "Привіт! "

        if self._has_greeting_prefix(reply_text, language):
            return reply_text
        return f"{prefix}{reply_text}"

    def _finalize_general_reply_text(
        self,
        sender_id: str,
        user_text: str,
        reply_text: str,
        intent: IntentType,
    ) -> str:
        finalized = self._prepend_first_greeting_if_needed(
            sender_id=sender_id,
            user_text=user_text,
            reply_text=reply_text,
            intent=intent,
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
            reply_text = (
                "Не вдалося розпізнати аудіо. Напишіть, будь ласка, повідомлення текстом "
                "або надішліть голосове ще раз."
            )

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
            if self._looks_like_booking_pause_or_postpone(message.user_message):
                booking_result = self.booking_service.process_booking_message(
                    sender_id=message.sender_id,
                    message_text=message.user_message,
                    source_channel=message.platform,
                )
                if booking_result is not None:
                    return self._build_booking_reply_result(
                        message=message,
                        reply_text=booking_result["reply_text"],
                        intent_value="booking_flow",
                        booking_result=booking_result,
                    )

            if self._looks_like_capability_question(message.user_message):
                return self._build_capability_question_result(message)

            if (
                booking_state == BookingState.WAITING_FOR_TIME
                and self._looks_like_product_question_during_booking(message.user_message)
            ):
                reply_text = self._build_booking_product_question_reply(
                    message=message,
                    booking_state=booking_state,
                    include_booking_prompt=not self._wants_more_info_before_booking(message.user_message),
                )
                return self._build_direct_reply_result(
                    message=message,
                    reply_text=reply_text,
                    intent_value="booking_product_question",
                    routing_category="answered_basic",
                    intent_for_policy=IntentType.GENERAL_QUESTION,
                )

            if (
                booking_state == BookingState.WAITING_FOR_TIME
                and self.intent_service.detect_intent(message.user_message) == IntentType.INTEREST_SIGNAL
            ):
                reply_text = self.reply_service.generate_reply(message, intent=IntentType.INTEREST_SIGNAL)
                return self._build_direct_reply_result(
                    message=message,
                    reply_text=reply_text,
                    intent_value="interest_signal",
                    routing_category="answered_basic",
                    intent_for_policy=IntentType.INTEREST_SIGNAL,
                )

            if (
                booking_state == BookingState.WAITING_FOR_TIME
                and self._looks_like_availability_question(message.user_message)
            ):
                booking_result = self.booking_service.handle_availability_question(
                    sender_id=message.sender_id,
                    message_text=message.user_message,
                    source_channel=message.platform,
                )
                return self._build_booking_reply_result(
                    message=message,
                    reply_text=booking_result["reply_text"],
                    intent_value="booking_availability_question",
                    booking_result=booking_result,
                )

            if booking_state == BookingState.WAITING_FOR_CONTACT:
                contact_details = self.booking_service.extract_contact_details(message.user_message)
                if (
                    not contact_details["has_name"]
                    and not contact_details["has_phone"]
                    and not contact_details["has_email"]
                    and self._looks_like_product_question_during_booking(message.user_message)
                ):
                    reply_text = self._build_booking_product_question_reply(
                        message=message,
                        booking_state=booking_state,
                    )
                    return self._build_direct_reply_result(
                        message=message,
                        reply_text=reply_text,
                        intent_value="booking_product_question",
                        routing_category="answered_basic",
                        intent_for_policy=IntentType.GENERAL_QUESTION,
                    )

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
            reply_text = self._avoid_exact_repeat(message.sender_id, reply_text)
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
        ):
            confirmed_result = self._handle_confirmed_booking_message(message)
            if confirmed_result is not None:
                return confirmed_result

        if self._looks_like_language_request(message.user_message):
            language = self.reply_service.detect_user_language(message.user_message)
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.get_language_request_reply(language),
                intent_value="language_request",
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        if self._looks_like_noise_only_message(message.user_message):
            language = self.reply_service.detect_user_language(message.user_message)
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.get_safe_fallback_reply(language),
                intent_value="general_question",
                routing_category="safe_handoff",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        if self._looks_like_availability_question(message.user_message):
            booking_result = self.booking_service.handle_availability_question(
                sender_id=message.sender_id,
                message_text=message.user_message,
                source_channel=message.platform,
            )
            return self._build_booking_reply_result(
                message=message,
                reply_text=booking_result["reply_text"],
                intent_value="booking_availability_question",
                booking_result=booking_result,
            )

        if self._looks_like_call_explanation_question(message.user_message):
            language = self.reply_service.detect_user_language(message.user_message)
            reply_text = self.booking_service.get_call_explanation_reply(language)
            return self._build_booking_reply_result(
                message=message,
                reply_text=reply_text,
                intent_value="booking_call_explanation",
            )

        if self._looks_like_after_hours_question(message.user_message):
            language = self.reply_service.detect_user_language(message.user_message)
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.get_after_hours_reply(language),
                intent_value="after_hours_question",
                routing_category="answered_basic",
            )

        if self._looks_like_more_details_request(message.user_message):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self._get_more_details_reply(),
                intent_value="more_details",
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        if self._looks_like_capability_question(message.user_message):
            return self._build_capability_question_result(message)

        if (
            self._has_recent_soft_call_cta(message.sender_id)
            and self._looks_like_interest_booking_acceptance(message.user_message)
        ):
            booking_result = self.booking_service.start_booking_flow(
                sender_id=message.sender_id,
                message_text="дзвінок",
                source_channel=message.platform,
            )
            return self._build_booking_reply_result(
                message=message,
                reply_text=booking_result["reply_text"],
                intent_value="booking_request",
                booking_result=booking_result,
            )

        if (
            self._has_recent_interest_qualification(message.sender_id)
            and self.intent_service.detect_intent(message.user_message) == IntentType.INTEREST_SIGNAL
        ):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self._get_interest_acceptance_followup_reply(),
                intent_value="interest_followup",
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        if (
            self._has_recent_interest_qualification(message.sender_id)
            and self._looks_like_interest_booking_acceptance(message.user_message)
        ):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self._get_interest_acceptance_followup_reply(),
                intent_value="interest_followup",
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        if (
            self._has_recent_niche_reply(message.sender_id)
            and self._looks_like_business_details(message.user_message)
            and not self._looks_like_booking_message(message.user_message)
            and not self._looks_like_reschedule_request(message.user_message)
            and not self._looks_like_cancel_request(message.user_message)
        ):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self._get_business_context_reply(message.user_message),
                intent_value="business_context_followup",
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        if self._looks_like_buying_signal(message.user_message):
            language = self.reply_service.detect_user_language(message.user_message)
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=IntentType.BUYING_SIGNAL),
                intent_value="buying_signal",
                routing_category="consultation_soft_cta",
                intent_for_policy=IntentType.BUYING_SIGNAL,
            )

        if (
            self._has_recent_intro_offer(message.sender_id)
            and self._looks_like_business_details(message.user_message)
            and not self._looks_like_booking_message(message.user_message)
            and not self._looks_like_reschedule_request(message.user_message)
            and not self._looks_like_cancel_request(message.user_message)
        ):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=IntentType.GENERAL_QUESTION),
                intent_value="business_context_followup",
                routing_category="consultation_cta",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        early_intent = self.intent_service.detect_intent(message.user_message)
        if early_intent in {IntentType.HESITATION, IntentType.START_REQUIREMENTS}:
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=early_intent),
                intent_value=early_intent.value,
                routing_category="answered_basic",
                intent_for_policy=early_intent,
            )

        if self._looks_like_price_objection(message.user_message):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=IntentType.GENERAL_QUESTION),
                intent_value=IntentType.GENERAL_QUESTION.value,
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        contextual_short_reply = self._get_contextual_short_reply(message.user_message)
        if contextual_short_reply:
            return self._build_direct_reply_result(
                message=message,
                reply_text=contextual_short_reply,
                intent_value="contextual_short_reply",
            )

        if self._looks_like_skepticism(message.user_message):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self._get_skepticism_reply(message.user_message),
                intent_value="skepticism",
                routing_category="answered_basic",
                intent_for_policy=IntentType.GENERAL_QUESTION,
            )

        current_intent = self.intent_service.detect_intent(message.user_message)
        if current_intent in {
            IntentType.HESITATION,
            IntentType.BUYING_SIGNAL,
            IntentType.START_REQUIREMENTS,
        }:
            routing_category = (
                "consultation_soft_cta"
                if current_intent == IntentType.BUYING_SIGNAL
                else "answered_basic"
            )
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=current_intent),
                intent_value=current_intent.value,
                routing_category=routing_category,
                intent_for_policy=current_intent,
            )

        if current_intent in {
            IntentType.PRICE,
            IntentType.CHANNELS,
            IntentType.INDUSTRIES,
            IntentType.USE_CASES,
        }:
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=current_intent),
                intent_value=current_intent.value,
                routing_category="answered_basic",
                intent_for_policy=current_intent,
            )

        if (
            self._has_recent_price_reply(message.sender_id)
            and self._looks_like_business_details(message.user_message)
            and not self._looks_like_hesitation(message.user_message)
            and not self._looks_like_booking_message(message.user_message)
            and not self._looks_like_reschedule_request(message.user_message)
            and not self._looks_like_cancel_request(message.user_message)
        ):
            return self._build_direct_reply_result(
                message=message,
                reply_text=self._get_price_followup_case_reply(message.user_message),
                intent_value="price_followup_case_details",
                routing_category="consultation_cta",
                intent_for_policy=IntentType.BOOKING_REQUEST,
            )

        niche_fit_reply = self._get_niche_fit_reply(message.user_message)
        if niche_fit_reply:
            return self._build_direct_reply_result(
                message=message,
                reply_text=niche_fit_reply,
                intent_value="niche_fit",
                routing_category="consultation_cta",
            )

        intent = self.intent_service.detect_intent(message.user_message)
        intent_value = intent.value
        logger.info("Intent detected: %s", intent)

        if intent == IntentType.REJECTION:
            language = self.reply_service.detect_user_language(message.user_message)
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.get_rejection_reply(
                    language,
                    repeated=self._has_recent_rejection_reply(message.sender_id),
                ),
                intent_value=intent_value,
                routing_category="answered_basic",
                intent_for_policy=IntentType.REJECTION,
            )

        if intent == IntentType.FRUSTRATED:
            return self._build_direct_reply_result(
                message=message,
                reply_text=self.reply_service.generate_reply(message, intent=intent),
                intent_value=intent_value,
                routing_category="answered_basic",
                intent_for_policy=IntentType.FRUSTRATED,
            )

        history = self.memory_service.get_history(message.sender_id)
        question_level, question_reason = self.reply_service.classify_question_level(
            user_text=message.user_message,
            intent=intent,
            history=history,
        )
        logger.info("Question level: %s", question_level)
        logger.debug("Question level reason: %s", question_reason)

        force_booking = (
            self._looks_like_booking_message(message.user_message)
            or self._looks_like_datetime_only_message(message.user_message)
        )

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
        reply_text = self._avoid_exact_repeat(message.sender_id, reply_text)
        if booking_result is None and routing_category != "safe_handoff":
            reply_text = self._finalize_general_reply_text(
                sender_id=message.sender_id,
                user_text=message.user_message,
                reply_text=reply_text,
                intent=intent,
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
