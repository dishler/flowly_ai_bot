from app.application.dto.normalized_message import NormalizedMessage
from app.application.services.ai_service import AIService
from app.application.services.memory_service import MemoryService


class ReplyService:
    def __init__(
        self,
        ai_service: AIService,
        memory_service: MemoryService,
    ) -> None:
        self.ai_service = ai_service
        self.memory_service = memory_service

    def generate_reply(self, message: NormalizedMessage) -> str:
        history = self.memory_service.get_history(message.sender_id)

        ai_result = self.ai_service.try_generate_reply(
            user_message=message.user_message,
            history=history,
        )

        ai_reply_text = ai_result.get("reply_text")
        if ai_reply_text:
            return str(ai_reply_text)

        text = message.user_message.strip().lower()

        price_markers = [
            "price",
            "pricing",
            "cost",
            "how much",
            "ціна",
            "скільки",
            "вартість",
        ]

        if any(marker in text for marker in price_markers):
            return (
                "Базова ціна стартує від 300$. "
                "Щоб сказати точніше, треба коротко зрозуміти вашу задачу."
            )

        if "instagram" in text:
            return "Так, можемо допомогти і з Instagram."

        if "facebook" in text:
            return "Так, можемо допомогти і з Facebook."

        if (
            "call" in text
            or "consultation" in text
            or "book" in text
            or "booking" in text
            or "дзвінок" in text
            or "консультац" in text
        ):
            return "Можу запропонувати консультацію. Напишіть, будь ласка, зручний день і час."

        return "Дякую. Можете коротко описати вашу задачу?"