from typing import Any, Dict, List

from openai import OpenAI

from app.core.config import get_settings


class OpenAIClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client = OpenAI(api_key=self.settings.openai_api_key) if self.settings.openai_api_key else None

    def generate_reply(
        self,
        user_message: str,
        history: List[str],
    ) -> Dict[str, Any]:
        if not self.settings.openai_enabled:
            return {
                "used_ai": False,
                "stub": True,
                "reason": "OPENAI_ENABLED=false",
                "reply_text": None,
            }

        if not self.settings.openai_api_key or self.client is None:
            return {
                "used_ai": False,
                "stub": True,
                "reason": "Missing OPENAI_API_KEY",
                "reply_text": None,
            }

        system_prompt = (
            "You are an AI assistant for a business handling Instagram and Facebook DMs. "
            "Reply in the user's language. "
            "Keep replies short, natural, and practical. "
            "Do not sound robotic, overly formal, or salesy. "
            "Do not start replies with 'Yes —', 'Sure —', or similar filler unless really necessary. "
            "Your goal is to help the user and gently move the conversation toward a consultation when appropriate. "
            "If asked about price, say the base price starts from $300, but do not claim every task costs $300. "
            "If you need more information, ask at most 1–2 short questions in the same message. "
            "Prefer one compact paragraph over a long explanation. "
            "Do not invent calendar slots. "
            "Do not say booking is confirmed unless an event was actually created. "
            "If date/time is unclear, ask one short clarification question. "
            "If the user asks about Instagram lead generation, focus on target audience, lead volume, current account status, and next step. "
            "Do not use bullet points unless absolutely necessary."
        )

        history_text = "\n".join(history[-10:]) if history else "No prior history."

        user_prompt = (
            f"Conversation history:\n{history_text}\n\n"
            f"User message:\n{user_message}\n\n"
            "Write one short assistant reply for a DM conversation."
        )

        try:
            response = self.client.responses.create(
                model=self.settings.openai_model,
                input=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )

            reply_text = getattr(response, "output_text", None)

            if not reply_text:
                return {
                    "used_ai": False,
                    "stub": False,
                    "reason": "Empty OpenAI response",
                    "reply_text": None,
                }

            return {
                "used_ai": True,
                "stub": False,
                "reason": None,
                "reply_text": reply_text.strip(),
            }

        except Exception as exc:
            return {
                "used_ai": False,
                "stub": False,
                "reason": f"OpenAI error: {exc}",
                "reply_text": None,
            }