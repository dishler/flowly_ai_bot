import json
from typing import Any, Dict, List, Optional

from openai import OpenAI

from app.core.config import get_settings


class OpenAIClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client = OpenAI(api_key=self.settings.openai_api_key) if self.settings.openai_api_key else None

    def _normalize_history(self, history: Optional[List[Any]]) -> List[Dict[str, str]]:
        if not history:
            return []

        normalized_history: List[Dict[str, str]] = []

        for item in history:
            if isinstance(item, dict):
                role = str(item.get("role", "user")).strip() or "user"
                content = str(item.get("content", "")).strip()
                if content:
                    normalized_history.append(
                        {
                            "role": role,
                            "content": content,
                        }
                    )
                continue

            if isinstance(item, str):
                content = item.strip()
                if content:
                    normalized_history.append(
                        {
                            "role": "user",
                            "content": content,
                        }
                    )

        return normalized_history

    def _build_default_system_prompt(self) -> str:
        return (
            "You are an AI assistant for a business handling Instagram and Facebook DMs. "
            "Reply in the user's language. "
            "Keep replies short, natural, and practical. "
            "Do not sound robotic, overly formal, or salesy. "
            "Do not start replies with filler like 'Yes —', 'Sure —', or similar unless truly necessary. "
            "Your goal is to help the user and gently move the conversation toward a consultation when appropriate. "
            "If asked about price, say pricing starts from 300 USD, but do not present it as a fixed price for every case. "
            "If you need more information, ask at most 1 short clarifying question unless more is truly necessary. "
            "Prefer one compact paragraph over a long explanation. "
            "Do not invent calendar slots. "
            "Do not say booking is confirmed unless an event was actually created. "
            "If date or time is unclear, ask one short clarification question. "
            "Do not invent integrations, timelines, or guaranteed business results. "
            "Do not use bullet points unless absolutely necessary."
        )

    def _build_messages(
        self,
        user_message: str,
        history: List[Dict[str, str]],
        grounding_context: Optional[Dict[str, Any]] = None,
        system_instruction: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        messages: List[Dict[str, str]] = []

        base_system_prompt = self._build_default_system_prompt()
        if system_instruction:
            system_prompt = f"{base_system_prompt}\n\nAdditional instructions:\n{system_instruction.strip()}"
        else:
            system_prompt = base_system_prompt

        messages.append(
            {
                "role": "system",
                "content": system_prompt,
            }
        )

        if grounding_context:
            grounding_json = json.dumps(grounding_context, ensure_ascii=False, indent=2)
            messages.append(
                {
                    "role": "system",
                    "content": (
                        "Use the following business knowledge context as the only factual grounding source. "
                        "If some detail is not present here, do not invent it.\n\n"
                        f"{grounding_json}"
                    ),
                }
            )

        trimmed_history = history[-10:] if history else []
        for item in trimmed_history:
            role = item.get("role", "user")
            content = item.get("content", "").strip()
            if content:
                messages.append(
                    {
                        "role": role,
                        "content": content,
                    }
                )

        messages.append(
            {
                "role": "user",
                "content": user_message.strip(),
            }
        )

        return messages

    def _messages_to_responses_input(self, messages: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        response_input: List[Dict[str, Any]] = []

        for message in messages:
            role = message["role"]
            content = message["content"]
            response_input.append(
                {
                    "role": role,
                    "content": [
                        {
                            "type": "input_text",
                            "text": content,
                        }
                    ],
                }
            )

        return response_input

    def generate_reply(
        self,
        user_message: str,
        history: Optional[List[Any]] = None,
        grounding_context: Optional[Dict[str, Any]] = None,
        system_instruction: Optional[str] = None,
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

        cleaned_user_message = user_message.strip()
        if not cleaned_user_message:
            return {
                "used_ai": False,
                "stub": False,
                "reason": "Empty user message",
                "reply_text": None,
            }

        normalized_history = self._normalize_history(history)

        messages = self._build_messages(
            user_message=cleaned_user_message,
            history=normalized_history,
            grounding_context=grounding_context,
            system_instruction=system_instruction,
        )

        try:
            response = self.client.responses.create(
                model=self.settings.openai_model,
                input=self._messages_to_responses_input(messages),
            )

            reply_text = getattr(response, "output_text", None)

            if not reply_text:
                return {
                    "used_ai": False,
                    "stub": False,
                    "reason": "Empty OpenAI response",
                    "reply_text": None,
                }

            cleaned_reply = reply_text.strip()
            if not cleaned_reply:
                return {
                    "used_ai": False,
                    "stub": False,
                    "reason": "Blank OpenAI response",
                    "reply_text": None,
                }

            return {
                "used_ai": True,
                "stub": False,
                "reason": None,
                "reply_text": cleaned_reply,
            }

        except Exception as exc:
            return {
                "used_ai": False,
                "stub": False,
                "reason": f"OpenAI error: {exc}",
                "reply_text": None,
            }