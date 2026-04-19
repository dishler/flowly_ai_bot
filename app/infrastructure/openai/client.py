import json
import logging
from typing import Any, Dict, List, Optional

from openai import OpenAI

from app.core.config import get_settings

logger = logging.getLogger(__name__)


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
            "You are Flowly’s sales assistant. "
            "Flowly is a service that builds AI bots for Instagram and Facebook DMs to capture leads, "
            "qualify them, and guide them toward booking.\n\n"

            "Strict rules:\n"
            "- Answer only in the context of Flowly’s actual service.\n"
            "- Do not act as a general marketing, design, branding, customer support, or business consultant.\n"
            "- Do not switch to broad generic advice.\n"
            "- Always interpret the user’s question in the context of Flowly’s service, unless the user clearly asks about something else.\n"
            "- If the user asks whether the service fits a business type, explain how Flowly can work for that type of business.\n"
            "- If the user asks what value it gives, explain business outcomes of Flowly specifically: faster replies, fewer lost leads, more bookings, less manual work.\n"
            "- If the user asks about many repetitive inquiries, explain how Flowly automates handling typical inbound messages.\n"
            "- If the user asks how it works, explain the Flowly service process simply and clearly.\n"
            "- If the user asks what is included, answer only with what is actually included in the service.\n"
            "- If the user asks about price, say pricing starts from 300 USD, but do not present it as a fixed price for every case.\n"
            "- Use the knowledge base as the only factual grounding source.\n"
            "- Do not invent features, channels, integrations, timelines, guarantees, or business results that are not confirmed in the grounding context.\n"
            "- If some detail is not present in the grounding context, keep the answer general and do not guess.\n"
            "- Reply in the user’s language.\n"
            "- Keep replies short, natural, and practical, usually 2–4 sentences.\n"
            "- Prefer one compact paragraph.\n"
            "- Do not use bullet points unless absolutely necessary.\n"
            "- Do not sound robotic, overly formal, or overly salesy.\n"
            "- Do not say you are an AI assistant.\n"
            "- Do not start replies with filler like 'Yes —', 'Sure —', or similar unless truly necessary.\n"
            "- Do not invent calendar slots.\n"
            "- Do not say booking is confirmed unless an event was actually created.\n"
            "- If date or time is unclear, ask one short clarification question.\n\n"

            "Behavior:\n"
            "- Be clear, grounded, concise, and helpful.\n"
            "- When appropriate, gently move the conversation toward a short consultation.\n"
            "- Use soft CTAs like offering to briefly review the user’s case.\n"
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
            system_prompt = f"{base_system_prompt}\nAdditional instructions:\n{system_instruction.strip()}"
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

    def _extract_reply_text(self, response: Any) -> Optional[str]:
        direct_output_text = getattr(response, "output_text", None)
        if isinstance(direct_output_text, str) and direct_output_text.strip():
            return direct_output_text.strip()

        output_items = getattr(response, "output", None)
        if not output_items:
            return None

        for item in output_items:
            content_items = getattr(item, "content", None) or []
            for content_item in content_items:
                text_value = getattr(content_item, "text", None)
                if isinstance(text_value, str) and text_value.strip():
                    return text_value.strip()

        return None

    def generate_reply(
        self,
        user_message: str,
        history: Optional[List[Any]] = None,
        grounding_context: Optional[Dict[str, Any]] = None,
        system_instruction: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self.settings.openai_enabled:
            logger.debug("OpenAI generate_reply fallback: OPENAI_ENABLED is false")
            return {
                "used_ai": False,
                "stub": True,
                "reason": "OPENAI_ENABLED=false",
                "reply_text": None,
            }

        if not self.settings.openai_api_key or self.client is None:
            logger.debug("OpenAI generate_reply fallback: missing OPENAI_API_KEY/client")
            return {
                "used_ai": False,
                "stub": True,
                "reason": "Missing OPENAI_API_KEY",
                "reply_text": None,
            }

        cleaned_user_message = user_message.strip()
        if not cleaned_user_message:
            logger.debug("OpenAI generate_reply fallback: empty user message")
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
            logger.debug(
                "OpenAI generate_reply request: model=%s history_count=%s has_grounding=%s has_system_instruction=%s",
                self.settings.openai_model,
                len(normalized_history),
                bool(grounding_context),
                bool(system_instruction),
            )

            response = self.client.responses.create(
                model=self.settings.openai_model,
                input=self._messages_to_responses_input(messages),
            )

            reply_text = self._extract_reply_text(response)

            if not reply_text:
                logger.debug("OpenAI generate_reply fallback: empty response text")
                return {
                    "used_ai": False,
                    "stub": False,
                    "reason": "Empty OpenAI response",
                    "reply_text": None,
                }

            cleaned_reply = reply_text.strip()
            if not cleaned_reply:
                logger.debug("OpenAI generate_reply fallback: blank response text")
                return {
                    "used_ai": False,
                    "stub": False,
                    "reason": "Blank OpenAI response",
                    "reply_text": None,
                }

            logger.debug("OpenAI generate_reply success: reply_text extracted")
            return {
                "used_ai": True,
                "stub": False,
                "reason": None,
                "reply_text": cleaned_reply,
            }

        except Exception as exc:
            logger.exception("OpenAI generate_reply exception: %s", exc)
            return {
                "used_ai": False,
                "stub": False,
                "reason": f"OpenAI error: {exc}",
                "reply_text": None,
            }