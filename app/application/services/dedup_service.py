from typing import Set


class DedupService:
    def __init__(self) -> None:
        self._seen_message_ids: Set[str] = set()

    def is_duplicate(self, message_mid: str) -> bool:
        return message_mid in self._seen_message_ids

    def mark_processed(self, message_mid: str) -> None:
        self._seen_message_ids.add(message_mid)