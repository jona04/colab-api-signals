import httpx
from typing import Optional


class TelegramNotifier:
    """
    Simple async Telegram notifier using Bot API.
    """

    def __init__(self, bot_token: str, chat_id: str, base_url: str = "https://api.telegram.org"):
        if not bot_token:
            raise ValueError("bot_token is required")
        if not chat_id:
            raise ValueError("chat_id is required")

        self._bot_token = bot_token
        self._chat_id = chat_id
        self._base_url = f"{base_url}/bot{bot_token}"

    async def send_message(self, text: str, parse_mode: Optional[str] = "Markdown") -> None:
        """
        Sends a text message to the configured chat_id.
        """
        payload = {
            "chat_id": self._chat_id,
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{self._base_url}/sendMessage", json=payload)
            resp.raise_for_status()
