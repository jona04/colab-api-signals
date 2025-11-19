import httpx
import logging

class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str | int, logger: logging.Logger | None = None):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._client = httpx.AsyncClient(timeout=10.0)

    async def send_message(self, text: str) -> None:
        if not text:
            return

        # 1) Trunca mensagem pra não estourar limite do Telegram
        MAX_LEN = 3800  # 4096 é o hard limit
        if len(text) > MAX_LEN:
            text = text[:MAX_LEN - 50] + "\n\n[... mensagem truncada ...]"

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            # MELHOR: sem parse_mode no começo, pra descartar erros de Markdown.
            # "parse_mode": "MarkdownV2",
        }

        resp = await self._client.post(url, json=payload)
        if resp.status_code >= 400:
            # Tenta extrair o erro real do Telegram
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text}

            self._logger.error(
                "Telegram error %s: %s",
                resp.status_code,
                data,
            )
            # Não levanta exceção aqui pra não matar a pipeline
            return
