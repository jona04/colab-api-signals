from abc import ABC, abstractmethod


class TriggerEventRepository(ABC):
    """
    Repository contract for trigger-event idempotency.

    The goal is to ensure api-signals processes a closed candle at most once
    per (indicator_set_id, ts). This prevents duplicated processing if:
      - api-market-data retries the webhook
      - network timeouts cause a second POST
      - multiple workers deliver the same event
    """

    @abstractmethod
    async def mark_if_new(self, indicator_set_id: str, ts: int) -> bool:
        """
        Atomically mark an event as processed if it has not been processed yet.

        Args:
            indicator_set_id: Indicator set logical id (cfg_hash).
            ts: Candle close timestamp in milliseconds.

        Returns:
            True if this event was newly marked (caller should process),
            False if it was already processed (caller should skip).
        """
        raise NotImplementedError
