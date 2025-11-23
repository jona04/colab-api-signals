# apps/api-signals/core/services/idempotency_key_service.py

import hashlib
import json
from typing import Optional

from core.domain.entities.signal_entity import SignalEntity


class IdempotencyKeyService:
    """
    Builds deterministic idempotency keys for signal steps.

    Idea:
      - Same (strategy_id, ts, signal_type, action) => same key.
      - Retries of the same step reuse the same key.
    """

    def build_for_signal_step(
        self,
        signal: SignalEntity,
        action: str,
        suffix: Optional[str] = None,
    ) -> str:
        """
        Build a stable key for a given signal + step action.

        :param signal: Signal document (must contain strategy_id, ts, signal_type).
        :param action: Step action, e.g. 'COLLECT', 'WITHDRAW', 'OPEN', etc.
        :param suffix: Optional extra disambiguator (rarely needed).
        """
        base = {
            "strategy_id": signal.strategy_id,
            "ts": signal.ts,
            "signal_type": signal.signal_type,
            "action": str(action or ""),
        }
        if suffix:
            base["suffix"] = str(suffix)

        raw = json.dumps(base, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
