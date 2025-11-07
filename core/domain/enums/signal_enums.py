# apps/api-signals/core/domain/enums/signal_enums.py

from enum import Enum


class SignalStatus(str, Enum):
    """
    Lifecycle of a signal produced by strategy evaluation.
    """
    PENDING = "PENDING"     # created by EvaluateActiveStrategiesUseCase
    EXECUTED = "EXECUTED"   # successfully sent/applied to vault
    FAILED = "FAILED"       # tried to execute but hit an error


class SignalType(str, Enum):
    """
    High-level intent of what needs to happen on the vault / LP.

    We keep them generic enough to map into the concrete HTTP calls
    on the vault controller (collect -> swap -> rebalance).
    """

    # Open a brand new range / first position.
    OPEN_NEW_RANGE = "OPEN_NEW_RANGE"

    # Just realign current range to desired Pa/Pb and caps (no swap).
    REBALANCE_TO_RANGE = "REBALANCE_TO_RANGE"

    # Full maintenance flow on an existing vault:
    # 1) collect fees
    # 2) optionally swap tokens to desired ratio
    # 3) rebalance to target ticks/prices
    FULL_MAINTENANCE = "FULL_MAINTENANCE"
    # (a.k.a collect+swap+rebalance)
