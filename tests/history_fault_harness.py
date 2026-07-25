from __future__ import annotations

from collections import deque
from typing import Any


class FaultSequence:
    """Deterministic provider/storage fault sequence for recovery tests."""

    def __init__(self, *outcomes: Any) -> None:
        self.outcomes = deque(outcomes)
        self.calls = 0

    def __call__(self, *_args: Any, **_kwargs: Any) -> Any:
        self.calls += 1
        if not self.outcomes:
            raise AssertionError("fault sequence was exhausted")
        outcome = self.outcomes.popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome
