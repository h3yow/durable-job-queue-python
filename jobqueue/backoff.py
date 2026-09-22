"""可配置、单调递增的退避策略。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float = 0.1
    factor: float = 2.0
    max_seconds: float = 60.0

    def delay_for(self, attempt: int) -> float:
        """第 attempt 次失败(从 1 计)后的重排延迟, 单调递增且有上限。"""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        return min(self.base_seconds * (self.factor ** (attempt - 1)), self.max_seconds)
