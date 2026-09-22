"""退避策略：可配置、单调非递减、支持确定性抖动。

delay_for(n)：n 为已经失败的尝试次数（>=1），返回下一次重排的延迟秒数。
序列 base, base*factor, base*factor^2, ... 封顶 max_delay：factor >= 1
保证在封顶前单调非递减，封顶后恒为 max_delay，因此整体单调非递减。
开启 jitter 时采用乘以常数 + 由 n 决定的确定性系数，抖动幅度随延迟缩放，
封顶后恒等，单调性不被破坏；默认 jitter=0，序列完全确定、可测试。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BackoffPolicy:
    base_delay: float = 1.0
    factor: float = 2.0
    max_delay: float = 60.0
    jitter: float = 0.0  # 0~1，抖动比例；0 表示完全确定

    def __post_init__(self) -> None:
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.factor < 1:
            raise ValueError("factor must be >= 1 to keep backoff monotonic")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be within [0, 1]")

    def delay_for(self, failed_attempts: int) -> float:
        """第 failed_attempts 次失败之后，下一次执行前的延迟（秒）。"""
        n = max(1, failed_attempts)
        raw = self.base_delay * (self.factor ** (n - 1))
        delay = min(self.max_delay, raw)
        if self.jitter:
            # 确定性伪随机系数（不依赖随机种子，测试可复现）
            h = ((n * 1103515245 + 12345) & 0x7FFFFFFF) / 0x7FFFFFFF
            delay = min(self.max_delay, delay * (1 + self.jitter * (h - 0.5)))
        return max(0.0, delay)
