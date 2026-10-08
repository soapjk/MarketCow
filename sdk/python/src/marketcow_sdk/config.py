from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TimeoutConfig:
    connect: float = 3.0
    read: float = 15.0
    write: float = 10.0
    pool: float = 5.0

    def __post_init__(self) -> None:
        if min(self.connect, self.read, self.write, self.pool) <= 0:
            raise ValueError("timeouts must be positive")


@dataclass(frozen=True)
class RetryConfig:
    maximum_attempts: int = 3
    base_delay_seconds: float = 0.25
    maximum_delay_seconds: float = 5.0
    jitter_seconds: float = 0.1

    def __post_init__(self) -> None:
        if not 1 <= self.maximum_attempts <= 10:
            raise ValueError("maximum_attempts must be between 1 and 10")
        if min(self.base_delay_seconds, self.maximum_delay_seconds, self.jitter_seconds) < 0:
            raise ValueError("retry delays must not be negative")
        if self.base_delay_seconds > self.maximum_delay_seconds:
            raise ValueError("base retry delay must not exceed maximum delay")


@dataclass(frozen=True)
class ClientConfig:
    base_url: str
    timeout: TimeoutConfig = field(default_factory=TimeoutConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    user_agent: str = "marketcow-client/0.1.0"
    maximum_response_bytes: int = 16 * 1024 * 1024
    trust_env: bool = False

    def __post_init__(self) -> None:
        normalized = self.base_url.rstrip("/")
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("base_url must use http or https")
        if not self.user_agent.strip():
            raise ValueError("user_agent must not be empty")
        if not 1024 <= self.maximum_response_bytes <= 256 * 1024 * 1024:
            raise ValueError("maximum_response_bytes is outside the supported range")
        object.__setattr__(self, "base_url", normalized)
