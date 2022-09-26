from asyncio import Event
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Optional, Protocol


class CacheTaskExecutionInfo:
    fail: bool = True
    retries: int = 0
    backoff_in_seconds: int = 0


@dataclass
class CachedValue:
    last_fetched: Optional[datetime] = None
    inflight: Optional[Event] = None
    value: Any = None


class AsyncFunction(Protocol):
    def __call__(self, *args: Any, **kwds: Any) -> Awaitable[Any]:
        pass


class SyncFunction(Protocol):
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass
