from asyncio import Event
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, List, Optional, Protocol, Union


class CacheTaskExecutionInfo:
    fail: bool = True
    retries: int = 0
    backoff_in_seconds: int = 0
    wrap_async_exit_stack: Union[bool, List[str]] = False


@dataclass
class CachedValue:
    last_fetched: Optional[datetime] = None
    inflight: Optional[Event] = None
    value: Any = None
    exit_stack: Optional[AsyncExitStack] = None

    def destroy_value(self) -> None:
        self.last_fetched = None
        self.value = None
        self.exit_stack = None


class AsyncFunction(Protocol):
    def __call__(self, *args: Any, **kwds: Any) -> Awaitable[Any]:
        pass


class SyncFunction(Protocol):
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass
