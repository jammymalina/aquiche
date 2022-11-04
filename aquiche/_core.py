from asyncio import Event
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Union


@dataclass
class CacheTaskExecutionInfo:
    fail: bool = True
    retries: int = 0
    backoff_in_seconds: Union[int, float] = 0
    wrap_async_exit_stack: Union[bool, List[str]] = False


@dataclass
class CachedItem:
    value: Any
    last_fetched: datetime
    is_error: bool


# Internal cached value
@dataclass
class CachedValue:
    last_fetched: Optional[datetime] = None
    inflight: Optional[Event] = None
    value: Any = None
    exit_stack: Optional[AsyncExitStack] = None
    is_error: bool = False

    def destroy_value(self) -> None:
        self.last_fetched = None
        self.value = None
        self.exit_stack = None
        self.is_error = False
