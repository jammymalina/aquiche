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


@dataclass
class CachedValue:
    last_fetched: Optional[datetime] = None
    value: Any = None
    is_error: bool = False

    def destroy_value(self) -> None:
        ...
