import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Any, Awaitable, Optional


class CacheTaskExecutionInfo:
    fail: bool = True
    retries: int = 0
    backoff_in_seconds: int = 0


@dataclass
class CachedValue:
    last_fetched: Optional[datetime] = None
    inflight: Optional[asyncio.Condition] = None
    value: Any = None


class CachedRecord:
    __mutex: asyncio.Lock
    __refresh_interval: Optional[timedelta]
    __cached_value: CachedValue

    def __init__(self, refresh_interval: Optional[timedelta] = None) -> None:
        self.__mutex = asyncio.Lock()
        self.__refresh_interval = refresh_interval
        self.__cached_value = CachedValue()

    async def get_cached(self, task: Awaitable[Any], task_exec_info: Optional[CacheTaskExecutionInfo] = None) -> Any:
        async with self.__mutex:
            if self.__cached_value.last_fetched is not None and (
                self.__refresh_interval is None
                or (datetime.now(timezone.utc) - self.__cached_value.last_fetched) < self.__refresh_interval
            ):
                return self.__cached_value.value

            if self.__cached_value.inflight is None:
                self.__cached_value.inflight = asyncio.Condition()
                task_exec_info = task_exec_info or CacheTaskExecutionInfo()
                await self.__execute_cache_task(task=task, exec_info=task_exec_info)

        await self.__cached_value.inflight.wait()
        return self.__cached_value.value

    async def __execute_cache_task(self, task: Awaitable[Any], exec_info: CacheTaskExecutionInfo) -> None:
        retry_iter = 0
        while True:
            try:
                self.__cached_value.value = await task
                self.__cached_value.last_fetched = datetime.now(timezone.utc)
            except Exception as err:
                if retry_iter >= exec_info.retries:
                    if exec_info.fail:
                        raise
                    self.__cached_value.value = err
                    self.__cached_value.last_fetched = datetime.now(timezone.utc)

                if exec_info.backoff_in_seconds != 0:
                    sleep_seconds = exec_info.backoff_in_seconds * 2**retry_iter + random.uniform(0, 1)
                    await asyncio.sleep(sleep_seconds)

                retry_iter += 1
            finally:
                if self.__cached_value.inflight is not None:
                    self.__cached_value.inflight.notify_all()
