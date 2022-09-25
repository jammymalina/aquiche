from asyncio import Lock, Event, sleep as asleep, iscoroutinefunction
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Any, Awaitable, Optional, Tuple


class CacheTaskExecutionInfo:
    fail: bool = True
    retries: int = 0
    backoff_in_seconds: int = 0


@dataclass
class CachedValue:
    last_fetched: Optional[datetime] = None
    inflight: Optional[Event] = None
    value: Any = None


class CachedRecord:
    __mutex: Lock
    __refresh_interval: Optional[timedelta]
    __cached_value: CachedValue

    def __init__(self, refresh_interval: Optional[timedelta] = None) -> None:
        self.__mutex = Lock()
        self.__refresh_interval = refresh_interval
        self.__cached_value = CachedValue()

    async def get_cached(self, task: Awaitable[Any], task_exec_info: Optional[CacheTaskExecutionInfo] = None) -> Any:
        event = Event()
        await self.__mutex.acquire()

        if self.__cached_value.last_fetched is not None and (
            self.__refresh_interval is None
            or (datetime.now(timezone.utc) - self.__cached_value.last_fetched) < self.__refresh_interval
        ):
            self.__mutex.release()
            return self.__cached_value.value

        if self.__cached_value.inflight is not None:
            event = self.__cached_value.inflight
            self.__mutex.release()
        else:
            self.__cached_value.inflight = Event()
            event = self.__cached_value.inflight
            self.__mutex.release()

            task_exec_info = task_exec_info or CacheTaskExecutionInfo()
            await self.__store_cache(task=task, exec_info=task_exec_info)

        await event.wait()

        return self.__cached_value.value

    async def __store_cache(self, task: Awaitable[Any], exec_info: CacheTaskExecutionInfo) -> None:
        if self.__cached_value.inflight is None:
            raise RuntimeError("Aquiche internal error - possible deadlock")
        value, is_error = await self.__execute_task(task=task, exec_info=exec_info)

        async with self.__mutex:
            event = self.__cached_value.inflight
            self.__cached_value.value = value
            self.__cached_value.last_fetched = datetime.now(timezone.utc)
            self.__cached_value.inflight = None
            event.set()

        if is_error and exec_info.fail:
            raise value

    async def __execute_task(self, task: Awaitable[Any], exec_info: CacheTaskExecutionInfo) -> Tuple[Any, bool]:
        while True:
            try:
                if iscoroutinefunction(task):
                    return (await task(), False)
                return (await task, False)
            except Exception as err:
                if retry_iter >= exec_info.retries:
                    return err, True

                if exec_info.backoff_in_seconds != 0:
                    sleep_seconds = exec_info.backoff_in_seconds * 2**retry_iter + random.uniform(0, 1)
                    await asleep(sleep_seconds)

                retry_iter += 1
