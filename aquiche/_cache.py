from asyncio import Lock, Event, sleep as asleep
from datetime import datetime, timezone
import random
from typing import Any, Optional, Tuple, Union

from aquiche._core import AsyncFunction, CachedValue, CacheTaskExecutionInfo
from aquiche import errors
from aquiche._expiration import AsyncCacheExpiration, CacheExpiration, NonExpiringCacheExpiration


class AsyncCachedRecord:
    __lock: Lock
    __get_function: AsyncFunction
    __get_exec_info: CacheTaskExecutionInfo
    __cached_value: CachedValue
    __expiration: Union[CacheExpiration, AsyncCacheExpiration]

    def __init__(
        self,
        get_function: AsyncFunction,
        get_exec_info: Optional[CacheTaskExecutionInfo] = None,
        expiration: Optional[CacheExpiration] = None,
    ) -> None:
        self.__lock = Lock()
        self.__get_function = get_function
        self.__get_exec_info = get_exec_info or CacheTaskExecutionInfo()
        self.__cached_value = CachedValue()
        self.__expiration = expiration or NonExpiringCacheExpiration()

    async def get_cached(self) -> Any:
        event = Event()
        await self.__lock.acquire()

        if self.__cached_value.last_fetched is not None:
            is_expired = (
                await self.__expiration.is_value_expired(self.__cached_value)
                if isinstance(self.__expiration, AsyncCacheExpiration)
                else self.__expiration.is_value_expired(self.__cached_value)
            )
            if not is_expired:
                self.__lock.release()
                return self.__cached_value.value

        if self.__cached_value.inflight is not None:
            event = self.__cached_value.inflight
            self.__lock.release()
        else:
            self.__cached_value.inflight = Event()
            event = self.__cached_value.inflight
            self.__lock.release()

            await self.__store_cache()

        await event.wait()

        return self.__cached_value.value

    async def __store_cache(self) -> None:
        if self.__cached_value.inflight is None:
            raise errors.DeadlockError()
        value, is_error = await self.__execute_task()

        async with self.__lock:
            event = self.__cached_value.inflight
            self.__cached_value.value = value
            self.__cached_value.last_fetched = datetime.now(timezone.utc)
            self.__cached_value.inflight = None
            event.set()

        if is_error and self.__get_exec_info.fail:
            raise value

    async def __execute_task(self) -> Tuple[Any, bool]:
        retry_iter = 0
        while True:
            try:
                return (await self.__get_function(), False)
            except Exception as err:
                if retry_iter >= self.__get_exec_info.retries:
                    return err, True

                if self.__get_exec_info.backoff_in_seconds != 0:
                    sleep_seconds = self.__get_exec_info.backoff_in_seconds * 2**retry_iter + random.uniform(0, 1)
                    await asleep(sleep_seconds)

                retry_iter += 1
