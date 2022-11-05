from asyncio import Lock, Event, sleep as asleep
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
import random
from typing import Any, Awaitable, Callable, Optional, Tuple, Union

from aquiche import errors
from aquiche._core import CachedValue, CacheTaskExecutionInfo
from aquiche._expiration import (
    AsyncCacheExpiration,
    CacheExpiration,
)
from aquiche.utils._async_utils import AsyncWrapperMixin


@dataclass
class AsyncCachedValue(CachedValue):
    inflight: Optional[Event] = None
    exit_stack: Optional[AsyncExitStack] = None

    def destroy_value(self) -> None:
        self.last_fetched = None
        self.value = None
        self.exit_stack = None
        self.is_error = False


class AsyncCachedRecord(AsyncWrapperMixin):
    __lock: Lock
    __get_function: Callable[..., Awaitable[Any]]
    __get_exec_info: CacheTaskExecutionInfo
    __cached_value: AsyncCachedValue
    __expiration: Union[CacheExpiration, AsyncCacheExpiration]
    __negative_expiration: Union[CacheExpiration, AsyncCacheExpiration]

    def __init__(
        self,
        get_function: Callable[..., Awaitable[Any]],
        get_exec_info: CacheTaskExecutionInfo,
        expiration: Union[AsyncCacheExpiration, CacheExpiration],
        negative_expiration: Union[AsyncCacheExpiration, CacheExpiration],
    ) -> None:
        self.__lock = Lock()
        self.__get_function = get_function  # type: ignore
        self.__get_exec_info = get_exec_info
        self.__cached_value = AsyncCachedValue()
        self.__expiration = expiration
        self.__negative_expiration = negative_expiration

    async def get_cached(self) -> Any:
        event = None
        await self.__lock.acquire()

        if self.__cached_value.last_fetched is not None:
            if not await self.is_expired():
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

    async def destroy(self) -> None:
        if self.__cached_value.last_fetched is None:
            return

        if self.__cached_value.exit_stack is not None:
            await self.__cached_value.exit_stack.aclose()

        self.__cached_value.destroy_value()

    async def is_expired(self) -> bool:
        if self.__cached_value.last_fetched is None:
            return False

        expiration = None
        if self.__cached_value.is_error:
            expiration = self.__negative_expiration
        else:
            expiration = self.__expiration

        if isinstance(expiration, AsyncCacheExpiration):
            return await expiration.is_value_expired(self.__cached_value)
        return expiration.is_value_expired(self.__cached_value)

    async def __store_cache(self) -> None:
        if self.__cached_value.inflight is None:
            raise errors.DeadlockError()

        value, is_successful = await self.__execute_task()

        async with self.__lock:
            await self.destroy()
            event = self.__cached_value.inflight
            self.__cached_value.last_fetched = datetime.now(timezone.utc)
            self.__cached_value.inflight = None
            if is_successful:
                value, is_successful = await self.__safe_wrap_exit_stack(value)
            self.__cached_value.value = value
            self.__cached_value.is_error = not is_successful
            event.set()

        if not is_successful and self.__get_exec_info.fail:
            raise value

    async def __execute_task(self) -> Tuple[Any, bool]:
        retry_iter = 0
        while True:
            try:
                return (await self.__get_function(), True)
            except Exception as err:
                if retry_iter >= self.__get_exec_info.retries:
                    return err, False

                if self.__get_exec_info.backoff_in_seconds != 0:
                    sleep_seconds = self.__get_exec_info.backoff_in_seconds * 2**retry_iter + random.uniform(0, 1)
                    await asleep(sleep_seconds)

                retry_iter += 1

    async def __safe_wrap_exit_stack(self, value: Any) -> Tuple[Any, bool]:
        try:
            exit_stack, value = await self.wrap_async_exit_stack(
                value=value, wrap_config=self.__get_exec_info.wrap_async_exit_stack
            )
            self.__cached_value.exit_stack = exit_stack
            return value, True
        except Exception as err:
            return err, False
