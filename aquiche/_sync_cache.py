from dataclasses import dataclass
from datetime import datetime, timezone
import random
from threading import Event, RLock
from time import sleep
from typing import Any, Callable, Optional, Tuple, Union

from aquiche import errors
from aquiche._core import CachedValue, CacheTaskExecutionInfo
from aquiche._expiration import (
    AsyncCacheExpiration,
    CacheExpiration,
)


@dataclass
class SyncCachedValue(CachedValue):
    inflight: Optional[Event] = None

    def destroy_value(self) -> None:
        self.last_fetched = None
        self.value = None
        self.is_error = False


class SyncCachedRecord:
    __lock: RLock
    __get_function: Callable[..., Any]
    __get_exec_info: CacheTaskExecutionInfo
    __cached_value: SyncCachedValue
    __expiration: CacheExpiration
    __negative_expiration: CacheExpiration

    def __init__(
        self,
        get_function: Callable[..., Any],
        get_exec_info: CacheTaskExecutionInfo,
        expiration: Union[CacheExpiration, AsyncCacheExpiration],
        negative_expiration: Union[CacheExpiration, AsyncCacheExpiration],
    ) -> None:
        expiration, negative_expiration = self.__validate_expirations(expiration, negative_expiration)
        self.__lock = RLock()
        self.__get_function = get_function  # type: ignore
        self.__get_exec_info = get_exec_info
        self.__cached_value = SyncCachedValue()
        self.__expiration = expiration
        self.__negative_expiration = negative_expiration

    def get_cached(self) -> Any:
        event = None
        self.__lock.acquire()

        if self.__cached_value.last_fetched is not None:
            if not self.is_expired():
                self.__lock.release()
                return self.__cached_value.value

        if self.__cached_value.inflight is not None:
            event = self.__cached_value.inflight
            self.__lock.release()
        else:
            self.__cached_value.inflight = Event()
            event = self.__cached_value.inflight
            self.__lock.release()

            self.__store_cache()

        event.wait()

        return self.__cached_value.value

    def destroy(self) -> None:
        if self.__cached_value.last_fetched is None:
            return

        self.__cached_value.destroy_value()

    def is_expired(self) -> bool:
        if self.__cached_value.last_fetched is None:
            return False

        if self.__cached_value.is_error:
            return self.__negative_expiration.is_value_expired(self.__cached_value)

        return self.__expiration.is_value_expired(self.__cached_value)

    def __validate_expirations(
        self,
        expiration: Union[CacheExpiration, AsyncCacheExpiration],
        negative_expiration: Union[CacheExpiration, AsyncCacheExpiration],
    ) -> Tuple[CacheExpiration, CacheExpiration]:
        error_messages = []
        if isinstance(expiration, AsyncCacheExpiration):
            error_messages.append("invalid expiration, use values that evaluate to 'sync' objects")
        if isinstance(negative_expiration, AsyncCacheExpiration):
            error_messages.append("invalid negative expiration, use values that evaluate to 'sync' objects")

        if len(error_messages) > 0:
            raise errors.InvalidCacheConfig(error_messages)
        return expiration, negative_expiration  # type: ignore

    def __store_cache(self) -> None:
        if self.__cached_value.inflight is None:
            raise errors.DeadlockError()

        value, is_successful = self.__execute_task()

        with self.__lock:
            self.destroy()
            event = self.__cached_value.inflight
            self.__cached_value.last_fetched = datetime.now(timezone.utc)
            self.__cached_value.inflight = None
            self.__cached_value.value = value
            self.__cached_value.is_error = not is_successful
            event.set()

        if not is_successful and self.__get_exec_info.fail:
            raise value

    def __execute_task(self) -> Tuple[Any, bool]:
        retry_iter = 0
        while True:
            try:
                return (self.__get_function(), True)
            except Exception as err:
                if retry_iter >= self.__get_exec_info.retries:
                    return err, False

                if self.__get_exec_info.backoff_in_seconds != 0:
                    sleep_seconds = self.__get_exec_info.backoff_in_seconds * 2**retry_iter + random.uniform(0, 1)
                    sleep(sleep_seconds)

                retry_iter += 1
