from abc import ABCMeta, abstractmethod
from asyncio import iscoroutine
from datetime import datetime, timedelta, timezone
from typing import Union

from aquiche._core import AsyncFunction, CachedValue, SyncFunction
from aquiche.utils._async_utils import awaitify


def __get_cache_func_value(cached_value: CachedValue) -> dict:
    assert cached_value.last_fetched is not None
    return {"value": cached_value.value, "last_fetched": cached_value.last_fetched}


class CacheExpiration(metaclass=ABCMeta):
    @abstractmethod
    def is_value_expired(self, value: CachedValue) -> bool:
        ...


class AsyncCacheExpiration(metaclass=ABCMeta):
    @abstractmethod
    async def is_value_expired(self, value: CachedValue) -> bool:
        ...


class NonExpiringCacheExpiration(CacheExpiration):
    def is_value_expired(self, value: CachedValue) -> bool:
        return False


class DateCacheExpiration(CacheExpiration):
    __expiry_date: datetime

    def __init__(self, expiry_date: datetime) -> None:
        super().__init__()
        if expiry_date.tzinfo is not None and expiry_date.tzinfo.utcoffset(expiry_date) is not None:
            self.__expiry_date = expiry_date
        else:
            self.__expiry_date = expiry_date.replace(tzinfo=timezone.utc)

    def is_value_expired(self, value: CachedValue) -> bool:
        if value.last_fetched is None:
            return True
        return value.last_fetched >= self.__expiry_date


class RefreshingCacheExpiration(CacheExpiration):
    __refresh_interval: timedelta

    def __init__(self, refresh_interval: timedelta) -> None:
        super().__init__()
        self.refresh_interval = refresh_interval

    def is_value_expired(self, value: CachedValue) -> bool:
        if value.last_fetched is None:
            return True
        return (datetime.now(timezone.utc) - value.last_fetched) >= self.__refresh_interval


class SyncFuncCacheExpiration(CacheExpiration):
    __func: SyncFunction

    def __init__(self, func: SyncFunction) -> None:
        super().__init__()
        self.__func = func

    def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = self.__func(__get_cache_func_value(value))
        cache_expiration = get_cache_expiration(expiry_value)

        if isinstance(cache_expiration, AsyncCacheExpiration):
            raise ValueError("Invalid expiration resolver - use sync function")

        return cache_expiration.is_value_expired(value)


class AsyncFuncCacheExpiration(AsyncCacheExpiration):
    __func: AsyncFunction

    def __init__(self, func: Union[SyncFunction, AsyncFunction]) -> None:
        super().__init__()
        self.__func = awaitify(func)

    async def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = await self.__func(__get_cache_func_value(value))
        expiry_value = self.__func()
        cache_expiration = get_cache_expiration(expiry_value)
        if isinstance(cache_expiration, CacheExpiration):
            return cache_expiration.is_value_expired(value)
        return await cache_expiration.is_value_expired(value)


def get_cache_expiration(
    value: Union[int, float, str], prefer_async: bool = True
) -> Union[CacheExpiration, AsyncCacheExpiration]:
    if isinstance(value, (float, int)):
        return __get_cache_expiration_from_num(value)
    if isinstance(value, str):
        return __get_cache_expiration_from_str(value)
    if iscoroutine(value):
        return AsyncFuncCacheExpiration(value)
    if callable(value):
        return AsyncFuncCacheExpiration(value) if prefer_async else SyncFuncCacheExpiration(value)
    raise ValueError("Invalid cache expiration value")


def __get_cache_expiration_from_num(value: Union[int, float]) -> CacheExpiration:
    # if the int is large enough we assume it's a timestamp
    value = int(value)
    if value > int(1e8):
        return DateCacheExpiration(expiry_date=datetime.fromtimestamp(value, tz=timezone.utc))
    # otherwise assume it's a refresh interval in seconds
    delta = timedelta(seconds=value)
    return RefreshingCacheExpiration(refresh_interval=delta)


def __get_cache_expiration_from_str(value: str) -> CacheExpiration:
    # iso format
    try:
        return DateCacheExpiration(expiry_date=datetime.fromisoformat(value))
    except ValueError:
        pass
    raise ValueError("Invalid cache expiration value")
