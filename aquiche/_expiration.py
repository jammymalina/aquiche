from abc import ABCMeta, abstractmethod
from asyncio import iscoroutine
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Union

from aquiche import errors
from aquiche._core import AsyncFunction, CachedValue, SyncFunction
from aquiche.utils._async_utils import awaitify
from aquiche.utils._attr_utils import rgetattr
from aquiche.utils._time_parse import parse_datetime, parse_date, parse_duration, parse_time


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


class SyncAttributeCacheExpiration(CacheExpiration):
    attribute_path: str

    def __init__(self, attribute_path: str) -> None:
        super().__init__()
        self.attribute_path = attribute_path.strip().lstrip("$.")

    def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = rgetattr(obj=value.value, attr=self.attribute_path)
        cache_expiration = get_cache_expiration(expiry_value)
        __validate_sync_expiration(cache_expiration=cache_expiration, value=value)
        return cache_expiration.is_value_expired(value)  # type: ignore


class SyncFuncCacheExpiration(CacheExpiration):
    __func: SyncFunction

    def __init__(self, func: SyncFunction) -> None:
        super().__init__()
        self.__func = func

    def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = self.__func(__get_cache_func_value(value))
        cache_expiration = get_cache_expiration(expiry_value)
        __validate_sync_expiration(cache_expiration=cache_expiration, value=value)
        return cache_expiration.is_value_expired(value)  # type: ignore


class AsyncAttributeCacheExpiration(AsyncCacheExpiration):
    attribute_path: str

    def __init__(self, attribute_path: str) -> None:
        super().__init__()
        self.attribute_path = attribute_path.strip().lstrip("$.")

    async def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = rgetattr(obj=value.value, attr=self.attribute_path)
        cache_expiration = get_cache_expiration(expiry_value)
        if isinstance(cache_expiration, CacheExpiration):
            return cache_expiration.is_value_expired(value)
        return await cache_expiration.is_value_expired(value)


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
        return __get_cache_expiration_from_str(value=value.strip(), prefer_async=prefer_async)
    if iscoroutine(value):
        return AsyncFuncCacheExpiration(value)
    if callable(value):
        return AsyncFuncCacheExpiration(value) if prefer_async else SyncFuncCacheExpiration(value)
    raise ValueError("Invalid cache expiration value")


def __get_cache_expiration_from_num(value: Union[int, float]) -> CacheExpiration:
    # if the number is large enough we assume it's a timestamp
    value = int(value)
    if value > int(1e8):
        return DateCacheExpiration(expiry_date=parse_datetime(value))
    # otherwise assume it's a refresh interval in seconds
    return RefreshingCacheExpiration(refresh_interval=parse_duration(value))


def __get_cache_expiration_from_str(value: str, prefer_async: bool) -> Union[CacheExpiration, AsyncCacheExpiration]:
    if value.startswith("$."):
        return (
            AsyncAttributeCacheExpiration(attribute_path=value)
            if prefer_async
            else SyncAttributeCacheExpiration(attribute_path=value)
        )

    parsed_value = None
    parse_functions = (parse_datetime, parse_date, parse_time, parse_duration)
    for parse_function in parse_functions:
        try:
            parsed_value = parse_function(value)
            break
        except Exception:
            pass

    if parsed_value is None:
        raise errors.InvalidTimeFormatError(value)

    if isinstance(parsed_value, datetime):
        return DateCacheExpiration(expiry_date=parsed_value)

    if isinstance(parsed_value, date):
        return DateCacheExpiration(expiry_date=datetime.combine(parsed_value, datetime.min.time(), tzinfo=timezone.utc))

    if isinstance(parsed_value, time):
        return DateCacheExpiration(expiry_date=datetime.combine(date.today(), parsed_value))

    if isinstance(parsed_value, timedelta):
        return RefreshingCacheExpiration(refresh_interval=parsed_value)

    raise errors.InvalidTimeFormatError(value)


def __validate_sync_expiration(cache_expiration: Union[CacheExpiration, AsyncCacheExpiration], value: Any) -> None:
    if isinstance(cache_expiration, AsyncCacheExpiration):
        raise ValueError(f"Invalid cache expiration value '{value}': it resolves to async expiration")
