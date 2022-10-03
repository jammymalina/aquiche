from abc import ABCMeta, abstractmethod
from asyncio import iscoroutinefunction
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Coroutine, Union

from aquiche import errors
from aquiche._core import AsyncFunction, CachedValue, SyncFunction
from aquiche.utils._async_utils import awaitify
from aquiche.utils._extraction_utils import extract_from_obj
from aquiche.utils._time_parse import parse_datetime, parse_date, parse_duration, parse_time


def _get_cache_func_value(cached_value: CachedValue) -> dict:
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


def _validate_sync_expiration(cache_expiration: Union[CacheExpiration, AsyncCacheExpiration], value: Any) -> None:
    if isinstance(cache_expiration, AsyncCacheExpiration):
        if (iscoroutinefunction(value) or callable(value)) and hasattr(value, "__name__"):
            value = value.__name__

        raise errors.InvalidSyncExpirationType(value)


class NonExpiringCacheExpiration(CacheExpiration):
    def is_value_expired(self, value: CachedValue) -> bool:
        return value.last_fetched is None

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, NonExpiringCacheExpiration)


class DateCacheExpiration(CacheExpiration):
    __expiry_date: datetime

    def __init__(self, expiry_date: datetime) -> None:
        super().__init__()
        if expiry_date.tzinfo is not None and expiry_date.tzinfo.utcoffset(expiry_date) is not None:
            self.__expiry_date = expiry_date
        else:
            self.__expiry_date = expiry_date.replace(tzinfo=timezone.utc)

    @property
    def expiry_date(self) -> datetime:
        return self.__expiry_date

    def is_value_expired(self, value: CachedValue) -> bool:
        if value.last_fetched is None:
            return True
        return value.last_fetched >= self.expiry_date

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, DateCacheExpiration) and self.expiry_date == other.expiry_date


class RefreshingCacheExpiration(CacheExpiration):
    __refresh_interval: timedelta

    def __init__(self, refresh_interval: timedelta) -> None:
        super().__init__()
        self.__refresh_interval = refresh_interval

    @property
    def refresh_interval(self) -> timedelta:
        return self.__refresh_interval

    def is_value_expired(self, value: CachedValue) -> bool:
        if value.last_fetched is None:
            return True
        return (datetime.now(timezone.utc) - value.last_fetched) >= self.refresh_interval

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, RefreshingCacheExpiration) and self.refresh_interval == other.refresh_interval


class SyncAttributeCacheExpiration(CacheExpiration):
    __attribute_path: str

    def __init__(self, attribute_path: str) -> None:
        super().__init__()
        self.__attribute_path = attribute_path.strip().lstrip("$.")

    @property
    def attribute_path(self) -> str:
        return self.__attribute_path

    def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = extract_from_obj(obj=value.value, attribute_path=self.attribute_path)
        cache_expiration = get_cache_expiration(value=expiry_value, prefer_async=False)
        _validate_sync_expiration(cache_expiration=cache_expiration, value=expiry_value)
        return cache_expiration.is_value_expired(value)  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SyncAttributeCacheExpiration) and self.attribute_path == other.attribute_path


class SyncFuncCacheExpiration(CacheExpiration):
    __func: SyncFunction

    def __init__(self, func: SyncFunction) -> None:
        super().__init__()
        self.__func = func

    @property
    def func(self) -> SyncFunction:
        return self.__func

    def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = self.func(_get_cache_func_value(value))
        cache_expiration = get_cache_expiration(expiry_value, prefer_async=False)
        _validate_sync_expiration(cache_expiration=cache_expiration, value=expiry_value)
        return cache_expiration.is_value_expired(value)  # type: ignore

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SyncFuncCacheExpiration) and self.func == other.func


class AsyncAttributeCacheExpiration(AsyncCacheExpiration):
    __attribute_path: str

    def __init__(self, attribute_path: str) -> None:
        super().__init__()
        self.__attribute_path = attribute_path.strip().lstrip("$.")

    @property
    def attribute_path(self) -> str:
        return self.__attribute_path

    async def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = extract_from_obj(obj=value.value, attribute_path=self.attribute_path)
        cache_expiration = get_cache_expiration(expiry_value)
        if isinstance(cache_expiration, CacheExpiration):
            return cache_expiration.is_value_expired(value)
        return await cache_expiration.is_value_expired(value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, AsyncAttributeCacheExpiration) and self.attribute_path == other.attribute_path


class AsyncFuncCacheExpiration(AsyncCacheExpiration):
    __func: AsyncFunction

    def __init__(self, func: Union[SyncFunction, AsyncFunction, Coroutine]) -> None:
        super().__init__()
        self.__func = awaitify(func)

    @property
    def func(self) -> AsyncFunction:
        return self.__func

    async def is_value_expired(self, value: CachedValue) -> bool:
        expiry_value = await self.__func(_get_cache_func_value(value))
        cache_expiration = get_cache_expiration(expiry_value)
        if isinstance(cache_expiration, CacheExpiration):
            return cache_expiration.is_value_expired(value)
        return await cache_expiration.is_value_expired(value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, AsyncFuncCacheExpiration) and self.func == other.func


def get_cache_expiration(
    value: Union[int, float, str, bytes, date, datetime, time, timedelta, Coroutine, Callable],
    prefer_async: bool = True,
) -> Union[CacheExpiration, AsyncCacheExpiration]:
    if isinstance(value, (float, int)):
        return __get_cache_expiration_from_num(value)
    if isinstance(value, (str, bytes)):
        return __get_cache_expiration_from_str(value=value.strip(), prefer_async=prefer_async)
    if isinstance(value, (date, datetime, time, timedelta)):
        return __get_cache_expiration_from_time(value)
    if iscoroutinefunction(value):
        return AsyncFuncCacheExpiration(value)
    if callable(value):
        return AsyncFuncCacheExpiration(value) if prefer_async else SyncFuncCacheExpiration(value)
    raise errors.InvalidExpirationType()


def __get_cache_expiration_from_num(value: Union[int, float]) -> CacheExpiration:
    # if the number is large enough we assume it's a timestamp
    value = int(value)
    if value > int(1e8):
        return DateCacheExpiration(expiry_date=parse_datetime(value))
    # otherwise assume it's a refresh interval in seconds
    return RefreshingCacheExpiration(refresh_interval=parse_duration(value))


def __get_cache_expiration_from_str(
    value: Union[str, bytes], prefer_async: bool
) -> Union[CacheExpiration, AsyncCacheExpiration]:
    value = __parse_value_to_str(value)

    if value.startswith("$."):
        return (
            AsyncAttributeCacheExpiration(attribute_path=value)
            if prefer_async
            else SyncAttributeCacheExpiration(attribute_path=value)
        )

    parsed_value: Any = None
    parse_functions = (parse_duration, parse_datetime, parse_date, parse_time)
    for parse_function in parse_functions:
        try:
            parsed_value = parse_function(value)
            break
        except Exception:
            pass

    if parsed_value is None:
        raise errors.InvalidTimeFormatError(value)

    return __get_cache_expiration_from_time(parsed_value)


def __get_cache_expiration_from_time(
    value: Union[date, datetime, time, timedelta]
) -> Union[CacheExpiration, AsyncCacheExpiration]:
    if isinstance(value, datetime):
        return DateCacheExpiration(expiry_date=value)

    if isinstance(value, date):
        return DateCacheExpiration(expiry_date=datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc))

    if isinstance(value, time):
        return DateCacheExpiration(expiry_date=datetime.combine(date.today(), value))

    if isinstance(value, timedelta):
        return RefreshingCacheExpiration(refresh_interval=value)

    raise errors.InvalidTimeFormatError(value)


def __parse_value_to_str(value: Union[str, bytes]) -> str:
    if isinstance(value, str):
        return value
    return value.decode()
