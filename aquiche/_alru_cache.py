from asyncio import iscoroutinefunction
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from functools import partial, update_wrapper
import sys
from threading import RLock
from typing import Awaitable, Callable, Iterable, List, Optional, Protocol, TypeVar, Union

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from aquiche._cache import SyncCachedRecord
from aquiche._cache_params import CacheParameters, validate_cache_params
from aquiche._core import CacheTaskExecutionInfo
from aquiche.errors import InvalidCacheConfig
from aquiche._expiration import (
    CacheExpirationValue,
    DurationExpirationValue,
    get_cache_expiration,
    NonExpiringCacheExpiration,
)
from aquiche._hash import make_key
from aquiche._repository import CacheRepository, LRUCacheRepository
from aquiche.utils._time_parse import parse_duration
from aquiche.utils._singleton import Singleton

T = TypeVar("T")
P = ParamSpec("P")
C = TypeVar("C", bound=Callable)


@dataclass
class CacheInfo:
    hits: int = 0
    misses: int = 0
    maxsize: Optional[int] = None
    current_size: int = 0
    last_expiration_check: Optional[datetime] = None


class AquicheFunctionWrapper(Protocol[C]):
    cache_info: Callable[[], CacheInfo]
    cache_clear: Callable[[], None]
    cache_parameters: Callable[[], CacheParameters]
    remove_expired: Callable[[], Union[None, Awaitable[None]]]
    destroy: Callable[[], Union[None, Awaitable[None]]]

    __call__: C


def __parse_duration_to_timedelta(duration: Optional[DurationExpirationValue]) -> Optional[timedelta]:
    if duration is None:
        return None
    if isinstance(duration, timedelta):
        return duration
    return parse_duration(duration)


class CacheCleanupRegistry(metaclass=Singleton):
    __destroy_callbacks: List[Callable[[], Union[None, Awaitable[None]]]]
    __clear_callbacks: List[Callable[[], Union[None, Awaitable[None]]]]

    def __init__(self) -> None:
        self.__destroy_callbacks = []
        self.__clear_callbacks = []

    def register_destroy_callback(self, destroy_callback: Callable[[], Union[None, Awaitable[None]]]) -> None:
        self.__destroy_callbacks.append(destroy_callback)

    def get_destroy_callbacks(self) -> Iterable[Callable[[], Union[None, Awaitable[None]]]]:
        return iter(self.__destroy_callbacks)

    def register_clear_callback(self, clear_callback: Callable[[], Union[None, Awaitable[None]]]) -> None:
        self.__clear_callbacks.append(clear_callback)

    def get_clear_callbacks(self) -> Iterable[Callable[[], Union[None, Awaitable[None]]]]:
        return iter(self.__clear_callbacks)


def alru_cache(
    __func: Optional[Callable[P, T]] = None,
    enabled: bool = True,
    maxsize: Optional[int] = None,
    expiration: Optional[CacheExpirationValue] = None,
    expired_items_auto_removal_period: Optional[DurationExpirationValue] = "10 minutes",
    wrap_async_exit_stack: Union[bool, List[str], None] = None,
    negative_cache: bool = False,
    negative_expiration: Optional[CacheExpirationValue] = "10 seconds",
    retry_count: int = 0,
    backoff_in_seconds: Union[int, float] = 0,
) -> AquicheFunctionWrapper[Callable[P, T]]:
    validate_cache_params(
        enabled=enabled,
        maxsize=maxsize,
        expiration=expiration,
        expired_items_auto_removal_period=expired_items_auto_removal_period,
        wrap_async_exit_stack=wrap_async_exit_stack,
        negative_cache=negative_cache,
        negative_expiration=negative_expiration,
        retry_count=retry_count,
        backoff_in_seconds=backoff_in_seconds,
    )
    cache_params = CacheParameters(
        enabled=enabled,
        maxsize=maxsize,
        expiration=expiration,
        expired_items_auto_removal_period=expired_items_auto_removal_period,
        wrap_async_exit_stack=wrap_async_exit_stack,
        negative_cache=negative_cache,
        negative_expiration=negative_expiration,
        retry_count=retry_count,
        backoff_in_seconds=backoff_in_seconds,
    )
    if maxsize is not None:
        # Negative maxsize is treated as 0
        maxsize = max(maxsize, 0)
    # Negative retry count is treated as 0
    retry_count = max(retry_count, 0)

    if callable(__func):
        # The user_function was passed in directly via the hidden __func argument
        user_function = __func
        if iscoroutinefunction(user_function):
            wrapper = _async_lru_cache_wrapper(
                user_function,
                **asdict(cache_params),
            )
        else:
            wrapper = _sync_lru_cache_wrapper(
                user_function,
                **asdict(cache_params),
            )
        wrapper.cache_parameters = lambda: cache_params  # type: ignore
        return update_wrapper(wrapper, user_function)

    def decorating_function(user_function: Callable[P, T]):
        if iscoroutinefunction(user_function):
            wrapper = _async_lru_cache_wrapper(
                user_function,
                **asdict(cache_params),
            )
        else:
            wrapper = _sync_lru_cache_wrapper(
                user_function,
                **asdict(cache_params),
            )
        wrapper.cache_parameters = lambda: cache_params  # type: ignore
        return update_wrapper(wrapper, user_function)

    return decorating_function  # type: ignore


async def clear_all() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for clear_callback in cleanup_repository.get_clear_callbacks():
        if iscoroutinefunction(clear_callback):
            await clear_callback()
        else:
            clear_callback()


def clear_all_sync() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for clear_callback in cleanup_repository.get_clear_callbacks():
        if not iscoroutinefunction(clear_callback):
            clear_callback()


async def clear_all_async() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for clear_callback in cleanup_repository.get_clear_callbacks():
        if iscoroutinefunction(clear_callback):
            await clear_callback()


async def destroy_all() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for destroy_callback in cleanup_repository.get_destroy_callbacks():
        if iscoroutinefunction(destroy_callback):
            await destroy_callback()
        else:
            destroy_callback()


def destroy_all_sync() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for destroy_callback in cleanup_repository.get_destroy_callbacks():
        if not iscoroutinefunction(destroy_callback):
            destroy_callback()


async def destroy_all_async() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for destroy_callback in cleanup_repository.get_destroy_callbacks():
        if iscoroutinefunction(destroy_callback):
            await destroy_callback()


def _sync_lru_cache_wrapper(
    user_function: Callable[P, T],
    enabled: bool,
    maxsize: Optional[int],
    expiration: Optional[CacheExpirationValue],
    expired_items_auto_removal_period: Optional[DurationExpirationValue],
    wrap_async_exit_stack: Union[bool, List[str], None],
    negative_cache: bool,
    negative_expiration: Optional[CacheExpirationValue],
    retry_count: int,
    backoff_in_seconds: Union[int, float],
) -> AquicheFunctionWrapper[Callable[P, T]]:
    if wrap_async_exit_stack:
        raise InvalidCacheConfig(["wrap_async_exit_stack can only be used with async functions"])

    sentinel = object()  # unique object used to signal cache misses

    cache: CacheRepository = LRUCacheRepository(maxsize=maxsize)
    cleanup_repository = CacheCleanupRegistry()

    hits = misses = 0
    lock = RLock()  # because cache updates aren't thread-safe
    last_expiration_check = datetime.fromtimestamp(0, tz=timezone.utc)
    expiry_period = __parse_duration_to_timedelta(expired_items_auto_removal_period)

    def __is_cache_enabled() -> bool:
        if maxsize == 0:
            return False
        if callable(enabled):
            return enabled()
        return enabled

    def __remove_expired() -> None:
        nonlocal last_expiration_check
        last_expiration_check = datetime.now(timezone.utc)
        removed_items = cache.filter(lambda _key, record: not record.is_expired())
        for removed_item in removed_items:
            removed_item.destroy()

    def __schedule_remove_expired() -> None:
        if expiry_period is None:
            return
        if datetime.now(timezone.utc) - last_expiration_check >= expiry_period:
            __remove_expired()

    if not __is_cache_enabled():

        def wrapper(*args, **kwargs) -> T:
            # No caching -- just a statistics update
            nonlocal cache, misses
            misses += 1
            result = user_function(*args, **kwargs)
            return result

    elif maxsize is None:

        def wrapper(*args, **kwargs) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            __schedule_remove_expired()
            key = make_key(*args, **kwargs)
            record = cache.get_no_adjust(key=key, default_value=sentinel)
            if record is not sentinel:
                hits += 1
                result = record.get_cached()
                return result
            misses += 1
            record = record = SyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),
                get_exec_info=CacheTaskExecutionInfo(
                    fail=not negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
            )
            cache.add_no_adjust(key=key, value=record)
            result = record.get_cached()
            return result

    else:

        def wrapper(*args, **kwargs) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            __schedule_remove_expired()
            key = make_key(*args, **kwargs)
            with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return result.get_cached()
                misses += 1
            record = SyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),
                get_exec_info=CacheTaskExecutionInfo(
                    fail=not negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
            )
            result = record.get_cached()
            with lock:
                cache.add(key=key, value=record)

            return result

    def cache_info() -> CacheInfo:
        """Report cache statistics"""
        with lock:
            return CacheInfo(
                hits=hits,
                misses=misses,
                maxsize=maxsize,
                current_size=cache.get_size(),
                last_expiration_check=last_expiration_check,
            )

    def cache_clear() -> None:
        """Clear the cache and cache statistics"""
        nonlocal cache, hits, misses
        with lock:
            cache.every(lambda _key, value: value.destroy())
            cache.clear()
            hits = misses = 0

    def remove_expired() -> None:
        """Remove expired items from the cache"""
        with lock:
            __remove_expired()

    def destroy() -> None:
        """Destroys the cache (for sync cache not that relevant)"""
        cache_clear()

    cleanup_repository.register_destroy_callback(destroy)
    cleanup_repository.register_clear_callback(cache_clear)

    wrapper.cache_info = cache_info  # type: ignore
    wrapper.cache_clear = cache_clear  # type: ignore
    wrapper.cache_parameters = CacheParameters  # type: ignore
    wrapper.remove_expired = remove_expired  # type: ignore
    wrapper.destroy = destroy  # type: ignore
    return wrapper  # type: ignore


def _async_lru_cache_wrapper(
    user_function: Callable[P, T],
    enabled: Union[bool, Callable[[], bool]],
    maxsize: Optional[int],
    expiration: Optional[CacheExpirationValue],
    expired_items_auto_removal_period: Union[str, bytes, int, float, timedelta, None],
    wrap_async_exit_stack: Union[bool, List[str], None],
    negative_cache: bool,
    negative_expiration: Optional[CacheExpirationValue],
    retry_count: int,
    backoff_in_seconds: Union[int, float],
) -> AquicheFunctionWrapper[Callable[P, T]]:
    sentinel = object()  # unique object used to signal cache misses

    cache: CacheRepository = LRUCacheRepository(maxsize=maxsize)
    cleanup_repository = CacheCleanupRegistry()

    hits = misses = 0
    lock = RLock()  # because cache updates aren't thread-safe
    last_expiration_check = datetime.fromtimestamp(0, tz=timezone.utc)
    expiry_period = __parse_duration_to_timedelta(expired_items_auto_removal_period)

    def __is_cache_enabled() -> bool:
        if maxsize == 0:
            return False
        if callable(enabled):
            return enabled()
        return enabled

    def __remove_expired() -> None:
        nonlocal last_expiration_check
        last_expiration_check = datetime.now(timezone.utc)
        removed_items = cache.filter(lambda _key, record: not record.is_expired())
        for removed_item in removed_items:
            removed_item.destroy()

    def __schedule_remove_expired() -> None:
        if expiry_period is None:
            return
        if datetime.now(timezone.utc) - last_expiration_check >= expiry_period:
            __remove_expired()

    if not __is_cache_enabled():

        def wrapper(*args, **kwargs) -> T:
            # No caching -- just a statistics update
            nonlocal cache, misses
            misses += 1
            result = user_function(*args, **kwargs)
            return result

    elif maxsize is None:

        def wrapper(*args, **kwargs) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            __schedule_remove_expired()
            key = make_key(*args, **kwargs)
            record = cache.get_no_adjust(key=key, default_value=sentinel)
            if record is not sentinel:
                hits += 1
                result = record.get_cached()
                return result
            misses += 1
            record = record = SyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),
                get_exec_info=CacheTaskExecutionInfo(
                    fail=not negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
            )
            cache.add_no_adjust(key=key, value=record)
            result = record.get_cached()
            return result

    else:

        def wrapper(*args, **kwargs) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            __schedule_remove_expired()
            key = make_key(*args, **kwargs)
            with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return result.get_cached()
                misses += 1
            record = SyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),
                get_exec_info=CacheTaskExecutionInfo(
                    fail=not negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
            )
            result = record.get_cached()
            with lock:
                cache.add(key=key, value=record)

            return result

    def cache_info() -> CacheInfo:
        """Report cache statistics"""
        with lock:
            return CacheInfo(
                hits=hits,
                misses=misses,
                maxsize=maxsize,
                current_size=cache.get_size(),
                last_expiration_check=last_expiration_check,
            )

    def cache_clear() -> None:
        """Clear the cache and cache statistics"""
        nonlocal cache, hits, misses
        with lock:
            cache.every(lambda _key, value: value.destroy())
            cache.clear()
            hits = misses = 0

    def remove_expired() -> None:
        """Remove expired items from the cache"""
        with lock:
            __remove_expired()

    def destroy() -> None:
        """Destroys the cache (for sync cache not that relevant)"""
        cache_clear()

    cleanup_repository.register_destroy_callback(destroy)
    cleanup_repository.register_clear_callback(cache_clear)

    wrapper.cache_info = cache_info  # type: ignore
    wrapper.cache_clear = cache_clear  # type: ignore
    wrapper.cache_parameters = CacheParameters  # type: ignore
    wrapper.remove_expired = remove_expired  # type: ignore
    wrapper.destroy = destroy  # type: ignore
    return wrapper  # type: ignore
