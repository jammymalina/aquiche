from asyncio import create_task, Event, gather, iscoroutinefunction, Lock, sleep as asleep
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

from aquiche._cache import AsyncCachedRecord, SyncCachedRecord
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
    cache_info: Union[Callable[..., CacheInfo], Callable[..., Awaitable[CacheInfo]]]
    cache_clear: Union[Callable[..., None], Callable[..., Awaitable[None]]]
    cache_parameters: Callable[..., CacheParameters]
    remove_expired: Union[Callable[..., None], Callable[..., Awaitable[None]]]
    destroy: Union[Callable[..., None], Callable[..., Awaitable[None]]]

    __call__: C


def __parse_duration_to_timedelta(duration: Optional[DurationExpirationValue]) -> Optional[timedelta]:
    if duration is None:
        return None
    if isinstance(duration, timedelta):
        return duration
    return parse_duration(duration)


class CacheCleanupRegistry(metaclass=Singleton):
    __destroy_callbacks: List[Union[Callable[..., None], Callable[..., Awaitable[None]]]]
    __clear_callbacks: List[Union[Callable[..., None], Callable[..., Awaitable[None]]]]

    def __init__(self) -> None:
        self.__destroy_callbacks = []
        self.__clear_callbacks = []

    def register_destroy_callback(
        self, destroy_callback: Union[Callable[..., None], Callable[..., Awaitable[None]]]
    ) -> None:
        self.__destroy_callbacks.append(destroy_callback)

    def get_destroy_callbacks(self) -> Iterable[Union[Callable[..., None], Callable[..., Awaitable[None]]]]:
        return iter(self.__destroy_callbacks)

    def register_clear_callback(
        self, clear_callback: Union[Callable[..., None], Callable[..., Awaitable[None]]]
    ) -> None:
        self.__clear_callbacks.append(clear_callback)

    def get_clear_callbacks(self) -> Iterable[Union[Callable[..., None], Callable[..., Awaitable[None]]]]:
        return iter(self.__clear_callbacks)


def alru_cache(
    __func: Union[Callable[P, T], None] = None,
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

    if __func is not None and callable(__func):
        # The user_function was passed in directly via the hidden __func argument
        user_function = __func
        if iscoroutinefunction(user_function):
            wrapper = _async_lru_cache_wrapper(
                user_function=user_function,
                **asdict(cache_params),
            )
        else:
            wrapper = _sync_lru_cache_wrapper(
                user_function=user_function,
                **asdict(cache_params),
            )
        wrapper.cache_parameters = lambda: cache_params  # type: ignore
        return update_wrapper(wrapper, user_function)  # type: ignore

    def decorating_function(user_function: Union[Callable[P, T], Callable[P, Awaitable[T]]]):
        if iscoroutinefunction(user_function):
            wrapper = _async_lru_cache_wrapper(
                user_function=user_function,
                **asdict(cache_params),
            )
        else:
            wrapper = _sync_lru_cache_wrapper(
                user_function=user_function,
                **asdict(cache_params),
            )
        wrapper.cache_parameters = lambda: cache_params  # type: ignore
        return update_wrapper(wrapper, user_function)

    return decorating_function  # type: ignore


async def clear_all() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for clear_callback in cleanup_repository.get_clear_callbacks():
        if iscoroutinefunction(clear_callback):
            await clear_callback()  # type: ignore
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
            await clear_callback()  # type: ignore


async def destroy_all() -> None:
    cleanup_repository = CacheCleanupRegistry()

    for destroy_callback in cleanup_repository.get_destroy_callbacks():
        if iscoroutinefunction(destroy_callback):
            await destroy_callback()  # type: ignore
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
            await destroy_callback()  # type: ignore


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
            key = make_key(*args, **kwargs)

            with lock:
                __schedule_remove_expired()

                record = cache.get_no_adjust(key)
                if record is not None:
                    hits += 1
                    return record.get_cached()
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
                cache.add_no_adjust(key=key, value=record)

            return result

    else:

        def wrapper(*args, **kwargs) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            key = make_key(*args, **kwargs)

            with lock:
                __schedule_remove_expired()

                record = cache.get(key)
                if record is not None:
                    hits += 1
                    return record.get_cached()
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
                cache.add(key, record)

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
    cache: CacheRepository = LRUCacheRepository(maxsize=maxsize)
    cleanup_repository = CacheCleanupRegistry()

    hits = misses = 0
    lock = Lock()  # because cache updates aren't concurrency-safe
    destroy_event = Event()
    last_expiration_check = datetime.fromtimestamp(0, tz=timezone.utc)
    expiry_period = __parse_duration_to_timedelta(expired_items_auto_removal_period)
    expiry_cleanup_task = create_task(__schedule_remove_expired())

    def __is_cache_enabled() -> bool:
        if maxsize == 0:
            return False
        if callable(enabled):
            return enabled()
        return enabled

    async def __expiry_record_lambda(_key: str, record: AsyncCachedRecord) -> bool:
        return await record.is_expired()

    async def __destroy_record_lambda(_key: str, record: AsyncCachedRecord) -> None:
        await record.destroy()

    async def __remove_expired() -> None:
        nonlocal last_expiration_check
        last_expiration_check = datetime.now(timezone.utc)

        removed_items = await cache.filter_async(__expiry_record_lambda)
        await gather(*(record.destroy() for record in removed_items))

    async def __schedule_remove_expired() -> None:
        nonlocal last_expiration_check

        if expiry_period is None:
            return

        while not destroy_event.is_set():
            await asleep(expiry_period.total_seconds())
            async with lock:
                await __remove_expired()
                last_expiration_check = datetime.now(timezone.utc)

    if not __is_cache_enabled():

        async def wrapper(*args, **kwargs) -> T:
            # No caching -- just a statistics update
            nonlocal cache, misses
            misses += 1
            result = await user_function(*args, **kwargs)  # type: ignore
            return result

    elif maxsize is None:

        async def wrapper(*args, **kwargs) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            key = make_key(*args, **kwargs)

            async with lock:
                record = cache.get_no_adjust(key)
                if record is not None:
                    hits += 1
                    return await record.get_cached()
                misses += 1

            record = AsyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),  # type: ignore
                get_exec_info=CacheTaskExecutionInfo(
                    fail=not negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=wrap_async_exit_stack or False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=True, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration, prefer_async=True, default_expiration=NonExpiringCacheExpiration()
                ),
            )
            result = await record.get_cached()

            async with lock:
                cache.add_no_adjust(key=key, value=record)

            return result

    else:

        async def wrapper(*args, **kwargs) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            key = make_key(*args, **kwargs)

            async with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return await result.get_cached()
                misses += 1

            record = AsyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),  # type: ignore
                get_exec_info=CacheTaskExecutionInfo(
                    fail=not negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=wrap_async_exit_stack or False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=True, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration, prefer_async=True, default_expiration=NonExpiringCacheExpiration()
                ),
            )
            result = await record.get_cached()

            async with lock:
                cache.add(key=key, value=record)

            return result

    async def cache_info() -> CacheInfo:
        """Report cache statistics"""
        async with lock:
            return CacheInfo(
                hits=hits,
                misses=misses,
                maxsize=maxsize,
                current_size=cache.get_size(),
                last_expiration_check=last_expiration_check,
            )

    async def cache_clear() -> None:
        """Clear the cache and cache statistics"""
        nonlocal cache, hits, misses
        async with lock:
            await cache.every_async(__destroy_record_lambda)
            cache.clear()
            hits = misses = 0

    async def remove_expired() -> None:
        """Remove expired items from the cache"""
        async with lock:
            await __remove_expired()

    async def destroy() -> None:
        """Destroys the cache"""
        nonlocal destroy_event, expiry_cleanup_task
        destroy_event.set()
        await expiry_cleanup_task
        await cache_clear()

    cleanup_repository.register_destroy_callback(destroy)
    cleanup_repository.register_clear_callback(cache_clear)

    wrapper.cache_info = cache_info  # type: ignore
    wrapper.cache_clear = cache_clear  # type: ignore
    wrapper.cache_parameters = CacheParameters  # type: ignore
    wrapper.remove_expired = remove_expired  # type: ignore
    wrapper.destroy = destroy  # type: ignore
    return wrapper  # type: ignore
