from asyncio import iscoroutinefunction
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from functools import partial, update_wrapper
import sys
from threading import RLock
from typing import Any, Callable, List, Optional, Protocol, TypeVar, Union

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
    RefreshingCacheExpiration,
)
from aquiche._hash import make_key
from aquiche._repository import CacheRepository, LRUCacheRepository

T = TypeVar("T")
P = ParamSpec("P")
C = TypeVar("C", bound=Callable)

DEFAULT_NEGATIVE_CACHE_DURATION_SECONDS = 10


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

    __call__: C


def alru_cache(
    __func: Optional[Callable[P, T]],
    enabled: Union[bool, Callable[[], bool]] = True,
    maxsize: Optional[int] = None,
    expiration: Optional[CacheExpirationValue] = None,
    expired_items_auto_removal_period: Optional[DurationExpirationValue] = "10minutes",
    wrap_async_exit_stack: Union[bool, List[str], None] = None,
    negative_cache: bool = False,
    negative_expiration: Optional[CacheExpirationValue] = "30s",
    retry_count: int = 0,
    backoff_in_seconds: Union[int, float] = 0,
) -> Union[AquicheFunctionWrapper[Callable[P, T]], Callable[[Callable[P, T]], AquicheFunctionWrapper[Callable[P, T]]]]:
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

    return decorating_function


def _sync_lru_cache_wrapper(
    user_function: Callable[P, T],
    enabled: Union[bool, Callable[[], bool]],
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
        raise InvalidCacheConfig(["wrap_async_exit_stack can only ne used with async functions"])

    sentinel = object()  # unique object used to signal cache misses

    cache: CacheRepository = LRUCacheRepository(maxsize=maxsize)
    hits = misses = 0
    lock = RLock()  # because cache updates aren't thread-safe

    def __is_cache_enabled() -> bool:
        if maxsize == 0:
            return False
        if callable(enabled):
            return enabled()
        return enabled

    def __remove_expired() -> None:
        pass

    if not __is_cache_enabled():

        def wrapper(*args, **kwargs) -> T:
            # No caching -- just a statistics update and potential cleanup
            nonlocal cache, misses
            cache.clear()
            misses += 1
            result = user_function(*args, **kwargs)
            return result

    elif maxsize is None:

        def wrapper(*args, **kwargs) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            key = make_key(args, kwargs)
            record = cache.get_no_adjust(key=key, default_value=sentinel)
            if record is not sentinel:
                hits += 1
                result = record.get_cached()
                return result
            misses += 1
            record = record = SyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),
                get_exec_info=CacheTaskExecutionInfo(
                    fail=negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration,
                    prefer_async=False,
                    default_expiration=RefreshingCacheExpiration(
                        timedelta(seconds=DEFAULT_NEGATIVE_CACHE_DURATION_SECONDS)
                    ),
                ),
            )
            cache.add_no_adjust(key=key, value=record)
            result = record.get_cached()
            return result

    else:

        def wrapper(*args, **kwargs) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            key = make_key(args, kwargs)
            with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return result.get_cached()
                misses += 1
            record = SyncCachedRecord(
                get_function=partial(user_function, *args, **kwargs),
                get_exec_info=CacheTaskExecutionInfo(
                    fail=negative_cache,
                    retries=retry_count,
                    backoff_in_seconds=backoff_in_seconds,
                    wrap_async_exit_stack=False,
                ),
                expiration=get_cache_expiration(
                    expiration, prefer_async=False, default_expiration=NonExpiringCacheExpiration()
                ),
                negative_expiration=get_cache_expiration(
                    negative_expiration,
                    prefer_async=False,
                    default_expiration=RefreshingCacheExpiration(
                        timedelta(seconds=DEFAULT_NEGATIVE_CACHE_DURATION_SECONDS)
                    ),
                ),
            )
            result = record.get_cached()
            with lock:
                cache.add(key=key, value=record)
            return result

    def cache_info():
        """Report cache statistics"""
        with lock:
            return CacheInfo(hits=hits, misses=misses, maxsize=maxsize, current_size=cache.get_size())

    def cache_clear():
        """Clear the cache and cache statistics"""
        nonlocal cache, hits, misses
        with lock:
            cache.clear()
            hits = misses = 0

    wrapper.cache_info = cache_info  # type: ignore
    wrapper.cache_clear = cache_clear  # type: ignore
    wrapper.cache_parameters = CacheParameters  # type: ignore
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
    hits = misses = 0
    lock = RLock()  # because cache updates aren't thread-safe

    def __is_cache_enabled() -> bool:
        if maxsize == 0:
            return False
        if callable(enabled):
            return enabled()
        return enabled

    if not __is_cache_enabled():

        def wrapper(*args, **kwargs) -> T:
            # No caching -- just a statistics update
            nonlocal misses
            misses += 1
            result = user_function(*args, **kwargs)
            return result

    elif maxsize is None:

        def wrapper(*args, **kwargs) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            key = make_key(args, kwargs)
            result = cache.get_no_adjust(key=key, default_value=sentinel)
            if result is not sentinel:
                hits += 1
                return result
            misses += 1
            result = user_function(*args, **kwargs)
            cache.add_no_adjust(key, result)
            return result

    else:

        def wrapper(*args, **kwargs) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            key = make_key(args, kwargs)
            with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return result
                misses += 1
            result = user_function(*args, **kwargs)
            with lock:
                cache.add(key=key, value=result)
            return result

    def cache_info():
        """Report cache statistics"""
        with lock:
            return CacheInfo(hits=hits, misses=misses, maxsize=maxsize, current_size=cache.get_size())

    def cache_clear():
        """Clear the cache and cache statistics"""
        nonlocal cache, hits, misses
        with lock:
            cache.clear()
            hits = misses = 0

    wrapper.cache_info = cache_info  # type: ignore
    wrapper.cache_clear = cache_clear  # type: ignore
    wrapper.cache_parameters = CacheParameters  # type: ignore
    return wrapper  # type: ignore
