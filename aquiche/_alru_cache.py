from asyncio import iscoroutinefunction
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from functools import update_wrapper
import sys
from threading import RLock
from typing import Any, Callable, List, Optional, Protocol, Tuple, TypeVar, Union, get_args

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from aquiche.errors import InvalidCacheConfig
from aquiche._expiration import CacheExpirationValue, DurationExpirationValue
from aquiche._hash import make_key
from aquiche._repository import CacheRepository, LRUCacheRepository

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


@dataclass
class CacheParameters:
    enabled: Union[bool, Callable] = False
    maxsize: Optional[int] = None
    expiration: Optional[CacheExpirationValue] = None
    expired_items_auto_removal_period: Optional[DurationExpirationValue] = None
    wrap_async_exit_stack: Union[bool, List[str], None] = None
    negative_cache: bool = False
    retry_count: int = 0
    backoff_in_seconds: Union[int, float] = 0


class AquicheFunctionWrapper(Protocol[C]):
    cache_info: Callable[[], CacheInfo]
    cache_clear: Callable[[], None]
    cache_parameters: Callable[[], CacheParameters]

    __call__: C


def __extract_type_names(types: Tuple[Any, ...]) -> str:
    return "|".join(map(lambda t: t.__name__, types))


def __validate_cache_params(
    enabled: Union[bool, Callable[[], bool]],
    maxsize: Optional[int],
    expiration: Optional[CacheExpirationValue],
    expired_items_auto_removal_period: Optional[DurationExpirationValue],
    wrap_async_exit_stack: Union[bool, List[str], None],
    negative_cache: bool,
    retry_count: int,
    backoff_in_seconds: Union[int, float],
) -> None:
    errors = []
    if not isinstance(enabled, (bool)) or callable(enabled):
        errors += ["enabled should be either bool or a callable function"]
    if maxsize is not None and not isinstance(maxsize, int):
        errors += ["maxsize should be int or None"]
    if not isinstance(expiration, get_args(CacheExpirationValue)):
        errors += [f"expiration should be one of these types: {__extract_type_names(get_args(CacheExpirationValue))}"]
    if not (
        expired_items_auto_removal_period is None
        or isinstance(expired_items_auto_removal_period, get_args(DurationExpirationValue))
    ):
        errors += [
            "expired_items_auto_removal_period should be either None or one of these types:"
            + __extract_type_names(get_args(DurationExpirationValue))
        ]
    if not (
        wrap_async_exit_stack is None
        or isinstance(wrap_async_exit_stack, bool)
        or (
            isinstance(wrap_async_exit_stack, list)
            and all((isinstance(wrapper, str) for wrapper in wrap_async_exit_stack))
        )
    ):
        errors += ["wrap_async_exit_stack should be either None, bool or a callable function"]

    if not isinstance(negative_cache, bool):
        errors += ["negative_cache should be bool"]
    if not isinstance(retry_count, int):
        errors += ["retry_count should be an integer"]
    if not isinstance(backoff_in_seconds, (int, float)):
        errors += ["backoff_in_seconds should be a number"]

    if errors:
        raise InvalidCacheConfig(errors)


def alru_cache(
    __func: Optional[Callable[P, T]],
    enabled: Union[bool, Callable[[], bool]] = True,
    maxsize: Optional[int] = None,
    expiration: Optional[CacheExpirationValue] = None,
    expired_items_auto_removal_period: Optional[DurationExpirationValue] = "10minutes",
    wrap_async_exit_stack: Union[bool, List[str], None] = None,
    negative_cache: bool = False,
    retry_count: int = 0,
    backoff_in_seconds: Union[int, float] = 0,
) -> Union[AquicheFunctionWrapper[Callable[P, T]], Callable[[Callable[P, T]], AquicheFunctionWrapper[Callable[P, T]]]]:
    __validate_cache_params(
        enabled=enabled,
        maxsize=maxsize,
        expiration=expiration,
        expired_items_auto_removal_period=expired_items_auto_removal_period,
        wrap_async_exit_stack=wrap_async_exit_stack,
        negative_cache=negative_cache,
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
    expired_items_auto_removal_period: Union[str, bytes, int, float, timedelta, None],
    wrap_async_exit_stack: Union[bool, List[str], None],
    negative_cache: bool,
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

    if not __is_cache_enabled():

        def wrapper(*args, **kwds) -> T:
            # No caching -- just a statistics update and potential cleanup
            # TODO: Cleanup
            nonlocal misses
            misses += 1
            result = user_function(*args, **kwds)
            return result

    elif maxsize is None:

        def wrapper(*args, **kwds) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            key = make_key(args, kwds)
            result = cache.get_no_adjust(key=key, default_value=sentinel)
            if result is not sentinel:
                hits += 1
                return result
            misses += 1
            result = user_function(*args, **kwds)
            cache.add_no_adjust(key, result)
            return result

    else:

        def wrapper(*args, **kwds) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            key = make_key(args, kwds)
            with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return result
                misses += 1
            result = user_function(*args, **kwds)
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


def _async_lru_cache_wrapper(
    user_function: Callable[P, T],
    enabled: Union[bool, Callable[[], bool]],
    maxsize: Optional[int],
    expiration: Optional[CacheExpirationValue],
    expired_items_auto_removal_period: Union[str, bytes, int, float, timedelta, None],
    wrap_async_exit_stack: Union[bool, List[str], None],
    negative_cache: bool,
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

        def wrapper(*args, **kwds) -> T:
            # No caching -- just a statistics update
            nonlocal misses
            misses += 1
            result = user_function(*args, **kwds)
            return result

    elif maxsize is None:

        def wrapper(*args, **kwds) -> T:
            # Simple caching without ordering or size limit
            nonlocal hits, misses
            key = make_key(args, kwds)
            result = cache.get_no_adjust(key=key, default_value=sentinel)
            if result is not sentinel:
                hits += 1
                return result
            misses += 1
            result = user_function(*args, **kwds)
            cache.add_no_adjust(key, result)
            return result

    else:

        def wrapper(*args, **kwds) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            key = make_key(args, kwds)
            with lock:
                result = cache.get(key)
                if result is not None:
                    hits += 1
                    return result
                misses += 1
            result = user_function(*args, **kwds)
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
