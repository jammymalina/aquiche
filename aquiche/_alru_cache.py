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
    expiration_check_inter: DurationExpirationValue = 0
    wrap_async_exit_stack: Optional[Union[bool, List[str]]] = None


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
    expiration_check_inter: DurationExpirationValue,
    wrap_async_exit_stack: Optional[Union[bool, List[str]]],
) -> None:
    errors = []
    if not isinstance(enabled, (bool)) or callable(enabled):
        errors += ["enabled should be either bool or a callable function"]
    if maxsize is not None and not isinstance(maxsize, int):
        errors += ["maxsize should be int or None"]
    if not isinstance(expiration, get_args(CacheExpirationValue)):
        errors += [f"expiration should be one of these types: {__extract_type_names(get_args(CacheExpirationValue))}"]
    if not isinstance(expiration_check_inter, get_args(DurationExpirationValue)):
        errors += [
            "expiration_check_inter should be one of these types:"
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

    if errors:
        raise InvalidCacheConfig(errors)


def alru_cache(
    __func: Optional[Callable[P, T]],
    enabled: Union[bool, Callable[[], bool]] = True,
    maxsize: Optional[int] = None,
    expiration: Optional[CacheExpirationValue] = None,
    expiration_check_inter: Union[str, bytes, int, float, timedelta] = "10minutes",
    wrap_async_exit_stack: Optional[Union[bool, List[str]]] = None,
) -> Union[AquicheFunctionWrapper[Callable[P, T]], Callable[[Callable[P, T]], AquicheFunctionWrapper[Callable[P, T]]]]:
    __validate_cache_params(
        enabled=enabled,
        maxsize=maxsize,
        expiration=expiration,
        expiration_check_inter=expiration_check_inter,
        wrap_async_exit_stack=wrap_async_exit_stack,
    )
    cache_params = CacheParameters(
        enabled=enabled,
        maxsize=maxsize,
        expiration=expiration,
        expiration_check_inter=expiration_check_inter,
        wrap_async_exit_stack=wrap_async_exit_stack,
    )
    if maxsize is not None:
        # Negative maxsize is treated as 0
        maxsize = max(maxsize, 0)

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
    expiration_check_inter: Union[str, bytes, int, float, timedelta],
    wrap_async_exit_stack: Optional[Union[bool, List[str]]],
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


# TODO
def _async_lru_cache_wrapper(
    user_function: Callable[P, T],
    enabled: Union[bool, Callable[[], bool]],
    maxsize: Optional[int],
    expiration: Optional[CacheExpirationValue],
    expiration_check_inter: Union[str, bytes, int, float, timedelta],
    wrap_async_exit_stack: Optional[Union[bool, List[str]]],
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
