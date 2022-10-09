from dataclasses import asdict, dataclass
from datetime import timedelta
from functools import update_wrapper
import sys
from threading import RLock
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple, TypeVar, Union, get_args

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from aquiche.errors import InvalidCacheConfig
from aquiche._hash import make_key
from aquiche._expiration import CacheExpirationValue, DurationExpirationValue

T = TypeVar("T")
P = ParamSpec("P")
C = TypeVar("C", bound=Callable)


@dataclass
class CacheInfo:
    hits: int = 0
    misses: int = 0
    maxsize: Optional[int] = None
    current_size: int = 0


@dataclass
class CacheParameters:
    enabled: Union[bool, Callable] = False
    maxsize: Optional[int] = None
    expiration: Optional[CacheExpirationValue] = None
    expiration_check_inter: DurationExpirationValue = 0
    wrap_async_exit_stack: Union[bool, List[str]] = False


class AquicheFunctionWrapper(Protocol[C]):
    cache_info: Callable[[], CacheInfo]
    cache_clear: Callable[[], None]
    cache_parameters: Callable[[], CacheParameters]

    __call__: C


def __extract_type_names(types: Tuple[Any, ...]) -> str:
    return "|".join(map(lambda t: t.__name__, types))


def __validate_cache_params(
    enabled: Union[bool, Callable[[], bool]],
    maxsize: int,
    expiration: Optional[CacheExpirationValue],
    expiration_check_inter: DurationExpirationValue,
    wrap_async_exit_stack: Union[bool, List[str]],
) -> None:
    errors = []
    if not isinstance(enabled, (bool)) or callable(enabled):
        errors += ["enabled should be either bool or a callable function"]
    if not isinstance(maxsize, int):
        errors += ["maxsize should be int"]
    if not isinstance(expiration, get_args(CacheExpirationValue)):
        errors += [f"expiration should be one of these types: {__extract_type_names(get_args(CacheExpirationValue))}"]
    if not isinstance(expiration_check_inter, get_args(DurationExpirationValue)):
        errors += [
            "expiration_check_inter should be one of these types:"
            + __extract_type_names(get_args(DurationExpirationValue))
        ]
    if not (
        isinstance(wrap_async_exit_stack, bool)
        or (
            isinstance(wrap_async_exit_stack, list)
            and all((isinstance(wrapper, str) for wrapper in wrap_async_exit_stack))
        )
    ):
        errors += ["wrap_async_exit_stack should be either bool or a callable function"]

    if errors:
        raise InvalidCacheConfig(errors)


def alru_cache(
    __func: Optional[Callable[P, T]],
    enabled: Union[bool, Callable[[], bool]] = True,
    maxsize: int = 128,
    expiration: Optional[CacheExpirationValue] = None,
    expiration_check_inter: Union[str, bytes, int, float, timedelta] = "10minutes",
    wrap_async_exit_stack: Union[bool, List[str]] = False,
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
    # Negative maxsize is treated as 0
    maxsize = max(maxsize, 0)

    if callable(__func):
        # The user_function was passed in directly via the hidden __func argument
        user_function = __func
        wrapper = _lru_cache_wrapper(
            user_function,
            **asdict(cache_params),
        )
        wrapper.cache_parameters = lambda: cache_params  # type: ignore
        return update_wrapper(wrapper, user_function)

    def decorating_function(user_function: Callable[P, T]):
        wrapper = _lru_cache_wrapper(
            user_function,
            **asdict(cache_params),
        )
        wrapper.cache_parameters = lambda: cache_params  # type: ignore
        return update_wrapper(wrapper, user_function)

    return decorating_function


def _lru_cache_wrapper(
    user_function: Callable[P, T],
    enabled: Union[bool, Callable[[], bool]] = True,
    maxsize: int = 128,
    expiration: Optional[CacheExpirationValue] = None,
    expiration_check_inter: Union[str, bytes, int, float, timedelta] = "10minutes",
    wrap_async_exit_stack: Union[bool, List[str]] = False,
) -> AquicheFunctionWrapper[Callable[P, T]]:
    # Constants shared by all lru cache instances:
    sentinel = object()  # unique object used to signal cache misses
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3  # names for the link fields

    cache: Dict = {}
    hits = misses = 0
    full = False
    cache_get = cache.get  # bound method to lookup a key or return None
    cache_len = cache.__len__  # get cache size without calling len()
    lock = RLock()  # because linkedlist updates aren't threadsafe
    root: List = []  # root of the circular doubly linked list
    root[:] = [root, root, None, None]  # initialize by pointing to self

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
            result = cache_get(key, sentinel)
            if result is not sentinel:
                hits += 1
                return result
            misses += 1
            result = user_function(*args, **kwds)
            cache[key] = result
            return result

    else:

        def wrapper(*args, **kwds) -> T:
            # Size limited caching that tracks accesses by recency
            nonlocal root, hits, misses, full
            key = make_key(args, kwds)
            with lock:
                link = cache_get(key)
                if link is not None:
                    # Move the link to the front of the circular queue
                    link_prev, link_next, _key, result = link
                    link_prev[NEXT] = link_next
                    link_next[PREV] = link_prev
                    last = root[PREV]
                    last[NEXT] = root[PREV] = link
                    link[PREV] = last
                    link[NEXT] = root
                    hits += 1
                    return result
                misses += 1
            result = user_function(*args, **kwds)
            with lock:
                if key in cache:
                    # Getting here means that this same key was added to the
                    # cache while the lock was released.  Since the link
                    # update is already done, we need only return the
                    # computed result and update the count of misses.
                    pass
                elif full:
                    # Use the old root to store the new key and result.
                    oldroot = root
                    oldroot[KEY] = key
                    oldroot[RESULT] = result
                    # Empty the oldest link and make it the new root.
                    # Keep a reference to the old key and old result to
                    # prevent their ref counts from going to zero during the
                    # update. That will prevent potentially arbitrary object
                    # clean-up code (i.e. __del__) from running while we're
                    # still adjusting the links.
                    root = oldroot[NEXT]
                    oldkey = root[KEY]
                    oldresult = root[RESULT]
                    root[KEY] = root[RESULT] = None
                    # Now update the cache dictionary.
                    del cache[oldkey]
                    # Save the potentially reentrant cache[key] assignment
                    # for last, after the root and links have been put in
                    # a consistent state.
                    cache[key] = oldroot
                else:
                    # Put result in a new link at the front of the queue.
                    last = root[PREV]
                    link = [last, root, key, result]
                    last[NEXT] = root[PREV] = cache[key] = link
                    # Use the cache_len bound method instead of the len() function
                    # which could potentially be wrapped in an lru_cache itself.
                    full = cache_len() >= maxsize
            return result

    def cache_info():
        """Report cache statistics"""
        with lock:
            return CacheInfo(hits=hits, misses=misses, maxsize=maxsize, current_size=cache_len())

    def cache_clear():
        """Clear the cache and cache statistics"""
        nonlocal hits, misses, full
        with lock:
            cache.clear()
            root[:] = [root, root, None, None]
            hits = misses = 0
            full = False

    wrapper.cache_info = cache_info  # type: ignore
    wrapper.cache_clear = cache_clear  # type: ignore
    wrapper.cache_parameters = CacheParameters  # type: ignore
    return wrapper  # type: ignore
