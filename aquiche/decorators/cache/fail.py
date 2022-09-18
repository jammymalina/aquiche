from functools import wraps
from typing import Optional, Tuple, Type, Union

from ..._typing import CallableCacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _ContextCacheDetect, _empty, context_cache_detect

__all__ = ("failover",)


def fast_condition(getter, setter=None):
    def _fast_condition(result, args, kwargs, key=""):
        if getter(key):
            return False
        if setter:
            setter(key, result)
        return True

    return _fast_condition


def failover(
    backend: Backend,
    ttl: int,
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    key: Optional[str] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "fail",
):
    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: Union[CacheDetect, _ContextCacheDetect] = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            try:
                result = await func(*args, **kwargs)
            except exceptions as exc:
                cached = await backend.get(_cache_key, default=_empty)
                if cached is not _empty:
                    _from_cache._set(
                        _cache_key,
                        ttl=ttl,
                        exc=exc,
                        name="failover",
                        template=_key_template,
                    )
                    return cached
                raise exc
            else:
                if condition(result, args, kwargs, _cache_key):
                    await backend.set(_cache_key, result, expire=ttl)
                return result

        return _wrap

    return _decor
