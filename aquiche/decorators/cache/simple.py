from functools import wraps
from typing import Optional, Union

from ..._typing import CallableCacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _ContextCacheDetect, _empty, context_cache_detect

__all__ = ("cache",)


def cache(
    backend: Backend,
    ttl: int,
    key: Optional[str] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "",
):
    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: Union[CacheDetect, _ContextCacheDetect] = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                _from_cache._set(_cache_key, ttl=ttl, name="simple", template=_key_template)
                return cached
            result = await func(*args, **kwargs)
            if condition(result, args, kwargs, _cache_key):
                await backend.set(_cache_key, result, expire=ttl)
            return result

        return _wrap

    return _decor
