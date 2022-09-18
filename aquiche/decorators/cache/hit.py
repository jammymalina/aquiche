import asyncio
from functools import wraps
from typing import Optional, Union

from ..._typing import CallableCacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _ContextCacheDetect, _empty, context_cache_detect

__all__ = ("hit",)


def hit(
    backend: Backend,
    ttl: int,
    cache_hits: int,
    update_after: Optional[int] = None,
    key: Optional[str] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "hit",
):
    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: Union[CacheDetect, _ContextCacheDetect] = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            result, hits = await asyncio.gather(
                backend.get(_cache_key, default=_empty),
                backend.incr(_cache_key + ":counter"),
            )
            if hits == 1:
                asyncio.create_task(backend.expire(_cache_key + ":counter", ttl))
            if result is not _empty and hits and hits <= cache_hits:
                _from_cache._set(
                    _cache_key,
                    ttl=ttl,
                    cache_hits=cache_hits,
                    name="hit",
                    backend=backend.name,
                    template=_key_template,
                )
                if update_after and hits == update_after:
                    asyncio.create_task(_get_and_save(func, args, kwargs, backend, _cache_key, ttl, condition))
                return result
            return await _get_and_save(func, args, kwargs, backend, _cache_key, ttl, condition)

        return _wrap

    return _decor


async def _get_and_save(func, args, kwargs, backend, key, ttl, store):
    result = await func(*args, **kwargs)
    if store(result, args, kwargs, key=key):
        await asyncio.gather(
            backend.delete(key + ":counter"),
            backend.set(key, result, expire=ttl),
        )

    return result
