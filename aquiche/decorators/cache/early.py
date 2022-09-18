import asyncio
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Union

from ..._typing import CallableCacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _ContextCacheDetect, _empty, context_cache_detect

__all__ = ("early",)
_LOCK_SUFFIX = ":lock"


def early(
    backend: Backend,
    ttl: int,
    key: Optional[str] = None,
    early_ttl: Optional[int] = None,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "early",
):
    new_early_ttl: int = int(ttl * 0.33) if early_ttl is None else early_ttl

    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix + ":v2")
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: Union[CacheDetect, _ContextCacheDetect] = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                _from_cache._set(
                    _cache_key,
                    ttl=ttl,
                    early_ttl=new_early_ttl,
                    name="early",
                    template=_key_template,
                )
                early_expire_at, result = cached
                if early_expire_at <= datetime.utcnow() and await backend.set(
                    _cache_key + _LOCK_SUFFIX, "1", expire=new_early_ttl, exist=False
                ):
                    asyncio.create_task(
                        _get_result_for_early(
                            backend,
                            func,
                            args,
                            kwargs,
                            _cache_key,
                            ttl,
                            new_early_ttl,
                            condition,
                        )
                    )
                return result
            return await _get_result_for_early(
                backend, func, args, kwargs, _cache_key, ttl, new_early_ttl, condition, unlock=True
            )

        return _wrap

    return _decor


async def _get_result_for_early(
    backend: Backend, func, args, kwargs, key, ttl: int, early_ttl: int, condition, unlock=False
):
    try:
        result = await func(*args, **kwargs)
        if condition(result, args, kwargs, key):
            early_expire_at = datetime.utcnow() + timedelta(seconds=early_ttl)
            await backend.set(key, [early_expire_at, result], expire=ttl)
        return result
    finally:
        if unlock:
            asyncio.create_task(backend.delete(key + _LOCK_SUFFIX))
