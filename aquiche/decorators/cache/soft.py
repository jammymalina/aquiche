from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Tuple, Type, Union

from ..._typing import CallableCacheCondition
from ...backends.interface import Backend
from ...formatter import register_template
from ...key import get_cache_key, get_cache_key_template
from .defaults import CacheDetect, _ContextCacheDetect, _empty, context_cache_detect

__all__ = ("soft",)


def soft(
    backend: Backend,
    ttl: int,
    key: Optional[str] = None,
    soft_ttl: Optional[int] = None,
    exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
    condition: CallableCacheCondition = lambda *args, **kwargs: True,
    prefix: str = "soft",
):
    new_soft_ttl = int(ttl * 0.33) if soft_ttl is None else soft_ttl

    def _decor(func):
        _key_template = get_cache_key_template(func, key=key, prefix=prefix)
        register_template(func, _key_template)

        @wraps(func)
        async def _wrap(*args, _from_cache: Union[CacheDetect, _ContextCacheDetect] = context_cache_detect, **kwargs):
            _cache_key = get_cache_key(func, _key_template, args, kwargs)
            cached = await backend.get(_cache_key, default=_empty)
            if cached is not _empty:
                soft_expire_at, result = cached
                if soft_expire_at > datetime.utcnow():
                    _from_cache._set(
                        _cache_key,
                        ttl=ttl,
                        soft_ttl=new_soft_ttl,
                        name="soft",
                        template=_key_template,
                    )
                    return result

            try:
                result = await func(*args, **kwargs)
            except exceptions:
                if cached is not _empty:
                    _, result = cached
                    _from_cache._set(
                        _cache_key,
                        ttl=ttl,
                        soft_ttl=new_soft_ttl,
                        name="soft",
                        template=_key_template,
                    )
                    return result
                raise
            else:
                if condition(result, args, kwargs, _cache_key):
                    soft_expire_at = datetime.utcnow() + timedelta(seconds=new_soft_ttl)
                    await backend.set(_cache_key, [soft_expire_at, result], expire=ttl)
                return result

        return _wrap

    return _decor
