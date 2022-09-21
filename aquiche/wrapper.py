import asyncio
from contextlib import contextmanager
from functools import partial, wraps
from typing import Any, AsyncIterable, Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Type, Union

from . import decorators, validation
from ._cache_condition import create_time_condition, get_cache_condition
from ._settings import CacheSettings, get_backend_from_settings
from ._typing import TTL, CacheCondition
from .backends.interface import Backend
from .backends.memory import Memory
from .disable_control import _is_disabled_middleware
from .key import ttl_to_seconds


def _create_auto_init():
    lock = asyncio.Lock()

    async def _auto_init(call, *args, backend=None, cmd=None, **kwargs):
        if backend.is_init:
            return await call(*args, **kwargs)
        async with lock:
            if not backend.is_init:
                await backend.init()

        return await call(*args, **kwargs)

    return _auto_init


class Cache(Backend):
    default_prefix = ""
    _backends: Dict[str, Tuple[Any, Tuple[Callable]]]
    _default_middlewares: Iterable[Any]
    _name: Optional[str]
    _default_fail_exceptions: Union[Type[Exception], Iterable[Type[Exception]]]

    def __init__(self, name: Optional[str] = None):
        self._backends = {}
        self._default_middlewares = (
            _is_disabled_middleware,
            _create_auto_init(),
            validation._invalidate_middleware,
        )
        self._name = name
        self._default_fail_exceptions = Exception
        self._add_backend(Memory)

    detect = decorators.context_cache_detect

    def set_default_fail_exceptions(self, *exc: Type[Exception]) -> None:
        self._default_fail_exceptions = exc

    def disable(self, *cmds: str, prefix: str = ""):
        return self._get_backend(prefix).disable(*cmds)

    def disable_all(self, *cmds: str):
        for backend, _ in self._backends.values():
            backend.disable(*cmds)

    def enable(self, *cmds: str, prefix: str = ""):
        return self._get_backend(prefix).enable(*cmds)

    def enable_all(self, *cmds: str):
        for backend, _ in self._backends.values():
            backend.enable(*cmds)

    @contextmanager
    def disabling(self, *cmds: str, prefix: str = ""):
        self.disable(*cmds, prefix=prefix)
        yield
        self.enable(*cmds, prefix=prefix)

    def is_disabled(self, *cmds: str, prefix: str = ""):
        return self._get_backend(prefix).is_disabled(*cmds)

    def is_enabled(self, *cmds: str, prefix: str = ""):
        return not self.is_disabled(*cmds, prefix=prefix)

    def _get_backend_and_config(self, key: str) -> Tuple[Any, Tuple[Callable]]:
        for prefix in sorted(self._backends.keys(), reverse=True):
            if key.startswith(prefix):
                return self._backends[prefix]
        return self._backends[self.default_prefix]

    def _get_backend(self, key: str) -> Any:
        backend, _ = self._get_backend_and_config(key)
        return backend

    def setup(self, settings: CacheSettings, middlewares: Tuple = (), prefix: str = default_prefix):
        backend = get_backend_from_settings(settings)

        self._add_backend(backend, middlewares, prefix)
        return self._backends[prefix][0]

    def _add_backend(self, backend: Any, middlewares=(), prefix: str = default_prefix):
        self._backends[prefix] = (
            backend,
            self._default_middlewares + middlewares,
        )

    async def init(self, *args, **kwargs):
        if args or kwargs:
            self.setup(*args, **kwargs)
        for backend, _ in self._backends.values():
            await backend.init()

    @property
    def is_init(self) -> bool:
        for backend, _ in self._backends.values():
            if not backend.is_init:
                return False
        return True

    def _with_middlewares(self, cmd: str, key):
        backend, middlewares = self._get_backend_and_config(key)
        return self._with_middlewares_for_backend(cmd, backend, middlewares)

    def _with_middlewares_for_backend(self, cmd: str, backend, middlewares):
        call = getattr(backend, cmd)
        for middleware in middlewares:
            call = partial(middleware, call, cmd=cmd, backend=backend)
        return call

    async def set(
        self,
        key: str,
        value: Any,
        expire: Union[float, TTL, None] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        return await self._with_middlewares("set", key)(key=key, value=value, expire=expire, exist=exist)

    async def set_raw(self, key: str, value: Any, **kwargs):
        return await self._with_middlewares("set_raw", key)(key=key, value=value, **kwargs)

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._with_middlewares("get", key)(key=key, default=default)

    async def get_raw(self, key: str) -> Any:
        return await self._with_middlewares("get_raw", key)(key=key)

    async def keys_match(self, pattern: str) -> AsyncIterable[str]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(_pattern):
            return backend.keys_match(_pattern)

        for middleware in middlewares:
            call = partial(middleware, call, cmd="keys_match", backend=backend)
        async for key in (await call(pattern)):
            yield key

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterable[str]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(_pattern):
            return backend.scan(_pattern)

        for middleware in middlewares:
            call = partial(middleware, call, cmd="scan", backend=backend)
        async for key in (await call(pattern)):
            yield key

    async def get_match(
        self, pattern: str, batch_size: int = 100, default: Optional[Any] = None
    ) -> AsyncIterable[Tuple[str, Any]]:
        backend, middlewares = self._get_backend_and_config(pattern)

        async def call(_pattern, _batch_size, _default):
            return backend.get_match(_pattern, batch_size=_batch_size, default=_default)

        for middleware in middlewares:
            call = partial(middleware, call, cmd="get_match", backend=backend)
        async for key, value in (await call(pattern, batch_size, default)):
            yield key, value

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any, ...]:
        backends: Dict[Any, List[str]] = {}
        for key in keys:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        result = {}
        for _keys in backends.values():
            _values = await self._with_middlewares("get_many", _keys[0])(*_keys, default=default)
            result.update(dict(zip(_keys, _values)))
        return tuple(result.get(key) for key in keys)

    async def set_many(self, pairs: Mapping[str, Any], expire: Union[float, TTL, None] = None):
        backends: Dict[Any, List[str]] = {}
        for key in pairs:
            backend = self._get_backend(key)
            backends.setdefault(backend, []).append(key)
        for backend, keys in backends.items():
            data = {key: pairs[key] for key in keys}
            await self._with_middlewares("set_many", keys[0])(data, expire=expire)

    async def get_bits(self, key: str, *indices: int, size: int = 1) -> Tuple[int, ...]:
        return await self._with_middlewares("get_bits", key)(key, *indices, size=size)

    def incr_bits(self, key: str, *indices: int, size: int = 1, by: int = 1):
        return self._with_middlewares("incr_bits", key)(key, *indices, size=size, by=by)

    async def incr(self, key: str) -> int:
        return await self._with_middlewares("incr", key)(key=key)

    def delete(self, key: str):
        return self._with_middlewares("delete", key)(key=key)

    def delete_match(self, pattern: str):
        return self._with_middlewares("delete_match", pattern)(pattern=pattern)

    def expire(self, key: str, timeout: TTL):
        return self._with_middlewares("expire", key)(key=key, timeout=timeout)

    def get_expire(self, key: str):
        return self._with_middlewares("get_expire", key)(key=key)

    def exists(self, key: str):
        return self._with_middlewares("exists", key)(key=key)

    def set_lock(self, key: str, value: Any, expire: TTL):
        return self._with_middlewares("set_lock", key)(key=key, value=value, expire=expire)

    def unlock(self, key: str, value: str):
        return self._with_middlewares("unlock", key)(key=key, value=value)

    def _bytes(self, key: str):
        return self._with_middlewares("get_size_bytes", key)(key)

    def ping(self, message: Optional[bytes] = None) -> str:
        message = b"PING" if message is None else message
        return self._with_middlewares("ping", message.decode())(message=message)

    async def clear(self):
        for backend, _ in self._backends.values():
            await self._with_middlewares_for_backend("clear", backend, self._default_middlewares)()

    def close(self):
        for backend, _ in self._backends.values():
            backend.close()

    async def is_locked(
        self,
        key: str,
        wait: TTL = None,
        step: Union[int, float] = 0.1,
    ) -> bool:
        return await self._with_middlewares("is_locked", key)(key=key, wait=wait, step=step)

    def _wrap_on(self, decorator_fabric, upper, **decor_kwargs):
        if upper:
            return self._wrap_with_condition(decorator_fabric, **decor_kwargs)
        return self._wrap(decorator_fabric, **decor_kwargs)

    def _wrap(self, decorator_fabric, lock=False, time_condition=None, **decor_kwargs):
        def _decorator(func):
            if time_condition is not None:
                condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
                decor_kwargs["condition"] = condition

            decorator = decorator_fabric(self, **decor_kwargs)(func)

            @wraps(func)
            async def _call(*args, **kwargs):
                if lock:
                    _locked = decorators.locked(self, key=decor_kwargs.get("key"), ttl=decor_kwargs["ttl"])
                    return await _locked(decorator)(*args, **kwargs)
                else:
                    return await decorator(*args, **kwargs)

            _call.direct = func
            return _call

        return _decorator

    def _wrap_with_condition(self, decorator_fabric, condition, lock=False, time_condition=None, **decor_kwargs):
        def _decorator(func):
            _condition = condition
            if time_condition is not None:
                _condition, _decor = create_time_condition(time_condition)
                func = _decor(func)
            decorator_fabric(self, **decor_kwargs)(func)  # to register cache templates

            @wraps(func)
            async def _call(*args, **kwargs):
                with decorators.context_cache_detect as detect:

                    def new_condition(result, _args, _kwargs, key):
                        if detect.keys:
                            return False
                        return _condition(result, _args, _kwargs, key=key) if _condition else result is not None

                    decorator = decorator_fabric(self, **decor_kwargs, condition=new_condition)
                    if lock:
                        _locked = decorators.locked(self, key=decor_kwargs.get("key"), ttl=decor_kwargs["ttl"])
                        result = await _locked(decorator(func))(*args, **kwargs)
                    else:
                        result = await decorator(func)(*args, **kwargs)

                return result

            _call.direct = func
            return _call

        return _decorator

    # DecoratorS
    def __call__(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "",
        upper: bool = False,
        lock: bool = False,
    ):
        return self._wrap_on(
            decorators.cache,
            upper,
            lock=lock,
            ttl=ttl_to_seconds(ttl),
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    cache = __call__

    def failover(
        self,
        ttl: TTL,
        exceptions: Union[Type[Exception], Iterable[Type[Exception]], None] = None,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "fail",
    ):
        exceptions = exceptions or self._default_fail_exceptions
        return self._wrap_with_condition(
            decorators.failover,
            ttl=ttl_to_seconds(ttl),
            exceptions=exceptions,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def early(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        early_ttl: Optional[TTL] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "early",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.early,
            upper,
            ttl=ttl_to_seconds(ttl),
            key=key,
            early_ttl=ttl_to_seconds(early_ttl),
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def soft(
        self,
        ttl: TTL,
        key: Optional[str] = None,
        soft_ttl: Optional[TTL] = None,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "soft",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.soft,
            upper,
            ttl=ttl_to_seconds(ttl),
            key=key,
            soft_ttl=ttl_to_seconds(soft_ttl),
            exceptions=exceptions,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def hit(
        self,
        ttl: TTL,
        cache_hits: int,
        update_after: int = 0,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "hit",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl_to_seconds(ttl),
            cache_hits=cache_hits,
            update_after=update_after,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def dynamic(
        self,
        ttl: TTL = 60 * 60 * 24,
        key: Optional[str] = None,
        condition: CacheCondition = None,
        time_condition: Optional[TTL] = None,
        prefix: str = "dynamic",
        upper: bool = False,
    ):
        return self._wrap_on(
            decorators.hit,
            upper,
            ttl=ttl_to_seconds(ttl),
            cache_hits=3,
            update_after=1,
            key=key,
            condition=get_cache_condition(condition),
            time_condition=ttl_to_seconds(time_condition),
            prefix=prefix,
        )

    def invalidate(
        self,
        func,
        args_map: Optional[Dict[str, str]] = None,
        defaults: Optional[Dict] = None,
    ):
        return validation.invalidate(
            backend=self,
            target=func,
            args_map=args_map,
            defaults=defaults,
        )

    invalidate_func = validation.invalidate_func

    def circuit_breaker(
        self,
        errors_rate: int,
        period: TTL,
        ttl: TTL,
        exceptions: Union[Type[Exception], Tuple[Type[Exception]], None] = None,
        key: Optional[str] = None,
        prefix: str = "circuit_breaker",
    ):
        return decorators.circuit_breaker(
            backend=self,
            errors_rate=errors_rate,
            period=ttl_to_seconds(period) or 0,
            ttl=ttl_to_seconds(ttl) or 0,
            exceptions=exceptions or self._default_fail_exceptions,
            key=key,
            prefix=prefix,
        )
