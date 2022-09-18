import asyncio
import re
import time
from collections import OrderedDict
from typing import Any, AsyncIterable, Mapping, Optional, Tuple, Union

from ..utils import Bitarray, get_obj_size
from ..key import ttl_to_seconds
from .._typing import TTL
from .interface import Backend

__all__ = ["Memory"]

_missed = object()


class Memory(Backend):
    """
    In-memory backend lru with ttl
    """

    name = "mem"

    def __init__(self, size: int, check_interval_seconds: Union[float, int] = 1):
        self.store: OrderedDict = OrderedDict()
        self._check_interval_seconds = check_interval_seconds
        self.size = size
        self.__is_init = False
        self.__remove_expired_stop = asyncio.Event()
        self.__remove_expired_task = None
        super().__init__()

    async def init(self):
        self.__is_init = True
        self.__remove_expired_stop = asyncio.Event()
        self.__remove_expired_task = asyncio.create_task(self._remove_expired())

    @property
    def is_init(self) -> bool:
        return self.__is_init

    async def _remove_expired(self):
        while not self.__remove_expired_stop.is_set():
            for key in dict(self.store):
                await self.get(key)
            await asyncio.sleep(self._check_interval_seconds)

    async def clear(self):
        self.store = OrderedDict()

    async def set(
        self,
        key: str,
        value: Any,
        expire: TTL = None,
        exist: Optional[bool] = None,
    ) -> bool:
        expire_seconds = ttl_to_seconds(expire)
        if exist is not None:
            if not (key in self.store) is exist:
                return False
        self._set(key, value, expire_seconds)
        return True

    async def set_raw(self, key: str, value: Any, **kwargs: Any):
        self.store[key] = value

    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._get(key, default=default)

    async def get_raw(self, key: str):
        return self.store.get(key)

    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Any, ...]:
        return tuple(self._get(key, default=default) for key in keys)

    async def set_many(self, pairs: Mapping[str, Any], expire: TTL = None):
        expire_seconds = ttl_to_seconds(expire)
        for key, value in pairs.items():
            self._set(key, value, expire_seconds)

    async def keys_match(self, pattern: str) -> AsyncIterable[str]:
        pattern = pattern.replace("*", ".*")
        regexp = re.compile(pattern)
        for key in dict(self.store):
            if regexp.fullmatch(key):
                yield key

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterable[str]:
        async for key in self.keys_match(pattern):
            yield key

    async def incr(self, key: str):
        value = self._get(key, default=0)
        if not isinstance(value, int):
            raise ValueError("Function incr can be called only on int value")
        value += 1
        self._set(key=key, value=value)
        return value

    async def exists(self, key: str):
        return self._key_exist(key)

    async def delete(self, key: str):
        return self._delete(key)

    def _delete(self, key: str) -> bool:
        if key in self.store:
            del self.store[key]
            return True
        return False

    async def delete_match(self, pattern: str):
        async for key in self.keys_match(pattern):
            self._delete(key)

    async def get_match(
        self, pattern: str, batch_size: int = None, default: Optional[Any] = None
    ) -> AsyncIterable[Tuple[str, Any]]:
        async for key in self.keys_match(pattern):
            yield key, self._get(key, default=default)

    async def expire(self, key: str, timeout: TTL):
        timeout_seconds = ttl_to_seconds(timeout)
        if not self._key_exist(key):
            return
        value = self._get(key, default=_missed)
        if value is _missed:
            return
        self._set(key, value, timeout_seconds)

    async def get_expire(self, key: str) -> int:
        if key not in self.store:
            return -1
        expire_at, _ = self.store[key]
        return round(expire_at - time.time()) if expire_at is not None else -1

    async def get_bits(self, key: str, *indices: int, size: int = 1) -> Tuple[int, ...]:
        value = self._get(key, default=Bitarray("0"))
        if not isinstance(value, Bitarray):
            raise ValueError("Function get_bits can be called only on Bitarray value")
        return tuple(value.get(index, size) for index in indices)

    async def incr_bits(self, key: str, *indices: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        array: Optional[Bitarray] = self._get(key)
        if array is None:
            array = Bitarray("0")
            self._set(key, array)
        result = []
        for index in indices:
            array.incr(index, size, by)
            result.append(array.get(index, size))
        return tuple(result)

    def _set(self, key: str, value: Any, expire: Union[int, float, None] = None):
        expire = time.time() + expire if expire else None
        if expire is None and key in self.store:
            expire, _ = self.store[key]
        self.store[key] = (expire, value)
        self.store.move_to_end(key)
        if len(self.store) > self.size:
            self.store.popitem(last=False)

    def _get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        value = self.store.get(key)
        if value is None:
            return default
        self.store.move_to_end(key)
        expire_at, value = value
        if expire_at and expire_at < time.time():
            self._delete(key)
            return default
        return value

    def _key_exist(self, key):
        return self._get(key, default=_missed) is not _missed

    async def set_lock(self, key: str, value: Any, expire: TTL) -> bool:
        expire_seconds = ttl_to_seconds(expire)
        return await self.set(key, value, expire=expire_seconds, exist=False)

    async def is_locked(self, key: str, wait: TTL = None, step: float = 0.1) -> bool:
        wait_seconds = ttl_to_seconds(wait)
        if wait_seconds is None:
            return self._key_exist(key)
        wait_iter = float(wait_seconds)
        while wait_iter > 0:
            if not self._key_exist(key):
                return False
            wait_iter -= step
            await asyncio.sleep(step)
        return self._key_exist(key)

    async def unlock(self, key: str, value: Any) -> bool:
        return self._delete(key)

    async def get_size(self, key: str) -> int:
        if key in self.store:
            return get_obj_size(self.store[key])
        return 0

    def close(self):
        self.__remove_expired_stop.set()
        if self.__remove_expired_task:
            self.__remove_expired_task.cancel()
            self.__remove_expired_task = None
        super().close()
