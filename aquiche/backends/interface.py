from abc import ABC, abstractmethod
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterable, AsyncIterator, Mapping, Optional, Tuple

from .._typing import TTL


class LockedException(Exception):
    pass


class Backend(ABC):
    name: str = ""

    @property
    def is_init(self) -> bool:
        return False

    @abstractmethod
    async def init(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    async def set(
        self,
        key: str,
        value: Any,
        expire: TTL = None,
        exist: Optional[bool] = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def set_raw(self, key: str, value: Any, **kwargs: Any):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key: str, default: Optional[Any] = None) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def get_raw(self, key: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    async def get_many(self, *keys: str, default: Optional[Any] = None) -> Tuple[Optional[Any], ...]:
        raise NotImplementedError

    @abstractmethod
    async def set_many(self, pairs: Mapping[str, Any], expire: TTL = None):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def keys_match(self, pattern: str) -> AsyncIterable[str]:
        raise NotImplementedError

    @abstractmethod
    def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterable[str]:
        raise NotImplementedError

    @abstractmethod
    async def incr(self, key: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def delete_match(self, pattern: str):
        raise NotImplementedError

    @abstractmethod
    def get_match(
        self, pattern: str, batch_size: int = 100, default: Optional[Any] = None
    ) -> AsyncIterable[Tuple[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    async def expire(self, key: str, timeout: TTL):
        raise NotImplementedError

    @abstractmethod
    async def get_expire(self, key: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def get_bits(self, key: str, *indices: int, size: int = 1) -> Tuple[int, ...]:
        raise NotImplementedError

    @abstractmethod
    async def incr_bits(self, key: str, *indices: int, size: int = 1, by: int = 1) -> Tuple[int, ...]:
        raise NotImplementedError

    @abstractmethod
    async def get_size(self, key: str) -> int:
        """
        Return size in bites that allocated by a value for given key
        """

    @abstractmethod
    async def clear(self):
        raise NotImplementedError

    @abstractmethod
    async def set_lock(self, key: str, value: Any, expire: TTL) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def is_locked(
        self,
        key: str,
        wait: TTL = None,
        step: float = 0.1,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def unlock(self, key: str, value: Any) -> bool:
        raise NotImplementedError

    @asynccontextmanager
    async def lock(self, key: str, expire: TTL) -> AsyncIterator[Any]:
        identifier = str(uuid.uuid4())
        lock = await self.set_lock(key, identifier, expire=expire)
        if not lock:
            raise LockedException(f"Key {key} is already locked")
        try:
            yield
        finally:
            await self.unlock(key, identifier)
