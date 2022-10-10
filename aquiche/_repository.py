from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional


class CacheRepository(metaclass=ABCMeta):
    @abstractmethod
    def add(self, key: str, value: Any) -> None:
        ...

    @abstractmethod
    def get(self, key: str) -> Any:
        ...

    @abstractmethod
    def get_no_adjust(self, key, default_value: Optional[Any] = None) -> Any:
        ...

    @abstractmethod
    def add_no_adjust(self, key: str, value: Any) -> None:
        ...

    @abstractmethod
    def has(self, key: str) -> bool:
        ...

    @abstractmethod
    def clear(self) -> None:
        ...

    @abstractmethod
    def get_size(self) -> int:
        ...


class LRUCacheRepository(CacheRepository):
    # names for the link fields
    PREV: int = 0
    NEXT: int = 1
    KEY: int = 2
    RESULT: int = 3

    __cache: Dict[str, Any]
    __root: List
    __maxsize: int
    __full: bool

    def __init__(self, maxsize: Optional[int] = None) -> None:
        super().__init__()
        self.__cache = {}
        self.__root = []
        self.__root[:] = [self.__root, self.__root, None, None]
        self.__maxsize = maxsize or 0
        self.__full = False

    def add(self, key: str, value: Any) -> None:
        if self.has(key):
            # Getting here means that this same key was added to the
            # cache while the lock was released.  Since the link
            # update is already done, we need only return the
            # computed result and update the count of misses.
            pass
        elif self.__full:
            # Use the old root to store the new key and result.
            oldroot = self.__root
            oldroot[self.KEY] = key
            oldroot[self.RESULT] = value
            # Empty the oldest link and make it the new root.
            # Keep a reference to the old key and old result to
            # prevent their ref counts from going to zero during the
            # update. That will prevent potentially arbitrary object
            # clean-up code (i.e. __del__) from running while we're
            # still adjusting the links.
            root = oldroot[self.NEXT]
            oldkey = root[self.KEY]
            _oldresult = root[self.RESULT]
            root[self.KEY] = root[self.RESULT] = None
            # Now update the cache dictionary.
            del self.__cache[oldkey]
            # Save the potentially reentrant cache[key] assignment
            # for last, after the root and links have been put in
            # a consistent state.
            self.__cache[key] = oldroot
            self.__root = root
        else:
            # Put result in a new link at the front of the queue.
            last = root[self.PREV]
            link = [last, root, key, value]
            last[self.NEXT] = root[self.PREV] = self.__cache[key] = link
            # Use the cache_len bound method instead of the len() function
            # which could potentially be wrapped in an lru_cache itself.
            self.__full = self.get_size() >= self.__maxsize

    def get(self, key: str) -> Optional[Any]:
        link = self.__cache.get(key)
        if link is not None:
            # Move the link to the front of the circular queue
            link_prev, link_next, _key, result = link
            link_prev[self.NEXT] = link_next
            link_next[self.PREV] = link_prev
            last = self.__root[self.PREV]
            last[self.NEXT] = self.__root[self.PREV] = link
            link[self.PREV] = last
            link[self.NEXT] = self.__root
            return result
        return None

    def get_no_adjust(self, key, default_value: Optional[Any] = None) -> Any:
        return self.__cache.get(key, default_value)

    def add_no_adjust(self, key: str, value: Any) -> None:
        self.__cache[key] = value

    def has(self, key: str) -> bool:
        return key in self.__cache

    def clear(self) -> None:
        self.__cache.clear()
        self.__root[:] = [self.__root, self.__root, None, None]

    def get_size(self) -> int:
        return len(self.__cache)
