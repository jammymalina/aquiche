from abc import ABCMeta, abstractmethod
from typing import Any, Callable, Dict, List, Optional


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
            old_root = self.__root
            old_root[self.KEY] = key
            old_root[self.RESULT] = value
            # Empty the oldest link and make it the new root.
            # Keep a reference to the old key and old result to
            # prevent their ref counts from going to zero during the
            # update. That will prevent potentially arbitrary object
            # clean-up code (i.e. __del__) from running while we're
            # still adjusting the links.
            root = old_root[self.NEXT]
            old_key = root[self.KEY]
            root[self.KEY] = root[self.RESULT] = None
            # Now update the cache dictionary.
            del self.__cache[old_key]
            # Save the potentially reentrant cache[key] assignment
            # for last, after the root and links have been put in
            # a consistent state.
            self.__cache[key] = old_root
            self.__root = root
        else:
            # Put result in a new link at the front of the queue.
            last = self.__root[self.PREV]
            link = [last, self.__root, key, value]
            last[self.NEXT] = self.__root[self.PREV] = self.__cache[key] = link
            # Use the cache_len bound method instead of the len() function
            # which could potentially be wrapped in an lru_cache itself.
            self.__full = (self.__maxsize != 0) and self.get_size() >= self.__maxsize

    def get(self, key: str) -> Optional[Any]:
        link = self.__cache.get(key)
        print(self.__cache)
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

    def filter(self, condition: Callable[[str, Any], bool]) -> None:
        size = self.get_size()
        link_index = 0
        link = self.__root
        while link_index < size:
            link_prev, link_next, key, result = link
            if not condition(key, result):
                if self.__root == link:
                    self.__root = link_next

                link_next[self.PREV] = link_prev
                link_prev[self.NEXT] = link_next

                link[self.KEY] = link[self.RESULT] = None

                del self.__cache[key]

            link = link_next
            link_index += 1

    def every(self, f: Callable[[str, Any], None]) -> None:
        link = self.__root
        for _i in range(self.get_size()):
            _link_prev, link_next, key, result = link
            f(key, result)
            link = link_next

    def has(self, key: str) -> bool:
        return key in self.__cache

    def clear(self) -> None:
        self.__cache.clear()
        self.__root[:] = [self.__root, self.__root, None, None]

    def get_size(self) -> int:
        return len(self.__cache)
