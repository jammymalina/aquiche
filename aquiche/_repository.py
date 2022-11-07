from abc import ABCMeta, abstractmethod
from asyncio import gather
from typing import Any, Awaitable, Callable, Dict, Hashable, List, Optional


class CacheRepository(metaclass=ABCMeta):
    @abstractmethod
    def add(self, key: Hashable, value: Any) -> None:
        ...

    @abstractmethod
    def get(self, key: Hashable) -> Any:
        ...

    @abstractmethod
    def get_no_adjust(self, key: Hashable) -> Any:
        ...

    @abstractmethod
    def add_no_adjust(self, key: Hashable, value: Any) -> None:
        ...

    @abstractmethod
    def filter(self, condition: Callable[[Any], bool]) -> List[Any]:
        ...

    @abstractmethod
    async def filter_async(self, condition: Callable[[Any], Awaitable[bool]]) -> List[Any]:
        ...

    @abstractmethod
    def every(self, apply_function: Callable[[Any], None]) -> None:
        ...

    @abstractmethod
    async def every_async(self, apply_function: Callable[[Any], Awaitable[None]]) -> None:
        ...

    @abstractmethod
    def has(self, key: Hashable) -> bool:
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

    __cache: Dict[Hashable, Any]
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

    def add(self, key: Hashable, value: Any) -> None:
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
            # still adjusting the links
            root = old_root[self.NEXT]
            old_key = root[self.KEY]
            root[self.KEY] = root[self.RESULT] = None
            # Now update the cache dictionary.
            del self.__cache[old_key]
            # Save the potentially reentrant cache[key] assignment
            # for last, after the root and links have been put in
            # a consistent state
            self.__cache[key] = old_root
            self.__root = root
        else:
            # Put result in a new link at the front of the queue.
            last = self.__root[self.PREV]
            link = [last, self.__root, key, value]
            last[self.NEXT] = self.__root[self.PREV] = self.__cache[key] = link
            # Use the cache_len bound method instead of the len() function
            # which could potentially be wrapped in an lru_cache itself
            self.__full = (self.__maxsize != 0) and self.get_size() >= self.__maxsize

    def get(self, key: Hashable) -> Optional[Any]:
        link = self.__cache.get(key)
        if link is None:
            return None
        # Move the link to the front of the circular queue
        link_prev, link_next, _key, result = link
        link_prev[self.NEXT] = link_next
        link_next[self.PREV] = link_prev
        last = self.__root[self.PREV]
        last[self.NEXT] = self.__root[self.PREV] = link
        link[self.PREV] = last
        link[self.NEXT] = self.__root
        return result

    def get_no_adjust(self, key: Hashable) -> Any:
        link = self.__cache.get(key)
        if link is None:
            return None
        return link[self.RESULT]

    def add_no_adjust(self, key: Hashable, value: Any) -> None:
        last = self.__root[self.PREV]
        link = [last, self.__root, key, value]
        last[self.NEXT] = self.__root[self.PREV] = self.__cache[key] = link
        self.__full = (self.__maxsize != 0) and self.get_size() >= self.__maxsize

    def filter(self, condition: Callable[[Any], bool]) -> List[Any]:
        removed_items = []

        link = self.__root[self.NEXT]

        while link is not self.__root:
            key, value = link[self.NEXT], link[self.RESULT]
            if not condition(value):
                removed_items.append(self.__delete_node(link))
            link = link[self.NEXT]
        return removed_items

    async def filter_async(self, condition: Callable[[Any], Awaitable[bool]]) -> List[Any]:
        removed_items = []

        link = self.__root[self.NEXT]

        while link is not self.__root:
            value = link[self.RESULT]
            if not await condition(value):
                removed_items.append(self.__delete_node(link))
            link = link[self.NEXT]
        return removed_items

    def every(self, apply_function: Callable[[Any], None]) -> None:
        for link in self.__cache.values():
            result = link[self.RESULT]
            apply_function(result)

    async def every_async(self, apply_function: Callable[[Any], Awaitable[None]]) -> None:
        apply_tasks = (apply_function(link[self.RESULT]) for link in self.__cache.values())
        await gather(*apply_tasks)

    def has(self, key: Hashable) -> bool:
        return key in self.__cache

    def clear(self) -> None:
        self.__cache.clear()
        self.__root[:] = [self.__root, self.__root, None, None]

    def get_size(self) -> int:
        return len(self.__cache)

    def __delete_node(self, link: List) -> Any:
        link_next, key, result = link[self.NEXT], link[self.KEY], link[self.RESULT]
        if self.__root is link:
            self.__root = link_next

        link_iter = self.__root
        while link_iter[self.NEXT] is not link:
            link_iter = link_iter[self.NEXT]

        link_iter[self.NEXT] = link_next

        del self.__cache[key]

        return result
