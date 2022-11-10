from asyncio import Task, iscoroutinefunction
from typing import Awaitable, Callable, Iterable, List, Set, Union

from aquiche.utils._async_utils import awaitify
from aquiche.utils._singleton import Singleton


class CacheCleanupRegistry(metaclass=Singleton):
    __clear_callbacks: List[Union[Callable[..., None], Callable[..., Awaitable[None]]]]

    def __init__(self) -> None:
        self.__clear_callbacks = []

    def register_clear_callback(
        self, clear_callback: Union[Callable[..., None], Callable[..., Awaitable[None]]]
    ) -> None:
        self.__clear_callbacks.append(clear_callback)

    def get_clear_callbacks(self) -> Iterable[Union[Callable[..., None], Callable[..., Awaitable[None]]]]:
        return iter(self.__clear_callbacks)

    def get_async_callbacks(
        self, callbacks: Iterable[Union[Callable[..., None], Callable[..., Awaitable[None]]]]
    ) -> Iterable[Callable[..., Awaitable[None]]]:
        return (
            callback_function if iscoroutinefunction(callback_function) else awaitify(callback_function)  # type: ignore
            for callback_function in callbacks
        )


class DestroyRecordTaskRegistry(metaclass=Singleton):
    __tasks: Set[Task]

    def __init__(self) -> None:
        self.__tasks = set()

    def add_task(self, task: Task) -> None:
        self.__tasks.add(task)
        task.add_done_callback(self.__tasks.discard)

    def get_tasks(self) -> List[Task]:
        return list(self.__tasks)
