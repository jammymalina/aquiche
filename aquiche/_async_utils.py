from asyncio import iscoroutinefunction
from typing import Any, Awaitable, Callable


def awaitify(func: Callable) -> Callable[..., Awaitable[Any]]:
    if iscoroutinefunction(func):
        return func

    async def async_func(*args, **kwargs):
        return func(*args, **kwargs)

    return async_func
