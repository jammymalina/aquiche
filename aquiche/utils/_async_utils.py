from asyncio import iscoroutinefunction
from typing import Union

from aquiche._core import AsyncFunction, SyncFunction


def awaitify(func: Union[SyncFunction, AsyncFunction]) -> AsyncFunction:
    if iscoroutinefunction(func):
        return func

    async def async_func(*args, **kwargs):
        return func(*args, **kwargs)

    return async_func
