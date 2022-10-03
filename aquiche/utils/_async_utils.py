from asyncio import iscoroutinefunction
from inspect import isawaitable
from typing import Coroutine, Union

from aquiche._core import AsyncFunction, SyncFunction


def awaitify(func: Union[SyncFunction, AsyncFunction, Coroutine]) -> AsyncFunction:
    if iscoroutinefunction(func) and callable(func):
        return func

    if callable(func):

        async def async_func(*args, **kwargs):
            return func(*args, **kwargs)

        return async_func

    if isawaitable(func):

        async def async_await_func(*_args, **_kwargs):
            return await func

        return async_await_func

    async def async_return_func(*_args, **_kwargs):
        return func

    return async_return_func
