from asyncio import iscoroutinefunction
from contextlib import AsyncExitStack
from inspect import isawaitable
from typing import Any, Coroutine, List, Optional, Tuple, Union

from aquiche._core import AsyncFunction, SyncFunction
from aquiche.utils._extraction_utils import extract_from_obj, set_value_obj


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


class AsyncWrapperMixin:
    async def wrap_async_exit_stack(
        self, value: Any, wrap_config: Union[bool, List[str]]
    ) -> Tuple[Optional[AsyncExitStack], Any]:
        exit_stack = AsyncExitStack()
        try:
            if isinstance(wrap_config, bool):
                if not wrap_config:
                    return None, value
                wrapped_value = await exit_stack.enter_async_context(value)
                return exit_stack, wrapped_value

            for attr_path in wrap_config:
                wrapped_attr_val = await exit_stack.enter_async_context(
                    extract_from_obj(obj=value, attribute_path=attr_path)
                )
                set_value_obj(obj=value, attribute_path=attr_path, value=wrapped_attr_val)

            return exit_stack, value
        except:
            await exit_stack.aclose()
            raise
