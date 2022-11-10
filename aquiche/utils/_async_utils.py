from asyncio import iscoroutinefunction
from contextlib import AsyncExitStack
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Coroutine, List, Optional, Tuple, Union

from aquiche.utils._extraction_utils import extract_from_obj, set_value_obj


def awaitify(
    func: Union[Callable[..., Any], Callable[..., Awaitable[Any]], Coroutine]
) -> Callable[..., Awaitable[Any]]:
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
        self, value: Any, wrap_config: Union[bool, str, List[str]]
    ) -> Tuple[Optional[AsyncExitStack], Any]:
        exit_stack = AsyncExitStack()
        try:
            if isinstance(wrap_config, bool):
                return await self.__wrap_result(wrap_enabled=wrap_config, value=value, exit_stack=exit_stack)

            if isinstance(wrap_config, str):
                return await self.__wrap_all(wrap_pattern=wrap_config, value=value, exit_stack=exit_stack)

            return await self.__wrap_attr_paths(attr_paths=wrap_config, value=value, exit_stack=exit_stack)
        except:
            await exit_stack.aclose()
            raise

    async def __wrap_result(
        self, wrap_enabled: bool, value, exit_stack: AsyncExitStack
    ) -> Tuple[Optional[AsyncExitStack], Any]:
        if not wrap_enabled:
            return None, value
        wrapped_value = await exit_stack.enter_async_context(value)
        return exit_stack, wrapped_value

    async def __wrap_all(
        self, wrap_pattern: str, value, exit_stack: AsyncExitStack
    ) -> Tuple[Optional[AsyncExitStack], Any]:
        if wrap_pattern != "*":
            return None, value

        if isinstance(value, list):
            return exit_stack, [await exit_stack.enter_async_context(item) for item in value]

        data = value
        if not isinstance(data, dict):
            data = value.__dict__

        for key, val in data.items():
            wrapped_key_val = await exit_stack.enter_async_context(val)
            if not callable(val):
                set_value_obj(obj=value, attribute_path=key, value=wrapped_key_val)

        return exit_stack, value

    async def __wrap_attr_paths(
        self, attr_paths: List[str], value: Any, exit_stack: AsyncExitStack
    ) -> Tuple[Optional[AsyncExitStack], Any]:
        sentinel = object()

        for attr_path in attr_paths:
            attr_path = attr_path.strip()
            check_missing_values = True
            if attr_path.endswith(":ignore_missing"):
                check_missing_values = False
                attr_path.rstrip(":ignore_missing")
            attribute_value = extract_from_obj(
                obj=value,
                attribute_path=attr_path,
                check_attribute_exists=check_missing_values,
                default_value=sentinel,
            )

            if attribute_value is not sentinel:
                wrapped_attr_val = await exit_stack.enter_async_context(attribute_value)
                set_value_obj(obj=value, attribute_path=attr_path, value=wrapped_attr_val)

        return exit_stack, value
