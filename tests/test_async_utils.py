import asyncio
from dataclasses import dataclass
from typing import Any, Iterable

import pytest
from pytest_mock import MockerFixture
from unittest.mock import call, MagicMock

from aquiche.errors import ExtractionError
from aquiche.utils._async_utils import awaitify, AsyncWrapperMixin


@dataclass
class WrapperTest:
    value: Any


class Person:
    def __init__(self):
        self.pet = Pet()
        self.residence = Residence()

    def __eq__(self, obj: object) -> bool:
        if not isinstance(obj, Person):
            return False
        return self.pet == obj.pet and self.residence == obj.residence


class Pet:
    def __init__(self, name="Fido", species="Dog"):
        self.name = name
        self.species = species

    def __eq__(self, obj: object) -> bool:
        if not isinstance(obj, Pet):
            return False
        return self.name == obj.name and self.species == obj.species


class Residence:
    def __init__(self, type="House", sqft=200):
        self.type = type
        self.sqft = sqft

    def __eq__(self, obj: object) -> bool:
        if not isinstance(obj, Residence):
            return False
        return self.type == obj.type and self.sqft == obj.sqft


@pytest.fixture
def async_exit_stack(mocker: MockerFixture) -> Iterable[MagicMock]:
    async_exit_stack_mock = mocker.MagicMock()
    async_exit_stack_mock.enter_async_context = mocker.AsyncMock(side_effect=lambda val: WrapperTest(val))
    async_exit_stack_mock.aclose = mocker.AsyncMock(return_value=None)
    mocker.patch("aquiche.utils._async_utils.AsyncExitStack", return_value=async_exit_stack_mock)
    yield async_exit_stack_mock


def sync_sum_function(a: int, b: int) -> int:
    return a + b


async def async_sum_function(a: int, b: int) -> int:
    return a + b


async def test_awaitify_sync() -> None:
    """It should create async equivalent of the passed sync function"""
    async_sum = awaitify(sync_sum_function)
    result = await async_sum(10, 12)
    assert result == 22


async def test_awaitify_task() -> None:
    """It should transform awaitable task to async function"""
    task = asyncio.create_task(async_sum_function(32, 12))
    async_sum = awaitify(task)
    result = await async_sum()
    assert result == 44


async def test_awaitify_async() -> None:
    """It should return the same async function"""
    async_sum = awaitify(async_sum_function)
    result = await async_sum(32, 12)
    assert result == 44


async def test_async_mixin(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap the whole object with async exit stack"""
    mixin = AsyncWrapperMixin()

    _exit_stack, value = await mixin.wrap_async_exit_stack(42, True)

    assert value == WrapperTest(42)
    async_exit_stack.enter_async_context.assert_awaited_once_with(42)


async def test_async_mixin_paths_dict(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap the selected keys with async exit stack, dict"""
    mixin = AsyncWrapperMixin()

    data = {
        "a": 1,
        "b": {
            "c": {
                "d": 15,
                "e": {
                    "g": 12,
                    "f": 100,
                    "a": 200,
                    "d": 350,
                    "z": 1,
                    "n": 232,
                },
            }
        },
    }
    _exit_stack, wrapped_data = await mixin.wrap_async_exit_stack(data, ["$.b.c.e.g", "$.b.c.e.a", "$.b.c.e.n", "$.a"])

    assert wrapped_data == {
        "a": WrapperTest(1),
        "b": {
            "c": {
                "d": 15,
                "e": {
                    "g": WrapperTest(12),
                    "f": 100,
                    "a": WrapperTest(200),
                    "d": 350,
                    "z": 1,
                    "n": WrapperTest(232),
                },
            }
        },
    }
    # original data is modified as well
    assert wrapped_data == data
    assert async_exit_stack.enter_async_context.await_count == 4
    async_exit_stack.enter_async_context.assert_has_awaits([call(12), call(200), call(232), call(1)], any_order=True)


async def test_async_mixin_paths_object(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap the selected keys with async exit stack, object"""
    mixin = AsyncWrapperMixin()

    data = Person()
    _exit_stack, wrapped_data = await mixin.wrap_async_exit_stack(data, ["pet.name", "residence.type"])

    assert data.pet.name == WrapperTest("Fido")
    assert data.residence.type == WrapperTest("House")
    # original data is modified as well
    assert data == wrapped_data
    assert async_exit_stack.enter_async_context.await_count == 2
    async_exit_stack.enter_async_context.assert_has_awaits([call("Fido"), call("House")], any_order=True)


async def test_missing_value_ignore_missing(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap the selected keys with async exit stack and ignore the missing values"""
    mixin = AsyncWrapperMixin()

    data = Person()
    _exit_stack, wrapped_data = await mixin.wrap_async_exit_stack(
        data, ["pet.name", "residence.type", "doom.boom.bottle.of.rum:ignore_missing"]
    )

    assert data.pet.name == WrapperTest("Fido")
    assert data.residence.type == WrapperTest("House")
    assert data == wrapped_data
    assert async_exit_stack.enter_async_context.await_count == 2
    async_exit_stack.enter_async_context.assert_has_awaits([call("Fido"), call("House")], any_order=True)


async def test_missing_value_missing_fail(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap the selected keys with async exit stack and ignore the missing values"""
    mixin = AsyncWrapperMixin()

    data = Person()

    with pytest.raises(ExtractionError) as err_info:
        _exit_stack, wrapped_data = await mixin.wrap_async_exit_stack(
            data, ["pet.name", "residence.type", "doom.boom.bottle.of.rum"]
        )
    assert (
        str(err_info.value)
        == "Unable to extract value from an object, path does not point to any valid value: 'doom.boom.bottle.of.rum'"
    )


async def test_wrap_all_dict(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap all the values in dictionary with async exit stack"""
    mixin = AsyncWrapperMixin()

    data = {
        "a": 1,
        "b": 12,
        "c": 155,
        "d": 246,
        "e": 19,
    }
    _exit_stack, wrapped_data = await mixin.wrap_async_exit_stack(data, "*")

    assert wrapped_data == {
        "a": WrapperTest(1),
        "b": WrapperTest(12),
        "c": WrapperTest(155),
        "d": WrapperTest(246),
        "e": WrapperTest(19),
    }
    # original data is modified as well
    assert wrapped_data == data
    assert async_exit_stack.enter_async_context.await_count == 5
    async_exit_stack.enter_async_context.assert_has_awaits(
        [call(1), call(12), call(155), call(246), call(19)], any_order=True
    )


async def test_wrap_all_object(mocker: MockerFixture, async_exit_stack: MagicMock) -> None:
    """It should wrap all the values in the object with async exit stack"""
    mixin = AsyncWrapperMixin()

    data = Person()
    data.pet = Pet(name="Salem", species="Cat")
    data.residence = Residence(type="Studio", sqft=100)

    _exit_stack, wrapped_data = await mixin.wrap_async_exit_stack(data, "*")

    assert wrapped_data.pet == WrapperTest(Pet(name="Salem", species="Cat"))
    assert wrapped_data.residence == WrapperTest(Residence(type="Studio", sqft=100))
    # original data is modified as well
    assert wrapped_data == data
    assert async_exit_stack.enter_async_context.await_count == 2
    async_exit_stack.enter_async_context.assert_has_awaits(
        [call(Pet(name="Salem", species="Cat")), call(Residence(type="Studio", sqft=100))], any_order=True
    )
