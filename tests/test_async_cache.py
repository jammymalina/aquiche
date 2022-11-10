from typing import Any, Dict, Iterable
from unittest.mock import ANY, call, MagicMock

import pytest
from pytest_mock import MockerFixture

from aquiche import (
    alru_cache,
    CacheInfo,
    clear_all,
    clear_all_sync,
    await_exit_stack_close_operations,
    cancel_exit_stack_close_operations,
    Key,
)
from aquiche._core import CachedValue


@pytest.fixture
def async_context_manager(mocker: MockerFixture) -> Iterable[MagicMock]:
    context_manager = mocker.MagicMock()
    context_manager.__aenter__ = mocker.AsyncMock(return_value=None)
    context_manager.__aexit__ = mocker.AsyncMock(return_value=None)
    yield context_manager


@pytest.mark.freeze_time
async def test_async_cache_default_params(mocker: MockerFixture) -> None:
    """It should cache the results of the async function, default cache settings, default decorator"""

    counter = mocker.AsyncMock(return_value=None)

    @alru_cache
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    values = ["a", "bb", "ccc", "ddd", "a", "ddd", "bb"]
    results = [await cache_function(value) for value in values]

    assert counter.call_count == len(set(values))
    assert results == [1, 2, 3, 3, 1, 3, 2]
    assert await cache_function.cache_info() == CacheInfo(
        hits=3,
        misses=4,
        maxsize=None,
        current_size=4,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
async def test_async_cache_default_params_decorator_variation(mocker: MockerFixture) -> None:
    """It should cache the results of the async function, default cache settings"""

    counter = mocker.AsyncMock(return_value=None)

    @alru_cache()
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    values = ["a", "bb", "ccc", "ddd", "a", "ddd", "bb"]
    results = [await cache_function(value) for value in values]

    assert counter.call_count == len(set(values))
    assert results == [1, 2, 3, 3, 1, 3, 2]
    assert await cache_function.cache_info() == CacheInfo(
        hits=3,
        misses=4,
        maxsize=None,
        current_size=4,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
async def test_async_cache_key_decorator_variation(mocker: MockerFixture) -> None:
    """It should cache the results of the function, key template is set"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(key="env:{environment}:id:{user[id]}")
    async def get_username(environment: str, user: Dict) -> str:
        nonlocal counter
        await counter()
        return user["username"]

    values = [
        ("dev", {"id": "id1", "username": "file.peter"}),
        ("prod", {"id": "id2", "username": "doe.jane"}),
        ("dev", {"id": "id1", "username": "file.peter"}),  # duplicate
        ("prod", {"id": "id1", "username": "file.peter"}),
        ("prod", {"id": "id3", "username": "smith.john"}),
        ("dev", {"id": "id5", "username": "brave.richard"}),
        ("prod", {"id": "id3", "username": "smith.john"}),  # duplicate
    ]
    results = [await get_username(environment, value) for environment, value in values]

    assert counter.await_count == 5
    assert results == [
        "file.peter",
        "doe.jane",
        "file.peter",
        "file.peter",
        "smith.john",
        "brave.richard",
        "smith.john",
    ]
    assert await get_username.cache_info() == CacheInfo(
        hits=2,
        misses=5,
        maxsize=None,
        current_size=5,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
async def test_async_cache_single_key(mocker: MockerFixture) -> None:
    """It should cache the results of the function, single key is always used - all functions calls share the single cache value"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(key=Key.SINGLE_KEY)
    async def get_username(environment: str) -> int:
        nonlocal counter
        await counter()
        return len(environment)

    values = [
        "prod",
        "dev",
        "system",
        "random_stage",
    ]
    results = [await get_username(environment) for environment in values]

    assert counter.await_count == 1
    assert results == [4, 4, 4, 4]
    assert await get_username.cache_info() == CacheInfo(
        hits=3,
        misses=1,
        maxsize=None,
        current_size=1,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
async def test_async_cache_maxsize(mocker: MockerFixture) -> None:
    """It should cache the results of the async function, up to the maxsize"""

    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(maxsize=5)
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    values = ["a", "bb", "ccc", "ddd", "a", "ddd", "bb", "g", "z", "l", "o", "p", "r"]
    results = [await cache_function(value) for value in values]

    assert results == [1, 2, 3, 3, 1, 3, 2, 1, 1, 1, 1, 1, 1]
    assert await cache_function.cache_info() == CacheInfo(
        hits=ANY,
        misses=ANY,
        maxsize=5,
        current_size=5,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
async def test_async_cache_enabled(mocker: MockerFixture) -> None:
    """It should cache the values since cache is enabled"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(enabled=True)
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    await cache_function("a")
    await cache_function("a")
    await cache_function("a")

    assert counter.call_count == 1
    assert (await cache_function.cache_info()).current_size == 1


@pytest.mark.freeze_time
async def test_async_cache_disabled(mocker: MockerFixture) -> None:
    """It should not cache the values since the cache is disabled"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(enabled=False)
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    await cache_function("a")
    await cache_function("a")
    await cache_function("a")

    assert counter.call_count == 3
    assert (await cache_function.cache_info()).current_size == 0


@pytest.mark.freeze_time
async def test_async_clear_cache(mocker: MockerFixture) -> None:
    """It should clear the cache"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    await cache_function("a")
    await cache_function("a")

    assert counter.call_count == 1

    await cache_function.clear_cache()
    assert (await cache_function.cache_info()).current_size == 0

    await cache_function("a")

    assert counter.call_count == 2


@pytest.mark.freeze_time
async def test_async_clear_cache(mocker: MockerFixture) -> None:
    """It should clear the cache"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    await cache_function("a")
    await cache_function("a")

    assert counter.call_count == 1

    await cache_function.clear_cache()
    await cache_function("a")

    assert counter.call_count == 2


@pytest.mark.freeze_time
async def test_async_cache_destroy_all(mocker: MockerFixture) -> None:
    """It should clear all the caches"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache
    async def cache_function_a(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    @alru_cache
    async def cache_function_b(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    await cache_function_a("a")
    await cache_function_a("a")
    await cache_function_b("b")
    await cache_function_b("b")

    assert counter.call_count == 2

    await clear_all()
    assert (await cache_function_a.cache_info()).current_size == 0
    assert (await cache_function_b.cache_info()).current_size == 0

    await cache_function_a("a")
    await cache_function_a("a")
    await cache_function_b("b")
    await cache_function_b("b")

    assert counter.call_count == 4


@pytest.mark.freeze_time
async def test_async_cache_destroy_ignore_async(mocker: MockerFixture) -> None:
    """It should not clear any of the async caches since we are only clearing the sync ones"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache
    async def cache_function_a(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    @alru_cache
    async def cache_function_b(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    await cache_function_a("a")
    await cache_function_a("a")
    await cache_function_b("b")
    await cache_function_b("b")

    assert (await cache_function_a.cache_info()).current_size == 1
    assert (await cache_function_a.cache_info()).current_size == 1
    assert counter.call_count == 2

    clear_all_sync()
    assert (await cache_function_a.cache_info()).current_size == 1
    assert (await cache_function_b.cache_info()).current_size == 1

    await cache_function_a("a")
    await cache_function_a("a")
    await cache_function_b("b")
    await cache_function_b("b")

    assert counter.call_count == 2


@pytest.mark.freeze_time
async def test_async_cache_function_expiry(mocker: MockerFixture) -> None:
    """It should expire the value based on the expiration function result"""
    counter = mocker.AsyncMock(return_value=None)

    def is_item_expired(value: CachedValue) -> bool:
        return value.value > 1

    @alru_cache(expiration=is_item_expired)
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    values = ["a"] * 10 + ["bb"] * 10
    results = [await cache_function(value) for value in values]

    assert results == [1] * 10 + [2] * 10
    assert counter.await_count == 11


@pytest.mark.freeze_time
async def test_async_cache_async_function_expiry(mocker: MockerFixture) -> None:
    """It should expire the value based on the expiration async function result"""
    counter = mocker.AsyncMock(return_value=None)

    async def is_item_expired(value: CachedValue) -> bool:
        return value.value > 1

    @alru_cache(expiration=is_item_expired)
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    values = ["a"] * 10 + ["bb"] * 10
    results = [await cache_function(value) for value in values]

    assert results == [1] * 10 + [2] * 10
    assert counter.await_count == 11


@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
async def test_async_cache_data_pointer_expiry(mocker: MockerFixture) -> None:
    """It should expire the value based on the data the pointer points to"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(expiration="$.token.expiration")
    async def cache_function(value: str) -> Any:
        nonlocal counter
        await counter(value)
        expiry = "2022-10-30T00:00:00+0000"
        if len(value) > 1:
            expiry = "2022-01-30T00:00:00+0000"

        return {"token": {"expiration": expiry}}

    values = ["a"] * 10 + ["bb"] * 10
    for value in values:
        await cache_function(value)

    assert counter.call_count == 11
    counter.assert_has_awaits([call("a")] * 1 + [call("bb")] * 10, any_order=True)


@pytest.mark.freeze_time
async def test_async_auto_expired_items_removal(mocker: MockerFixture, freezer: Any) -> None:
    """It should automatically clear the expired items from the cache"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(expiration="12h", expired_items_auto_removal_period="12h")
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    freezer.move_to("2022-01-01")
    await cache_function("a")
    await cache_function("a")
    assert counter.call_count == 1
    assert (await cache_function.cache_info()).current_size == 1

    freezer.move_to("2022-01-02")
    await cache_function("b")
    assert (await cache_function.cache_info()).current_size == 1


@pytest.mark.freeze_time
async def test_async_expired_items_removal_manual(mocker: MockerFixture, freezer: Any) -> None:
    """It should clear clear the expired items from the cache when removal function is explicitly called"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(expiration="12h", expired_items_auto_removal_period="10d")
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    freezer.move_to("2022-01-01")
    await cache_function("a")
    await cache_function("a")
    assert (await cache_function.cache_info()).current_size == 1

    freezer.move_to("2022-01-02")
    await cache_function.remove_expired()
    assert (await cache_function.cache_info()).current_size == 0


@pytest.mark.freeze_time
async def test_async_disabled_auto_expired_items_removal(mocker: MockerFixture, freezer: Any) -> None:
    """It should not clear the expired items from the cache the expiry period is explicitly not set"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(expiration="12h", expired_items_auto_removal_period=None)
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        return len(value)

    freezer.move_to("2022-01-01")
    await cache_function("a")
    await cache_function("a")
    assert (await cache_function.cache_info()).current_size == 1

    freezer.move_to("2022-01-02")
    await cache_function("b")
    assert (await cache_function.cache_info()).current_size == 2


@pytest.mark.freeze_time
async def test_async_negative_cache_disabled(mocker: MockerFixture) -> None:
    """It should not store the exception in the cache but rethrow the error"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(negative_cache=False)
    async def cache_function() -> int:
        nonlocal counter
        await counter()
        raise Exception("Doom has fallen upon us")

    with pytest.raises(Exception, match="Doom has fallen upon us"):
        await cache_function()


@pytest.mark.freeze_time
async def test_async_negative_cache_disabled_default(mocker: MockerFixture) -> None:
    """It should not store the exception in the cache by default"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache
    async def cache_function() -> int:
        nonlocal counter
        await counter()
        raise Exception("Doom has fallen upon us")

    with pytest.raises(Exception, match="Doom has fallen upon us"):
        await cache_function()


@pytest.mark.freeze_time
async def test_async_negative_cache_enabled(mocker: MockerFixture) -> None:
    """It should store the exception in the cache"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(negative_cache=True)
    async def cache_function(_value: str) -> int:
        nonlocal counter
        await counter()
        raise Exception("Doom has fallen upon us")

    await cache_function("a")
    error = await cache_function("a")

    assert isinstance(error, Exception)
    assert str(error) == "Doom has fallen upon us"


@pytest.mark.freeze_time
async def test_async_negative_cache_expiry(mocker: MockerFixture, freezer: Any) -> None:
    """It should expire negative results faster"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(negative_cache=True, negative_expiration="10m", expiration="3d")
    async def cache_function(value: str) -> int:
        nonlocal counter
        await counter()
        if value == "a":
            raise Exception("Doom has fallen upon us")
        return len(value)

    freezer.move_to("2022-01-01")
    await cache_function("a")
    await cache_function("b")

    assert counter.call_count == 2
    assert (await cache_function.cache_info()).current_size == 2

    freezer.move_to("2022-01-02")
    await cache_function.remove_expired()

    assert (await cache_function.cache_info()).current_size == 1


@pytest.mark.freeze_time
async def test_async_retry_cache(mocker: MockerFixture) -> None:
    """It should retry when the function throws an error if retry count is set"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(negative_cache=True, retry_count=3, backoff_in_seconds=0)
    async def cache_function(_value: str) -> int:
        nonlocal counter
        await counter()
        raise Exception("Doom has fallen upon us")

    await cache_function("a")

    assert counter.call_count == 4


@pytest.mark.freeze_time
async def test_wrap_exit_stack(mocker: MockerFixture, async_context_manager: MagicMock) -> None:
    """It should wrap the value with the async exit stack and close the async exit stack on clear"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(wrap_async_exit_stack=True)
    async def cache_function(_value: str) -> int:
        nonlocal counter
        await counter()
        return async_context_manager

    await cache_function("a")
    await clear_all()

    async_context_manager.__aenter__.assert_awaited_once()
    async_context_manager.__aexit__.assert_awaited_once()


@pytest.mark.freeze_time
async def test_wrap_exit_stack_delay_cancel(mocker: MockerFixture, async_context_manager: MagicMock) -> None:
    """It should wrap the value with the async exit stack and cancel the async exit stack close operations"""
    counter = mocker.AsyncMock(return_value=None)

    @alru_cache(wrap_async_exit_stack=True, exit_stack_close_delay="1day")
    async def cache_function(_value: str) -> int:
        nonlocal counter
        await counter()
        return async_context_manager

    await cache_function("a")
    await clear_all()
    await cancel_exit_stack_close_operations()

    async_context_manager.__aenter__.assert_awaited_once()
    async_context_manager.__aexit__.assert_not_awaited()
