from typing import Any, Dict
from unittest.mock import ANY, call

import pytest
from pytest_mock import MockerFixture

from aquiche import (
    alru_cache,
    CacheInfo,
    clear_all,
    clear_all_sync,
    Key,
)
from aquiche._core import CachedItem
from aquiche.errors import InvalidCacheConfig


@pytest.mark.freeze_time
def test_cache_default_params(mocker: MockerFixture) -> None:
    """It should cache the results of the simple function, default cache settings, default decorator"""

    counter = mocker.MagicMock(return_value=None)

    @alru_cache
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    values = ["a", "bb", "ccc", "ddd", "a", "ddd", "bb"]
    results = [cache_function(value) for value in values]

    assert counter.call_count == len(set(values))
    assert results == [1, 2, 3, 3, 1, 3, 2]
    assert cache_function.cache_info() == CacheInfo(
        hits=3,
        misses=4,
        maxsize=None,
        current_size=4,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
def test_cache_default_params_decorator_variation(mocker: MockerFixture) -> None:
    """It should cache the results of the function, default cache settings"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache()
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    values = ["a", "bb", "ccc", "ddd", "a", "ddd", "bb"]
    results = [cache_function(value) for value in values]

    assert counter.call_count == len(set(values))
    assert results == [1, 2, 3, 3, 1, 3, 2]
    assert cache_function.cache_info() == CacheInfo(
        hits=3,
        misses=4,
        maxsize=None,
        current_size=4,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
def test_cache_key_decorator_variation(mocker: MockerFixture) -> None:
    """It should cache the results of the function, key template is set"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(key="env:{environment}:id:{user[id]}")
    def get_username(environment: str, user: Dict) -> str:
        nonlocal counter
        counter()
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
    results = [get_username(environment, value) for environment, value in values]

    assert counter.call_count == 5
    assert results == [
        "file.peter",
        "doe.jane",
        "file.peter",
        "file.peter",
        "smith.john",
        "brave.richard",
        "smith.john",
    ]
    assert get_username.cache_info() == CacheInfo(
        hits=2,
        misses=5,
        maxsize=None,
        current_size=5,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
def test_cache_single_key(mocker: MockerFixture) -> None:
    """It should cache the results of the function, single key is always used - all functions calls share the single cache value"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(key=Key.SINGLE_KEY)
    def get_username(environment: str) -> int:
        nonlocal counter
        counter()
        return len(environment)

    values = [
        "prod",
        "dev",
        "system",
        "random_stage",
    ]
    results = [get_username(environment) for environment in values]

    assert counter.call_count == 1
    assert results == [4, 4, 4, 4]
    assert get_username.cache_info() == CacheInfo(
        hits=3,
        misses=1,
        maxsize=None,
        current_size=1,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
def test_cache_maxsize(mocker: MockerFixture) -> None:
    """It should cache the results of the function, up to the maxsize"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(maxsize=5)
    def cache_function(value: str) -> int:
        nonlocal counter
        return len(value)

    values = ["a", "bb", "ccc", "ddd", "a", "ddd", "bb", "g", "z", "l", "o", "p", "r"]
    results = [cache_function(value) for value in values]

    assert results == [1, 2, 3, 3, 1, 3, 2, 1, 1, 1, 1, 1, 1]
    assert cache_function.cache_info() == CacheInfo(
        hits=ANY,
        misses=ANY,
        maxsize=5,
        current_size=5,
        last_expiration_check=ANY,
    )


@pytest.mark.freeze_time
def test_cache_enabled(mocker: MockerFixture) -> None:
    """It should cache the values since cache is enabled"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(enabled=True)
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    cache_function("a")
    cache_function("a")
    cache_function("a")

    assert counter.call_count == 1
    assert cache_function.cache_info().current_size == 1


@pytest.mark.freeze_time
def test_cache_disabled(mocker: MockerFixture) -> None:
    """It should not cache the values since the cache is disabled"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(enabled=False)
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    cache_function("a")
    cache_function("a")
    cache_function("a")

    assert counter.call_count == 3
    assert cache_function.cache_info().current_size == 0


@pytest.mark.freeze_time
def test_clear_cache(mocker: MockerFixture) -> None:
    """It should clear the cache"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    cache_function("a")
    cache_function("a")

    assert counter.call_count == 1

    cache_function.clear_cache()
    assert cache_function.cache_info().current_size == 0

    cache_function("a")

    assert counter.call_count == 2


@pytest.mark.freeze_time
def test_clear_cache(mocker: MockerFixture) -> None:
    """It should clear the cache"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    cache_function("a")
    cache_function("a")

    assert counter.call_count == 1

    cache_function.clear_cache()
    cache_function("a")

    assert counter.call_count == 2


@pytest.mark.freeze_time
async def test_clear_cache(mocker: MockerFixture) -> None:
    """It should clear all the caches"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache
    def cache_function_a(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    @alru_cache
    def cache_function_b(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    cache_function_a("a")
    cache_function_a("a")
    cache_function_b("b")
    cache_function_b("b")

    assert counter.call_count == 2

    await clear_all()
    assert cache_function_a.cache_info().current_size == 0
    assert cache_function_b.cache_info().current_size == 0

    cache_function_a("a")
    cache_function_a("a")
    cache_function_b("b")
    cache_function_b("b")

    assert counter.call_count == 4


@pytest.mark.freeze_time
def test_clear_cache_sync(mocker: MockerFixture) -> None:
    """It should clear all the sync caches"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache
    def cache_function_a(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    @alru_cache
    def cache_function_b(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    cache_function_a("a")
    cache_function_a("a")
    cache_function_b("b")
    cache_function_b("b")

    assert cache_function_a.cache_info().current_size == 1
    assert cache_function_a.cache_info().current_size == 1
    assert counter.call_count == 2

    clear_all_sync()
    assert cache_function_a.cache_info().current_size == 0
    assert cache_function_b.cache_info().current_size == 0

    cache_function_a("a")
    cache_function_a("a")
    cache_function_b("b")
    cache_function_b("b")

    assert counter.call_count == 4


@pytest.mark.freeze_time
def test_cache_function_expiry(mocker: MockerFixture) -> None:
    """It should expire the value based on the expiration function result"""
    counter = mocker.MagicMock(return_value=None)

    def is_item_expired(value: CachedItem) -> bool:
        return value.value > 1

    @alru_cache(expiration=is_item_expired)
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    values = ["a"] * 10 + ["bb"] * 10
    results = [cache_function(value) for value in values]

    assert results == [1] * 10 + [2] * 10
    assert counter.call_count == 11


@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_cache_data_pointer_expiry(mocker: MockerFixture) -> None:
    """It should expire the value based on the data the pointer points to"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(expiration="$.token.expiration")
    def cache_function(value: str) -> Any:
        nonlocal counter
        counter(value)
        expiry = "2022-10-30T00:00:00+0000"
        if len(value) > 1:
            expiry = "2022-01-30T00:00:00+0000"

        return {"token": {"expiration": expiry}}

    values = ["a"] * 10 + ["bb"] * 10
    for value in values:
        cache_function(value)

    assert counter.call_count == 11
    counter.assert_has_calls([call("a")] * 1 + [call("bb")] * 10, any_order=True)


@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_cache_async_function_expiry_fail(mocker: MockerFixture) -> None:
    """It should fail when we pass the async function to the sync cache"""
    counter = mocker.MagicMock(return_value=None)

    async def is_item_expired(value: CachedItem) -> bool:
        return value.value > 1

    @alru_cache(expiration=is_item_expired)
    def cache_function(value: str) -> Any:
        nonlocal counter
        counter()
        return len(value)

    with pytest.raises(InvalidCacheConfig) as err_info:
        cache_function("a")

    assert (
        str(err_info.value) == "Invalid cache params - invalid expiration, use values that evaluate to 'sync' objects"
    )


@pytest.mark.freeze_time
def test_auto_expired_items_removal(mocker: MockerFixture, freezer: Any) -> None:
    """It should automatically clear the expired items from the cache (in sync cache the decorated function still has to be called for the items to be cleared)"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(expiration="12h", expired_items_auto_removal_period="12h")
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    freezer.move_to("2022-01-01")
    cache_function("a")
    cache_function("a")
    assert counter.call_count == 1
    assert cache_function.cache_info().current_size == 1

    freezer.move_to("2022-01-02")
    cache_function("b")
    assert cache_function.cache_info().current_size == 1


@pytest.mark.freeze_time
def test_expired_items_removal_manual(mocker: MockerFixture, freezer: Any) -> None:
    """It should clear clear the expired items from the cache when removal function is explicitly called"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(expiration="12h", expired_items_auto_removal_period="10d")
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    freezer.move_to("2022-01-01")
    cache_function("a")
    cache_function("a")
    assert cache_function.cache_info().current_size == 1

    freezer.move_to("2022-01-02")
    cache_function.remove_expired()
    assert cache_function.cache_info().current_size == 0


@pytest.mark.freeze_time
def test_disabled_auto_expired_items_removal(mocker: MockerFixture, freezer: Any) -> None:
    """It should not clear the expired items from the cache the expiry period is explicitly not set"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(expiration="12h", expired_items_auto_removal_period=None)
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        return len(value)

    freezer.move_to("2022-01-01")
    cache_function("a")
    cache_function("a")
    assert cache_function.cache_info().current_size == 1

    freezer.move_to("2022-01-02")
    cache_function("b")
    assert cache_function.cache_info().current_size == 2


@pytest.mark.freeze_time
def test_negative_cache_disabled(mocker: MockerFixture) -> None:
    """It should not store the exception in the cache but rethrow the error"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(negative_cache=False)
    def cache_function() -> int:
        nonlocal counter
        counter()
        raise Exception("Doom has fallen upon us")

    with pytest.raises(Exception, match="Doom has fallen upon us"):
        cache_function()


@pytest.mark.freeze_time
def test_negative_cache_disabled_default(mocker: MockerFixture) -> None:
    """It should not store the exception in the cache by default"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache
    def cache_function() -> int:
        nonlocal counter
        counter()
        raise Exception("Doom has fallen upon us")

    with pytest.raises(Exception, match="Doom has fallen upon us"):
        cache_function()


@pytest.mark.freeze_time
def test_negative_cache_enabled(mocker: MockerFixture) -> None:
    """It should store the exception in the cache"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(negative_cache=True)
    def cache_function(_value: str) -> int:
        nonlocal counter
        counter()
        raise Exception("Doom has fallen upon us")

    cache_function("a")
    error = cache_function("a")

    assert isinstance(error, Exception)
    assert str(error) == "Doom has fallen upon us"


@pytest.mark.freeze_time
def test_negative_cache_expiry(mocker: MockerFixture, freezer: Any) -> None:
    """It should expire negative results faster"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(negative_cache=True, negative_expiration="10m", expiration="3d")
    def cache_function(value: str) -> int:
        nonlocal counter
        counter()
        if value == "a":
            raise Exception("Doom has fallen upon us")
        return len(value)

    freezer.move_to("2022-01-01")
    cache_function("a")
    cache_function("b")

    assert counter.call_count == 2
    assert cache_function.cache_info().current_size == 2

    freezer.move_to("2022-01-02")
    cache_function.remove_expired()

    assert cache_function.cache_info().current_size == 1


@pytest.mark.freeze_time
def test_retry_cache(mocker: MockerFixture) -> None:
    """It should retry when the function throws an error if retry count is set"""
    counter = mocker.MagicMock(return_value=None)

    @alru_cache(negative_cache=True, retry_count=3, backoff_in_seconds=0)
    def cache_function(_value: str) -> int:
        nonlocal counter
        counter()
        raise Exception("Doom has fallen upon us")

    cache_function("a")

    assert counter.call_count == 4


@pytest.mark.freeze_time
def test_invalid_cache_config_wrap_async_exit_stack() -> None:
    """It should throw an invalid cache error if we try to wrap result in the async exit stack"""
    with pytest.raises(InvalidCacheConfig) as err_info:

        @alru_cache(wrap_async_exit_stack=True)
        def _cache_function(_value: str) -> int:
            raise Exception("Doom has fallen upon us")

    assert str(err_info.value) == "Invalid cache params - wrap_async_exit_stack can only be used with async functions"
