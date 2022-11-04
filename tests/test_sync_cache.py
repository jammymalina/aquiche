from typing import Any
from unittest.mock import ANY

import pytest
from pytest_mock import MockerFixture

from aquiche._alru_cache import (
    alru_cache,
    CacheInfo,
    clear_all,
    clear_all_sync,
)
from aquiche.errors import InvalidCacheConfig


@pytest.mark.freeze_time
def test_simple_cache_default_params(mocker: MockerFixture) -> None:
    """it should cache the results of the simple function, default cache settings, default decorator"""

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
def test_simple_cache_default_params_decorator_variation(mocker: MockerFixture) -> None:
    """it should cache the results of the simple function, default cache settings"""

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
def test_simple_clear_cache(mocker: MockerFixture) -> None:
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
def test_simple_clear_cache(mocker: MockerFixture) -> None:
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
async def test_simple_clear_cache(mocker: MockerFixture) -> None:
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
def test_simple_clear_cache_sync(mocker: MockerFixture) -> None:
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
