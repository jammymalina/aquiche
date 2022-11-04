from uuid import uuid4

from pytest_mock import MockerFixture
from unittest.mock import ANY, call

from aquiche._repository import LRUCacheRepository


def test_lru_cache_repository_add() -> None:
    "It should add all the values to the lru cache repository"
    cache_repo = LRUCacheRepository()

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)

    assert cache_repo.get("a") == 10
    assert cache_repo.get("b") == 20
    assert cache_repo.get("c") == 30
    assert cache_repo.get("d") == 40
    assert cache_repo.get("e") == 50


def test_lru_cache_repository_add_maxsize() -> None:
    "It should store only certain number of values in the lru cache repository"
    cache_repo = LRUCacheRepository(maxsize=2)

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)

    assert cache_repo.get("a") is None
    assert cache_repo.get("b") is None
    assert cache_repo.get("c") is None
    assert cache_repo.get("d") == 40
    assert cache_repo.get("e") == 50


def test_lru_cache_every(mocker: MockerFixture) -> None:
    """It should run the function on every key-value pair"""
    apply_function = mocker.MagicMock(return_value=None)

    cache_repo = LRUCacheRepository(maxsize=15)

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)

    cache_repo.every(apply_function)

    assert apply_function.call_count == 5
    apply_function.assert_has_calls(
        [
            call("a", 10),
            call("b", 20),
            call("c", 30),
            call("d", 40),
            call("e", 50),
        ],
        any_order=True,
    )


async def test_lru_cache_every_async(mocker: MockerFixture) -> None:
    """It should run the async function on every key-value pair"""
    apply_function = mocker.AsyncMock(return_value=None)

    cache_repo = LRUCacheRepository(maxsize=15)

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)

    await cache_repo.every_async(apply_function)

    assert apply_function.call_count == 5
    apply_function.assert_has_calls(
        [
            call("a", 10),
            call("b", 20),
            call("c", 30),
            call("d", 40),
            call("e", 50),
        ],
        any_order=True,
    )


def test_lru_cache_filter(mocker: MockerFixture) -> None:
    """It should filter out odd numbers"""
    filter_function = mocker.MagicMock(side_effect=lambda _key, val: val % 2 == 0 or val is None)
    apply_function = mocker.MagicMock(return_value=None)

    cache_repo = LRUCacheRepository()
    nums = [1, 18, 22, 33, 14, 19, 21, 18, 20, 13, 11]
    for num in nums:
        cache_repo.add(str(uuid4()), num)

    removed_items = cache_repo.filter(filter_function)
    cache_repo.every(apply_function)

    assert cache_repo.get_size() == 5
    assert removed_items == [1, 33, 19, 21, 13, 11]
    assert filter_function.call_count == len(nums)
    assert apply_function.call_count == 5
    apply_function.assert_has_calls(
        [
            call(ANY, 18),
            call(ANY, 22),
            call(ANY, 14),
            call(ANY, 18),
            call(ANY, 20),
        ],
        any_order=True,
    )


async def test_lru_cache_filter_async(mocker: MockerFixture) -> None:
    """It should filter out odd numbers, using async function"""
    filter_function = mocker.AsyncMock(side_effect=lambda _key, val: val % 2 == 0 or val is None)
    apply_function = mocker.AsyncMock(return_value=None)

    cache_repo = LRUCacheRepository()
    nums = [1, 18, 22, 33, 14, 19, 21, 18, 20, 13, 11]
    for num in nums:
        cache_repo.add(str(uuid4()), num)

    removed_items = await cache_repo.filter_async(filter_function)
    await cache_repo.every_async(apply_function)

    assert cache_repo.get_size() == 5
    assert removed_items == [1, 33, 19, 21, 13, 11]
    assert filter_function.call_count == len(nums)
    assert apply_function.call_count == 5
    apply_function.assert_has_calls(
        [
            call(ANY, 18),
            call(ANY, 22),
            call(ANY, 14),
            call(ANY, 18),
            call(ANY, 20),
        ],
        any_order=True,
    )
