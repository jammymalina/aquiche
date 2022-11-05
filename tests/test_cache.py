from asyncio import gather, sleep as asleep
from threading import Thread

from pytest_mock import MockerFixture

from aquiche._async_cache import AsyncCachedRecord, AsyncCachedValue
from aquiche._core import CacheTaskExecutionInfo
from aquiche._expiration import NonExpiringCacheExpiration
from aquiche._sync_cache import SyncCachedRecord, SyncCachedValue


def test_default_async_cached_value() -> None:
    """It should create default AsyncCachedValue item"""
    cached_value = AsyncCachedValue()

    assert cached_value.value is None
    assert cached_value.inflight is None
    assert cached_value.last_fetched is None


def test_default_sync_cached_value() -> None:
    """It should create default SyncCachedValue item"""
    cached_value = SyncCachedValue()

    assert cached_value.value is None
    assert cached_value.inflight is None
    assert cached_value.last_fetched is None


async def test_cache_stampede(mocker: MockerFixture) -> None:
    """It should execute task only once even when multiple cache calls at the same are made"""

    async def test_function() -> int:
        # We try to suspend the execution
        await asleep(0)
        return 42

    get_function = mocker.AsyncMock(side_effect=test_function)
    cached_record = AsyncCachedRecord(
        get_function=get_function,
        get_exec_info=CacheTaskExecutionInfo(),
        expiration=NonExpiringCacheExpiration(),
        negative_expiration=NonExpiringCacheExpiration(),
    )

    values = await gather(
        cached_record.get_cached(),
        cached_record.get_cached(),
        cached_record.get_cached(),
        cached_record.get_cached(),
    )

    for value in values:
        assert value == 42
    get_function.assert_called_once()


def test_cache_stampede_sync(mocker: MockerFixture) -> None:
    """It should execute task only once even when multiple cache calls at the same are made"""
    get_function = mocker.MagicMock(return_value=42)
    cached_record = SyncCachedRecord(
        get_function=get_function,
        get_exec_info=CacheTaskExecutionInfo(),
        expiration=NonExpiringCacheExpiration(),
        negative_expiration=NonExpiringCacheExpiration(),
    )

    threads = []
    for _index in range(5):
        x = Thread(target=lambda record: record.get_cached(), args=(cached_record,))
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()

    get_function.assert_called_once()
