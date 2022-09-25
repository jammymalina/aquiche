from asyncio import sleep as asleep, gather
import functools

from pytest_mock import MockerFixture

from aquiche.core._cache import CachedValue, CachedRecord, CacheTaskExecutionInfo


def test_default_cached_value() -> None:
    """It should create default CachedValue item"""
    cached_value = CachedValue()

    assert cached_value.value is None
    assert cached_value.inflight is None
    assert cached_value.last_fetched is None


async def test_cache_stampede(mocker: MockerFixture) -> None:
    """It should execute task only once even when multiple cache calls at the same are made"""
    cached_record = CachedRecord()
    task = mocker.AsyncMock(return_value=42)

    values = await gather(
        cached_record.get_cached(task=task, task_exec_info=CacheTaskExecutionInfo()),
        cached_record.get_cached(task=task, task_exec_info=CacheTaskExecutionInfo()),
        cached_record.get_cached(task=task, task_exec_info=CacheTaskExecutionInfo()),
        cached_record.get_cached(task=task, task_exec_info=CacheTaskExecutionInfo()),
    )

    for value in values:
        assert value == 42
    task.assert_called_once()
