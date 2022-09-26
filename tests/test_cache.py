from asyncio import gather

from pytest_mock import MockerFixture

from aquiche._core import CachedValue
from aquiche._cache import AsyncCachedRecord


def test_default_cached_value() -> None:
    """It should create default CachedValue item"""
    cached_value = CachedValue()

    assert cached_value.value is None
    assert cached_value.inflight is None
    assert cached_value.last_fetched is None


async def test_cache_stampede(mocker: MockerFixture) -> None:
    """It should execute task only once even when multiple cache calls at the same are made"""
    get_function = mocker.AsyncMock(return_value=42)
    cached_record = AsyncCachedRecord(get_function=get_function)

    values = await gather(
        cached_record.get_cached(),
        cached_record.get_cached(),
        cached_record.get_cached(),
        cached_record.get_cached(),
    )

    for value in values:
        assert value == 42
    get_function.assert_called_once()
