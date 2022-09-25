from aquiche._async_utils import awaitify


def sync_sum_function(a: int, b: int) -> int:
    return a + b


async def async_sum_function(a: int, b: int) -> int:
    return a + b


async def test_awaitify_sync() -> None:
    """It should create async equivalent of the passed sync function"""
    async_sum = awaitify(sync_sum_function)
    result = await async_sum(10, 12)
    assert result == 22


async def test_awaitify_async() -> None:
    """It should return the same async function"""
    async_sum = awaitify(async_sum_function)
    result = await async_sum(32, 12)
    assert result == 44
