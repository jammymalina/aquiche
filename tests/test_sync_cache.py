import pytest

from aquiche._alru_cache import alru_cache, CacheInfo

from pytest_mock import MockerFixture
from unittest.mock import ANY


@pytest.mark.freeze_time
def test_simple_cache_default_params(mocker: MockerFixture) -> None:
    """it should cache the results of the simple function, default cache settings"""

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
    assert cache_function.cache_info() == CacheInfo(
        hits=3,
        misses=4,
        maxsize=None,
        current_size=4,
        last_expiration_check=ANY,
    )
