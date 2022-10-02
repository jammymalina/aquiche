from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pytest_mock import MockerFixture

from aquiche import errors
from aquiche._core import CachedValue
from aquiche._expiration import (
    CacheExpiration,
    NonExpiringCacheExpiration,
    DateCacheExpiration,
    RefreshingCacheExpiration,
    SyncAttributeCacheExpiration,
    SyncFuncCacheExpiration,
    AsyncAttributeCacheExpiration,
    AsyncFuncCacheExpiration,
    get_cache_expiration,
)


@pytest.mark.parametrize(
    "value,result",
    [
        (CachedValue(last_fetched=None, value=None), True),
        (
            CachedValue(
                last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            False,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            False,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=30, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            False,
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_non_expiring_cache_expiration(value: CachedValue, result: bool) -> None:
    """It should never expire the cache"""
    cache_expiration = NonExpiringCacheExpiration()
    assert cache_expiration.is_value_expired(value) == result


@pytest.mark.parametrize(
    "value,result",
    [
        (CachedValue(last_fetched=None, value=None), True),
        (
            CachedValue(
                last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            False,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            False,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=25, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            True,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=26, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            True,
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_cache_expiration(value: CachedValue, result: bool) -> None:
    """It should expire the cache based on the set date"""
    cache_expiration = DateCacheExpiration(
        expiry_date=datetime(year=2022, month=9, day=25, hour=0, minute=0, second=0, tzinfo=timezone.utc)
    )
    assert cache_expiration.is_value_expired(value) == result


@pytest.mark.parametrize(
    "value,result",
    [
        (CachedValue(last_fetched=None, value=None), True),
        (
            CachedValue(
                last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            True,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            True,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=29, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            True,
        ),
        (
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=29, hour=1, minute=0, second=0, tzinfo=timezone.utc),
                value=None,
            ),
            False,
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_cache_refresh_expiration(value: CachedValue, result: bool) -> None:
    """It should expire the cache based on time interval"""
    cache_expiration = RefreshingCacheExpiration(refresh_interval=timedelta(days=1))
    assert cache_expiration.is_value_expired(value) == result


@pytest.mark.parametrize(
    "value,result",
    [
        (
            # expired date
            CachedValue(
                last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": "1999-01-01"}}},
            ),
            True,
        ),
        (
            # refresh interval - 5 days (should refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": 432000}}},
            ),
            True,
        ),
        (
            # refresh interval - 5 days (should not refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=29, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": 432000}}},
            ),
            False,
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_sync_attribute_cache_expiration(value: CachedValue, result: bool) -> None:
    """It should expire the cache based on the attribute that the path refers to"""
    cache_expiration = SyncAttributeCacheExpiration(attribute_path="$.data.nested.expiration")
    assert cache_expiration.is_value_expired(value) == result


@pytest.mark.parametrize(
    "value",
    [
        CachedValue(last_fetched=None, value=None),
        CachedValue(
            last_fetched=datetime(year=2022, month=9, day=29, hour=0, minute=0, second=0, tzinfo=timezone.utc),
            value={"data": {"nested": {"random": 432000}}},
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_sync_attribute_cache_expiration_invalid_path(value: CachedValue) -> None:
    """It should throw an error if the attribute path is invalid or points to an invalid object"""
    with pytest.raises(errors.ExtractionError):
        cache_expiration = SyncAttributeCacheExpiration(attribute_path="$.data.nested.expiration")
        cache_expiration.is_value_expired(value)


@pytest.mark.parametrize(
    "value,result",
    [
        (
            # expired date
            CachedValue(
                last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": "1999-01-01"}}},
            ),
            True,
        ),
        (
            # refresh interval - 5 days (should refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": 432000}}},
            ),
            True,
        ),
        (
            # refresh interval - 5 days (should not refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=29, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": 432000}}},
            ),
            False,
        ),
        (
            # refresh interval - function that returns 5 days (should refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": lambda _: 432000}}},
            ),
            True,
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_sync_func_cache_expiration(value: CachedValue, result: bool) -> None:
    """It should expire the cache based on the result of the function"""
    cache_expiration = SyncFuncCacheExpiration(
        func=lambda cache_value: cache_value["value"]["data"]["nested"]["expiration"]
    )
    assert cache_expiration.is_value_expired(value) == result


@pytest.mark.parametrize(
    "value",
    [
        CachedValue(
            last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
            value={"data": {"nested": {"expiration_key": "id1"}}},
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_sync_func_cache_expiration_invalid_sync_type(mocker: MockerFixture, value: CachedValue) -> None:
    """It should throw an error if the sync cache expiration requires to call async"""
    async_func = mocker.AsyncMock(return_value=432000)
    with pytest.raises(errors.InvalidSyncExpirationType) as err_info:
        cache_expiration = SyncFuncCacheExpiration(func=lambda _: async_func)
        cache_expiration.is_value_expired(value)
    assert str(err_info.value) == "Invalid cache expiration value '_execute_mock_call': it resolves to async expiration"


@pytest.mark.parametrize(
    "value,result",
    [
        (
            # expired date
            CachedValue(
                last_fetched=datetime(year=2000, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": "1999-01-01"}}},
            ),
            True,
        ),
        (
            # refresh interval - 5 days (should refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": 432000}}},
            ),
            True,
        ),
        (
            # refresh interval - 5 days (should not refresh)
            CachedValue(
                last_fetched=datetime(year=2022, month=9, day=29, hour=0, minute=0, second=0, tzinfo=timezone.utc),
                value={"data": {"nested": {"expiration": 432000}}},
            ),
            False,
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
async def test_async_attribute_cache_expiration(value: CachedValue, result: bool) -> None:
    """It should expire the cache based on the attribute that the path refers to, async is allowed"""
    cache_expiration = AsyncAttributeCacheExpiration(attribute_path="$.data.nested.expiration")
    assert await cache_expiration.is_value_expired(value) == result


@pytest.mark.parametrize(
    "value",
    [
        CachedValue(
            last_fetched=datetime(year=2022, month=9, day=24, hour=0, minute=0, second=0, tzinfo=timezone.utc),
            value={"data": {"nested": {"expiration_key": "id1"}}},
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
async def test_async_attribute_cache_expiration_async(mocker: MockerFixture, value: CachedValue) -> None:
    """It should refresh the value based on the result of the async function"""
    async_func = mocker.AsyncMock(return_value=432000)
    cache_expiration = AsyncFuncCacheExpiration(func=async_func)
    assert await cache_expiration.is_value_expired(value)


@pytest.mark.parametrize(
    "value,result",
    [
        # Refreshing cache expiration
        (432000, RefreshingCacheExpiration(refresh_interval=timedelta(days=5))),
        ("5days", RefreshingCacheExpiration(refresh_interval=timedelta(days=5))),
        ("100s 10m", RefreshingCacheExpiration(refresh_interval=timedelta(seconds=100, minutes=10))),
        ("100 seconds 10 minutes", RefreshingCacheExpiration(refresh_interval=timedelta(seconds=100, minutes=10))),
        # Date
        (
            1641021310,
            DateCacheExpiration(datetime(year=2022, month=1, day=1, hour=7, minute=15, second=10, tzinfo=timezone.utc)),
        ),
        (
            1641021310 * 1000,
            DateCacheExpiration(datetime(year=2022, month=1, day=1, hour=7, minute=15, second=10, tzinfo=timezone.utc)),
        ),
        (
            "2022-01-01T07:15:10.000Z",
            DateCacheExpiration(datetime(year=2022, month=1, day=1, hour=7, minute=15, second=10, tzinfo=timezone.utc)),
        ),
    ],
)
def test_get_expiration_simple_values(value: Any, result: CacheExpiration) -> None:
    """It should get cache expiration from the simple value"""
    cache_expiration = get_cache_expiration(value)
    assert cache_expiration == result
