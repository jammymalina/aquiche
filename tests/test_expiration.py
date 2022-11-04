from datetime import date, datetime, time, timedelta, timezone
import re
from typing import Any

import pytest
from pytest_mock import MockerFixture

from aquiche import errors
from aquiche._core import CachedValue
from aquiche._expiration import (
    BoolCacheExpiration,
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


@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_bool_expiration() -> None:
    """It should expire the cache when the value is set to True and vice-versa"""
    value = CachedValue()

    cache_expiration = BoolCacheExpiration(True)
    assert cache_expiration.is_value_expired(value)

    cache_expiration = BoolCacheExpiration(False)
    assert not cache_expiration.is_value_expired(value)

    cache_expiration = get_cache_expiration(True)
    assert isinstance(cache_expiration, BoolCacheExpiration)


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
    """It should expire the cache based on the time interval"""
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
        func=lambda cache_value: cache_value.value["data"]["nested"]["expiration"]
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
    assert (
        re.match(r"^Invalid cache expiration value '.*'\: it resolves to async expiration$", str(err_info.value))
        is not None
    )


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
    async_mock = mocker.AsyncMock(return_value=432000)
    cache_expiration = AsyncFuncCacheExpiration(func=async_mock)

    assert await cache_expiration.is_value_expired(value)


@pytest.mark.parametrize(
    "value,result",
    [
        # Refreshing cache expiration
        (30, RefreshingCacheExpiration(refresh_interval=timedelta(seconds=30))),
        ("30", RefreshingCacheExpiration(refresh_interval=timedelta(seconds=30))),
        ("15:30", RefreshingCacheExpiration(refresh_interval=timedelta(minutes=15, seconds=30))),
        ("10:15:30", RefreshingCacheExpiration(refresh_interval=timedelta(hours=10, minutes=15, seconds=30))),
        ("4 15:30", RefreshingCacheExpiration(refresh_interval=timedelta(days=4, minutes=15, seconds=30))),
        ("4 10:15:30", RefreshingCacheExpiration(refresh_interval=timedelta(days=4, hours=10, minutes=15, seconds=30))),
        (432000, RefreshingCacheExpiration(refresh_interval=timedelta(days=5))),
        ("5days", RefreshingCacheExpiration(refresh_interval=timedelta(days=5))),
        ("P5D", RefreshingCacheExpiration(refresh_interval=timedelta(days=5))),
        ("100s 10m", RefreshingCacheExpiration(refresh_interval=timedelta(seconds=100, minutes=10))),
        ("100 seconds 10 minutes", RefreshingCacheExpiration(refresh_interval=timedelta(seconds=100, minutes=10))),
        (timedelta(days=1, hours=8), RefreshingCacheExpiration(refresh_interval=timedelta(days=1, hours=8))),
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
        (
            datetime(year=2022, month=1, day=1, hour=7, minute=15, second=10, tzinfo=timezone.utc),
            DateCacheExpiration(datetime(year=2022, month=1, day=1, hour=7, minute=15, second=10, tzinfo=timezone.utc)),
        ),
        (
            date(year=2022, month=1, day=1),
            DateCacheExpiration(datetime(year=2022, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
        ),
        (
            time(hour=10, minute=30, second=11, tzinfo=timezone.utc),
            DateCacheExpiration(
                datetime(year=2022, month=9, day=30, hour=10, minute=30, second=11, tzinfo=timezone.utc)
            ),
        ),
    ],
)
@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_get_expiration_simple_values(value: Any, result: CacheExpiration) -> None:
    """It should get cache expiration from the simple value"""
    cache_expiration = get_cache_expiration(value)
    assert cache_expiration == result


@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_get_attribute_expiration() -> None:
    """It should return sync attribute expiration when preferred async is set to False and async one when it is set to True, default is async"""
    sync_expiration = get_cache_expiration("$.data.nested.expiration", prefer_async=False)
    async_expiration = get_cache_expiration("$.data.nested.expiration", prefer_async=True)
    default_expiration = get_cache_expiration("$.data.nested.expiration")

    assert isinstance(sync_expiration, SyncAttributeCacheExpiration)
    assert isinstance(async_expiration, AsyncAttributeCacheExpiration)
    assert isinstance(default_expiration, AsyncAttributeCacheExpiration)


@pytest.mark.freeze_time("2022-09-30T00:00:00+0000")
def test_get_async_func_expiration(mocker: MockerFixture) -> None:
    """It should always get async func expiration when async function is passed"""

    async def get_async_expiration() -> int:
        return 42

    async_mock = mocker.AsyncMock(return_value=30)
    cache_expiration_a = get_cache_expiration(async_mock, prefer_async=True)
    cache_expiration_b = get_cache_expiration(async_mock, prefer_async=False)
    cache_expiration_c = get_cache_expiration(get_async_expiration, prefer_async=False)

    assert isinstance(cache_expiration_a, AsyncFuncCacheExpiration)
    assert isinstance(cache_expiration_b, AsyncFuncCacheExpiration)
    assert isinstance(cache_expiration_c, AsyncFuncCacheExpiration)
