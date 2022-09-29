from datetime import datetime, timedelta, timezone

import pytest

from aquiche import errors
from aquiche._core import CachedValue
from aquiche._expiration import (
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
    """It should expire the cache based attribute path refers to"""
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
