# aquiche

Fast, unopinionated, minimalist cache decorator for [python](https://www.python.org).

[<img alt="Link to the github repository" src="https://img.shields.io/badge/github-jammymalina/aquiche-8da0cb?style=for-the-badge&labelColor=555555&logo=github">](https://github.com/jammymalina/aquiche)
[<img alt="Link to the build" src="https://img.shields.io/github/workflow/status/jammymalina/aquiche/CI?style=for-the-badge">](https://github.com/jammymalina/aquiche/actions?query=branch%3Amain+)
[<img alt="Link to PyPi" src="https://img.shields.io/pypi/v/aquiche?style=for-the-badge">](https://pypi.org/project/aquiche)
<img alt="Supported python versions" src="https://img.shields.io/pypi/pyversions/aquiche?style=for-the-badge">

```python
import httpx
from aquiche import alru_cache

@alru_cache(wrap_async_exit_stack=True)
async def get_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url="https://api.data.io")


@alru_cache(expiration="10minutes")
async def get_data(id: str) -> dict:
    client = get_client()
    res = await client.get(f"/data/{id}")
    return res.json()

async def main() -> None:
    data = await get_data("id1")
    print("Received the data!!!")
```

## Installation

This is a [python](https://www.python.org) module available through the
[pypi registry](https://pypi.org).

Before installing, [download and install Python](https://www.python.org/downloads).
Python 3.8 or higher is required.

Installation is done using the
[`pip install` command](https://packaging.python.org/en/latest/tutorials/installing-packages):

```console
$ pip install aquiche
```

## Features

- Robust caching
- Supports both multithreaded and async environment
- Works with both "sync" functions and async functions/coroutines
- No cache stampede/cascading failure - no multiple redundant calls when your cache is being hit at the same time
- Wide range of options on setting the expiration
- Negative cache support
- Focus on high performance
- Contains typings
- High test coverage

## Guide

The decorator is very similar to [`functools.lru_cache`](https://docs.python.org/3/library/functools.html#functools.lru_cache). It can simply be used without any options, both `@alru_cache` and `@alru_cache()` are supported.

### Enable/Disable

Cache can be enabled or disabled. **It is not checked actively during the runtime!** You cannot update the value once the function is wrapped. If you want to check actively if the cache is enabled use expiration param.

```python
@alru_cache(enabled=True|False)
def cache_function(value: str) -> int:
    return len(value)
```

### Maxsize

The maxsize param behaves the same as in `functools.lru_cache`. If set to `None` the cache can grow without bound. If set, the memoizing decorator saves up to the maxsize most recent calls. It is set to `None` by default.

```python
from aquiche import alru_cache

@alru_cache(maxsize=5)
async def cache_function_limited(value: str) -> int:
    return len(value)

@alru_cache(maxsize=None)
async def cache_function_unlimited(value: str) -> int:
    return len(value)
```

### Expiration

It is possible to set when the function call expires. Expired functions call will automatically be called again next time the function is being called. To save the memory it is possible to set `expired_items_auto_removal_period` to automatically remove items after certain period of time. If set to `None` expired items are not removed but stay in the cache. It is set to `None` by default. The decorated function still needs to be called for the removal to happen - the expiration removal task is not automatically scheduled. It is recommended to rather use `maxsize` to limit the memory consumption and keep the param set to `None`.

Possible expiration options:

- `None`, the function call never expires
- `True|False`, if set to `True` the value is expired and function will be called again
- `int|float`, based on the value it will either be treated as the `timedelta` or unix timestamp
- `datetime`, TTL (Time To Live)/the expiration date, if it contains no timezone the UTC timezone is automatically added
- `time`, the function call will expire today at this time
- `timedelta`, TTR (Time To Refresh/refresh interval), the function call will refresh the value each `timedelta` period
- `datetime|time|timedelta` string, the string that can be parsed to `datetime|time|timedelta`, supported formats: ISO 8601, human readable formats, uses the same (or nearly the same) resolution as [pydantic](https://pydantic-docs.helpmanual.io)
- Data pointer string e.g. `$.response.data.expiry`, the pointer can point to any of the other expiration values
- Function, the `CachedItem` object (for more information see the example below) will be passed as an argument to the function. The error will be thrown if you try to use this option while decorating a "sync" function. It is possible to return any of the other expiration options from the function
- Async function/coroutine, the `CachedItem` object (for more information see the example below) will be passed as an argument to the function. It can only be used when decorating an async function/coroutine. It is possible to return any of the other expiration options from the function

It is set to `None` by default.

#### Datetime, Date and Time Expiration

```python
from aquiche import alru_cache
from datetime import datetime, timedelta

# Datetime expiration
# The record should be removed after 1 day (the function needs to be called)
@alru_cache(expiration="2012-04-23T09:15:00Z", expired_items_auto_removal_period="1 day")
async def get_data(value: str) -> int:
    return len(value)

# Datetime expiration
@alru_cache(
    expiration=datetime(2017, 5, 5, 19, 27, 24),
    expired_items_auto_removal_period="1 hour"
)
async def get_data(value: str) -> int:
    return len(value)

# Date expiration, expires at midnight (UTC)
@alru_cache(expiration="2012-04-56", expired_items_auto_removal_period=timedelta(seconds=300))
async def get_data(value: str) -> int:
    return len(value)

# Time expiration, expires the same day
# If the time does not contain timezone it defaults to UTC
@alru_cache(expiration="11:05:00Z", expired_items_auto_removal_period="10 minutes")
async def get_data(value: str) -> int:
    return len(value)

"""
Other possible datetime/date/time values:

# values in seconds
'1494012444.883309' -> datetime(2017, 5, 5, 19, 27, 24, 883_309, tzinfo=timezone.utc)
1_494_012_444.883_309 -> datetime(2017, 5, 5, 19, 27, 24, 883_309, tzinfo=timezone.utc)
'1494012444' -> datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)
b'1494012444' -> datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)
1_494_012_444 -> datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)

# values in ms
'1494012444000.883309' -> datetime(2017, 5, 5, 19, 27, 24, 883, tzinfo=timezone.utc)
'-1494012444000.883309' -> datetime(1922, 8, 29, 4, 32, 35, 999117, tzinfo=timezone.utc)
1_494_012_444_000 -> datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)
'2012-04-23T09:15:00' -> datetime(2012, 4, 23, 9, 15)
'2012-4-9 4:8:16' -> datetime(2012, 4, 9, 4, 8, 16)
'2012-04-23T09:15:00Z' -> datetime(2012, 4, 23, 9, 15, 0, 0, timezone.utc)
'2012-4-9 4:8:16-0320' -> datetime(2012, 4, 9, 4, 8, 16, 0, create_tz(-200))
'2012-04-23T10:20:30.400+02:30' -> datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(150))
'2012-04-23T10:20:30.400+02' -> datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(120))
'2012-04-23T10:20:30.400-02' -> datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(-120))
b'2012-04-23T10:20:30.400-02' -> datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(-120))
datetime(2017, 5, 5) -> datetime(2017, 5, 5)
0 -> datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

d1494012444.883309' -> date(2017, 5, 5)
d'1494012444.883309' -> date(2017, 5, 5)
d_494_012_444.883_309 -> date(2017, 5, 5)
d1494012444' -> date(2017, 5, 5)
d_494_012_444 -> date(2017, 5, 5)
d -> date(1970, 1, 1)
d2012-04-23' -> date(2012, 4, 23)
d'2012-04-23' -> date(2012, 4, 23)
d2012-4-9' -> date(2012, 4, 9)
date(2012, 4, 9) -> date(2012, 4, 9)

'09:15:00' -> time(9, 15)
'10:10' -> time(10, 10)
'10:20:30.400' -> time(10, 20, 30, 400_000)
b'10:20:30.400' -> time(10, 20, 30, 400_000)
'4:8:16' -> time(4, 8, 16)
time(4, 8, 16) -> time(4, 8, 16)
3610 -> time(1, 0, 10)
3600.5 -> time(1, 0, 0, 500000)
86400 - 1 -> time(23, 59, 59)
'11:05:00-05:30' -> time(11, 5, 0, tzinfo=create_tz(-330))
'11:05:00-0530' -> time(11, 5, 0, tzinfo=create_tz(-330))
'11:05:00Z' -> time(11, 5, 0, tzinfo=timezone.utc)
'11:05:00+00' -> time(11, 5, 0, tzinfo=timezone.utc)
'11:05-06' -> time(11, 5, 0, tzinfo=create_tz(-360))
'11:05+06' -> time(11, 5, 0, tzinfo=create_tz(360))
"""
```

#### Duration/Timedelta Expiration

```python
from aquiche import alru_cache
from datetime import timedelta

# Refreshes a value every 10 minutes
@alru_cache(expiration=timedelta(minutes=10))
async def cache_function(value: str) -> int:
    return len(value)

# Refreshes a value every 1 day, 42 minutes and 6 seconds
# This can be shortened to `1d42m6s`
@alru_cache(expiration="1 day 42 minutes 6 seconds")
async def cache_function(value: str) -> int:
    return len(value)

# Refreshes a value every minute
@alru_cache(expiration="1m")
async def cache_function(value: str) -> int:
    return len(value)

"""
Other possible refresh/timedelta values

timedelta(seconds=30), timedelta(seconds=30)
'30' -> timedelta(seconds=30)
30 -> timedelta(seconds=30)
30.1 -> timedelta(seconds=30, milliseconds=100)
9.9e-05 -> timedelta(microseconds=99)

minutes seconds
'15:30' -> timedelta(minutes=15, seconds=30)
'5:30' -> timedelta(minutes=5, seconds=30)

hours minutes seconds
'10:15:30' -> timedelta(hours=10, minutes=15, seconds=30)
'1:15:30' -> timedelta(hours=1, minutes=15, seconds=30)
'100:200:300' -> timedelta(hours=100, minutes=200, seconds=300)

days
'4 15:30' -> timedelta(days=4, minutes=15, seconds=30)
'4 10:15:30' -> timedelta(days=4, hours=10, minutes=15, seconds=30)

fractions of seconds
'15:30.1' -> timedelta(minutes=15, seconds=30, milliseconds=100)
'15:30.01' -> timedelta(minutes=15, seconds=30, milliseconds=10)
'15:30.001' -> timedelta(minutes=15, seconds=30, milliseconds=1)
'15:30.0001' -> timedelta(minutes=15, seconds=30, microseconds=100)
'15:30.00001' -> timedelta(minutes=15, seconds=30, microseconds=10)
'15:30.000001' -> timedelta(minutes=15, seconds=30, microseconds=1)
b'15:30.000001' -> timedelta(minutes=15, seconds=30, microseconds=1)

iso_8601
'P4Y', errors.DurationError
'P4M', errors.DurationError
'P4W', errors.DurationError
'P4D' -> timedelta(days=4)
'P0.5D' -> timedelta(hours=12)
'PT5H' -> timedelta(hours=5)
'PT5M' -> timedelta(minutes=5)
'PT5S' -> timedelta(seconds=5)
'PT0.000005S' -> timedelta(microseconds=5)
b'PT0.000005S' -> timedelta(microseconds=5)


'1m 10h 10s' -> timedelta(seconds=36070)
'1minute 10hour 10second' -> timedelta(seconds=36070)
'1minutes 10hours 10seconds' -> timedelta(seconds=36070)
'1 minutes 2 days 10 hours 10 seconds' -> timedelta(seconds=208870)
'1 m 2 d 10 h 10 s' -> '1 minute 2 days 10 hours 10 seconds' -> timedelta(seconds=208870)
'1 minute 2 day 10 hours 10 seconds' -> timedelta(seconds=208870)
"""
```

#### Data Pointer Expiration

The pointer can point to any expiration value. The data pointer can resolve either dictionaries or objects. If you need more flexibility or you are facing issues with the pointer it is recommended to use function/coroutine expiration. That way you ensure you are returning the right value.

```python
from aquiche import alru_cache
from typing import Any

@alru_cache(expiration="$.token.expiration")
async def cache_function(value: str) -> Any:
    expiry = True

    if value == "user":
        expiry = "30 minutes"
    if value == "service":
        expiry = "12 hours"
    if value == "role":
        expiry = "2022-01-30T00:00:00+0000"

    return {"token": {"expiration": expiry}}
```

#### Function and Coroutine Expiration

It is possible to use both functions and coroutines to set the expiration. If the decorated function is a sync one then it is possible to only use the sync function to set the expiration. `CachedItem` object is passed to the expiration function.

```python
from aquiche import alru_cache, CachedItem

"""
@dataclass
class CachedItem:
    value: Any -> the returned value or the raised exception*
    last_fetched: datetime -> when was the value last fetched
    is_error: bool -> set to true if the value contains a raised exception*

*Only possible when the negative cache is enabled
"""

async def is_item_expired_async(item: CachedItem) -> bool:
    return item.value > 1

def is_item_expired(item: CachedItem) -> str | datetime:
    if item.value > 10:
        return "10s"
    if item.value > 5:
        return datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)
    return "1 day"

# Async function/Coroutine
# It can be used with both sync and async function expirations
@alru_cache(expiration=is_item_expired_async)
async def cache_function_async_1(value: str) -> int:
    return len(value)

@alru_cache(expiration=is_item_expired)
async def cache_function_async_2(value: str) -> int:
    return len(value)

# Sync function can only be used with sync function expiration
@alru_cache(expiration=is_item_expired)
def cache_function_sync(value: str) -> int:
    return len(value)

# Using this function would throw an error
# @alru_cache(expiration=is_item_expired_async)
# def cache_function_sync(value: str) -> int:
#     return len(value)
```

### "Wrapping" the Async Clients in AsyncExitStack

The param `wrap_async_exit_stack` simplifies caching of the async clients. When the function is called the returned client(s) (or any other value for the matter) enters the async context. When the client expires or the cache is cleared, the context is automatically cleaned up and renewed if needed. It is recommended to manually clear the cache on the shutdown so the context is closed.

```python
import httpx
from database_client import DatabaseClient
from aquiche import alru_cache
from typing import Any

@alru_cache(wrap_async_exit_stack=True)
async def get_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url="https://api.data.io")

async def main() -> None:
    client = await get_client()
    # Perform the needed actions
    await client.post("/raspberry_pi")
    # Once done clear the cache
    await get_client.clear_cache()

# In case of returning multiple clients a list of data pointers can be used to wrap them
# If the data pointer does not point to any value from the result then the error is thrown
# To prevent the error from happening you can append suffix :ignore_missing to the data pointer
@alru_cache(wrap_async_exit_stack=["$.clients.database:ignore_missing", "$.clients.http"])
def get_clients() -> Any:
    return {
        "token": "p@ss123",
        "clients": {
            # Both clients will use the same AsyncExitStack context
            "http": httpx.AsyncClient(),
            "database": DatabaseClient()
        }
    }
```
