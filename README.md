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
- Optional negative cache
- Focus on high performance
- Contains typings
- High test coverage

## Guide

The decorator is very similar to [`functools.lru_cache`](https://docs.python.org/3/library/functools.html#functools.lru_cache). It can simply be used without any options, both `@alru_cache` and `@alru_cache()` are supported.

### Enable/Disable

Cache can be enabled or disabled. **It is checked actively during the runtime!** You cannot update the value once the function is wrapped.

```python
@alru_cache(enabled=True|False)
def cache_function(value: str) -> int:
    return len(value)
```

### Maxsize

The maxsize param behaves the same as in `functools.lru_cache`. If set to `None` the cache can grow without bound. If set the memoizing decorator saves up to the maxsize most recent calls. It's set to `None` by default.

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

It is possible to set when the function call expires. Possible expiration options:

- `None`, the function call never expires
- `True|False`, if set to `True` the value is expired
- `int|float`, based on the value it will either be treated as the `timedelta` or unix timestamp
- `datetime`, the expiration date, if it contains no timezone the UTC timezone automatically added
- `time`, the function call will expire today at this time
- `timedelta`, refresh interval, the function will refresh each `timedelta` period
- `datetime|time|timedelta` string, the string that can be parsed to `datetime|time|timedelta`, supported formats: ISO 8601, human readable formats, uses the same (or nearly the same) resolution as [pydantic](https://pydantic-docs.helpmanual.io)
- Data pointer string e.g. `$.response.data.expiration_data`, the pointer can point to any of the other expiration values (including the pointer itself)
- Function, the `CachedValue` object (for more information see the example below) will be passed as an argument to the function. The error will be thrown if you try to use this option while decorating a "sync" function. It is possible to return any of the other expiration options
- Async function/coroutine, the `CachedValue` object (for more information see the example below) will be passed as an argument to the function. It can only be used when decorating an async function/coroutine. It is possible to return any of the other expiration options

It is set to `None` by default.
