from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union, get_args

from aquiche._expiration import CacheExpirationValue, DurationExpirationValue
from aquiche._hash import KeyType
from aquiche.errors import InvalidCacheConfig


@dataclass
class CacheParameters:
    enabled: bool = False
    key: Optional[KeyType] = None
    maxsize: Optional[int] = None
    expiration: Optional[CacheExpirationValue] = None
    expired_items_auto_removal_period: Optional[DurationExpirationValue] = None
    async_context: Union[bool, str, List[str], None] = None
    negative_cache: bool = False
    negative_expiration: Optional[CacheExpirationValue] = None
    retry_count: int = 0
    backoff_in_seconds: Union[int, float] = 0


def __extract_type_names(types: Tuple[Any, ...]) -> str:
    return "|".join(map(lambda t: t.__name__, types))


def validate_cache_params(
    enabled: bool,
    key: Optional[KeyType],
    maxsize: Optional[int],
    expiration: Optional[CacheExpirationValue],
    expired_items_auto_removal_period: Optional[DurationExpirationValue],
    async_context: Union[bool, str, List[str], None],
    refresh_threshold: Optional[DurationExpirationValue],
    negative_cache: bool,
    negative_expiration: Optional[CacheExpirationValue],
    retry_count: int,
    backoff_in_seconds: Union[int, float],
) -> None:
    errors = []
    if not isinstance(enabled, bool):
        errors += ["enabled should be either bool or a callable function"]

    if not (key is None or isinstance(key, get_args(KeyType))):
        errors += [
            f"key should be either None or one of these types: {__extract_type_names(get_args(CacheExpirationValue))}"
        ]

    if not (maxsize is None or isinstance(maxsize, int)):
        errors += ["maxsize should be int or None"]

    if not (expiration is None or isinstance(expiration, get_args(CacheExpirationValue))):
        errors += [
            f"expiration should be either None or one of these types: {__extract_type_names(get_args(CacheExpirationValue))}"
        ]

    if not (negative_expiration is None or isinstance(negative_expiration, get_args(CacheExpirationValue))):
        errors += [
            f"negative expiration should be either None or one of these types: {__extract_type_names(get_args(CacheExpirationValue))}"
        ]

    if not (
        expired_items_auto_removal_period is None
        or isinstance(expired_items_auto_removal_period, get_args(DurationExpirationValue))
    ):
        errors += [
            "expired_items_auto_removal_period should be either None or one of these types:"
            + __extract_type_names(get_args(DurationExpirationValue))
        ]

    if not (refresh_threshold is None or isinstance(refresh_threshold, get_args(DurationExpirationValue))):
        errors += [
            "refresh_threshold should be either None or one of these types:"
            + __extract_type_names(get_args(DurationExpirationValue))
        ]

    if not (
        async_context is None
        or isinstance(async_context, bool)
        or async_context == "*"
        or (isinstance(async_context, list) and all((isinstance(wrapper, str) for wrapper in async_context)))
    ):
        errors += ["async_context should be either None, bool, '*', list[str]"]

    if not isinstance(negative_cache, bool):
        errors += ["negative_cache should be bool"]

    if not isinstance(retry_count, int):
        errors += ["retry_count should be an integer"]

    if not isinstance(backoff_in_seconds, (int, float)):
        errors += ["backoff_in_seconds should be a number"]

    if errors:
        raise InvalidCacheConfig(errors)
