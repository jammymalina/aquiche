# Stolen from pydantic who stole it from django

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, Optional, Type, Union

from aquiche import errors
from aquiche.utils._sum_expression_parser import SumExpressionParser, SumExpressionParserConfig

DATE_EXPR = r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
TIME_EXPR = (
    r"(?P<hour>\d{1,2}):(?P<minute>\d{1,2})"
    r"(?::(?P<second>\d{1,2})(?:\.(?P<microsecond>\d{1,6})\d{0,6})?)?"
    r"(?P<tzinfo>Z|[+-]\d{2}(?::?\d{2})?)?$"
)

date_re = re.compile(f"{DATE_EXPR}$")
time_re = re.compile(TIME_EXPR)
datetime_re = re.compile(f"{DATE_EXPR}[T ]{TIME_EXPR}")

standard_duration_re = re.compile(
    r"^"
    r"(?:(?P<days>-?\d+) (days?, )?)?"
    r"((?:(?P<hours>-?\d+):)(?=\d+:\d+))?"
    r"(?:(?P<minutes>-?\d+):)?"
    r"(?P<seconds>-?\d+)"
    r"(?:\.(?P<microseconds>\d{1,6})\d{0,6})?"
    r"$"
)

# Support the sections of ISO 8601 date representation that are accepted by timedelta
iso8601_duration_re = re.compile(
    r"^(?P<sign>[-+]?)"
    r"P"
    r"(?:(?P<days>\d+(.\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+(.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(.\d+)?)S)?"
    r")?"
    r"$"
)

custom_duration_parser = SumExpressionParser(
    SumExpressionParserConfig(
        case_sensitive=False,
        value_mapping={
            "second": 1,
            "seconds": 1,
            "s": 1,
            "minute": 60,
            "minutes": 60,
            "m": 60,
            "hour": 3600,
            "hours": 3600,
            "h": 3600,
            "day": 24 * 3600,
            "days": 24 * 3600,
            "d": 24 * 3600,
        },
    )
)

EPOCH = datetime(1970, 1, 1)
# if greater than this, the number is in ms, if less than or equal it's in seconds
# (in seconds this is 11th October 2603, in ms it's 20th August 1970)
MS_WATERSHED = int(2e10)
# slightly more than datetime.max in ns - (datetime.max - EPOCH).total_seconds() * 1e9
MAX_NUMBER = int(3e20)
StrBytesIntFloat = Union[str, bytes, int, float]


def get_numeric(value: StrBytesIntFloat, native_expected_type: str) -> Union[None, int, float]:
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)
    except ValueError:
        return None
    except TypeError as err:
        raise TypeError(f"invalid type; expected {native_expected_type}, string, bytes, int or float") from err


def from_unix_seconds(seconds: Union[int, float]) -> datetime:
    if seconds > MAX_NUMBER:
        return datetime.max
    if seconds < -MAX_NUMBER:
        return datetime.min

    while abs(seconds) > MS_WATERSHED:
        seconds /= 1000
    parsed_date = EPOCH + timedelta(seconds=seconds)
    return parsed_date.replace(tzinfo=timezone.utc)


def _parse_timezone(value: Optional[str], error: Type[Exception]) -> Union[None, int, timezone]:
    if value == "Z":
        return timezone.utc
    if value is not None:
        offset_mins = int(value[-2:]) if len(value) > 3 else 0
        offset = 60 * int(value[1:3]) + offset_mins
        if value[0] == "-":
            offset = -offset
        try:
            return timezone(timedelta(minutes=offset))
        except ValueError as err:
            raise error(value) from err
    return None


def parse_date(value: Union[date, StrBytesIntFloat]) -> date:
    if isinstance(value, date):
        if isinstance(value, datetime):
            return value.date()
        return value

    number = get_numeric(value, "date")
    if number is not None:
        return from_unix_seconds(number).date()

    if isinstance(value, bytes):
        value = value.decode()

    match = date_re.match(value)  # type: ignore
    if match is None:
        raise errors.DateError(value)

    date_params = {k: int(v) for k, v in match.groupdict().items()}

    try:
        return date(**date_params)
    except ValueError as err:
        raise errors.DateError(value) from err


def parse_time(value: Union[time, StrBytesIntFloat]) -> time:
    if isinstance(value, time):
        return value

    number = get_numeric(value, "time")
    if number is not None:
        if number >= 86400:
            # doesn't make sense since the time time loop back around to 0
            raise errors.TimeError(value)
        return (datetime.min + timedelta(seconds=number)).time()

    if isinstance(value, bytes):
        value = value.decode()

    match = time_re.match(value)  # type: ignore
    if match is None:
        raise errors.TimeError(value)

    parsed_params = match.groupdict()
    if parsed_params["microsecond"]:
        parsed_params["microsecond"] = parsed_params["microsecond"].ljust(6, "0")

    tzinfo = _parse_timezone(parsed_params.pop("tzinfo"), errors.TimeError)
    time_params: Dict[str, Union[None, int, timezone]] = {k: int(v) for k, v in parsed_params.items() if v is not None}
    time_params["tzinfo"] = tzinfo

    try:
        return time(**time_params)  # type: ignore
    except ValueError as err:
        raise errors.TimeError(value) from err


def parse_datetime(value: Union[datetime, StrBytesIntFloat]) -> datetime:
    if isinstance(value, datetime):
        return value

    number = get_numeric(value, "datetime")
    if number is not None:
        return from_unix_seconds(number)

    if isinstance(value, bytes):
        value = value.decode()

    match = datetime_re.match(value)  # type: ignore
    if match is None:
        raise errors.DateTimeError(value)

    parsed_params = match.groupdict()
    if parsed_params["microsecond"]:
        parsed_params["microsecond"] = parsed_params["microsecond"].ljust(6, "0")

    tzinfo = _parse_timezone(parsed_params.pop("tzinfo"), errors.DateTimeError)
    datetime_params: Dict[str, Union[None, int, timezone]] = {
        k: int(v) for k, v in parsed_params.items() if v is not None
    }
    datetime_params["tzinfo"] = tzinfo

    try:
        return datetime(**datetime_params)  # type: ignore
    except ValueError as err:
        raise errors.DateTimeError(value) from err


def parse_duration(value: StrBytesIntFloat) -> timedelta:
    if isinstance(value, timedelta):
        return value

    if isinstance(value, (int, float)):
        # below code requires a string
        value = f"{value:f}"
    elif isinstance(value, bytes):
        value = value.decode()

    try:
        match = standard_duration_re.match(value) or iso8601_duration_re.match(value)
    except TypeError as err:
        raise TypeError("invalid type; expected timedelta, string, bytes, int or float") from err

    if not match:
        return __parse_duration_custom(value)

    parsed_params = match.groupdict()
    sign = -1 if parsed_params.pop("sign", "+") == "-" else 1
    if parsed_params.get("microseconds"):
        parsed_params["microseconds"] = parsed_params["microseconds"].ljust(6, "0")

    if parsed_params.get("seconds") and parsed_params.get("microseconds") and parsed_params["seconds"].startswith("-"):
        parsed_params["microseconds"] = "-" + parsed_params["microseconds"]

    timedelta_params = {k: float(v) for k, v in parsed_params.items() if v is not None}

    return sign * timedelta(**timedelta_params)


def __parse_duration_custom(value: str) -> timedelta:
    try:
        duration_seconds = custom_duration_parser.parse(value)
        return timedelta(seconds=duration_seconds)
    except Exception as err:
        raise errors.DurationError(value) from err
