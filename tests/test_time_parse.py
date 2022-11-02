from datetime import date, datetime, time, timedelta, timezone
import inspect
from typing import Any

import pytest

from aquiche import errors
from aquiche.utils._time_parse import parse_date, parse_datetime, parse_duration, parse_time


def create_tz(minutes):
    return timezone(timedelta(minutes=minutes))


@pytest.mark.parametrize(
    "value,result",
    [
        # Valid inputs
        ("1494012444.883309", date(2017, 5, 5)),
        (b"1494012444.883309", date(2017, 5, 5)),
        (1_494_012_444.883_309, date(2017, 5, 5)),
        ("1494012444", date(2017, 5, 5)),
        (1_494_012_444, date(2017, 5, 5)),
        (0, date(1970, 1, 1)),
        ("2012-04-23", date(2012, 4, 23)),
        (b"2012-04-23", date(2012, 4, 23)),
        ("2012-4-9", date(2012, 4, 9)),
        (date(2012, 4, 9), date(2012, 4, 9)),
        (datetime(2012, 4, 9, 12, 15), date(2012, 4, 9)),
        # Invalid inputs
        ("x20120423", errors.DateError),
        ("2012-04-56", errors.DateError),
        (19_999_999_999, date(2603, 10, 11)),  # just before watershed
        (20_000_000_001, date(1970, 8, 20)),  # just after watershed
        (1_549_316_052, date(2019, 2, 4)),  # time in s
        (1_549_316_052_104, date(2019, 2, 4)),  # time in ms
        (1_549_316_052_104_324, date(2019, 2, 4)),  # time in μs
        (1_549_316_052_104_324_096, date(2019, 2, 4)),  # time in ns
        ("infinity", date(9999, 12, 31)),
        ("inf", date(9999, 12, 31)),
        (float("inf"), date(9999, 12, 31)),
        ("infinity ", date(9999, 12, 31)),
        (int("1" + "0" * 100), date(9999, 12, 31)),
        (1e1000, date(9999, 12, 31)),
        ("-infinity", date(1, 1, 1)),
        ("-inf", date(1, 1, 1)),
        ("nan", ValueError),
    ],
)
def test_date_parsing(value: Any, result: Any) -> None:
    """It should parse or not parse dates"""
    if type(result) == type and issubclass(result, Exception):
        with pytest.raises(result):
            parse_date(value)
    else:
        assert parse_date(value) == result


@pytest.mark.parametrize(
    "value,result",
    [
        # Valid inputs
        ("09:15:00", time(9, 15)),
        ("10:10", time(10, 10)),
        ("10:20:30.400", time(10, 20, 30, 400_000)),
        (b"10:20:30.400", time(10, 20, 30, 400_000)),
        ("4:8:16", time(4, 8, 16)),
        (time(4, 8, 16), time(4, 8, 16)),
        (3610, time(1, 0, 10)),
        (3600.5, time(1, 0, 0, 500000)),
        (86400 - 1, time(23, 59, 59)),
        ("11:05:00-05:30", time(11, 5, 0, tzinfo=create_tz(-330))),
        ("11:05:00-0530", time(11, 5, 0, tzinfo=create_tz(-330))),
        ("11:05:00Z", time(11, 5, 0, tzinfo=timezone.utc)),
        ("11:05:00+00", time(11, 5, 0, tzinfo=timezone.utc)),
        ("11:05-06", time(11, 5, 0, tzinfo=create_tz(-360))),
        ("11:05+06", time(11, 5, 0, tzinfo=create_tz(360))),
        # Invalid inputs
        (86400, errors.TimeError),
        ("xxx", errors.TimeError),
        ("091500", errors.TimeError),
        (b"091500", errors.TimeError),
        ("09:15:90", errors.TimeError),
        ("11:05:00Y", errors.TimeError),
        ("11:05:00-25:00", errors.TimeError),
    ],
)
def test_time_parsing(value: Any, result: Any) -> None:
    """It should parse or not parse time"""
    if inspect.isclass(result) and issubclass(result, Exception):
        with pytest.raises(result):
            parse_time(value)
    else:
        assert parse_time(value) == result


@pytest.mark.parametrize(
    "value,result",
    [
        # Valid inputs
        # values in seconds
        ("1494012444.883309", datetime(2017, 5, 5, 19, 27, 24, 883_309, tzinfo=timezone.utc)),
        (1_494_012_444.883_309, datetime(2017, 5, 5, 19, 27, 24, 883_309, tzinfo=timezone.utc)),
        ("1494012444", datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)),
        (b"1494012444", datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)),
        (1_494_012_444, datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)),
        # values in ms
        ("1494012444000.883309", datetime(2017, 5, 5, 19, 27, 24, 883, tzinfo=timezone.utc)),
        ("-1494012444000.883309", datetime(1922, 8, 29, 4, 32, 35, 999117, tzinfo=timezone.utc)),
        (1_494_012_444_000, datetime(2017, 5, 5, 19, 27, 24, tzinfo=timezone.utc)),
        ("2012-04-23T09:15:00", datetime(2012, 4, 23, 9, 15)),
        ("2012-4-9 4:8:16", datetime(2012, 4, 9, 4, 8, 16)),
        ("2012-04-23T09:15:00Z", datetime(2012, 4, 23, 9, 15, 0, 0, timezone.utc)),
        ("2012-4-9 4:8:16-0320", datetime(2012, 4, 9, 4, 8, 16, 0, create_tz(-200))),
        ("2012-04-23T10:20:30.400+02:30", datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(150))),
        ("2012-04-23T10:20:30.400+02", datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(120))),
        ("2012-04-23T10:20:30.400-02", datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(-120))),
        (b"2012-04-23T10:20:30.400-02", datetime(2012, 4, 23, 10, 20, 30, 400_000, create_tz(-120))),
        (datetime(2017, 5, 5), datetime(2017, 5, 5)),
        (0, datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
        # Invalid inputs
        ("x20120423091500", errors.DateTimeError),
        ("2012-04-56T09:15:90", errors.DateTimeError),
        ("2012-04-23T11:05:00-25:00", errors.DateTimeError),
        (19_999_999_999, datetime(2603, 10, 11, 11, 33, 19, tzinfo=timezone.utc)),  # just before watershed
        (20_000_000_001, datetime(1970, 8, 20, 11, 33, 20, 1000, tzinfo=timezone.utc)),  # just after watershed
        (1_549_316_052, datetime(2019, 2, 4, 21, 34, 12, 0, tzinfo=timezone.utc)),  # time in s
        (1_549_316_052_104, datetime(2019, 2, 4, 21, 34, 12, 104_000, tzinfo=timezone.utc)),  # time in ms
        (1_549_316_052_104_324, datetime(2019, 2, 4, 21, 34, 12, 104_324, tzinfo=timezone.utc)),  # time in μs
        (1_549_316_052_104_324_096, datetime(2019, 2, 4, 21, 34, 12, 104_324, tzinfo=timezone.utc)),  # time in ns
        ("infinity", datetime(9999, 12, 31, 23, 59, 59, 999999)),
        ("inf", datetime(9999, 12, 31, 23, 59, 59, 999999)),
        ("inf ", datetime(9999, 12, 31, 23, 59, 59, 999999)),
        (1e50, datetime(9999, 12, 31, 23, 59, 59, 999999)),
        (float("inf"), datetime(9999, 12, 31, 23, 59, 59, 999999)),
        ("-infinity", datetime(1, 1, 1, 0, 0)),
        ("-inf", datetime(1, 1, 1, 0, 0)),
        ("nan", ValueError),
    ],
)
def test_datetime_parsing(value: Any, result: Any) -> None:
    """It should parse or not parse datetime"""
    if inspect.isclass(result) and issubclass(result, Exception):
        with pytest.raises(result):
            parse_datetime(value)
    else:
        assert parse_datetime(value) == result


@pytest.mark.parametrize(
    "delta",
    [
        timedelta(days=4, minutes=15, seconds=30, milliseconds=100),  # fractions of seconds
        timedelta(hours=10, minutes=15, seconds=30),  # hours, minutes, seconds
        timedelta(days=4, minutes=15, seconds=30),  # multiple days
        timedelta(days=1, minutes=00, seconds=00),  # single day
        timedelta(days=-4, minutes=15, seconds=30),  # negative durations
        timedelta(minutes=15, seconds=30),  # minute & seconds
        timedelta(seconds=30),  # seconds
    ],
)
def test_parse_python_format(delta: timedelta) -> Any:
    """It should parse native python duration"""
    assert parse_duration(delta) == delta
    assert parse_duration(str(delta)) == delta


@pytest.mark.parametrize(
    "value,result",
    [
        # seconds
        (timedelta(seconds=30), timedelta(seconds=30)),
        ("30", timedelta(seconds=30)),
        (30, timedelta(seconds=30)),
        (30.1, timedelta(seconds=30, milliseconds=100)),
        (9.9e-05, timedelta(microseconds=99)),
        # minutes seconds
        ("15:30", timedelta(minutes=15, seconds=30)),
        ("5:30", timedelta(minutes=5, seconds=30)),
        # hours minutes seconds
        ("10:15:30", timedelta(hours=10, minutes=15, seconds=30)),
        ("1:15:30", timedelta(hours=1, minutes=15, seconds=30)),
        ("100:200:300", timedelta(hours=100, minutes=200, seconds=300)),
        # days
        ("4 15:30", timedelta(days=4, minutes=15, seconds=30)),
        ("4 10:15:30", timedelta(days=4, hours=10, minutes=15, seconds=30)),
        # fractions of seconds
        ("15:30.1", timedelta(minutes=15, seconds=30, milliseconds=100)),
        ("15:30.01", timedelta(minutes=15, seconds=30, milliseconds=10)),
        ("15:30.001", timedelta(minutes=15, seconds=30, milliseconds=1)),
        ("15:30.0001", timedelta(minutes=15, seconds=30, microseconds=100)),
        ("15:30.00001", timedelta(minutes=15, seconds=30, microseconds=10)),
        ("15:30.000001", timedelta(minutes=15, seconds=30, microseconds=1)),
        (b"15:30.000001", timedelta(minutes=15, seconds=30, microseconds=1)),
        # negative
        ("-4 15:30", timedelta(days=-4, minutes=15, seconds=30)),
        ("-172800", timedelta(days=-2)),
        ("-15:30", timedelta(minutes=-15, seconds=30)),
        ("-1:15:30", timedelta(hours=-1, minutes=15, seconds=30)),
        ("-30.1", timedelta(seconds=-30, milliseconds=-100)),
        # iso_8601
        ("P4Y", errors.DurationError),
        ("P4M", errors.DurationError),
        ("P4W", errors.DurationError),
        ("P4D", timedelta(days=4)),
        ("P0.5D", timedelta(hours=12)),
        ("PT5H", timedelta(hours=5)),
        ("PT5M", timedelta(minutes=5)),
        ("PT5S", timedelta(seconds=5)),
        ("PT0.000005S", timedelta(microseconds=5)),
        (b"PT0.000005S", timedelta(microseconds=5)),
        # custom
        ("1m 10h 10s", timedelta(seconds=36070)),
        ("1minute 10hour 10second", timedelta(seconds=36070)),
        ("1minutes 10hours 10seconds", timedelta(seconds=36070)),
        ("1 minutes 2 days 10 hours 10 seconds", timedelta(seconds=208870)),
        ("1 m 2 d 10 h 10 s", timedelta(seconds=208870)),
        ("1 minute 2 day 10 hours 10 seconds", timedelta(seconds=208870)),
    ],
)
def test_parse_durations(value: Any, result: Any) -> None:
    """It should parse or not parse durations"""
    if inspect.isclass(result) and issubclass(result, Exception):
        with pytest.raises(result):
            parse_duration(value)
    else:
        assert parse_duration(value) == result
