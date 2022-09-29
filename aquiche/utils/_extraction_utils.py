from dataclasses import dataclass
import functools
from typing import Any, Dict

from aquiche import errors


@dataclass
class MissingValue:
    default_value: Any = None


def __rgetattr(obj: Any, path: str, default: Any = None):
    attrs = path.split(".")
    try:
        return functools.reduce(getattr, attrs, obj)
    except AttributeError:
        return default


def __deep_get(dictionary: Dict, keys: str, default: Any = None) -> Any:
    return functools.reduce(
        lambda d, key: d.get(key, default) if isinstance(d, dict) else MissingValue(default_value=default),
        keys.split("."),
        dictionary,
    )


def extract_from_obj(
    obj: Any, attribute_path: str, check_attribute_exists: bool = True, default_value: Any = None
) -> Any:
    value = MissingValue(default_value)
    if isinstance(obj, dict):
        value = __deep_get(dictionary=obj, keys=attribute_path, default=MissingValue(default_value))
    else:
        value = __rgetattr(obj=obj, path=attribute_path, default=MissingValue(default_value))
    if isinstance(value, MissingValue):
        if check_attribute_exists:
            raise errors.ExtractionError(attribute_path)
        return value.default_value

    return value
