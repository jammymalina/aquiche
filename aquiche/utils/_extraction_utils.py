from dataclasses import dataclass
import functools
from typing import Any, Dict

from aquiche import errors


@dataclass
class MissingValue:
    default_value: Any = None


def __rgetattr(obj: Any, path: str, default: Any = None) -> Any:
    attrs = path.split(".")
    try:
        return functools.reduce(getattr, attrs, obj)
    except AttributeError:
        return default


def __rsetattr(obj: Any, path: str, value: Any) -> None:
    current_attr, _, leftover_path = path.rpartition(".")
    return setattr(__rgetattr(obj=obj, path=current_attr) if current_attr else obj, leftover_path, value)


def __deep_get(dictionary: Dict, key_path: str, default: Any = None) -> Any:
    return functools.reduce(
        lambda d, key: d.get(key, default) if isinstance(d, dict) else MissingValue(default_value=default),
        key_path.split("."),
        dictionary,
    )


def __deep_set(dictionary: Dict, key_path: str, value: Any) -> None:
    dict_iter = dictionary
    key_iter = key_path.split(".")
    for key in key_iter[:-1]:
        dict_iter = dict_iter.setdefault(key, {})
    dict_iter[key_iter[-1]] = value


def extract_from_obj(
    obj: Any, attribute_path: str, check_attribute_exists: bool = True, default_value: Any = None
) -> Any:
    if not isinstance(attribute_path, str):
        raise errors.ExtractionError(attribute_path)
    attribute_path = attribute_path.strip().lstrip("$.")
    value = MissingValue(default_value)
    if isinstance(obj, dict):
        value = __deep_get(dictionary=obj, key_path=attribute_path, default=MissingValue(default_value))
    else:
        value = __rgetattr(obj=obj, path=attribute_path, default=MissingValue(default_value))
    if isinstance(value, MissingValue):
        if check_attribute_exists:
            raise errors.ExtractionError(attribute_path)
        return value.default_value

    return value


def set_value_obj(obj: Any, attribute_path: str, value: Any) -> None:
    if not isinstance(attribute_path, str):
        raise errors.ExtractionError(attribute_path)
    attribute_path = attribute_path.strip().lstrip("$.")
    if isinstance(obj, dict):
        return __deep_set(dictionary=obj, key_path=attribute_path, value=value)
    return __rsetattr(obj=obj, path=attribute_path, value=value)
