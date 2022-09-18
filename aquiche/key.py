import inspect
from datetime import timedelta
from functools import lru_cache
from typing import Any, Callable, Container, Dict, Optional, Tuple, Union

from ._typing import TTL
from .formatter import _ReplaceFormatter, default_formatter, template_to_pattern

_KWARGS = "__kwargs__"
_ARGS = "__args__"
_ARGS_KWARGS = (_ARGS, _KWARGS)


class WrongKeyException(ValueError):
    """Raised If key template have wrong parameter"""


def ttl_to_seconds(ttl: Union[int, float, TTL, None]) -> Optional[int]:
    timeout = ttl() if callable(ttl) else ttl
    if isinstance(timeout, float):
        return int(timeout)
    if isinstance(timeout, timedelta):
        return int(timeout.total_seconds())
    if isinstance(timeout, str):
        return _ttl_from_str(timeout)
    return timeout


_STR_TO_DELTA = {
    "h": timedelta(hours=1),
    "m": timedelta(minutes=1),
    "s": timedelta(seconds=1),
    "d": timedelta(days=1),
}


def _ttl_from_str(ttl: str) -> int:
    result = 0
    mul = ""
    for char in ttl.strip().lower():
        if char.isdigit():
            mul += char
        elif char in _STR_TO_DELTA:
            result += int(mul) * int(_STR_TO_DELTA[char].total_seconds())
            mul = ""
        else:
            raise ValueError(f"ttl '{ttl}' has wrong string representation")
    if mul != "" and not result:
        return int(mul)
    return result


def get_cache_key(
    func: Callable,
    template: Optional[str] = None,
    args: Tuple = (),
    kwargs: Optional[Dict] = None,
) -> str:
    kwargs = kwargs or {}
    key_values = _get_call_values(func, args, kwargs)
    _key_template = template or get_cache_key_template(func)
    return template_to_pattern(_key_template, _formatter=default_formatter, **key_values)


def get_func_params(func):
    signature = _get_func_signature(func)
    for param_name, param in signature.parameters.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            yield _KWARGS
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            yield _ARGS
        else:
            yield param_name


def get_cache_key_template(
    func: Callable, key: Optional[str] = None, prefix: str = "", exclude_parameters: Container = ()
) -> str:
    if key is None:
        key = generate_key_template(func, exclude_parameters)
    else:
        _check_key_params(key, list(get_func_params(func)))
    if prefix:
        key = f"{prefix}:{key}"
    return key


def generate_key_template(func: Callable, exclude_parameters: Container = ()):
    func_params = list(get_func_params(func))
    key_template = f"{func.__module__}:{func.__name__}"
    if func_params and func_params[0] == "self":
        key_template = f"{func.__module__}:{func.__qualname__}"
    for param_name in func_params:
        if param_name in exclude_parameters:
            continue
        if param_name in _ARGS_KWARGS:
            key_template += f":{{{param_name}}}"
        else:
            key_template += f":{param_name}:{{{param_name}}}"
    return key_template


class _Star:
    def __getattr__(self, item):
        return _Star()

    def __getitem__(self, item):
        return _Star()


def _check_key_params(key, func_params):
    func_params = {param: _Star() for param in func_params}
    errors = []

    def _default(name):
        errors.append(name)
        return "*"

    check = _ReplaceFormatter(default=_default)
    check.format(key, **func_params)
    if errors:
        raise WrongKeyException(f"Wrong parameter placeholder '{errors}' in the key ")


def get_call_values(func: Callable, args, kwargs) -> Dict:
    key_values = {}
    for _key, _value in _get_call_values(func, args, kwargs).items():
        if _key not in _ARGS_KWARGS:
            key_values[_key] = _value
    return key_values


@lru_cache(maxsize=100)
def _get_func_signature(func):
    return inspect.signature(func)


def _get_call_values(func, args, kwargs):
    signature = _get_func_signature(func).bind(*args, **kwargs)
    signature.apply_defaults()
    result = {}
    for _name, _value in signature.arguments.items():
        parameter: inspect.Parameter = signature.signature.parameters[_name]
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            result[_KWARGS] = _value
            result.update(_value)
        elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
            result[_ARGS] = _value
        else:
            result[_name] = _value
    return result
