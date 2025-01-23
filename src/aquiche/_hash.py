from enum import Enum
from inspect import signature as get_signature
from typing import Any, Callable, Hashable, Optional, Tuple, Union


class Key(Enum):
    SINGLE_KEY = 1


KeyType = Union[str, Key]


class HashedSeq(list):
    hash_value: str

    def __init__(self, tup: Tuple, hash_fn: Callable = hash):
        self[:] = tup
        self.hash_value = hash_fn(tup)

    def __hash__(self):
        return self.hash_value


def __make_key_from_args(args: Any, kwargs: Any, kwd_mark=(object(),)) -> Hashable:
    # All of code below relies on kwargs preserving the order input by the user.
    # Formerly, we sorted() the kwargs before looping.  The new way is *much*
    # faster; however, it means that f(x=1, y=2) will now be treated as a
    # distinct call from f(y=2, x=1) which will be cached separately.
    fast_types = ({int, str},)

    key = args
    if kwargs:
        key += kwd_mark
        for item in kwargs.items():
            key += item

    key += tuple(type(v) for v in args)
    if kwargs:
        key += tuple(type(v) for v in kwargs.values())
    elif len(key) == 1 and type(key[0]) in fast_types:
        return key[0]
    return hash(HashedSeq(key))


def __default_key_resolve(*args: Any, **kwargs: Any) -> Hashable:
    return __make_key_from_args(args, kwargs)


def __single_key_resolve(*_args, **_kwargs) -> Hashable:
    return "default_key"


def __get_template_key_resolver(key: str, user_function: Callable) -> Callable[..., Hashable]:
    signature = get_signature(user_function)

    def template_key_resolve(*args, **kwargs) -> str:
        bound_args = signature.bind(*args, **kwargs)
        bound_args.apply_defaults()

        all_kwargs = bound_args.arguments
        all_kwargs.pop("args", [])
        all_kwargs.update(all_kwargs.pop("kwargs", {}))

        return key.format(*args, **all_kwargs)

    return template_key_resolve


def get_key_resolver(key: Optional[KeyType], user_function: Callable) -> Callable[..., Hashable]:
    if isinstance(key, Key):
        return __single_key_resolve

    if isinstance(key, str):
        template_key_resolve = __get_template_key_resolver(key, user_function)
        return template_key_resolve

    return __default_key_resolve
