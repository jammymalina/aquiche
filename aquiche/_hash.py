from typing import Any, Callable, Tuple


class HashedSeq(list):
    hash_value: str

    def __init__(self, tup: Tuple, hash_fn: Callable = hash):
        self[:] = tup
        self.hash_value = hash_fn(tup)

    def __hash__(self):
        return self.hash_value


def __make_key_from_args(args: Any, kwds: Any, kwd_mark=(object(),)) -> Any:
    # All of code below relies on kwds preserving the order input by the user.
    # Formerly, we sorted() the kwds before looping.  The new way is *much*
    # faster; however, it means that f(x=1, y=2) will now be treated as a
    # distinct call from f(y=2, x=1) which will be cached separately.
    fast_types = ({int, str},)

    key = args
    if kwds:
        key += kwd_mark
        for item in kwds.items():
            key += item

    key += tuple(type(v) for v in args)
    if kwds:
        key += tuple(type(v) for v in kwds.values())
    elif len(key) == 1 and type(key[0]) in fast_types:
        return key[0]
    return HashedSeq(key)


def make_key(*args: Any, **kwargs: Any) -> Any:
    return __make_key_from_args(args, kwargs)