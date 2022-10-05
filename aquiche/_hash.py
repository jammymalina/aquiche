from typing import Callable, Tuple


class HashedSeq(list):
    hash_value: str

    def __init__(self, tup: Tuple, hash: Callable = hash):
        self[:] = tup
        self.hash_value = hash(tup)

    def __hash__(self):
        return self.hash_value
