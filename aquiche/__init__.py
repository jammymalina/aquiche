from .backends.interface import LockedException
from .decorators import context_cache_detect
from .wrapper import Cache

cache_detect = context_cache_detect

cache = Cache(name="default")
