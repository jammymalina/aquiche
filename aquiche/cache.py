from enum import Enum


class CacheType(str, Enum):
    LRU = "lru"
