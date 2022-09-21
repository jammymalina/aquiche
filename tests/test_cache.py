from aquiche.core._cache import CachedValue


def test_default_ceched_value():
    """It should create default CachedValue item"""
    cached_value = CachedValue()

    assert cached_value.value is None
    assert cached_value.inflight is None
    assert cached_value.last_fetched is None
