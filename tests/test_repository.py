from aquiche._repository import CacheRepository, LRUCacheRepository


def test_lru_cache_repository_add() -> None:
    "It should add all the values to the lru cache repository"
    cache_repo = LRUCacheRepository()

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)

    assert cache_repo.get("a") == 10
    assert cache_repo.get("b") == 20
    assert cache_repo.get("c") == 30
    assert cache_repo.get("d") == 40
    assert cache_repo.get("e") == 50


def test_lru_cache_repository_add_maxsize() -> None:
    "It should store only certain number of values in the lru cache repository"
    cache_repo = LRUCacheRepository(maxsize=2)

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)

    assert cache_repo.get("a") is None
    assert cache_repo.get("b") is None
    assert cache_repo.get("c") is None
    assert cache_repo.get("d") == 40
    assert cache_repo.get("e") == 50


def test_lru_cache_every(mocker: MockerFixture) -> None:
    """It should run the function on every key-value pair"""
    cache_repo = LRUCacheRepository(maxsize=2)

    cache_repo.add("a", 10)
    cache_repo.add("b", 20)
    cache_repo.add("c", 30)
    cache_repo.add("d", 40)
    cache_repo.add("e", 50)
