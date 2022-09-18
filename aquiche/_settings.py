from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Union

from .backends.interface import Backend
from .backends.memory import Memory
from .disable_control import ControlMixin


class BackendInvalidConfig(Exception):
    pass


class BackendType(str, Enum):
    MEMORY = "memory"


@dataclass
class MemoryBackendConfig:
    size: int = 1000
    check_interval_seconds: Union[int, float] = 1


BackendConfig = MemoryBackendConfig


@dataclass
class CacheSettings:
    type: BackendType = BackendType.MEMORY
    enabled: bool = True
    backend_config: BackendConfig = field(default_factory=lambda: MemoryBackendConfig())


def get_backend_from_settings(settings: CacheSettings) -> Any:
    if settings.type == BackendType.MEMORY:
        if not isinstance(settings.backend_config, MemoryBackendConfig):
            raise BackendInvalidConfig("Invalid memory backend configuration")

        class _MemoryBackendWithMixins(ControlMixin, Memory):
            pass

        return _MemoryBackendWithMixins(enabled=settings.enabled, **asdict(settings.backend_config))

    raise BackendInvalidConfig(f"Unsupported backend type: {settings.type}")
