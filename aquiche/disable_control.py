from contextvars import ContextVar
from typing import Any

_ALL = "_"


async def _is_disabled_middleware(call, *args, backend=None, cmd=None, **kwargs):
    if backend.is_disabled(cmd, _ALL):
        if cmd == "get":
            return kwargs.get("default")
        return None
    return await call(*args, **kwargs)


class ControlMixin:
    def __init__(self, *args, enabled: bool = True, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__disable = ContextVar(str(id(self)), default=())
        self._set_disabled(not enabled)

    @property
    def _disable(self):
        return list(self.__disable.get(()))

    def _set_disabled(self, value):
        if value is True:
            value = [
                _ALL,
            ]
        elif value is False:
            value = []
        self.__disable.set(tuple(value))

    def is_disabled(self, *cmds: str) -> bool:
        _disable = self._disable
        if not cmds and _disable:
            return True
        for cmd in cmds:
            if cmd.lower() in [c.lower() for c in _disable]:
                return True
        return False

    def is_enabled(self, *cmds) -> bool:
        return not self.is_disabled(*cmds)

    def disable(self, *cmds: str) -> None:
        _disable = self._disable
        if not cmds:
            _disable = [
                _ALL,
            ]
        if self._disable is False:
            _disable = []
        _disable.extend(cmds)
        self._set_disabled(_disable)

    def enable(self, *cmds: str):
        _disable = self._disable
        if not cmds:
            _disable = []
        for cmd in cmds:
            if cmd in _disable:
                _disable.remove(cmd)
        self._set_disabled(_disable)
