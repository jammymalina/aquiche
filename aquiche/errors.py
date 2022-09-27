from typing import Any, Callable, Tuple, Type

__all__ = (
    "DateError",
    "DateTimeError",
    "DurationError",
    "TimeError",
)


def cls_kwargs(cls: Type["AquicheErrorMixin"], ctx: Any) -> "AquicheErrorMixin":
    return cls(**ctx)


class AquicheErrorMixin:
    code: str
    msg_template: str

    def __init__(self, **ctx: Any) -> None:
        self.__dict__ = ctx

    def __str__(self) -> str:
        return self.msg_template.format(**self.__dict__)

    def __reduce__(self) -> Tuple[Callable[..., "AquicheErrorMixin"], Tuple[Type["AquicheErrorMixin"], Any]]:
        return cls_kwargs, (self.__class__, self.__dict__)


class AquicheTypeError(AquicheErrorMixin, TypeError):
    pass


class AquicheValueError(AquicheErrorMixin, ValueError):
    pass


class DateError(AquicheValueError):
    msg_template = "invalid date format"


class DateTimeError(AquicheValueError):
    msg_template = "invalid datetime format"


class DurationError(AquicheValueError):
    msg_template = "invalid duration format"


class TimeError(AquicheValueError):
    msg_template = "invalid time format"
