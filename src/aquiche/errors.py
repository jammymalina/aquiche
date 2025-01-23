from typing import Any, List


class AquicheError(Exception):
    message: str

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class AquicheValueError(AquicheError, ValueError):
    pass


class AquicheTypeError(AquicheError, ValueError):
    pass


class AquicheRuntimeError(AquicheError, RuntimeError):
    pass


class InvalidExpirationType(AquicheTypeError):
    def __init__(self) -> None:
        super().__init__("Unsupported cache expiration type")


class InvalidCacheConfig(AquicheTypeError):
    def __init__(self, errors: List[str]) -> None:
        super().__init__(f"Invalid cache params - {', '.join(errors)}")


class DateError(AquicheValueError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid date format: {value}")


class DateTimeError(AquicheValueError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid datetime format: {value}")


class DurationError(AquicheValueError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid duration format: {value}")


class TimeError(AquicheValueError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid time format: {value}")


class InvalidExpressionError(AquicheValueError):
    def __init__(self, expression: str, position: int, error_message: str) -> None:
        super().__init__(message=f"Invalid expression '{expression}': {error_message} at position {position}")


class InvalidTimeFormatError(AquicheValueError):
    def __init__(self, value: Any) -> None:
        super().__init__(
            f"Invalid cache expiration value '{value}': "
            + "it does not resolve to either datetime or timedelta, try to choose different format"
        )


class ExtractionError(AquicheValueError):
    def __init__(self, attribute_path: Any) -> None:
        super().__init__(
            f"Unable to extract value from an object, path does not point to any valid value: {attribute_path!r}"
        )


class InvalidSyncExpirationType(AquicheValueError):
    def __init__(self, value: Any) -> None:
        super().__init__(f"Invalid cache expiration value '{value}': it resolves to async expiration")


class DeadlockError(AquicheRuntimeError):
    def __init__(self) -> None:
        super().__init__("Aquiche internal error - potential deadlock")
