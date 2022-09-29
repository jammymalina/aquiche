from typing import Any


class AquicheError(ValueError):
    message: str

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class DateError(AquicheError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid date format: {value}")


class DateTimeError(AquicheError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid datetime format: {value}")


class DurationError(AquicheError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid duration format: {value}")


class TimeError(AquicheError):
    def __init__(self, value: Any) -> None:
        super().__init__(message=f"Invalid time format: {value}")


class InvalidExpressionError(AquicheError):
    def __init__(self, expression: str, position: int, error_message: str) -> None:
        super().__init__(message=f"Invalid expression '{expression}': {error_message} at position {position}")


class InvalidTimeFormatError(AquicheError):
    def __init__(self, value: Any) -> None:
        super().__init__(
            f"Invalid cache expiration value '{value}': "
            + "it does not resolve to either datetime or timedelta, try to choose different format"
        )


class ExtractionError(AquicheError):
    def __init__(self, attribute_path: Any) -> None:
        super().__init__(
            f"Unable to extract value from an object, path does not point to any valid value: {attribute_path}"
        )
