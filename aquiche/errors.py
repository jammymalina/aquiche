class AquicheError(Exception):
    message: str

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class DateError(AquicheError):
    def __init__(self) -> None:
        super().__init__(message="Invalid date format")


class DateTimeError(AquicheError):
    def __init__(self) -> None:
        super().__init__(message="Invalid datetime format")


class DurationError(AquicheError):
    def __init__(self) -> None:
        super().__init__(message="Invalid duration format")


class TimeError(AquicheError):
    def __init__(self) -> None:
        super().__init__(message="Invalid time format")


class InvalidExpressionError(AquicheError):
    def __init__(self, expression: str, position: int, error_message: str) -> None:
        super().__init__(message=f"Invalid expression '{expression}': {error_message} at position {position}")
