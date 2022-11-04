import pytest

from aquiche.utils._scanner import Scanner, TokenType


@pytest.mark.parametrize(
    "value",
    [
        "1seconds 2   minutes 1000 hours   @@@@@",
        "    1seconds 2   minutes 1000 hours     @@@@@   ",
    ],
)
def test_scanner(value: str) -> None:
    """It should scan the simple expression, trailing spaces are ignored, non-alphanumeric characters are not recognized"""
    scanner = Scanner()
    scanner.start_scan(value)

    assert scanner.token_type == TokenType.NUMBER
    assert scanner.token == "1"

    scanner.scan()
    assert scanner.token_type == TokenType.IDENTIFIER
    assert scanner.token == "seconds"

    scanner.scan()
    assert scanner.token_type == TokenType.NUMBER
    assert scanner.token == "2"

    scanner.scan()
    assert scanner.token_type == TokenType.IDENTIFIER
    assert scanner.token == "minutes"

    scanner.scan()
    assert scanner.token_type == TokenType.NUMBER
    assert scanner.token == "1000"

    scanner.scan()
    assert scanner.token_type == TokenType.IDENTIFIER
    assert scanner.token == "hours"

    for _ in range(5):
        scanner.scan()
        assert scanner.token_type == TokenType.NONE
        assert scanner.token == ""

    for _ in range(1000):
        scanner.scan()
        assert scanner.token_type == TokenType.TERMINATOR
