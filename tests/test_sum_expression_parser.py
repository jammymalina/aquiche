import pytest

from aquiche.utils._sum_expression_parser import SumExpressionParser, SumExpressionParserConfig

value_mapping = {
    "s": 1,
    "seconds": 1,
    "m": 60,
    "minutes": 60,
    "h": 60 * 60,
    "hours": 60 * 60,
}


@pytest.fixture
def parser_ignore_case() -> SumExpressionParser:
    config = SumExpressionParserConfig(case_sensitive=False, value_mapping=value_mapping)
    return SumExpressionParser(config)


@pytest.mark.parametrize(
    "input,result",
    [
        ("1m", 60),
        ("1s", 1),
        ("1h", 3600),
        ("1seconds 2   minutes 1000 hours", 1 + 2 * 60 + 1000 * 60 * 60),
        ("    1seconds 2   minutes 1000 hours     ", 1 + 2 * 60 + 1000 * 60 * 60),
        ("1000 HoUrs 2 MiNUTeS 1 SecOnDs", 1 + 2 * 60 + 1000 * 60 * 60),
    ],
)
def test_valid_expressions(parser_ignore_case: SumExpressionParser, input: str, result: int) -> None:
    """It should evaluate valid expressions"""
    assert parser_ignore_case.parse(input) == result
