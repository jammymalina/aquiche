from dataclasses import dataclass, field
from typing import Dict

from aquiche.errors import InvalidExpressionError
from aquiche.utils._scanner import Scanner, TokenType


@dataclass
class SumExpressionParserConfig:
    case_sensitive: bool = False
    value_mapping: Dict[str, int] = field(default_factory=lambda: {})


class SumExpressionParser:
    config: SumExpressionParserConfig
    scanner: Scanner

    def __init__(self, config: SumExpressionParserConfig) -> None:
        self.config = config
        if not self.config.case_sensitive:
            self.config.value_mapping = {k.lower(): v for k, v in self.config.value_mapping.items()}
        self.scanner = Scanner()

    def parse(self, input_string: str) -> int:
        self.scanner.start_scan(input_string)
        result = 0
        while self.scanner.token_type != TokenType.TERMINATOR:
            num_value = self.__evaluate_number()
            identifier_value = self.__evaluate_identifier()
            result += num_value * identifier_value
        return result

    def __evaluate_identifier(self) -> int:
        if self.scanner.token_type != TokenType.IDENTIFIER:
            raise InvalidExpressionError(
                expression=self.scanner.input, position=self.scanner.position, error_message="expected identifier"
            )

        token = self.scanner.token if self.config.case_sensitive else self.scanner.token.lower()
        if not token in self.config.value_mapping:
            raise InvalidExpressionError(
                expression=self.scanner.input, position=self.scanner.position, error_message="unknown identifier"
            )
        value = self.config.value_mapping[token]
        self.scanner.scan()
        return value

    def __evaluate_number(self) -> int:
        if self.scanner.token_type != TokenType.NUMBER:
            raise InvalidExpressionError(
                expression=self.scanner.input, position=self.scanner.position, error_message="expected number"
            )

        value = int(self.scanner.token)
        self.scanner.scan()
        return value
