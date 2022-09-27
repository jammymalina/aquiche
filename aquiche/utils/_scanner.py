from curses.ascii import isalpha, isdigit, isspace
from enum import Enum


TERMINATOR = "\0"


class TokenType(str, Enum):
    NONE = "NONE"
    NUMBER = "NUMBER"
    IDENTIFIER = "IDENTIFIER"
    TERMINATOR = "TERMINATOR"


class Scanner:
    input: str
    look: str
    index: int
    position: int
    token: str
    token_type: TokenType

    def __init__(self) -> None:
        self.input = ""
        self.look = ""
        self.index = 0
        self.token = ""
        self.token_type = TokenType.NONE
        self.position = 0

    def start_scan(self, input_string: str) -> None:
        self.input = input_string.strip()
        self.index = 0
        self.__next()
        self.scan()

    def scan(self) -> None:
        while isspace(self.look):
            self.__next()

        self.token = ""
        self.position = self.index - 1

        if isalpha(self.look):
            while True:
                self.token += self.look
                self.__next()
                if not isalpha(self.look):
                    break
            self.token_type = TokenType.IDENTIFIER
            return

        if isdigit(self.look):
            while True:
                self.token += self.look
                self.__next()
                if not isdigit(self.look):
                    break
            self.token_type = TokenType.NUMBER
            return

        if self.look == TERMINATOR:
            self.token_type = TokenType.TERMINATOR
            return

        self.token_type = TokenType.NONE
        self.__next()

    def __next(self):
        if self.index >= len(self.input):
            self.look = TERMINATOR
        else:
            self.look = self.input[self.index]
            self.index += 1
