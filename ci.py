import sys

import black
import pytest


def test() -> None:
    sys.exit(pytest.main(["-x", "tests"]))


def lint() -> None:
    sys.exit(black.main(["--line-length", "120", "--check", "."]))
