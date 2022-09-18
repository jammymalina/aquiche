import sys

import black
from mypy import api as mypy_api
from pylint import run_pylint
import pytest


def run_black_check() -> int:
    exit_code = 0
    try:
        black.main(["--line-length", "120", "--check", "."])
    except SystemExit as exc:
        exit_code = exc.code

    return exit_code


def run_pylint_check() -> int:
    exit_code = 0
    try:
        run_pylint(["--rcfile=.pylintrc", "aquiche"])
    except SystemExit as exc:
        exit_code = exc.code

    return exit_code


def validate_exit_code(exit_code: int) -> None:
    if exit_code == 0:
        return
    sys.exit(exit_code)


def test() -> None:
    sys.exit(pytest.main(["-x", "tests"]))


def lint() -> None:
    exit_code = 0

    # Black
    print("Running formatting check")
    exit_code = run_black_check()
    validate_exit_code(exit_code)

    # MyPy
    print("Running type check")
    type_outcome = mypy_api.run(["--config-file", "mypy.ini", "aquiche"])
    if type_outcome[0]:
        print("Type checking report:")
        print(type_outcome[0])

    if type_outcome[1]:
        print("Type error report:")
        print(type_outcome[1])

    validate_exit_code(type_outcome[2])

    # Pylint
    print("Running pylint")
    exit_code = run_pylint_check()
    validate_exit_code(exit_code)
