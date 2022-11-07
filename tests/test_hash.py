from typing import Dict, Optional

import pytest

from aquiche._hash import get_key_resolver, Key


def user_function(
    id: int, environment: str, *args, user: Optional[Dict] = None, token: str = "dummy_token", **kwargs
) -> None:
    pass


def test_single_key() -> None:
    """It should always return the same key"""
    key_resolve = get_key_resolver(Key.SINGLE_KEY, user_function)

    key = key_resolve("id1", "prod")

    assert key == "default_key"


@pytest.mark.parametrize(
    "template,result",
    [
        ("hej", "hej"),
        ("env:{environment}:id:{id}", "env:prod:id:id1"),
        ("id:{}:env:{}", "id:id1:env:prod"),
        ("env:{1}:id:{0}", "env:prod:id:id1"),
        ("{}", "id1"),
        ("{environment}", "prod"),
        ("env:{environment}:id:{id}:token:{token}", "env:prod:id:id1:token:dummy_token"),
        ("env:{environment}:id:{id}:token:{token}:user:{user}", "env:prod:id:id1:token:dummy_token:user:None"),
    ],
)
def test_template_keys_default(template: str, result: str) -> None:
    """It should resolve template key, using str format and default params"""
    key_resolve = get_key_resolver(template, user_function)
    assert key_resolve("id1", "prod") == result


@pytest.mark.parametrize(
    "template,result",
    [
        ("hej", "hej"),
        ("env:{environment}:id:{id}", "env:prod:id:id1"),
        ("id:{}:env:{}", "id:id1:env:prod"),
        ("env:{1}:id:{0}", "env:prod:id:id1"),
        ("{}", "id1"),
        ("{environment}", "prod"),
        ("env:{environment}:id:{id}:token:{token}", "env:prod:id:id1:token:secret_token"),
        (
            "env:{environment}:id:{id}:token:{token}:username:{user[username]}:password:{user[password]}",
            "env:prod:id:id1:token:secret_token:username:file.peter:password:random123",
        ),
    ],
)
def test_template_keys_without_args_kwargs(template: str, result: str) -> None:
    """It should resolve template key, using str format"""
    key_resolve = get_key_resolver(template, user_function)
    assert (
        key_resolve("id1", "prod", user={"username": "file.peter", "password": "random123"}, token="secret_token")
        == result
    )


@pytest.mark.parametrize(
    "template,result",
    [
        ("hej", "hej"),
        ("env:{environment}:id:{id}", "env:prod:id:id1"),
        ("id:{}:env:{}", "id:id1:env:prod"),
        ("env:{1}:id:{0}", "env:prod:id:id1"),
        ("{}", "id1"),
        ("{environment}", "prod"),
        ("env:{environment}:id:{id}:token:{token}", "env:prod:id:id1:token:secret_token"),
        (
            "env:{environment}:id:{id}:token:{token}:username:{user[username]}:password:{user[password]}",
            "env:prod:id:id1:token:secret_token:username:file.peter:password:random123",
        ),
        (
            "env:{environment}:id:{id}:token:{token}:username:{user[username]}:password:{user[password]}:{3}:{2}:{custom_arg}",
            "env:prod:id:id1:token:secret_token:username:file.peter:password:random123:foo:bar:lorem",
        ),
    ],
)
def test_template_keys_args_kwargs(template: str, result: str) -> None:
    """It should resolve template key, using str format and args, kwargs"""
    key_resolve = get_key_resolver(template, user_function)
    assert (
        key_resolve(
            "id1",
            "prod",
            "bar",
            "foo",
            user={"username": "file.peter", "password": "random123"},
            token="secret_token",
            custom_arg="lorem",
        )
        == result
    )
