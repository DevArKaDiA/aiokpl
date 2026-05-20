"""Tests for ``aiokpl.hashing``."""

from __future__ import annotations

import hashlib

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aiokpl.hashing import md5_hash_key, parse_explicit_hash_key

_UINT128_MAX = (1 << 128) - 1


def test_md5_hash_key_known_value() -> None:
    pk = "user-123"
    expected = int.from_bytes(hashlib.md5(pk.encode("utf-8")).digest(), "big")
    assert md5_hash_key(pk) == expected


def test_md5_hash_key_deterministic() -> None:
    assert md5_hash_key("abc") == md5_hash_key("abc")


def test_md5_hash_key_empty_string() -> None:
    expected = int.from_bytes(hashlib.md5(b"").digest(), "big")
    assert md5_hash_key("") == expected


@given(st.text())
def test_md5_hash_key_in_uint128_range(s: str) -> None:
    h = md5_hash_key(s)
    assert 0 <= h <= _UINT128_MAX


@given(st.text())
def test_md5_hash_key_idempotent(s: str) -> None:
    assert md5_hash_key(s) == md5_hash_key(s)


@pytest.mark.parametrize(
    "s,value",
    [
        ("0", 0),
        ("1", 1),
        ("123456789", 123456789),
        (str(_UINT128_MAX), _UINT128_MAX),
    ],
)
def test_parse_explicit_hash_key_valid(s: str, value: int) -> None:
    assert parse_explicit_hash_key(s) == value


@pytest.mark.parametrize(
    "bad",
    [
        "",
        " 1",
        "1 ",
        "+1",
        "-1",
        "1_000",
        "0x10",
        "1.0",
        "abc",
        "1e10",
        "١",  # noqa: RUF001 — arabic-indic digit one, intentionally non-ASCII
        str(_UINT128_MAX + 1),
        str(_UINT128_MAX + 1000),
    ],
)
def test_parse_explicit_hash_key_rejects(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_explicit_hash_key(bad)


@given(st.integers(min_value=0, max_value=_UINT128_MAX))
def test_parse_explicit_hash_key_roundtrip(n: int) -> None:
    assert parse_explicit_hash_key(str(n)) == n
