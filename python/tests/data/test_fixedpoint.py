"""Decimal string <-> int64 mantissa conversion (ADR 0005).

The conversion must be exact and must never pass through ``float``.
"""

import random
from decimal import Decimal

import pytest

from quantagent.data.fixedpoint import (
    INT64_MAX,
    INT64_MIN,
    MAX_EXP,
    FixedPointError,
    to_decimal_str,
    to_mantissa,
)


@pytest.mark.parametrize(
    ("text", "exp", "expected"),
    [
        ("123.45", 2, 12345),
        ("97123.10", 2, 9712310),
        ("0", 8, 0),
        ("7", 0, 7),
        ("7", 3, 7000),
        ("+2", 3, 2000),
        ("-1.5", 1, -15),
        ("-0.0", 2, 0),
        ("007.50", 2, 750),
        # Very small prices.
        ("0.00000001", 8, 1),
        ("0.000000000000000001", 18, 1),
        ("0.00001234", 8, 1234),
        # Very large quantities.
        ("1000000000000", 0, 10**12),
        ("123456789012345.67", 2, 12345678901234567),
        ("92233720368.54775807", 8, INT64_MAX),
        ("9223372036854775807", 0, INT64_MAX),
        ("-9223372036854775808", 0, INT64_MIN),
        # Trailing zeros beyond the exponent carry no extra precision.
        ("0.00100000", 5, 100),
        ("1.2300", 2, 123),
        ("5.000", 0, 5),
    ],
)
def test_to_mantissa_exact(text, exp, expected):
    result = to_mantissa(text, exp)
    assert result == expected
    assert type(result) is int


@pytest.mark.parametrize(
    ("text", "exp", "expected"),
    [
        # int(float(text) * 10**exp) gets each of these wrong (0.29 -> 28).
        ("0.29", 2, 29),
        ("1.15", 2, 115),
        ("4.35", 2, 435),
        ("0.57", 2, 57),
        # More significant digits than a float holds: wrong by any float route.
        ("1234567890.123456789", 9, 1234567890123456789),
        ("90071992547409.93", 2, 9007199254740993),  # 2**53 + 1
    ],
)
def test_to_mantissa_does_not_go_through_float(text, exp, expected):
    assert to_mantissa(text, exp) == expected


@pytest.mark.parametrize(
    ("text", "exp"),
    [
        ("0.001", 2),
        ("1.005", 2),
        ("0.000000001", 8),
        ("1.1", 0),
        ("0.0000000000000000001", 18),
        ("123.4500001", 6),
    ],
)
def test_to_mantissa_rejects_excess_precision(text, exp):
    with pytest.raises(FixedPointError, match="precision"):
        to_mantissa(text, exp)


@pytest.mark.parametrize(
    ("text", "exp"),
    [
        ("9223372036854775808", 0),
        ("-9223372036854775809", 0),
        ("92233720368.54775808", 8),
        ("10", 18),
        ("99999999999999999999", 0),
    ],
)
def test_to_mantissa_rejects_int64_overflow(text, exp):
    with pytest.raises(FixedPointError, match="int64"):
        to_mantissa(text, exp)


@pytest.mark.parametrize(
    "text",
    [
        "",
        " ",
        " 1",
        "1 ",
        "1\n",
        ".5",
        "5.",
        ".",
        "-",
        "+",
        "--1",
        "1.2.3",
        "1e-8",
        "1E5",
        "1,000",
        "1_000",
        "0x10",
        "NaN",
        "nan",
        "inf",
        "-Infinity",
        "１２",  # full-width digits
        "١",  # Arabic-Indic digit
        "1.５",
    ],
)
def test_to_mantissa_rejects_malformed_text(text):
    with pytest.raises(FixedPointError):
        to_mantissa(text, 2)


@pytest.mark.parametrize("value", [1.5, 1, Decimal("1.5"), None, b"1.5", True])
def test_to_mantissa_only_accepts_str(value):
    with pytest.raises(TypeError):
        to_mantissa(value, 2)


@pytest.mark.parametrize("exp", [-1, MAX_EXP + 1, 100])
def test_exponent_out_of_range(exp):
    with pytest.raises(FixedPointError, match="exponent"):
        to_mantissa("1", exp)
    with pytest.raises(FixedPointError, match="exponent"):
        to_decimal_str(1, exp)


@pytest.mark.parametrize("exp", [2.0, "2", None, True])
def test_exponent_must_be_int(exp):
    with pytest.raises(TypeError):
        to_mantissa("1", exp)
    with pytest.raises(TypeError):
        to_decimal_str(1, exp)


@pytest.mark.parametrize(
    ("mantissa", "exp", "expected"),
    [
        (1, 8, "0.00000001"),
        (-15, 1, "-1.5"),
        (12345, 0, "12345"),
        (0, 2, "0.00"),
        (100, 5, "0.00100"),
        (-1, 3, "-0.001"),
        (INT64_MAX, 8, "92233720368.54775807"),
        (INT64_MIN, 0, "-9223372036854775808"),
    ],
)
def test_to_decimal_str(mantissa, exp, expected):
    assert to_decimal_str(mantissa, exp) == expected


@pytest.mark.parametrize("mantissa", [INT64_MAX + 1, INT64_MIN - 1])
def test_to_decimal_str_rejects_out_of_range_mantissa(mantissa):
    with pytest.raises(FixedPointError, match="int64"):
        to_decimal_str(mantissa, 0)


@pytest.mark.parametrize("mantissa", [1.0, "1", None, True])
def test_to_decimal_str_only_accepts_int(mantissa):
    with pytest.raises(TypeError):
        to_decimal_str(mantissa, 2)


def test_round_trip_matches_decimal_oracle():
    rng = random.Random(20261110)
    edge = [0, 1, -1, INT64_MAX, INT64_MIN, 10**18, -(10**18)]
    mantissas = edge + [rng.randint(INT64_MIN, INT64_MAX) for _ in range(2000)]
    mantissas += [rng.randint(-(10**6), 10**6) for _ in range(2000)]
    for mantissa in mantissas:
        exp = rng.randint(0, MAX_EXP)
        text = to_decimal_str(mantissa, exp)
        assert Decimal(text) == Decimal(mantissa).scaleb(-exp), (mantissa, exp, text)
        assert to_mantissa(text, exp) == mantissa, (mantissa, exp, text)
