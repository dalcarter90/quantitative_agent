"""Exact conversion between decimal strings and int64 mantissas (ADR 0005).

A price or quantity is stored as ``mantissa`` with a per-instrument decimal
exponent: ``value = mantissa * 10**-exp``. Venues send decimal strings, and
this module turns them into mantissas using string and integer operations
only. Nothing here touches ``float``.
"""

import re

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

# 10**18 is the largest power of ten that fits in int64.
MAX_EXP = 18

# ASCII digits only: \d would also accept other Unicode digit characters.
_DECIMAL = re.compile(r"([+-]?)([0-9]+)(?:\.([0-9]+))?")


class FixedPointError(ValueError):
    """A value cannot be represented exactly as an int64 mantissa."""


def _check_exp(exp: int) -> None:
    if type(exp) is not int:
        raise TypeError(f"exponent must be int, got {type(exp).__name__}")
    if not 0 <= exp <= MAX_EXP:
        raise FixedPointError(f"exponent {exp} is outside 0..{MAX_EXP}")


def to_mantissa(text: str, exp: int) -> int:
    """Convert a plain decimal string to the mantissa at ``exp`` decimal places.

    Accepts an optional sign, digits, and an optional fraction ("123",
    "-0.0015"). Scientific notation, whitespace, separators and bare leading
    or trailing points are rejected. Fraction digits beyond ``exp`` are an
    error unless they are all zero, so no precision is ever dropped.
    """
    if not isinstance(text, str):
        raise TypeError(f"value must be a decimal string, got {type(text).__name__}")
    _check_exp(exp)

    match = _DECIMAL.fullmatch(text)
    if match is None:
        raise FixedPointError(f"not a plain decimal number: {text!r}")
    sign, whole, fraction = match.group(1), match.group(2), match.group(3) or ""

    if fraction[exp:].strip("0"):
        raise FixedPointError(f"{text!r} has more precision than {exp} decimal places")

    mantissa = int(whole + fraction[:exp].ljust(exp, "0"))
    if sign == "-":
        mantissa = -mantissa
    if not INT64_MIN <= mantissa <= INT64_MAX:
        raise FixedPointError(f"{text!r} at {exp} decimal places does not fit in int64")
    return mantissa


def to_decimal_str(mantissa: int, exp: int) -> str:
    """Render a mantissa as a decimal string with exactly ``exp`` fraction digits."""
    if type(mantissa) is not int:
        raise TypeError(f"mantissa must be int, got {type(mantissa).__name__}")
    _check_exp(exp)
    if not INT64_MIN <= mantissa <= INT64_MAX:
        raise FixedPointError(f"mantissa {mantissa} does not fit in int64")

    sign = "-" if mantissa < 0 else ""
    digits = str(abs(mantissa)).rjust(exp + 1, "0")
    if exp == 0:
        return sign + digits
    return f"{sign}{digits[:-exp]}.{digits[-exp:]}"
