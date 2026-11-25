"""Exact rational helpers used by the lineage ledger."""
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction
from typing import Any, Mapping


class LedgerValueError(ValueError):
    """Raised when a quantity or identifier payload is invalid."""


def parse_fraction(value: Any, field: str = "quantity") -> Fraction:
    """Parse JSON-native data without accepting lossy raw floats.

    Accepted forms:
      * integer;
      * exact decimal/rational string ("0.25", "1/3");
      * Decimal;
      * ``{"num": 1, "den": 3}``.
    """
    if isinstance(value, bool):
        raise LedgerValueError(f"{field}: boolean is not a quantity")
    if isinstance(value, Fraction):
        return value
    if isinstance(value, int):
        return Fraction(value, 1)
    if isinstance(value, Decimal):
        return Fraction(value)
    if isinstance(value, str):
        try:
            return Fraction(value.strip())
        except (ValueError, ZeroDivisionError) as exc:
            raise LedgerValueError(f"{field}: invalid rational {value!r}") from exc
    if isinstance(value, Mapping):
        if "num" not in value or "den" not in value:
            raise LedgerValueError(f"{field}: rational object needs num and den")
        try:
            num = int(value["num"])
            den = int(value["den"])
        except (TypeError, ValueError) as exc:
            raise LedgerValueError(f"{field}: num/den must be integers") from exc
        if den == 0:
            raise LedgerValueError(f"{field}: zero denominator")
        return Fraction(num, den)
    if isinstance(value, float):
        raise LedgerValueError(
            f"{field}: raw float is forbidden; pass a decimal string or num/den"
        )
    raise LedgerValueError(f"{field}: unsupported quantity type {type(value).__name__}")


def positive_fraction(value: Any, field: str = "quantity") -> Fraction:
    result = parse_fraction(value, field)
    if result <= 0:
        raise LedgerValueError(f"{field}: quantity must be positive")
    return result


def q_to_db(q: Fraction) -> tuple[str, str]:
    return str(q.numerator), str(q.denominator)


def q_from_db(num: Any, den: Any) -> Fraction:
    return Fraction(int(num), int(den))


def q_to_str(q: Fraction | None) -> str | None:
    if q is None:
        return None
    return str(q)  # Fraction.__str__ emits "3" or "1/3", never a rounded decimal.
