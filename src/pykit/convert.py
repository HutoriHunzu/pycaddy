from __future__ import annotations

from typing import Sequence, overload
from functools import singledispatch
from collections import Counter
import numpy as np  # used for ndarray + np.median
from pint import UnitRegistry, Quantity

ureg = UnitRegistry()

ROUND = 10


# ---------------------------------------------------------------------------
# Helper for one scalar
# ---------------------------------------------------------------------------
def _convert_number(value: float, origin_unit: str,
                    target_unit: str | None) -> Quantity:
    """
    Convert `value` from `origin_unit` to `target_unit` (or auto-compact if
    None) and *round the magnitude* to `ROUND` decimal places.
    """
    q = ureg.Quantity(value, origin_unit)
    out = q.to(target_unit) if target_unit else q.to_compact()

    # --- NEW: round the magnitude ------------------------------------------
    rounded_mag = round(float(out.magnitude), ROUND)
    return ureg.Quantity(rounded_mag, out.units)


# ---------------------------------------------------------------------------
# Overload stubs for static type checkers
# ---------------------------------------------------------------------------
@overload
def convert(value: float,
            origin_unit: str,
            target_unit: str | None = None) -> tuple[float, str]: ...


@overload
def convert(value: Sequence[float],
            origin_unit: str,
            target_unit: str | None = None) -> tuple[list[float], str]: ...


@overload
def convert(value: np.ndarray,
            origin_unit: str,
            target_unit: str | None = None) -> tuple[np.ndarray, str]: ...


# ---------------------------------------------------------------------------
# Generic implementation chosen at *run-time*
# ---------------------------------------------------------------------------
@singledispatch
def convert(value: float,
            origin_unit: str,
            target_unit: str | None = None) -> tuple[float, str]:
    """Convert a single number and return magnitude + pretty unit string."""
    out = _convert_number(value, origin_unit, target_unit)
    return out.magnitude, f"{out.units:~}"


# ---------------------------------------------------------------------------
# Specialisation for sequences & NumPy arrays
# ---------------------------------------------------------------------------
@convert.register(list)
@convert.register(tuple)
@convert.register(np.ndarray)
def _(value: Sequence[float] | np.ndarray,
      origin_unit: str,
      target_unit: str | None = None) -> tuple[list[float] | np.ndarray, str]:
    if len(value) == 0:
        return [], ''
        # raise ValueError("Empty sequence cannot be converted")

    # First pass: convert each element
    quantities = [_convert_number(x, origin_unit, target_unit) for x in value]

    if target_unit is None:
        # choose the most common auto-compacted unit
        most_common_unit, _ = Counter(q.units for q in quantities).most_common(1)[0]
        target_unit = most_common_unit
        quantities = [_convert_number(x, origin_unit, target_unit) for x in value]

    magnitudes: list[float] | np.ndarray
    magnitudes = [q.magnitude for q in quantities]
    if isinstance(value, np.ndarray):  # keep NumPy type
        magnitudes = np.asarray(magnitudes)

    return magnitudes, f"{quantities[0].units:~}"


def inverse(unit: str, target_unit: str):
    """
    Return the inverse of a unit, e.g. 'm' -> '1/m', 's' -> '1/s'.
    """
    quantity = 1 * ureg(unit)

    # Invert the time to get frequency
    inverse_quantity = (1 / quantity).to(target_unit)
    inverse_quantity = inverse_quantity.to_compact()
    return f'{inverse_quantity.units:~}'


def parse_quantity(s: str, unit: str | None) -> tuple[float, str]:
    """
    Create a Quantity object with the given value and unit.
    """
    q = ureg.Quantity(s)
    if unit:
        q = q.to(unit)
    return q.magnitude, f"{q.units:~}"



# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # print(convert(750_000, "ns"))  # → (0.75, 'millisecond')
    # print(convert([750_000, 1e6, 1.2e6], "ns"))  # → ([0.75, 1.0, 1.2], 'millisecond')
    # arr = np.array([3.0, 4.0, 5.0])
    # print(convert(arr, "m", "cm"))  # → (array([300., 400., 500.]), 'centimeter')

    arg = 130_000_000
    print(convert(arg, origin_unit='ns'))

    print(f"{inverse('us', 'Hz')=}")
