#!/usr/bin/env python3
"""Generate the immutable Q63 exp2 constants for temporal scoring.

Requires the pinned system libraries GNU MPFR 4.2.2 and GMP 6.3.0. The script
has no Python package dependencies; it calls libmpfr directly through ctypes.
"""

from __future__ import annotations

import ctypes
import hashlib
from ctypes.util import find_library

MPFR_VERSION = "4.2.2"
GMP_VERSION = "6.3.0"
PRECISION_BITS = 256
Q63_BITS = 63
ROUND_TO_NEAREST_TIES_EVEN = 0  # MPFR_RNDN
# Each of the 63 multiplications contributes at most half a Q63 unit from its
# frozen constant and half a unit from multiplication rounding. Truncating the
# fractional exponent after 63 bits contributes less than one more unit.
MAX_ABSOLUTE_ERROR_Q63_UNITS = 64


class MpfrValue(ctypes.Structure):
    """ABI layout of one __mpfr_struct on the pinned 64-bit generator host."""

    _fields_ = [
        ("precision", ctypes.c_long),
        ("sign", ctypes.c_int),
        ("exponent", ctypes.c_long),
        ("limbs", ctypes.POINTER(ctypes.c_ulong)),
    ]


def load_libraries() -> tuple[ctypes.CDLL, ctypes.CDLL]:
    if ctypes.sizeof(ctypes.c_ulong) != 8:
        raise RuntimeError("the pinned generator requires 64-bit unsigned long")

    mpfr_path = find_library("mpfr")
    gmp_path = find_library("gmp")
    if mpfr_path is None or gmp_path is None:
        raise RuntimeError("libmpfr and libgmp must be installed")

    mpfr = ctypes.CDLL(mpfr_path)
    gmp = ctypes.CDLL(gmp_path)
    mpfr.mpfr_get_version.restype = ctypes.c_char_p
    actual_mpfr = mpfr.mpfr_get_version().decode("ascii")
    actual_gmp = ctypes.c_char_p.in_dll(gmp, "__gmp_version").value.decode("ascii")
    if (actual_mpfr, actual_gmp) != (MPFR_VERSION, GMP_VERSION):
        raise RuntimeError(
            "generator version mismatch: "
            f"expected MPFR {MPFR_VERSION} / GMP {GMP_VERSION}, "
            f"found MPFR {actual_mpfr} / GMP {actual_gmp}"
        )
    return mpfr, gmp


def configure(mpfr: ctypes.CDLL) -> None:
    pointer = ctypes.POINTER(MpfrValue)
    mpfr.mpfr_init2.argtypes = [pointer, ctypes.c_long]
    mpfr.mpfr_clear.argtypes = [pointer]
    mpfr.mpfr_set_si.argtypes = [pointer, ctypes.c_long, ctypes.c_int]
    mpfr.mpfr_div_2ui.argtypes = [pointer, pointer, ctypes.c_ulong, ctypes.c_int]
    mpfr.mpfr_exp2.argtypes = [pointer, pointer, ctypes.c_int]
    mpfr.mpfr_mul_2ui.argtypes = [pointer, pointer, ctypes.c_ulong, ctypes.c_int]
    mpfr.mpfr_get_ui.argtypes = [pointer, ctypes.c_int]
    mpfr.mpfr_get_ui.restype = ctypes.c_ulong


def generate(mpfr: ctypes.CDLL) -> list[int]:
    exponent = MpfrValue()
    factor = MpfrValue()
    mpfr.mpfr_init2(ctypes.byref(exponent), PRECISION_BITS)
    mpfr.mpfr_init2(ctypes.byref(factor), PRECISION_BITS)
    try:
        constants = []
        for bit in range(1, Q63_BITS + 1):
            mpfr.mpfr_set_si(
                ctypes.byref(exponent), -1, ROUND_TO_NEAREST_TIES_EVEN
            )
            mpfr.mpfr_div_2ui(
                ctypes.byref(exponent),
                ctypes.byref(exponent),
                bit,
                ROUND_TO_NEAREST_TIES_EVEN,
            )
            mpfr.mpfr_exp2(
                ctypes.byref(factor),
                ctypes.byref(exponent),
                ROUND_TO_NEAREST_TIES_EVEN,
            )
            mpfr.mpfr_mul_2ui(
                ctypes.byref(factor),
                ctypes.byref(factor),
                Q63_BITS,
                ROUND_TO_NEAREST_TIES_EVEN,
            )
            constants.append(
                int(mpfr.mpfr_get_ui(ctypes.byref(factor), ROUND_TO_NEAREST_TIES_EVEN))
            )
        return constants
    finally:
        mpfr.mpfr_clear(ctypes.byref(exponent))
        mpfr.mpfr_clear(ctypes.byref(factor))


def canonical_payload(constants: list[int]) -> bytes:
    rows = ["q63-exp2-v1", *(f"{index}={value}" for index, value in enumerate(constants, 1))]
    return ("\n".join(rows) + "\n").encode("ascii")


def main() -> None:
    mpfr, _gmp = load_libraries()
    configure(mpfr)
    constants = generate(mpfr)
    digest = hashlib.sha256(canonical_payload(constants)).hexdigest()

    print(f"generator=GNU MPFR {MPFR_VERSION} / GMP {GMP_VERSION}")
    print(f"precision_bits={PRECISION_BITS}")
    print("canonical_payload=q63-exp2-v1\\n followed by index=value\\n")
    print("pub const Q63_EXP2_NEGATIVE_BINARY_POWERS: [u128; 63] = [")
    for value in constants:
        print(f"    {value},")
    print("];")
    print(f"constants_sha256={digest}")
    print(f"max_absolute_error_q63_units={MAX_ABSOLUTE_ERROR_Q63_UNITS}")


if __name__ == "__main__":
    main()
