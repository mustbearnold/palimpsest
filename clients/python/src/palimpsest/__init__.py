"""Small, dependency-free Python client for the Palimpsest HTTP API."""

from .client import (
    PalimpsestBinaryResponse,
    PalimpsestClient,
    PalimpsestConfigurationError,
    PalimpsestError,
    PalimpsestHttpError,
    PalimpsestResponse,
    PalimpsestProtocolError,
    PalimpsestTimeoutError,
    PalimpsestTransportError,
    PartialRememberError,
)

__all__ = [
    "PalimpsestBinaryResponse",
    "PalimpsestClient",
    "PalimpsestConfigurationError",
    "PalimpsestError",
    "PalimpsestHttpError",
    "PalimpsestResponse",
    "PalimpsestProtocolError",
    "PalimpsestTimeoutError",
    "PalimpsestTransportError",
    "PartialRememberError",
]
