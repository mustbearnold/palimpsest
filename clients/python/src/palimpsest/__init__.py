"""Small, dependency-free Python client for the Palimpsest HTTP API."""

from .client import (
    PalimpsestClient,
    PalimpsestConfigurationError,
    PalimpsestError,
    PalimpsestHttpError,
    PalimpsestResponse,
    PalimpsestProtocolError,
    PalimpsestTransportError,
    PartialRememberError,
)

__all__ = [
    "PalimpsestClient",
    "PalimpsestConfigurationError",
    "PalimpsestError",
    "PalimpsestHttpError",
    "PalimpsestResponse",
    "PalimpsestProtocolError",
    "PalimpsestTransportError",
    "PartialRememberError",
]
