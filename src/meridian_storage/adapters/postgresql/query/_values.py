# SPDX-License-Identifier: Apache-2.0
"""PostgreSQL driver-value normalization shared by query and DML compilation."""

from __future__ import annotations

import base64
import binascii
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from psycopg.types.json import Jsonb

from .._settings import FieldLayout

_BASE64URL = re.compile(r"^[A-Za-z0-9_-]*={0,2}$")


def decode_base64url(value: object, field_name: str) -> bytes:
    if not isinstance(value, str):
        raise TypeError(f"field {field_name!r} requires base64url text")
    if _BASE64URL.fullmatch(value) is None or len(value.rstrip("=")) % 4 == 1:
        raise ValueError(f"field {field_name!r} contains invalid base64url text")
    unpadded = value.rstrip("=")
    try:
        return base64.b64decode(
            unpadded + "=" * (-len(unpadded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"field {field_name!r} contains invalid base64url text") from exc


def wgs84_coordinates(value: object, field_name: str) -> tuple[float, float]:
    if not isinstance(value, Mapping) or set(value) != {"longitude", "latitude"}:
        raise ValueError(f"field {field_name!r} requires longitude and latitude")
    longitude = value["longitude"]
    latitude = value["latitude"]
    if (
        isinstance(longitude, bool)
        or isinstance(latitude, bool)
        or not isinstance(longitude, (int, float, Decimal))
        or not isinstance(latitude, (int, float, Decimal))
    ):
        raise TypeError(f"field {field_name!r} WGS84 coordinates must be numeric")
    normalized = (float(longitude), float(latitude))
    if (
        not all(math.isfinite(item) for item in normalized)
        or not -180 <= normalized[0] <= 180
        or not -90 <= normalized[1] <= 90
    ):
        raise ValueError(f"field {field_name!r} WGS84 coordinates are out of range")
    return normalized


def adapt_driver_value(field: FieldLayout, value: object) -> object:
    if field.cardinality == "many" or field.logical_type in {"json", "recordRef", "objectRef"}:
        return Jsonb(jsonable(value))
    if field.logical_type == "bytes":
        return decode_base64url(value, field.name)
    return value


def encode_query_value(field: FieldLayout, value: object) -> object:
    if field.cardinality == "many" or field.logical_type in {"json", "recordRef", "objectRef"}:
        return {"$postgresql": {"encoding": "jsonb", "value": jsonable(value)}}
    if field.logical_type == "bytes":
        decode_base64url(value, field.name)
        return {"$postgresql": {"encoding": "base64url", "value": value}}
    return value


def decode_query_value(value: object) -> object:
    if not isinstance(value, Mapping) or set(value) != {"$postgresql"}:
        return value
    specification = value["$postgresql"]
    if not isinstance(specification, Mapping) or set(specification) != {"encoding", "value"}:
        raise ValueError("compiled PostgreSQL parameter marker is invalid")
    encoding = specification["encoding"]
    encoded = specification["value"]
    if encoding == "jsonb":
        return Jsonb(jsonable(encoded))
    if encoding == "base64url":
        return decode_base64url(encoded, "query parameter")
    raise ValueError("compiled PostgreSQL parameter encoding is unsupported")


def jsonable(value: object) -> Any:
    """Normalize psycopg values to Meridian JSON values."""

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, timedelta):
        total = value.total_seconds()
        return f"PT{total:g}S"
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence):
        return [jsonable(item) for item in value]
    return str(value)


__all__ = [
    "adapt_driver_value",
    "decode_base64url",
    "decode_query_value",
    "encode_query_value",
    "jsonable",
    "wgs84_coordinates",
]
