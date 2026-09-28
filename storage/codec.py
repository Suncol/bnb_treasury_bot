"""Explicit tagged JSON preserves Decimal, UTC timestamps, enums and tuples."""

from dataclasses import fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
import json

from core import models

TYPES = {
    name: value
    for name, value in vars(models).items()
    if isinstance(value, type) and (is_dataclass(value) or issubclass(value, Enum))
}


def _encode(value):
    if isinstance(value, Enum):
        return {"type": type(value).__name__, "value": value.value}
    if isinstance(value, (Decimal, datetime)):
        return {
            "type": type(value).__name__,
            "value": str(value) if isinstance(value, Decimal) else value.isoformat(),
        }
    if is_dataclass(value):
        return {
            "type": type(value).__name__,
            "fields": {f.name: _encode(getattr(value, f.name)) for f in fields(value)},
        }
    if isinstance(value, tuple):
        return {"type": "tuple", "value": [_encode(x) for x in value]}
    if isinstance(value, list):
        return [_encode(x) for x in value]
    if isinstance(value, dict):
        return {k: _encode(v) for k, v in value.items()}
    return value


def _decode(value):
    kind = value.get("type")
    if kind == "Decimal":
        return Decimal(value["value"])
    if kind == "datetime":
        return datetime.fromisoformat(value["value"])
    if kind == "tuple":
        return tuple(value["value"])
    if kind in TYPES:
        return (
            TYPES[kind](**value["fields"])
            if "fields" in value
            else TYPES[kind](value["value"])
        )
    return value


def dumps(value) -> str:
    return json.dumps(
        _encode(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )


def loads(value: str):
    return json.loads(value, object_hook=_decode)
