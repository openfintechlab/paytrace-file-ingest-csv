# -*- coding: utf-8 -*-
"""Domain model for payment CSV records."""

from __future__ import annotations

import json
from dataclasses import field, make_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Self

from jsonschema import Draft202012Validator, FormatChecker

try:
    from utilities.Logging import Logging
except ImportError:  # pragma: no cover - package import fallback
    from src.utilities.Logging import Logging


_SCHEMA_PATH = Path(__file__).resolve().with_name("payment_instruction.schema.json")


@lru_cache(maxsize=1)
def _load_schema() -> dict[str, Any]:
    with _SCHEMA_PATH.open("r", encoding="utf-8") as schema_file:
        return json.load(schema_file, parse_float=Decimal)


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    return Draft202012Validator(_load_schema(), format_checker=FormatChecker())


def _schema_properties() -> dict[str, Any]:
    return _load_schema()["properties"]


def _required_fields() -> set[str]:
    return set(_load_schema().get("required", []))


def _type_names(field_schema: Mapping[str, Any]) -> tuple[str, ...]:
    field_type = field_schema.get("type")
    if isinstance(field_type, list):
        return tuple(str(type_name) for type_name in field_type)
    if isinstance(field_type, str):
        return (field_type,)
    return ()


def _allows_null(field_schema: Mapping[str, Any]) -> bool:
    return "null" in _type_names(field_schema)


def _non_null_type(field_schema: Mapping[str, Any]) -> str | None:
    for type_name in _type_names(field_schema):
        if type_name != "null":
            return type_name
    return None


def _annotation_for_field(field_schema: Mapping[str, Any], *, required: bool) -> Any:
    field_type = _non_null_type(field_schema)
    field_format = field_schema.get("format")

    annotation: Any
    if field_type == "number":
        annotation = Decimal
    elif field_type == "integer":
        annotation = int
    elif field_type == "boolean":
        annotation = bool
    elif field_type == "string" and field_format == "date-time":
        annotation = datetime
    elif field_type == "string" and field_format == "date":
        annotation = date
    elif field_type == "string":
        annotation = str
    else:
        annotation = Any

    if required and not _allows_null(field_schema):
        return annotation

    return annotation | None


def _dataclass_fields() -> list[tuple[str, Any] | tuple[str, Any, Any]]:
    fields: list[tuple[str, Any] | tuple[str, Any, Any]] = []
    required_fields = _required_fields()

    for field_name, field_schema in _schema_properties().items():
        required = field_name in required_fields
        annotation = _annotation_for_field(field_schema, required=required)
        if required and not _allows_null(field_schema):
            fields.append((field_name, annotation))
            continue

        fields.append((field_name, annotation, field(default=None)))

    return fields


PAYMENT_CSV_COLUMNS: tuple[str, ...] = tuple(_schema_properties().keys())


class _PaymentModelBase:
    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> Self:
        try:
            payload = _build_payload(data)
            _validate_payload(payload)
            return cls(**_coerce_model_payload(payload))
        except ValueError as exc:
            transfer_id = str(data.get("transfer_id", "")).strip() or "unknown"
            Logging.error("Payment parsing failed for transfer_id=%s: %s", transfer_id, exc)
            raise


PaymentModel = make_dataclass(
    "PaymentModel",
    _dataclass_fields(),
    bases=(_PaymentModelBase,),
    kw_only=True,
    slots=True,
)


def _build_payload(data: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    schema_properties = _schema_properties()
    unknown_fields = sorted(set(data.keys()) - set(schema_properties.keys()))
    if unknown_fields:
        raise ValueError(f"Unexpected payment fields: {', '.join(unknown_fields)}")

    for field_name, field_schema in schema_properties.items():
        if field_name not in data:
            continue
        raw_value = data[field_name]
        coerced_value = _coerce_input_value(raw_value, field_schema, field_name=field_name)
        if coerced_value is not None:
            payload[field_name] = coerced_value

    return payload


def _coerce_input_value(raw_value: Any, field_schema: Mapping[str, Any], *, field_name: str) -> Any:
    if raw_value is None:
        return None

    if isinstance(raw_value, str):
        raw_value = raw_value.strip()
        if raw_value == "":
            return None

    field_type = _non_null_type(field_schema)
    if field_type == "number":
        return _parse_decimal(raw_value, field_name=field_name)
    if field_type == "integer":
        return _parse_integer(raw_value, field_name=field_name)
    if field_type == "boolean":
        return _parse_boolean(raw_value, field_name=field_name)

    return raw_value


def _validate_payload(payload: Mapping[str, Any]) -> None:
    errors = sorted(_validator().iter_errors(payload), key=lambda error: list(error.absolute_path))
    if not errors:
        return

    messages: list[str] = []
    for error in errors:
        field_path = ".".join(str(part) for part in error.absolute_path) or "root"
        message = f"{field_path}: {error.message}"
        Logging.error("Payment schema validation error at %s: %s", field_path, error.message)
        messages.append(message)

    raise ValueError("; ".join(messages))


def _coerce_model_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    coerced: dict[str, Any] = {}

    for field_name, value in payload.items():
        coerced[field_name] = _coerce_output_value(
            value,
            _schema_properties()[field_name],
            field_name=field_name,
        )

    return coerced


def _coerce_output_value(value: Any, field_schema: Mapping[str, Any], *, field_name: str) -> Any:
    if value is None:
        return None

    field_type = _non_null_type(field_schema)
    field_format = field_schema.get("format")

    if field_type == "string" and field_format == "date-time":
        return _parse_datetime(str(value), field_name=field_name)
    if field_type == "string" and field_format == "date":
        return _parse_date(str(value), field_name=field_name)
    if field_type == "number":
        return _parse_decimal(value, field_name=field_name)
    if field_type == "integer":
        return _parse_integer(value, field_name=field_name)
    if field_type == "boolean":
        return _parse_boolean(value, field_name=field_name)

    return value


def _parse_datetime(value: str, *, field_name: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        if value.endswith("Z"):
            return datetime.fromisoformat(f"{value[:-1]}+00:00")
        raise ValueError(f"{field_name} must be ISO 8601 date-time") from None


def _parse_date(value: str, *, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _parse_decimal(value: Any, *, field_name: str) -> Decimal:
    if isinstance(value, Decimal):
        return value

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{field_name} must be a valid number") from exc


def _parse_integer(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a valid integer")

    try:
        return int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{field_name} must be a valid integer") from exc


def _parse_boolean(value: Any, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False

    raise ValueError(f"{field_name} must be a valid boolean")
