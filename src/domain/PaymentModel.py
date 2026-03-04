# -*- coding: utf-8 -*-
"""Domain model for payment CSV records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping

PAYMENT_CSV_COLUMNS: tuple[str, ...] = (
    "transfer_id",
    "transfer_type",
    "transaction_datetime",
    "amount",
    "currency",
    "purpose_code",
    "charge_bearer",
    "exchange_rate",
    "requested_execution_date",
    "debtor_name",
    "debtor_country",
    "debtor_account_scheme",
    "debtor_account_id",
    "debtor_bank_id_scheme",
    "debtor_bank_id",
    "creditor_name",
    "creditor_country",
    "creditor_account_scheme",
    "creditor_account_id",
    "creditor_bank_id_scheme",
    "creditor_bank_id",
    "remittance_unstructured",
    "remittance_reference",
)


@dataclass(slots=True)
class PaymentModel:
    transfer_id: str
    transfer_type: str
    transaction_datetime: datetime
    amount: Decimal
    currency: str
    purpose_code: str | None
    charge_bearer: str | None
    exchange_rate: Decimal | None
    requested_execution_date: date | None
    debtor_name: str
    debtor_country: str
    debtor_account_scheme: str
    debtor_account_id: str
    debtor_bank_id_scheme: str
    debtor_bank_id: str
    creditor_name: str
    creditor_country: str
    creditor_account_scheme: str
    creditor_account_id: str
    creditor_bank_id_scheme: str
    creditor_bank_id: str
    remittance_unstructured: str | None
    remittance_reference: str | None

    @classmethod
    def from_mapping(cls, data: Mapping[str, str]) -> "PaymentModel":
        transfer_type = _required(data, "transfer_type").upper()
        if transfer_type not in {"DOMESTIC", "CROSS_BORDER"}:
            raise ValueError("transfer_type must be DOMESTIC or CROSS_BORDER")

        charge_bearer = _optional(data, "charge_bearer")
        if charge_bearer is not None:
            charge_bearer = charge_bearer.upper()
            if charge_bearer not in {"DEBT", "CRED", "SHAR"}:
                raise ValueError("charge_bearer must be DEBT, CRED, or SHAR")

        currency = _required(data, "currency").upper()
        if len(currency) != 3:
            raise ValueError("currency must be a 3-letter ISO 4217 code")

        return cls(
            transfer_id=_required(data, "transfer_id"),
            transfer_type=transfer_type,
            transaction_datetime=_parse_datetime(_required(data, "transaction_datetime")),
            amount=_parse_decimal(_required(data, "amount"), field_name="amount"),
            currency=currency,
            purpose_code=_optional(data, "purpose_code"),
            charge_bearer=charge_bearer,
            exchange_rate=_parse_optional_decimal(_optional(data, "exchange_rate"), field_name="exchange_rate"),
            requested_execution_date=_parse_optional_date(
                _optional(data, "requested_execution_date"),
                field_name="requested_execution_date",
            ),
            debtor_name=_required(data, "debtor_name"),
            debtor_country=_parse_country(_required(data, "debtor_country"), field_name="debtor_country"),
            debtor_account_scheme=_required(data, "debtor_account_scheme").upper(),
            debtor_account_id=_required(data, "debtor_account_id"),
            debtor_bank_id_scheme=_required(data, "debtor_bank_id_scheme").upper(),
            debtor_bank_id=_required(data, "debtor_bank_id"),
            creditor_name=_required(data, "creditor_name"),
            creditor_country=_parse_country(_required(data, "creditor_country"), field_name="creditor_country"),
            creditor_account_scheme=_required(data, "creditor_account_scheme").upper(),
            creditor_account_id=_required(data, "creditor_account_id"),
            creditor_bank_id_scheme=_required(data, "creditor_bank_id_scheme").upper(),
            creditor_bank_id=_required(data, "creditor_bank_id"),
            remittance_unstructured=_optional(data, "remittance_unstructured"),
            remittance_reference=_optional(data, "remittance_reference"),
        )


def _required(data: Mapping[str, str], field_name: str) -> str:
    value = (data.get(field_name) or "").strip()
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def _optional(data: Mapping[str, str], field_name: str) -> str | None:
    value = data.get(field_name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _parse_datetime(value: str) -> datetime:
    raw = value.strip()
    try:
        if raw.endswith("Z"):
            return datetime.fromisoformat(f"{raw[:-1]}+00:00")
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("transaction_datetime must be ISO 8601") from exc


def _parse_decimal(value: str, field_name: str) -> Decimal:
    try:
        return Decimal(value.strip())
    except Exception as exc:
        raise ValueError(f"{field_name} must be a valid decimal value") from exc


def _parse_optional_decimal(value: str | None, field_name: str) -> Decimal | None:
    if value is None:
        return None
    return _parse_decimal(value, field_name=field_name)


def _parse_optional_date(value: str | None, field_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _parse_country(value: str, field_name: str) -> str:
    normalized = value.strip().upper()
    if len(normalized) != 2:
        raise ValueError(f"{field_name} must be a 2-letter ISO country code")
    return normalized
