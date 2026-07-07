import importlib
from dataclasses import fields
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from src.domain.PaymentModel import PaymentModel

payment_model_module = importlib.import_module("src.domain.PaymentModel")


def test_payment_model_fields_follow_json_schema_order():
    assert [field.name for field in fields(PaymentModel)] == list(payment_model_module.PAYMENT_CSV_COLUMNS)


def test_from_mapping_validates_against_json_schema_and_returns_typed_model():
    payment = PaymentModel.from_mapping(
        {
            "transfer_id": "PTX-0000001",
            "transfer_type": "DOMESTIC",
            "transaction_datetime": "2026-03-03T10:15:30Z",
            "requested_execution_date": "2026-03-04",
            "amount": "2500.00",
            "currency": "AED",
            "purpose_code": "SUPP",
            "charge_bearer": "SHAR",
            "debtor_name": "Sharjah Trading LLC",
            "debtor_country": "AE",
            "debtor_account_scheme": "IBAN",
            "debtor_account_id": "AE070331234567890123456",
            "debtor_bank_id_scheme": "BIC",
            "debtor_bank_id": "SIBUAEAD",
            "creditor_name": "Desert Supplies FZC",
            "creditor_country": "AE",
            "creditor_account_scheme": "IBAN",
            "creditor_account_id": "AE170540123456789012345",
            "creditor_bank_id_scheme": "BIC",
            "creditor_bank_id": "EBILAEAD",
            "remittance_reference": "INV-7843",
        }
    )

    assert payment.transfer_id == "PTX-0000001"
    assert payment.transaction_datetime == datetime(2026, 3, 3, 10, 15, 30, tzinfo=timezone.utc)
    assert payment.requested_execution_date == date(2026, 3, 4)
    assert payment.amount == Decimal("2500.00")
    assert payment.remittance_reference == "INV-7843"


def test_from_mapping_logs_schema_validation_errors(monkeypatch):
    logged_errors: list[tuple[str, tuple[object, ...]]] = []

    def _capture(message, *args, **kwargs):
        logged_errors.append((message, args))

    monkeypatch.setattr(payment_model_module.Logging, "error", _capture)

    with pytest.raises(ValueError, match="requested_execution_date|required property"):
        PaymentModel.from_mapping(
            {
                "transfer_id": "PTX-0000002",
                "transfer_type": "DOMESTIC",
                "transaction_datetime": "2026-03-03T10:15:30Z",
                "amount": "2500.00",
                "currency": "AED",
                "debtor_name": "Sharjah Trading LLC",
                "debtor_country": "AE",
                "debtor_account_scheme": "IBAN",
                "debtor_account_id": "AE070331234567890123456",
                "debtor_bank_id_scheme": "BIC",
                "debtor_bank_id": "SIBUAEAD",
                "creditor_name": "Desert Supplies FZC",
                "creditor_country": "AE",
                "creditor_account_scheme": "IBAN",
                "creditor_account_id": "AE170540123456789012345",
                "creditor_bank_id_scheme": "BIC",
                "creditor_bank_id": "EBILAEAD",
            }
        )

    assert any("Payment schema validation error" in entry[0] for entry in logged_errors)
    assert any("Payment parsing failed" in entry[0] for entry in logged_errors)
