# -*- coding: utf-8 -*-
"""Domain service for parsing payment rows from CSV."""

from __future__ import annotations

from typing import Mapping

from .PaymentModel import PAYMENT_CSV_COLUMNS, PaymentModel


class PaymentProcessor:
    """Parses CSV rows into PaymentModel."""

    def process_row(self, csv_row: list[str] | Mapping[str, str]) -> PaymentModel:
        if isinstance(csv_row, Mapping):
            return PaymentModel.from_mapping(csv_row)

        if len(csv_row) != len(PAYMENT_CSV_COLUMNS):
            raise ValueError(
                f"CSV row has {len(csv_row)} fields but {len(PAYMENT_CSV_COLUMNS)} are required"
            )

        row_mapping = dict(zip(PAYMENT_CSV_COLUMNS, csv_row))
        return PaymentModel.from_mapping(row_mapping)
