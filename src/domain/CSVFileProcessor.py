# -*- coding: utf-8 -*-
"""Domain service for processing claimed CSV payment files."""

from __future__ import annotations

import csv
import hashlib
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from utilities.ConfigLoader import ConfigLoader
    from utilities.DBHelper import DBHelper
    from utilities.Logging import Logging
    from utilities.RabbitMQHelper import RabbitMQHelper
except ModuleNotFoundError:  # pragma: no cover
    from src.utilities.ConfigLoader import ConfigLoader
    from src.utilities.DBHelper import DBHelper
    from src.utilities.Logging import Logging
    from src.utilities.RabbitMQHelper import RabbitMQHelper

from .PaymentProcessor import PaymentProcessor

if TYPE_CHECKING:
    from utilities.FileWatcher import ClaimedFile


ArchiveFile = Callable[[Path, str | None], Path]
MoveToError = Callable[[Path], Path]


class CSVFileProcessor:
    """Processes a claimed payment CSV file and dispatches its payment rows."""

    def __init__(
        self,
        *,
        archive_file: ArchiveFile,
        move_to_error: MoveToError,
        file_encoding: str | None = None,
        payment_processor: PaymentProcessor | None = None,
    ) -> None:
        self._archive_file = archive_file
        self._move_to_error = move_to_error
        self.file_encoding = file_encoding or str(ConfigLoader.get("OFTL_FWCSV_FILE_ENCODING", "utf-8"))
        self._payment_processor = payment_processor or PaymentProcessor()

    def process_claimed_file(self, claimed: "ClaimedFile") -> None:
        checksum = self._compute_sha256_streaming(claimed.claimed_path)
        existing = self._registry_get(claimed.fingerprint)

        if existing and existing.get("checksum_sha256") and existing["checksum_sha256"] != checksum:
            Logging.warning_context(
                "Checksum mismatch detected. Resetting checkpoint.",
                file_id=claimed.fingerprint,
                filename=claimed.source_name,
            )
            self._checkpoint_delete(claimed.fingerprint)

        if existing and existing.get("status") == "completed" and existing.get("checksum_sha256") == checksum:
            Logging.info_context(
                "Idempotency skip for completed file.",
                file_id=claimed.fingerprint,
                filename=claimed.source_name,
            )
            archived_path = self._archive_file(claimed.claimed_path, "duplicate")
            event_ts = datetime.now(timezone.utc)
            self._emit_processed_event(
                claimed,
                archived_path,
                int(existing.get("row_count", 0) or 0),
                checksum,
                event_ts,
                event_ts,
                status="error",
                error_message="Idempotency",
            )
            return

        start_ts = datetime.now(timezone.utc)
        self._registry_mark_started(claimed, checksum, start_ts)

        rows_processed = 0

        try:
            resume_row = self._checkpoint_get(claimed.fingerprint)
            rows_processed = self._stream_process_csv(claimed.claimed_path, claimed.fingerprint, resume_row)
            self._checkpoint_delete(claimed.fingerprint)
            archived_path = self._archive_file(claimed.claimed_path, None)
            end_ts = datetime.now(timezone.utc)
            self._registry_mark_completed(claimed, rows_processed, checksum, end_ts)
            self._emit_processed_event(claimed, archived_path, rows_processed, checksum, start_ts, end_ts)
        except Exception as exc:
            self._registry_mark_failed(claimed, rows_processed, str(exc))
            self._move_to_error(claimed.claimed_path)
            raise

    def _stream_process_csv(self, file_path: Path, file_id: str, resume_row: int) -> int:
        row_number = 0
        Logging.info_context("Starting processing file.", file_id=file_id, file_path=str(file_path), resume_row=resume_row)
        with file_path.open("r", encoding=self.file_encoding, newline="") as handle:
            reader = csv.reader(handle)
            header: list[str] | None = None
            for row in reader:
                row_number += 1
                if row_number == 1:
                    header = [column.strip() for column in row]
                    Logging.info_context("Skipping header row.", file_id=file_id, file_path=str(file_path))
                    continue

                if row_number <= resume_row:
                    continue

                row_payload: list[str] | dict[str, str] = row
                if header is not None:
                    if len(row) != len(header):
                        raise ValueError(
                            f"CSV row has {len(row)} fields but header defines {len(header)} columns"
                        )
                    row_payload = dict(zip(header, row))

                self._process_csv_row(row_payload, row_number, file_path, file_id=file_id)
                self._checkpoint_upsert(file_id, row_number)

        Logging.info_context("Completed processing file.", file_id=file_id, file_path=str(file_path), total_rows=row_number)
        return row_number

    def _process_csv_row(
        self,
        row: list[str] | dict[str, str],
        row_number: int,
        file_path: Path,
        *,
        file_id: str | None = None,
    ) -> None:
        try:
            parsed_payment = self._payment_processor.process_row(row)
            transfer_id = str(parsed_payment.transfer_id)
            request_queue = ""
            if parsed_payment.transfer_type == "DOMESTIC":
                request_queue = ConfigLoader.get("OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE", "CSV.PAYMENTS.DOMESTIC.REQ")
            elif parsed_payment.transfer_type == "CROSS_BORDER":
                request_queue = ConfigLoader.get("OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE", "CSV.PAYMENTS.CROSS_BORDER.REQ")
            else:
                raise ValueError(f"Unsupported transfer type: {parsed_payment.transfer_type}")

            if not request_queue:
                raise ValueError("RabbitMQ request queue is not configured for the parsed payment.")

            existing_dispatch = self._row_dispatch_get(transfer_id)
            if existing_dispatch and existing_dispatch.get("status") == "published":
                error_message = "Row rejected because transfer signature was already published."
                Logging.info_context(
                    "Skipping already published payment row.",
                    transfer_id=transfer_id,
                    file_id=file_id or "unknown",
                    row_number=row_number,
                    request_queue=request_queue,
                )
                self._emit_row_failed_event(
                    transfer_id=transfer_id,
                    file_id=file_id or "",
                    row_number=row_number,
                    file_path=file_path,
                    error_message=error_message,
                    failure_reason="redundant_signature",
                )
                return

            RabbitMQHelper.send_p2p_message(
                request_queue,
                parsed_payment,
                correlation_id=transfer_id,
                message_id=transfer_id,
                headers={"transfer_id": transfer_id, "file_id": file_id or "", "row_number": row_number},
            )
            self._row_dispatch_mark_published(
                transfer_id=transfer_id,
                file_id=file_id or "",
                row_number=row_number,
                request_queue=request_queue,
            )

            _ = (parsed_payment, row_number, file_path)
        except Exception as exc:
            transfer_id = ""
            if isinstance(row, dict):
                transfer_id = str(row.get("transfer_id", "") or "")
            if transfer_id:
                self._row_dispatch_mark_failed(
                    transfer_id=transfer_id,
                    file_id=file_id or "",
                    row_number=row_number,
                    error_message=str(exc),
                )
            self._emit_row_failed_event(
                transfer_id=transfer_id,
                file_id=file_id or "",
                row_number=row_number,
                file_path=file_path,
                error_message=str(exc),
                failure_reason="row_processing_failed",
            )
            Logging.error_context(
                "Error processing payment row.",
                transfer_id=transfer_id or "unknown",
                file_id=file_id or "unknown",
                file_path=str(file_path),
                row_number=row_number,
                error=str(exc),
            )
            raise

    @staticmethod
    def _compute_sha256_streaming(file_path: Path) -> str:
        digest = hashlib.sha256()
        with file_path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _registry_get(self, file_id: str) -> dict[str, Any] | None:
        rows = DBHelper.execute_select(
            "SELECT file_id, status, checksum_sha256, row_count FROM oftl_fwcsv_registry WHERE file_id = :file_id",
            {"file_id": file_id},
        )
        return rows[0] if rows else None

    def _registry_mark_started(self, claimed: "ClaimedFile", checksum: str, started_at: datetime) -> None:
        DBHelper.execute_update(
            """
            INSERT INTO oftl_fwcsv_registry (
                file_id, filename, file_size, mtime_ns, checksum_sha256, status, started_at, updated_at
            ) VALUES (
                :file_id, :filename, :file_size, :mtime_ns, :checksum_sha256, 'processing', :started_at, NOW()
            )
            ON CONFLICT (file_id) DO UPDATE
            SET
                filename = EXCLUDED.filename,
                file_size = EXCLUDED.file_size,
                mtime_ns = EXCLUDED.mtime_ns,
                checksum_sha256 = EXCLUDED.checksum_sha256,
                status = 'processing',
                started_at = EXCLUDED.started_at,
                error_message = NULL,
                updated_at = NOW()
            """,
            {
                "file_id": claimed.fingerprint,
                "filename": claimed.source_name,
                "file_size": claimed.size,
                "mtime_ns": claimed.mtime_ns,
                "checksum_sha256": checksum,
                "started_at": started_at,
            },
        )

    def _registry_mark_completed(
        self,
        claimed: "ClaimedFile",
        row_count: int,
        checksum: str,
        ended_at: datetime,
    ) -> None:
        DBHelper.execute_update(
            """
            UPDATE oftl_fwcsv_registry
            SET
                status = 'completed',
                row_count = :row_count,
                checksum_sha256 = :checksum,
                ended_at = :ended_at,
                updated_at = NOW()
            WHERE file_id = :file_id
            """,
            {
                "file_id": claimed.fingerprint,
                "row_count": row_count,
                "checksum": checksum,
                "ended_at": ended_at,
            },
        )

    def _registry_mark_failed(self, claimed: "ClaimedFile", row_count: int, error_message: str) -> None:
        DBHelper.execute_update(
            """
            UPDATE oftl_fwcsv_registry
            SET
                status = 'failed',
                row_count = :row_count,
                error_message = :error_message,
                ended_at = NOW(),
                updated_at = NOW()
            WHERE file_id = :file_id
            """,
            {
                "file_id": claimed.fingerprint,
                "row_count": row_count,
                "error_message": error_message[:2000],
            },
        )

    def _checkpoint_get(self, file_id: str) -> int:
        rows = DBHelper.execute_select(
            "SELECT row_number FROM oftl_fwcsv_checkpoint WHERE file_id = :file_id",
            {"file_id": file_id},
        )
        if not rows:
            return 0
        return int(rows[0].get("row_number", 0) or 0)

    def _checkpoint_upsert(self, file_id: str, row_number: int) -> None:
        DBHelper.execute_update(
            """
            INSERT INTO oftl_fwcsv_checkpoint (file_id, row_number, updated_at)
            VALUES (:file_id, :row_number, NOW())
            ON CONFLICT (file_id) DO UPDATE
            SET row_number = EXCLUDED.row_number,
                updated_at = NOW()
            """,
            {"file_id": file_id, "row_number": row_number},
        )

    def _checkpoint_delete(self, file_id: str) -> None:
        DBHelper.execute_delete(
            "DELETE FROM oftl_fwcsv_checkpoint WHERE file_id = :file_id",
            {"file_id": file_id},
        )

    def _row_dispatch_get(self, transfer_id: str) -> dict[str, Any] | None:
        rows = DBHelper.execute_select(
            """
            SELECT transfer_id, file_id, row_number, request_queue, status, published_at, error_message
            FROM oftl_fwcsv_row_dispatch
            WHERE transfer_id = :transfer_id
            """,
            {"transfer_id": transfer_id},
        )
        return rows[0] if rows else None

    def _row_dispatch_mark_published(
        self,
        *,
        transfer_id: str,
        file_id: str,
        row_number: int,
        request_queue: str,
    ) -> None:
        DBHelper.execute_update(
            """
            INSERT INTO oftl_fwcsv_row_dispatch (
                transfer_id, file_id, row_number, request_queue, status, published_at, updated_at, error_message
            ) VALUES (
                :transfer_id, :file_id, :row_number, :request_queue, 'published', NOW(), NOW(), NULL
            )
            ON CONFLICT (transfer_id) DO UPDATE
            SET
                file_id = EXCLUDED.file_id,
                row_number = EXCLUDED.row_number,
                request_queue = EXCLUDED.request_queue,
                status = 'published',
                published_at = NOW(),
                updated_at = NOW(),
                error_message = NULL
            """,
            {
                "transfer_id": transfer_id,
                "file_id": file_id,
                "row_number": row_number,
                "request_queue": request_queue,
            },
        )

    def _row_dispatch_mark_failed(
        self,
        *,
        transfer_id: str,
        file_id: str,
        row_number: int,
        error_message: str,
    ) -> None:
        DBHelper.execute_update(
            """
            INSERT INTO oftl_fwcsv_row_dispatch (
                transfer_id, file_id, row_number, request_queue, status, published_at, updated_at, error_message
            ) VALUES (
                :transfer_id, :file_id, :row_number, '', 'failed', NULL, NOW(), :error_message
            )
            ON CONFLICT (transfer_id) DO UPDATE
            SET
                file_id = EXCLUDED.file_id,
                row_number = EXCLUDED.row_number,
                status = 'failed',
                updated_at = NOW(),
                error_message = EXCLUDED.error_message
            """,
            {
                "transfer_id": transfer_id,
                "file_id": file_id,
                "row_number": row_number,
                "error_message": error_message[:2000],
            },
        )

    def _emit_processed_event(
        self,
        claimed: "ClaimedFile",
        archive_path: Path,
        row_count: int,
        checksum: str,
        started_at: datetime,
        ended_at: datetime,
        status: str | None = None,
        error_message: str | None = None,
    ) -> None:
        routing_key = str(ConfigLoader.get("OFTL_RABITMQ_PUBEVENT_EV001", "files.csv.loaded")).strip()
        if not routing_key:
            raise ValueError("OFTL_RABITMQ_PUBEVENT_EV001 must be configured for EV001 publishing.")

        event_id = str(uuid.uuid4())
        event = {
            "event_id": event_id,
            "event_code": "EV001",
            "event_type": routing_key,
            "event_version": "1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "paytrace-file-ingest-csv",
            "correlation_id": checksum,
            "causation_id": claimed.fingerprint,
            "payload": {
                "event": "file_processed",
                "file_id": claimed.fingerprint,
                "filename": claimed.source_name,
                "archive_path": str(archive_path),
                "checksum_sha256": checksum,
                "row_count": row_count,
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
            },
        }
        if status is not None:
            event["payload"]["status"] = status
        if error_message is not None:
            event["payload"]["error_message"] = error_message[:2000]

        exchange_name = str(ConfigLoader.get("OFTL_RABITMQ_PUBEVENT_EXCHANGE", "paytrace.events")).strip()
        if not exchange_name:
            raise ValueError("OFTL_RABITMQ_PUBEVENT_EXCHANGE must be configured for EV001 publishing.")

        RabbitMQHelper.publish_message(
            exchange_name,
            routing_key,
            event,
            exchange_type="topic",
            correlation_id=checksum,
            message_id=event_id,
            headers={"event_code": "EV001", "file_id": claimed.fingerprint},
        )
        Logging.info(f"Event with ID: {event_id} and code: {event['event_code']} published to topic: {routing_key} ")

    def _emit_row_failed_event(
        self,
        *,
        transfer_id: str,
        file_id: str,
        row_number: int,
        file_path: Path,
        error_message: str,
        failure_reason: str,
    ) -> None:
        routing_key = str(ConfigLoader.get("OFTL_RABITMQ_PUBEVENT_EV002", "files.csv.row.failed")).strip()
        if not routing_key:
            raise ValueError("OFTL_RABITMQ_PUBEVENT_EV002 must be configured for EV002 publishing.")

        exchange_name = str(ConfigLoader.get("OFTL_RABITMQ_PUBEVENT_EXCHANGE", "paytrace.events")).strip()
        if not exchange_name:
            raise ValueError("OFTL_RABITMQ_PUBEVENT_EXCHANGE must be configured for EV002 publishing.")

        event_id = str(uuid.uuid4())
        correlation_id = str(uuid.uuid4())
        event = {
            "event_id": event_id,
            "event_code": "EV002",
            "event_type": routing_key,
            "event_version": "1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "paytrace-file-ingest-csv",
            "correlation_id": correlation_id,
            "causation_id": transfer_id or file_id,
            "payload": {
                "event": "row_failed",
                "file_id": file_id,
                "filename": file_path.name,
                "row_number": row_number,
                "transfer_id": transfer_id,
                "failure_reason": failure_reason,
                "error_message": error_message[:2000],
            },
        }

        RabbitMQHelper.publish_message(
            exchange_name,
            routing_key,
            event,
            exchange_type="topic",
            correlation_id=correlation_id,
            message_id=event_id,
            headers={"event_code": "EV002", "file_id": file_id, "row_number": row_number},
        )
        Logging.info(f"Event with ID: {event_id} and code: {event['event_code']} published to topic: {routing_key} ")
