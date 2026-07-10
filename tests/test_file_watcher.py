import asyncio
import csv
import importlib
from threading import Event
from datetime import datetime as real_datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.utilities.FileWatcher as file_watcher_module
from src.utilities.ConfigLoader import ConfigLoader
from src.utilities.FileWatcher import ClaimedFile, FileWatcherAgent
from src.utilities.RabbitMQHelper import RabbitMQShutdownRequested

csv_processor_module = importlib.import_module("src.domain.CSVFileProcessor")


@pytest.fixture
def watcher_agent(tmp_path, monkeypatch):
    def _fake_get(cls, key, default=None):
        overrides = {
            "OFTL_FWCSV_ROOTDIR": str(tmp_path),
            "OFTL_FWCSV_STABILITY_WINDOW_SECONDS": 2,
            "OFTL_FWCSV_STABILITY_PROBE_SECONDS": 1,
            "OFTL_FWCSV_CHECKPOINT_EVERY_ROWS": 2,
        }
        return overrides.get(key, default)

    monkeypatch.setattr(ConfigLoader, "get", classmethod(_fake_get))
    agent = FileWatcherAgent()
    agent._bootstrap_directories()
    return agent


def test_atomic_claim_moves_file_to_processing(watcher_agent):
    inbox_file = watcher_agent.inbox_dir / "batch.csv"
    inbox_file.write_text("a,b\n1,2\n", encoding="utf-8")

    claimed = watcher_agent._atomic_claim(inbox_file)

    assert claimed is not None
    assert claimed.source_name == "batch.csv"
    assert claimed.claimed_path.parent == watcher_agent.processing_dir
    assert claimed.claimed_path.exists()
    assert not inbox_file.exists()

    second_claim = watcher_agent._atomic_claim(inbox_file)
    assert second_claim is None


def test_stability_window_requires_consecutive_unchanged_samples(watcher_agent, monkeypatch):
    stats = iter(
        [
            SimpleNamespace(st_size=100, st_mtime_ns=1),
            SimpleNamespace(st_size=101, st_mtime_ns=2),
            SimpleNamespace(st_size=101, st_mtime_ns=2),
            SimpleNamespace(st_size=101, st_mtime_ns=2),
        ]
    )

    monkeypatch.setattr(watcher_agent, "_stat", lambda _path: next(stats))

    async def _no_sleep(_seconds):
        return None

    monkeypatch.setattr(file_watcher_module.asyncio, "sleep", _no_sleep)

    result = asyncio.run(watcher_agent._is_file_stable(Path("dummy.csv")))
    assert result is True


def test_process_claimed_file_uses_checkpoint_resume_row(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    claimed = ClaimedFile(
        source_name="resume.csv",
        claimed_path=tmp_path / "processing" / "resume.csv",
        fingerprint="file-123",
        size=123,
        mtime_ns=456,
    )

    observed = {"resume_row": None, "checkpoint_deletes": 0}

    monkeypatch.setattr(processor, "_compute_sha256_streaming", lambda _path: "checksum-1")
    monkeypatch.setattr(processor, "_registry_get", lambda _file_id: {"status": "processing", "checksum_sha256": "checksum-1"})
    monkeypatch.setattr(processor, "_registry_mark_started", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(processor, "_registry_mark_completed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(processor, "_registry_mark_failed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(processor, "_checkpoint_get", lambda _file_id: 7)

    def _fake_stream(_path, _file_id, resume_row):
        observed["resume_row"] = resume_row
        return 11

    monkeypatch.setattr(processor, "_stream_process_csv", _fake_stream)

    def _fake_checkpoint_delete(_file_id):
        observed["checkpoint_deletes"] += 1

    monkeypatch.setattr(processor, "_checkpoint_delete", _fake_checkpoint_delete)
    monkeypatch.setattr(watcher_agent, "_move_to_archive", lambda _path, suffix=None: watcher_agent.archive_dir / "archived.csv")
    monkeypatch.setattr(processor, "_archive_file", lambda _path, _suffix=None: watcher_agent.archive_dir / "archived.csv")
    monkeypatch.setattr(processor, "_emit_processed_event", lambda *_args, **_kwargs: None)

    processor.process_claimed_file(claimed)

    assert observed["resume_row"] == 7
    assert observed["checkpoint_deletes"] == 1


def test_process_claimed_file_emits_ev001_error_for_idempotency_skip(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    claimed = ClaimedFile(
        source_name="duplicate.csv",
        claimed_path=tmp_path / "processing" / "duplicate.csv",
        fingerprint="file-123",
        size=123,
        mtime_ns=456,
    )
    archive_path = tmp_path / "archive" / "duplicate.duplicate.csv"
    emitted_events: list[dict[str, object]] = []
    archive_calls: list[tuple[Path, str | None]] = []

    monkeypatch.setattr(processor, "_compute_sha256_streaming", lambda _path: "checksum-1")
    monkeypatch.setattr(
        processor,
        "_registry_get",
        lambda _file_id: {"status": "completed", "checksum_sha256": "checksum-1", "row_count": 6},
    )
    monkeypatch.setattr(processor, "_archive_file", lambda path, suffix=None: archive_calls.append((path, suffix)) or archive_path)
    monkeypatch.setattr(processor, "_registry_mark_started", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not restart")))
    monkeypatch.setattr(processor, "_stream_process_csv", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not reprocess")))
    monkeypatch.setattr(processor, "_emit_processed_event", lambda *args, **kwargs: emitted_events.append({"args": args, "kwargs": kwargs}))

    processor.process_claimed_file(claimed)

    assert archive_calls == [(claimed.claimed_path, "duplicate")]
    assert len(emitted_events) == 1
    event_call = emitted_events[0]
    assert event_call["args"][:4] == (claimed, archive_path, 6, "checksum-1")
    assert event_call["kwargs"] == {"status": "error", "error_message": "Idempotency"}


def test_emit_processed_event_publishes_ev001_topic_message(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    claimed = ClaimedFile(
        source_name="payments.csv",
        claimed_path=tmp_path / "processing" / "payments.csv",
        fingerprint="file-123",
        size=123,
        mtime_ns=456,
    )
    archive_path = tmp_path / "archive" / "payments.20260425T130533Z.csv"
    started_at = real_datetime(2026, 4, 25, 13, 5, 33, 135320, tzinfo=timezone.utc)
    ended_at = real_datetime(2026, 4, 25, 13, 5, 33, 552729, tzinfo=timezone.utc)
    published: dict[str, object] = {}

    def _fake_get(cls, key, default=None):
        overrides = {
            "OFTL_RABITMQ_PUBEVENT_EV001": "files.csv.loaded",
            "OFTL_RABITMQ_PUBEVENT_EXCHANGE": "paytrace.events",
        }
        return overrides.get(key, default)

    def _capture_publish(exchange_name, routing_key, message, exchange_type=None, **kwargs):
        published["exchange_name"] = exchange_name
        published["routing_key"] = routing_key
        published["message"] = message
        published["exchange_type"] = exchange_type
        published["kwargs"] = kwargs
        return True

    monkeypatch.setattr(ConfigLoader, "get", classmethod(_fake_get))
    monkeypatch.setattr(csv_processor_module.uuid, "uuid4", lambda: "event-123")
    monkeypatch.setattr(csv_processor_module.RabbitMQHelper, "publish_message", _capture_publish)

    processor._emit_processed_event(
        claimed,
        archive_path,
        row_count=6,
        checksum="checksum-123",
        started_at=started_at,
        ended_at=ended_at,
    )

    assert published["exchange_name"] == "paytrace.events"
    assert published["routing_key"] == "files.csv.loaded"
    assert published["exchange_type"] == "topic"
    event = published["message"]
    assert event["event_id"] == "event-123"
    assert event["event_code"] == "EV001"
    assert event["event_type"] == "files.csv.loaded"
    assert event["event_version"] == "1.0"
    assert event["source"] == "paytrace-file-ingest-csv"
    assert event["correlation_id"] == "checksum-123"
    assert event["causation_id"] == "file-123"
    assert event["payload"] == {
        "event": "file_processed",
        "file_id": "file-123",
        "filename": "payments.csv",
        "archive_path": str(archive_path),
        "checksum_sha256": "checksum-123",
        "row_count": 6,
        "started_at": "2026-04-25T13:05:33.135320+00:00",
        "ended_at": "2026-04-25T13:05:33.552729+00:00",
    }
    assert published["kwargs"] == {
        "correlation_id": "checksum-123",
        "message_id": "event-123",
        "headers": {"event_code": "EV001", "file_id": "file-123"},
    }


def test_emit_processed_event_publishes_ev001_error_status(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    claimed = ClaimedFile(
        source_name="payments.csv",
        claimed_path=tmp_path / "processing" / "payments.csv",
        fingerprint="file-123",
        size=123,
        mtime_ns=456,
    )
    archive_path = tmp_path / "archive" / "payments.duplicate.csv"
    event_ts = real_datetime(2026, 4, 25, 13, 5, 33, 135320, tzinfo=timezone.utc)
    published: dict[str, object] = {}

    def _fake_get(cls, key, default=None):
        overrides = {
            "OFTL_RABITMQ_PUBEVENT_EV001": "files.csv.loaded",
            "OFTL_RABITMQ_PUBEVENT_EXCHANGE": "paytrace.events",
        }
        return overrides.get(key, default)

    def _capture_publish(exchange_name, routing_key, message, exchange_type=None, **kwargs):
        published["exchange_name"] = exchange_name
        published["routing_key"] = routing_key
        published["message"] = message
        published["exchange_type"] = exchange_type
        published["kwargs"] = kwargs
        return True

    monkeypatch.setattr(ConfigLoader, "get", classmethod(_fake_get))
    monkeypatch.setattr(csv_processor_module.uuid, "uuid4", lambda: "event-123")
    monkeypatch.setattr(csv_processor_module.RabbitMQHelper, "publish_message", _capture_publish)

    processor._emit_processed_event(
        claimed,
        archive_path,
        row_count=6,
        checksum="checksum-123",
        started_at=event_ts,
        ended_at=event_ts,
        status="error",
        error_message="Idempotency",
    )

    assert published["exchange_name"] == "paytrace.events"
    assert published["routing_key"] == "files.csv.loaded"
    assert published["exchange_type"] == "topic"
    event = published["message"]
    assert event["event_code"] == "EV001"
    assert event["event_type"] == "files.csv.loaded"
    assert event["payload"] == {
        "event": "file_processed",
        "file_id": "file-123",
        "filename": "payments.csv",
        "archive_path": str(archive_path),
        "checksum_sha256": "checksum-123",
        "row_count": 6,
        "started_at": "2026-04-25T13:05:33.135320+00:00",
        "ended_at": "2026-04-25T13:05:33.135320+00:00",
        "status": "error",
        "error_message": "Idempotency",
    }
    assert published["kwargs"] == {
        "correlation_id": "checksum-123",
        "message_id": "event-123",
        "headers": {"event_code": "EV001", "file_id": "file-123"},
    }


def test_emit_row_failed_event_publishes_ev002_topic_message(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    file_path = tmp_path / "processing" / "payments.csv"
    published: dict[str, object] = {}
    uuid_values = iter(["event-456", "correlation-789"])

    def _fake_get(cls, key, default=None):
        overrides = {
            "OFTL_RABITMQ_PUBEVENT_EV002": "files.csv.row.failed",
            "OFTL_RABITMQ_PUBEVENT_EXCHANGE": "paytrace.events",
        }
        return overrides.get(key, default)

    def _capture_publish(exchange_name, routing_key, message, exchange_type=None, **kwargs):
        published["exchange_name"] = exchange_name
        published["routing_key"] = routing_key
        published["message"] = message
        published["exchange_type"] = exchange_type
        published["kwargs"] = kwargs
        return True

    monkeypatch.setattr(ConfigLoader, "get", classmethod(_fake_get))
    monkeypatch.setattr(csv_processor_module.uuid, "uuid4", lambda: next(uuid_values))
    monkeypatch.setattr(csv_processor_module.RabbitMQHelper, "publish_message", _capture_publish)

    processor._emit_row_failed_event(
        transfer_id="PTX-ERR-1",
        file_id="file-123",
        row_number=2,
        file_path=file_path,
        error_message="validation failed",
        failure_reason="row_processing_failed",
    )

    assert published["exchange_name"] == "paytrace.events"
    assert published["routing_key"] == "files.csv.row.failed"
    assert published["exchange_type"] == "topic"
    event = published["message"]
    assert event["event_id"] == "event-456"
    assert event["event_code"] == "EV002"
    assert event["event_type"] == "files.csv.row.failed"
    assert event["event_version"] == "1.0"
    assert event["source"] == "paytrace-file-ingest-csv"
    assert event["correlation_id"] == "correlation-789"
    assert event["causation_id"] == "PTX-ERR-1"
    assert event["payload"] == {
        "event": "row_failed",
        "file_id": "file-123",
        "filename": "payments.csv",
        "row_number": 2,
        "transfer_id": "PTX-ERR-1",
        "failure_reason": "row_processing_failed",
        "error_message": "validation failed",
    }
    assert published["kwargs"] == {
        "correlation_id": "correlation-789",
        "message_id": "event-456",
        "headers": {"event_code": "EV002", "file_id": "file-123", "row_number": 2},
    }


def test_move_to_archive_creates_date_partitioned_path(watcher_agent, monkeypatch):
    source = watcher_agent.processing_dir / "sample.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")

    class _FixedDateTime:
        @staticmethod
        def now(_tz=None):
            return real_datetime(2026, 1, 15, 8, 30, 45, tzinfo=timezone.utc)

    monkeypatch.setattr(file_watcher_module, "datetime", _FixedDateTime)

    archived = watcher_agent._move_to_archive(source)

    assert archived.exists()
    assert not source.exists()
    assert archived.parent == watcher_agent.archive_dir / "2026" / "01" / "15"
    assert archived.name.startswith("sample.20260115T083045Z")
    assert archived.suffix == ".csv"


def test_stream_process_csv_uses_file_header_for_legacy_column_layout(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    csv_file = tmp_path / "legacy.csv"
    with csv_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
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
            ]
        )
        writer.writerow(
            [
                "PTX-0000001",
                "DOMESTIC",
                "2026-03-03T10:15:30Z",
                "2500.00",
                "AED",
                "SUPP",
                "SHAR",
                "",
                "2026-03-04",
                "Sharjah Trading LLC",
                "AE",
                "IBAN",
                "AE070331234567890123456",
                "OTHER",
                "SIBUAEAD",
                "Desert Supplies FZC",
                "AE",
                "IBAN",
                "AE170540123456789012345",
                "OTHER",
                "EBILAEAD",
                "Invoice 7843 - office supplies",
                "INV-7843",
            ]
        )

    observed: dict[str, object] = {}

    def _capture(row_payload, row_number, _file_path, *, file_id=None):
        observed["row_payload"] = row_payload
        observed["row_number"] = row_number
        observed["file_id"] = file_id

    monkeypatch.setattr(processor, "_process_csv_row", _capture)
    checkpoints: list[int] = []
    monkeypatch.setattr(processor, "_checkpoint_upsert", lambda _file_id, row_number: checkpoints.append(row_number))

    processed_rows = processor._stream_process_csv(csv_file, "file-123", resume_row=0)

    assert processed_rows == 1
    assert observed["row_number"] == 2
    assert observed["file_id"] == "file-123"
    assert observed["row_payload"]["remittance_reference"] == "INV-7843"
    assert observed["row_payload"]["remittance_unstructured"] == "Invoice 7843 - office supplies"
    assert "intermediary_bank_bic" not in observed["row_payload"]
    assert checkpoints == [2]


def test_process_claimed_file_marks_failed_with_data_row_count(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    claimed_path = tmp_path / "processing" / "failed.csv"
    claimed_path.parent.mkdir(parents=True, exist_ok=True)
    claimed_path.write_text("transfer_id,transfer_type\nPTX-1,DOMESTIC\n", encoding="utf-8")
    claimed = ClaimedFile(
        source_name="failed.csv",
        claimed_path=claimed_path,
        fingerprint="file-123",
        size=claimed_path.stat().st_size,
        mtime_ns=claimed_path.stat().st_mtime_ns,
    )
    failed_registry: dict[str, object] = {}

    monkeypatch.setattr(processor, "_compute_sha256_streaming", lambda _path: "checksum-1")
    monkeypatch.setattr(processor, "_registry_get", lambda _file_id: {"status": "processing", "checksum_sha256": "checksum-1"})
    monkeypatch.setattr(processor, "_registry_mark_started", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(processor, "_checkpoint_get", lambda _file_id: 0)
    monkeypatch.setattr(processor, "_checkpoint_delete", lambda _file_id: (_ for _ in ()).throw(AssertionError("should not delete checkpoint")))
    monkeypatch.setattr(processor, "_process_csv_row", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish failed")))
    monkeypatch.setattr(processor, "_move_to_error", lambda _path: tmp_path / "error" / "failed.csv")

    def _capture_failed(_claimed, row_count, error_message):
        failed_registry["row_count"] = row_count
        failed_registry["error_message"] = error_message

    monkeypatch.setattr(processor, "_registry_mark_failed", _capture_failed)

    with pytest.raises(RuntimeError, match="publish failed"):
        processor.process_claimed_file(claimed)

    assert failed_registry == {"row_count": 1, "error_message": "publish failed"}


def test_row_dispatch_writes_constraint_compatible_statuses(watcher_agent, monkeypatch):
    processor = watcher_agent._csv_file_processor
    updates: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        csv_processor_module.DBHelper,
        "execute_update",
        lambda query, params=None: updates.append((query, params or {})) or 1,
    )

    processor._row_dispatch_mark_published(
        transfer_id="PTX-001",
        file_id="file-123",
        row_number=2,
        request_queue="CSV.PAYMENTS.DOMESTIC.REQ",
    )
    processor._row_dispatch_mark_failed(
        transfer_id="PTX-002",
        file_id="file-123",
        row_number=3,
        error_message="validation failed",
    )

    assert "'PUBLISHED'" in updates[0][0]
    assert "'FAILED'" in updates[1][0]


def test_process_csv_row_reraises_rabbitmq_publish_errors(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    file_path = tmp_path / "failed.csv"
    parsed_payment = SimpleNamespace(transfer_type="DOMESTIC", transfer_id="PTX-ERR-1")

    monkeypatch.setattr(processor._payment_processor, "process_row", lambda _row: parsed_payment)
    monkeypatch.setattr(processor, "_row_dispatch_get", lambda _transfer_id: None)
    monkeypatch.setattr(processor, "_row_dispatch_mark_failed", lambda **_kwargs: None)
    monkeypatch.setattr(processor, "_emit_row_failed_event", lambda **_kwargs: None)
    monkeypatch.setattr(
        csv_processor_module.RabbitMQHelper,
        "send_p2p_message",
        lambda _queue_name, _message, **_kwargs: (_ for _ in ()).throw(RuntimeError("broker unavailable")),
    )

    with pytest.raises(RuntimeError, match="broker unavailable"):
        processor._process_csv_row({"transfer_type": "DOMESTIC", "transfer_id": "PTX-ERR-1"}, 2, file_path, file_id="file-123")


def test_process_csv_row_skips_already_published_transfer(monkeypatch, watcher_agent, tmp_path):
    processor = watcher_agent._csv_file_processor
    file_path = tmp_path / "already-published.csv"
    parsed_payment = SimpleNamespace(transfer_type="DOMESTIC", transfer_id="PTX-001")
    send_calls: list[str] = []
    emitted_events: list[dict[str, object]] = []

    monkeypatch.setattr(processor._payment_processor, "process_row", lambda _row: parsed_payment)
    monkeypatch.setattr(processor, "_row_dispatch_get", lambda _transfer_id: {"status": "published"})
    monkeypatch.setattr(processor, "_row_dispatch_mark_published", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("should not record again")))
    monkeypatch.setattr(processor, "_emit_row_failed_event", lambda **kwargs: emitted_events.append(kwargs))
    monkeypatch.setattr(
        csv_processor_module.RabbitMQHelper,
        "send_p2p_message",
        lambda _queue_name, _message, **_kwargs: send_calls.append("sent"),
    )

    processor._process_csv_row({"transfer_type": "DOMESTIC", "transfer_id": "PTX-001"}, 2, file_path, file_id="file-123")

    assert send_calls == []
    assert emitted_events == [
        {
            "transfer_id": "PTX-001",
            "file_id": "file-123",
            "row_number": 2,
            "file_path": file_path,
            "error_message": "Row rejected because transfer signature was already published.",
            "failure_reason": "redundant_signature",
        }
    ]


def test_process_csv_row_emits_ev002_for_processing_failure(watcher_agent, monkeypatch, tmp_path):
    processor = watcher_agent._csv_file_processor
    file_path = tmp_path / "failed.csv"
    emitted_events: list[dict[str, object]] = []

    monkeypatch.setattr(
        processor._payment_processor,
        "process_row",
        lambda _row: (_ for _ in ()).throw(ValueError("invalid amount")),
    )
    monkeypatch.setattr(processor, "_row_dispatch_mark_failed", lambda **_kwargs: None)
    monkeypatch.setattr(processor, "_emit_row_failed_event", lambda **kwargs: emitted_events.append(kwargs))

    with pytest.raises(ValueError, match="invalid amount"):
        processor._process_csv_row({"transfer_id": "PTX-ERR-2"}, 3, file_path, file_id="file-456")

    assert emitted_events == [
        {
            "transfer_id": "PTX-ERR-2",
            "file_id": "file-456",
            "row_number": 3,
            "file_path": file_path,
            "error_message": "invalid amount",
            "failure_reason": "row_processing_failed",
        }
    ]


def test_validate_dependencies_fails_fast_when_database_is_unavailable(monkeypatch, watcher_agent):
    monkeypatch.setattr(file_watcher_module.DBHelper, "initialize_connection", classmethod(lambda cls: False))

    with pytest.raises(RuntimeError, match="Database is required"):
        watcher_agent._validate_dependencies()


def test_validate_dependencies_requires_all_tables(monkeypatch, watcher_agent):
    monkeypatch.setattr(file_watcher_module.DBHelper, "initialize_connection", classmethod(lambda cls: True))

    def _fake_select(_query, params=None):
        qualified_name = params["qualified_name"]
        if qualified_name.endswith("oftl_fwcsv_row_dispatch"):
            return [{"relation_name": None}]
        return [{"relation_name": qualified_name}]

    monkeypatch.setattr(file_watcher_module.DBHelper, "execute_select", staticmethod(_fake_select))

    with pytest.raises(RuntimeError, match="oftl_fwcsv_row_dispatch"):
        watcher_agent._validate_required_tables()


def test_process_claimed_with_lock_skips_duplicate_active_path(watcher_agent, monkeypatch, tmp_path):
    claimed = ClaimedFile(
        source_name="duplicate.csv",
        claimed_path=tmp_path / "processing" / "duplicate.csv",
        fingerprint="file-123",
        size=123,
        mtime_ns=456,
    )
    claimed.claimed_path.parent.mkdir(parents=True, exist_ok=True)
    claimed.claimed_path.write_text("a,b\n1,2\n", encoding="utf-8")

    started = Event()
    release = Event()
    observed: list[str] = []

    def _fake_process(_claimed):
        observed.append("started")
        started.set()
        release.wait(timeout=2)
        observed.append("finished")

    monkeypatch.setattr(watcher_agent, "_process_claimed_file", _fake_process)

    async def _run_test():
        first = asyncio.create_task(watcher_agent._process_claimed_with_lock(claimed))
        await asyncio.to_thread(started.wait, 2)
        await watcher_agent._process_claimed_with_lock(claimed)
        release.set()
        await first

    asyncio.run(_run_test())

    assert observed == ["started", "finished"]


def test_worker_loop_propagates_rabbitmq_shutdown(monkeypatch, watcher_agent):
    async def _raise_shutdown(_path_str):
        raise RabbitMQShutdownRequested("RabbitMQ connection failed after configured retries. Exiting application with code 99.")

    monkeypatch.setattr(watcher_agent, "_handle_candidate", _raise_shutdown)

    async def _run_test():
        worker = asyncio.create_task(watcher_agent._worker_loop(1))
        await watcher_agent._queue.put("/tmp/example.csv")
        with pytest.raises(RabbitMQShutdownRequested):
            await worker

    asyncio.run(_run_test())
