import asyncio
import csv
from threading import Event
from datetime import datetime as real_datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.utilities.FileWatcher as file_watcher_module
from src.utilities.ConfigLoader import ConfigLoader
from src.utilities.FileWatcher import ClaimedFile, FileWatcherAgent
from src.utilities.RabbitMQHelper import RabbitMQShutdownRequested


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
    claimed = ClaimedFile(
        source_name="resume.csv",
        claimed_path=tmp_path / "processing" / "resume.csv",
        fingerprint="file-123",
        size=123,
        mtime_ns=456,
    )

    observed = {"resume_row": None, "checkpoint_deletes": 0}

    monkeypatch.setattr(watcher_agent, "_compute_sha256_streaming", lambda _path: "checksum-1")
    monkeypatch.setattr(watcher_agent, "_registry_get", lambda _file_id: {"status": "processing", "checksum_sha256": "checksum-1"})
    monkeypatch.setattr(watcher_agent, "_registry_mark_started", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(watcher_agent, "_registry_mark_completed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(watcher_agent, "_registry_mark_failed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(watcher_agent, "_checkpoint_get", lambda _file_id: 7)

    def _fake_stream(_path, _file_id, resume_row):
        observed["resume_row"] = resume_row
        return 11

    monkeypatch.setattr(watcher_agent, "_stream_process_csv", _fake_stream)

    def _fake_checkpoint_delete(_file_id):
        observed["checkpoint_deletes"] += 1

    monkeypatch.setattr(watcher_agent, "_checkpoint_delete", _fake_checkpoint_delete)
    monkeypatch.setattr(watcher_agent, "_move_to_archive", lambda _path, suffix=None: watcher_agent.archive_dir / "archived.csv")
    monkeypatch.setattr(watcher_agent, "_emit_processed_event", lambda *_args, **_kwargs: None)

    watcher_agent._process_claimed_file(claimed)

    assert observed["resume_row"] == 7
    assert observed["checkpoint_deletes"] == 1


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

    monkeypatch.setattr(watcher_agent, "_process_csv_row", _capture)
    checkpoints: list[int] = []
    monkeypatch.setattr(watcher_agent, "_checkpoint_upsert", lambda _file_id, row_number: checkpoints.append(row_number))

    processed_rows = watcher_agent._stream_process_csv(csv_file, "file-123", resume_row=0)

    assert processed_rows == 2
    assert observed["row_number"] == 2
    assert observed["file_id"] == "file-123"
    assert observed["row_payload"]["remittance_reference"] == "INV-7843"
    assert observed["row_payload"]["remittance_unstructured"] == "Invoice 7843 - office supplies"
    assert "intermediary_bank_bic" not in observed["row_payload"]
    assert checkpoints == [2]


def test_process_csv_row_reraises_rabbitmq_publish_errors(watcher_agent, monkeypatch, tmp_path):
    file_path = tmp_path / "failed.csv"
    parsed_payment = SimpleNamespace(transfer_type="DOMESTIC", transfer_id="PTX-ERR-1")

    monkeypatch.setattr(watcher_agent._payment_processor, "process_row", lambda _row: parsed_payment)
    monkeypatch.setattr(watcher_agent, "_row_dispatch_get", lambda _transfer_id: None)
    monkeypatch.setattr(watcher_agent, "_row_dispatch_mark_failed", lambda **_kwargs: None)
    monkeypatch.setattr(
        file_watcher_module.RabbitMQHelper,
        "send_p2p_message",
        lambda _queue_name, _message, **_kwargs: (_ for _ in ()).throw(RuntimeError("broker unavailable")),
    )

    with pytest.raises(RuntimeError, match="broker unavailable"):
        watcher_agent._process_csv_row({"transfer_type": "DOMESTIC", "transfer_id": "PTX-ERR-1"}, 2, file_path, file_id="file-123")


def test_process_csv_row_skips_already_published_transfer(monkeypatch, watcher_agent, tmp_path):
    file_path = tmp_path / "already-published.csv"
    parsed_payment = SimpleNamespace(transfer_type="DOMESTIC", transfer_id="PTX-001")
    send_calls: list[str] = []

    monkeypatch.setattr(watcher_agent._payment_processor, "process_row", lambda _row: parsed_payment)
    monkeypatch.setattr(watcher_agent, "_row_dispatch_get", lambda _transfer_id: {"status": "published"})
    monkeypatch.setattr(watcher_agent, "_row_dispatch_mark_published", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("should not record again")))
    monkeypatch.setattr(
        file_watcher_module.RabbitMQHelper,
        "send_p2p_message",
        lambda _queue_name, _message, **_kwargs: send_calls.append("sent"),
    )

    watcher_agent._process_csv_row({"transfer_type": "DOMESTIC", "transfer_id": "PTX-001"}, 2, file_path, file_id="file-123")

    assert send_calls == []


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
