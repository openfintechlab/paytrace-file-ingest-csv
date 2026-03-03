import asyncio
from datetime import datetime as real_datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import src.utilities.FileWatcher as file_watcher_module
from src.utilities.ConfigLoader import ConfigLoader
from src.utilities.FileWatcher import ClaimedFile, FileWatcherAgent


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
