# -*- coding: utf-8 -*-
"""Cross-platform CSV file watcher and streaming ingest utility."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from domain import CSVFileProcessor
except ModuleNotFoundError:  # pragma: no cover
    from src.domain import CSVFileProcessor

from .ConfigLoader import ConfigLoader
from .DBHelper import DBHelper
from .Logging import Logging
from .RabbitMQHelper import RabbitMQHelper, RabbitMQShutdownRequested
try:
    from watchfiles import Change, awatch

    WATCHFILES_AVAILABLE = True
except Exception:  # pragma: no cover
    Change = None
    awatch = None
    WATCHFILES_AVAILABLE = False


@dataclass(slots=True)
class ClaimedFile:
    source_name: str
    claimed_path: Path
    fingerprint: str
    size: int
    mtime_ns: int


class FileWatcherAgent:
    """Hybrid watcher/scanner with robust claim, checkpoint, and archiving semantics."""
    _REQUIRED_TABLES = (
        "oftl_fwcsv_registry",
        "oftl_fwcsv_checkpoint",
        "oftl_fwcsv_row_dispatch",
    )

    def __init__(self) -> None:
        self.root_dir = Path(ConfigLoader.get("OFTL_FWCSV_ROOTDIR", "./fwcsv")).resolve()
        self.inbox_dir = self.root_dir / "inbox"
        self.processing_dir = self.root_dir / "processing"
        self.archive_dir = self.root_dir / "archive"
        self.error_dir = self.root_dir / "error"

        self.scan_interval_seconds = float(ConfigLoader.get("OFTL_FWCSV_SCAN_INTERVAL_SECONDS", 10))
        self.stability_window_seconds = float(ConfigLoader.get("OFTL_FWCSV_STABILITY_WINDOW_SECONDS", 5))
        self.stability_probe_seconds = float(ConfigLoader.get("OFTL_FWCSV_STABILITY_PROBE_SECONDS", 1))
        self.claim_retry_seconds = float(ConfigLoader.get("OFTL_FWCSV_CLAIM_RETRY_SECONDS", 2))
        self.claim_max_wait_seconds = float(ConfigLoader.get("OFTL_FWCSV_CLAIM_MAX_WAIT_SECONDS", 60))
        self.worker_count = max(1, int(ConfigLoader.get("OFTL_FWCSV_WORKER_COUNT", 2)))
        self.queue_maxsize = max(1, int(ConfigLoader.get("OFTL_FWCSV_QUEUE_MAXSIZE", 1000)))
        self.checkpoint_every_rows = max(1, int(ConfigLoader.get("OFTL_FWCSV_CHECKPOINT_EVERY_ROWS", 1000)))
        self.file_encoding = str(ConfigLoader.get("OFTL_FWCSV_FILE_ENCODING", "utf-8"))

        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=self.queue_maxsize)
        self._enqueued_paths: set[str] = set()
        self._enqueued_lock = asyncio.Lock()
        self._active_processing_paths: set[str] = set()
        self._active_processing_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._csv_file_processor = CSVFileProcessor(
            archive_file=self._move_to_archive,
            move_to_error=self._move_to_error,
            file_encoding=self.file_encoding,
        )

    async def run_forever(self) -> None:
        Logging.info("Bootstrapping file watcher agent...")
        self._bootstrap_directories()
        self._validate_dependencies()

        Logging.info("File watcher root directory: %s", self.root_dir)
        Logging.info("Watchfiles available: %s", WATCHFILES_AVAILABLE)

        await self._enqueue_existing_files(self.inbox_dir)
        await self._enqueue_existing_files(self.processing_dir)

        tasks = [
            asyncio.create_task(self._worker_loop(index + 1), name=f"fwcsv-worker-{index + 1}")
            for index in range(self.worker_count)
        ]
        tasks.append(asyncio.create_task(self._periodic_scan_loop(), name="fwcsv-scan-loop"))

        if WATCHFILES_AVAILABLE:
            tasks.append(asyncio.create_task(self._watch_events_loop(), name="fwcsv-watch-loop"))
        try:
            await asyncio.gather(*tasks)            
        finally:
            Logging.info("Shutting down file watcher agent...")
            self._stop_event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()

    def _validate_dependencies(self) -> None:
        if not DBHelper.initialize_connection():
            raise RuntimeError("Database is required for checkpoint/idempotency and could not be initialized.")
        self._validate_required_tables()

    def _validate_required_tables(self) -> None:
        schema = str(ConfigLoader.get("OFTL_POSTGRESDB_SCHEMA", "public")).strip() or "public"
        missing_tables: list[str] = []

        for table_name in self._REQUIRED_TABLES:
            rows = DBHelper.execute_select(
                "SELECT to_regclass(:qualified_name) AS relation_name",
                {"qualified_name": f"{schema}.{table_name}"},
            )
            relation_name = rows[0].get("relation_name") if rows else None
            if relation_name is None:
                missing_tables.append(table_name)

        if missing_tables:
            raise RuntimeError(
                f"Missing required database tables in schema {schema}: {', '.join(sorted(missing_tables))}"
            )

        Logging.info_context(
            "Validated database dependencies for file watcher agent.",
            schema=schema,
            required_tables=",".join(self._REQUIRED_TABLES),
        )

    def _bootstrap_directories(self) -> None:
        for folder in (self.inbox_dir, self.processing_dir, self.archive_dir, self.error_dir):
            folder.mkdir(parents=True, exist_ok=True)
            Logging.info("Ensured directory exists: %s", folder)

    def _bootstrap_db_tables(self) -> None:
        if not DBHelper.initialize_connection():
            raise RuntimeError("Database is required for checkpoint/idempotency and could not be initialized.")

        DBHelper.execute_update(
            """
            CREATE TABLE IF NOT EXISTS oftl_fwcsv_registry (
                file_id VARCHAR(128) PRIMARY KEY,
                filename TEXT NOT NULL,
                file_size BIGINT NOT NULL,
                mtime_ns BIGINT NOT NULL,
                checksum_sha256 VARCHAR(64),
                status VARCHAR(32) NOT NULL,
                row_count BIGINT NOT NULL DEFAULT 0,
                error_message TEXT,
                started_at TIMESTAMPTZ,
                ended_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        DBHelper.execute_update(
            """
            CREATE TABLE IF NOT EXISTS oftl_fwcsv_checkpoint (
                file_id VARCHAR(128) PRIMARY KEY,
                row_number BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

    async def _watch_events_loop(self) -> None:
        assert awatch is not None

        async for changes in awatch(str(self.inbox_dir), recursive=True):
            for change, path_str in changes:
                if Change is not None:
                    allowed_changes = {Change.added, Change.modified}
                    moved_change = getattr(Change, "moved", None)
                    if moved_change is not None:
                        allowed_changes.add(moved_change)
                    if change not in allowed_changes:
                        continue
                if self._is_csv_path(path_str):
                    await self._enqueue_candidate(path_str)

    async def _periodic_scan_loop(self) -> None:
        while not self._stop_event.is_set():
            await self._enqueue_existing_files(self.inbox_dir)
            await self._enqueue_existing_files(self.processing_dir)
            await asyncio.sleep(self.scan_interval_seconds)

    async def _enqueue_existing_files(self, base_dir: Path) -> None:
        for path_str in self._scan_csv_files(base_dir):
            await self._enqueue_candidate(path_str)

    def _scan_csv_files(self, base_dir: Path) -> list[str]:
        discovered: list[str] = []
        stack = [base_dir]

        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False) and self._is_csv_path(entry.path):
                            discovered.append(entry.path)
            except FileNotFoundError:
                continue

        return discovered

    async def _enqueue_candidate(self, path_str: str) -> None:
        resolved = str(Path(path_str).resolve())

        async with self._enqueued_lock:
            if resolved in self._enqueued_paths:
                return
            self._enqueued_paths.add(resolved)

        try:
            await self._queue.put(resolved)
        except asyncio.CancelledError:
            async with self._enqueued_lock:
                self._enqueued_paths.discard(resolved)
            raise

    async def _worker_loop(self, worker_id: int) -> None:
        Logging.info("Worker-%s started", worker_id)

        while not self._stop_event.is_set():
            path_str = await self._queue.get()
            try:
                await self._handle_candidate(path_str)
            except RabbitMQShutdownRequested:
                self._stop_event.set()
                raise
            except Exception as exc:  # pragma: no cover
                Logging.error("Worker-%s failed for %s: %s", worker_id, path_str, exc)
            finally:
                async with self._enqueued_lock:
                    self._enqueued_paths.discard(path_str)
                self._queue.task_done()

    async def _handle_candidate(self, path_str: str) -> None:
        path = Path(path_str)

        if not path.exists() or not path.is_file():
            return

        if self.processing_dir in path.parents:
            try:
                claimed = self._build_claimed_from_processing(path)
            except FileNotFoundError:
                return
            await self._process_claimed_with_lock(claimed)
            return

        if self.inbox_dir not in path.parents:
            return

        claimed = await self._claim_when_ready(path)
        if claimed is not None:
            await self._process_claimed_with_lock(claimed)

    async def _process_claimed_with_lock(self, claimed: ClaimedFile) -> None:
        claim_key = str(claimed.claimed_path.resolve())

        async with self._active_processing_lock:
            if claim_key in self._active_processing_paths:
                Logging.info("Skipping already active claimed file: %s", claimed.claimed_path)
                return
            self._active_processing_paths.add(claim_key)

        try:
            await asyncio.to_thread(self._process_claimed_file, claimed)
        finally:
            async with self._active_processing_lock:
                self._active_processing_paths.discard(claim_key)

    async def _claim_when_ready(self, inbox_path: Path) -> ClaimedFile | None:
        waited = 0.0

        while waited <= self.claim_max_wait_seconds:
            if not inbox_path.exists():
                return None

            if await self._is_file_stable(inbox_path):
                claimed = self._atomic_claim(inbox_path)
                if claimed is not None:
                    return claimed

            await asyncio.sleep(self.claim_retry_seconds)
            waited += self.claim_retry_seconds

        Logging.warning("File never became claimable within timeout: %s", inbox_path)
        return None

    async def _is_file_stable(self, path: Path) -> bool:
        last_stat = self._stat(path)
        if last_stat is None:
            return False

        stable_for = 0.0

        while stable_for < self.stability_window_seconds:
            await asyncio.sleep(self.stability_probe_seconds)
            next_stat = self._stat(path)
            if next_stat is None:
                return False

            unchanged = (
                last_stat.st_size == next_stat.st_size
                and last_stat.st_mtime_ns == next_stat.st_mtime_ns
            )

            if unchanged:
                stable_for += self.stability_probe_seconds
            else:
                stable_for = 0.0

            last_stat = next_stat

        return True

    @staticmethod
    def _stat(path: Path) -> os.stat_result | None:
        try:
            return path.stat()
        except FileNotFoundError:
            return None

    def _atomic_claim(self, inbox_path: Path) -> ClaimedFile | None:
        try:
            stat_before_move = inbox_path.stat()
        except FileNotFoundError:
            return None

        destination = self._next_unique_path(self.processing_dir, inbox_path.name)

        try:
            os.rename(inbox_path, destination)
        except (FileNotFoundError, PermissionError):
            return None
        except OSError as exc:
            Logging.warning("Claim skipped for %s: %s", inbox_path, exc)
            return None

        return ClaimedFile(
            source_name=inbox_path.name,
            claimed_path=destination,
            fingerprint=self._build_fingerprint(inbox_path.name, stat_before_move.st_size, stat_before_move.st_mtime_ns),
            size=stat_before_move.st_size,
            mtime_ns=stat_before_move.st_mtime_ns,
        )

    def _build_claimed_from_processing(self, processing_path: Path) -> ClaimedFile:
        stat_value = processing_path.stat()
        return ClaimedFile(
            source_name=processing_path.name,
            claimed_path=processing_path,
            fingerprint=self._build_fingerprint(processing_path.name, stat_value.st_size, stat_value.st_mtime_ns),
            size=stat_value.st_size,
            mtime_ns=stat_value.st_mtime_ns,
        )

    @staticmethod
    def _build_fingerprint(filename: str, file_size: int, mtime_ns: int) -> str:
        payload = f"{filename}|{file_size}|{mtime_ns}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _process_claimed_file(self, claimed: ClaimedFile) -> None:
        self._csv_file_processor.process_claimed_file(claimed)

    def _move_to_archive(self, path: Path, suffix: str | None = None) -> Path:
        now_utc = datetime.now(timezone.utc)
        destination_dir = self.archive_dir / f"{now_utc.year:04d}" / f"{now_utc.month:02d}" / f"{now_utc.day:02d}"
        destination_dir.mkdir(parents=True, exist_ok=True)

        timestamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
        file_stem = Path(path.name).stem
        extension = Path(path.name).suffix or ".csv"
        suffix_segment = f".{suffix}" if suffix else ""
        target_name = f"{file_stem}.{timestamp}{suffix_segment}{extension}"
        destination = self._next_unique_path(destination_dir, target_name)
        os.rename(path, destination)
        return destination

    def _move_to_error(self, path: Path) -> Path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_stem = Path(path.name).stem
        extension = Path(path.name).suffix or ".csv"
        target_name = f"{file_stem}.{timestamp}.error{extension}"
        destination = self._next_unique_path(self.error_dir, target_name)

        if path.exists():
            os.rename(path, destination)

        return destination

    @staticmethod
    def _next_unique_path(directory: Path, file_name: str) -> Path:
        candidate = directory / file_name
        if not candidate.exists():
            return candidate

        stem = candidate.stem
        suffix = candidate.suffix
        counter = 1

        while True:
            maybe = directory / f"{stem}.{counter}{suffix}"
            if not maybe.exists():
                return maybe
            counter += 1

    @staticmethod
    def _is_csv_path(path_str: str) -> bool:
        return Path(path_str).suffix.lower() == ".csv"

