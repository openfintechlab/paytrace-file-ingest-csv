# PayTrace File Ingest CSV

## Introduction

This project provides a production-style CSV ingest worker for PayTrace, plus shared utilities:

- Environment/configuration loading from `.env` and process environment (`ConfigLoader`)
- Centralized application logging (`Logging`)
- PostgreSQL connection and query helpers using SQLAlchemy (`DBHelper`)
- CSV file watcher + scanner with claim/checkpoint/idempotency/archive semantics (`FileWatcherAgent`)
- A startup entrypoint (`src/main.py`) that starts the watcher service

## Project Structure

```text
src/
  main.py                  # Entrypoint (banner + starts FileWatcherAgent)
  utilities/
    ConfigLoader.py        # OFTL_* configuration discovery and access
    Logging.py             # Logging bootstrap and helper methods
    DBHelper.py            # Singleton DB engine/session helper + CRUD execution helpers
    FileWatcher.py         # CSV watcher/scanner and processing pipeline
tests/
  test_config_loader.py    # Unit tests for ConfigLoader
  test_routes.py           # Legacy test file (references FastAPI app not present in current src/main.py)
```

## Prerequisites

- Python 3.11+
- `uv` installed
- PostgreSQL (only required when DB operations are used)

## Quick Start

### 1. Create local environment file

```bash
cp .env.example .env
```

Update `.env` values for your local environment.

### 2. Install dependencies

```bash
uv sync
```

### 3. Run the file watcher service

```bash
uv run python src/main.py
```

## File Processing Flow

1. Watches `OFTL_FWCSV_ROOTDIR/inbox` for `.csv` files (also performs periodic scans).
2. Waits for file stability, then atomically moves the file to `processing/`.
3. Streams CSV rows and skips the header row.
4. Persists processing state in DB for idempotency and resume support.
5. Moves successful files to date-partitioned `archive/YYYY/MM/DD/`.
6. Moves failed files to `error/`.

`FileWatcher.py::_process_csv_row(...)` is the integration point for project-specific business logic.

## Configuration Reference

### Logging

- `OFTL_LOG_LEVEL` (default: `INFO`)
- `OFTL_LOG_FORMAT` (default: `[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s`)

### Service Metadata (used by `display_banner`)

- `OFTL_SCA_VERSION`
- `OFTL_SCA_CONTEXT_ROOT`
- `OFTL_SCA_HOST`
- `OFTL_SCA_PORT`

### Database

- `OFTL_POSTGRESDB_USERNAME`
- `OFTL_POSTGRESDB_PASSWORD`
- `OFTL_POSTGRESDB_HOST`
- `OFTL_POSTGRESDB_PORT`
- `OFTL_POSTGRESDB_NAME`
- `OFTL_POSTGRESDB_POOLSIZE` (optional, default: `10`)

### File Watcher (CSV Ingest)

- `OFTL_FWCSV_ROOTDIR` (default: `./fwcsv`)
- `OFTL_FWCSV_SCAN_INTERVAL_SECONDS` (default: `10`)
- `OFTL_FWCSV_STABILITY_WINDOW_SECONDS` (default: `5`)
- `OFTL_FWCSV_STABILITY_PROBE_SECONDS` (default: `1`)
- `OFTL_FWCSV_CLAIM_RETRY_SECONDS` (default: `2`)
- `OFTL_FWCSV_CLAIM_MAX_WAIT_SECONDS` (default: `60`)
- `OFTL_FWCSV_WORKER_COUNT` (default: `2`)
- `OFTL_FWCSV_QUEUE_MAXSIZE` (default: `1000`)
- `OFTL_FWCSV_CHECKPOINT_EVERY_ROWS` (default: `1000`)
- `OFTL_FWCSV_FILE_ENCODING` (default: `utf-8`)

## Database Objects

On startup, the service currently auto-creates:

- `oftl_fwcsv_registry` (file-level processing state and checksums)
- `oftl_fwcsv_checkpoint` (resume row checkpoints)

## Using DBHelper

`DBHelper` initializes a pooled SQLAlchemy engine and provides helper methods:

- `initialize_connection()`
- `dispose_connection()`
- `execute_select(query, params=None)`
- `execute_insert(query, params=None)`
- `execute_update(query, params=None)`
- `execute_delete(query, params=None)`

The helper validates required DB environment variables and performs a connectivity check (`SELECT 1`) during initialization.

## Testing

Run tests with:

```bash
uv run pytest tests -v
```

At present, `test_config_loader.py` aligns with the current utility code. `test_routes.py` is a legacy file that still assumes a FastAPI app exists.

## Major Libraries Used

- `environs`
- `sqlalchemy`
- `psycopg2-binary`
- `watchfiles`
- `pytest`
