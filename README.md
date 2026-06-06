# PayTrace File Ingest CSV

## Introduction

This project provides a production-style CSV ingest worker for PayTrace, plus shared utilities:

- Environment/configuration loading from `.env` and process environment (`ConfigLoader`)
- Centralized application logging (`Logging`)
- PostgreSQL connection and query helpers using SQLAlchemy (`DBHelper`)
- CSV file watcher + scanner with claim/checkpoint/idempotency/archive semantics (`FileWatcherAgent`)
- JSON Schema driven payment instruction validation, typing, and CSV column mapping (`src/domain/payment_instruction.schema.json`)
- A startup entrypoint (`src/main.py`) that prints the service banner, validates RabbitMQ connectivity, and starts the watcher service

## Project Structure

```text
src/
  main.py                  # Entrypoint (banner + starts FileWatcherAgent)
  domain/                  # Payment schema, typed model, and row parsing logic
    PaymentModel.py        # JSON Schema driven payment model and validation/coercion
    PaymentProcessor.py    # Maps CSV rows to the schema-driven PaymentModel
    payment_instruction.schema.json  # Single source of truth for payment fields and validation rules
  utilities/
    ConfigLoader.py        # OFTL_* configuration discovery and access
    Logging.py             # Logging bootstrap and helper methods
    DBHelper.py            # Singleton DB engine/session helper + CRUD execution helpers
    FileWatcher.py         # CSV watcher/scanner and processing pipeline
    RabbitMQHelper.py      # RabbitMQ queue/exchange messaging helper
tests/
  test_config_loader.py    # Unit tests for ConfigLoader
  test_file_watcher.py     # Unit tests for FileWatcherAgent
  test_payment_model.py    # Unit tests for PaymentModel and PaymentProcessor
  conftest.py              # Pytest configuration and fixtures
```

## Prerequisites

- Python 3.11+
- `uv` installed
- PostgreSQL
- RabbitMQ

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

The service validates RabbitMQ connectivity during startup. If RabbitMQ remains unavailable after the configured retry budget, the process exits with status code `99`.

## File Processing Flow

1. Watches `OFTL_FWCSV_ROOTDIR/inbox` for `.csv` files (also performs periodic scans).
2. Waits for file stability, then atomically moves the file to `processing/`.
3. Streams CSV rows and skips the header row.
4. Publishes each valid payment row to the configured domestic or cross-border RabbitMQ request queue.
5. Persists processing state in DB for idempotency, resume support, and row dispatch status. Row failures publish EV002; successful rows do not publish per-row events.
6. Moves successful files to date-partitioned `archive/YYYY/MM/DD/`.
7. Publishes an EV001 file-loaded event to the configured RabbitMQ topic exchange.
8. Moves failed files to `error/`.

`FileWatcher.py::_process_csv_row(...)` is the integration point for project-specific business logic.

## Payment Schema Model

`src/domain/payment_instruction.schema.json` is the single source of truth for payment instructions.

- `PaymentModel` builds its fields dynamically from the schema properties.
- `PAYMENT_CSV_COLUMNS` is derived from the schema property order and is used when parsing headerless rows.
- Type coercion is driven by schema definitions, including `date-time`, `date`, and numeric fields.
- Schema changes should be made in `payment_instruction.schema.json`; the model and CSV mapping update automatically on the next run.

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
- `OFTL_POSTGRESDB_NAME` (optional, default: `public`)
- `OFTL_POSTGRESDB_SCHEMA` (optional, default: `public`)
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

### RabbitMQ

- `OFTL_RABITMQ_HOST` (default: `localhost`)
- `OFTL_RABITMQ_PORT` (default: `5672`)
- `OFTL_RABITMQ_USERNAME` (default: `guest`)
- `OFTL_RABITMQ_PASSWORD_SECRET` (default: `guest`)
- `OFTL_RABITMQ_VHOST` (default: `/`)
- `OFTL_RABITMQ_HEARTBEAT` (default: `60`)
- `OFTL_RABITMQ_BLOCKED_CONNECTION_TIMEOUT` (default: `30`)
- `OFTL_RABITMQ_CONNECTION_ATTEMPTS` (default: `3`) - Total application-level connection retries for startup and reconnect after a broker loss
- `OFTL_RABITMQ_CONN_RETRYCOUNT` (legacy fallback) - Used only when `OFTL_RABITMQ_CONNECTION_ATTEMPTS` is not set
- `OFTL_RABITMQ_RETRY_DELAY` (default: `2`)
- `OFTL_RABITMQ_SOCKET_TIMEOUT` (default: `5`)
- `OFTL_RABITMQ_STACK_TIMEOUT` (default: `10`)
- `OFTL_RABITMQ_QUEUE_DURABLE` (default: `true`)
- `OFTL_RABITMQ_EXCHANGE_DURABLE` (default: `true`)
- `OFTL_RABITMQ_EXCHANGE_TYPE` (default: `direct`)
- `OFTL_RABITMQ_PUBEVENT_EXCHANGE` (default: `paytrace.events`) — Topic exchange for file lifecycle events
- `OFTL_RABITMQ_PUBEVENT_EV001` (default: `files.csv.loaded`) — Routing key for EV001 file loaded events
- `OFTL_RABITMQ_PUBEVENT_EV002` (default: `files.csv.row.failed`) — Routing key for EV002 row failed events
- `OFTL_RABITMQ_MESSAGE_PERSISTENT` (default: `true`)
- `OFTL_RABITMQ_PUBLISH_MANDATORY` (default: `false`)
- `OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE` (default: `CSV.PAYMENTS.DOMESTIC.REQ`) — Queue name for domestic payment transfer requests
- `OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE` (default: `CSV.PAYMENTS.CROSS_BORDER.REQ`) — Queue name for cross-border payment transfer requests

On startup, the helper validates RabbitMQ connectivity and exits the process with status code `99` if the broker remains unavailable after the configured retries.

During runtime, if RabbitMQ is lost while publishing:

- the helper retries queue or exchange publish operations up to `OFTL_RABITMQ_CONNECTION_ATTEMPTS`
- reconnect attempts use `OFTL_RABITMQ_HOST`, `OFTL_RABITMQ_PORT`, and `OFTL_RABITMQ_RETRY_DELAY`
- if RabbitMQ remains unavailable after the retry budget, the helper raises a shutdown signal and the service exits with status code `99`

### Published Messages

For each parsed payment row, the worker publishes the schema-driven payment payload to:

- `OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE` when `transfer_type` is `DOMESTIC`
- `OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE` when `transfer_type` is `CROSS_BORDER`

Successful rows do not publish a per-row event. When a row fails processing, including rejection because the transfer signature was already published, the worker publishes EV002 to `OFTL_RABITMQ_PUBEVENT_EXCHANGE` as a RabbitMQ `topic` exchange message. The routing key comes from `OFTL_RABITMQ_PUBEVENT_EV002` and defaults to `files.csv.row.failed`.

After a file is fully processed, archived, and marked completed in `oftl_fwcsv_registry`, the worker publishes EV001 to `OFTL_RABITMQ_PUBEVENT_EXCHANGE` as a RabbitMQ `topic` exchange message. The routing key comes from `OFTL_RABITMQ_PUBEVENT_EV001` and defaults to `files.csv.loaded`.

EV001 envelope:

```json
{
  "event_id": "generated UUID",
  "event_code": "EV001",
  "event_type": "files.csv.loaded",
  "event_version": "1.0",
  "timestamp": "UTC ISO-8601 timestamp",
  "source": "paytrace-file-ingest-csv",
  "correlation_id": "file checksum_sha256",
  "causation_id": "file_id",
  "payload": {
    "event": "file_processed",
    "file_id": "file fingerprint",
    "filename": "original CSV filename",
    "archive_path": "archived file path",
    "checksum_sha256": "file checksum_sha256",
    "row_count": 6,
    "started_at": "UTC ISO-8601 timestamp",
    "ended_at": "UTC ISO-8601 timestamp"
  }
}
```

EV002 envelope:

```json
{
  "event_id": "generated UUID",
  "event_code": "EV002",
  "event_type": "files.csv.row.failed",
  "event_version": "1.0",
  "timestamp": "UTC ISO-8601 timestamp",
  "source": "paytrace-file-ingest-csv",
  "correlation_id": "generated UUID",
  "causation_id": "transfer_id or file_id",
  "payload": {
    "event": "row_failed",
    "file_id": "file fingerprint",
    "filename": "CSV filename",
    "row_number": 2,
    "transfer_id": "payment transfer_id when available",
    "failure_reason": "row_processing_failed or redundant_signature",
    "error_message": "failure detail"
  }
}
```

## Database Objects

The service depends on these tables for idempotency, checkpointing, and dispatch tracking:

- `oftl_fwcsv_registry` (file-level processing state and checksums)
- `oftl_fwcsv_checkpoint` (resume row checkpoints)
- `oftl_fwcsv_row_dispatch` (per-transfer publish status and downstream processing status)

The current code does not auto-create these tables at startup. Provision them before running the worker.

## Testing

Run tests with:

```bash
uv run pytest tests -v
```

Run `uv sync` first to ensure all dependencies are available in the project environment. Test coverage includes:

- `test_config_loader.py` — Configuration loading and environment variable resolution
- `test_file_watcher.py` — File watcher scanning, claiming, and processing logic  
- `test_payment_model.py` — Payment model validation, type coercion, and CSV parsing
- `test_rabbitmq_helper.py` — RabbitMQ retry limits, reconnect-after-loss behavior, and shutdown signaling

## CSV File Format

The canonical field definitions live in `src/domain/payment_instruction.schema.json`. The table below mirrors the current schema for quick reference.

| Column Name              | Data Type        | Mandatory (Y/N) | Sample Value            | Description                                        | Allowed / Probable Values    | Open Standard Reference  |
| ------------------------ | ---------------- | --------------- | ----------------------- | -------------------------------------------------- | ---------------------------- | ------------------------ |
| transfer_id              | String (36)      | Y               | PTX-0000001             | Unique transaction identifier generated by sender  | Any unique string            | Internal / UUID          |
| transfer_type            | Enum             | Y               | DOMESTIC                | Indicates if transfer is domestic or international | DOMESTIC, CROSS_BORDER       | ISO20022 concept         |
| transaction_datetime     | ISO8601 Datetime | Y               | 2026-03-03T10:15:30Z    | Date and time when payment instruction is created  | ISO 8601 format              | ISO 8601                 |
| requested_execution_date | Date             | Y               | 2026-03-04              | Date when payment should be executed               | YYYY-MM-DD                   | ISO 8601                 |
| amount                   | Decimal (18,2)   | Y               | 2500.00                 | Payment amount                                     | Positive decimal value       | ISO20022 Amount          |
| currency                 | String (3)       | Y               | AED                     | Currency of payment                                | ISO currency codes           | ISO 4217                 |
| purpose_code             | String (4)       | N               | SUPP                    | Purpose of payment                                 | SALA, SUPP, INVC, TAXS, RENT | ISO20022 ExternalPurpose |
| charge_bearer            | Enum             | N               | SHAR                    | Who bears transaction charges                      | DEBT, CRED, SHAR             | ISO20022 ChargeBearer    |
| exchange_rate            | Decimal          | N               | 3.6725                  | FX rate if currency conversion required            | Positive decimal             | Market FX                |
| debtor_name              | String (140)     | Y               | Sharjah Trading LLC     | Name of payer                                      | Any legal entity/person name | ISO20022 Party           |
| debtor_country           | String (2)       | Y               | AE                      | Country of debtor                                  | ISO country codes            | ISO 3166-1               |
| debtor_account_scheme    | Enum             | Y               | IBAN                    | Account identifier type                            | IBAN, BBAN, OTHER            | ISO20022 AccountScheme   |
| debtor_account_id        | String (34)      | Y               | AE070331234567890123456 | Account number / IBAN                              | Bank defined                 | IBAN standard            |
| debtor_bank_id_scheme    | Enum             | Y               | BIC                     | Identifier scheme for bank                         | BIC, LOCAL                   | ISO20022                 |
| debtor_bank_id           | String (11)      | Y               | SIBUAEAD                | Debtor bank identifier                             | SWIFT/BIC                    | SWIFT BIC                |
| creditor_name            | String (140)     | Y               | Desert Supplies FZC     | Name of beneficiary                                | Any legal entity/person      | ISO20022 Party           |
| creditor_country         | String (2)       | Y               | AE                      | Country of creditor                                | ISO country codes            | ISO 3166-1               |
| creditor_account_scheme  | Enum             | Y               | IBAN                    | Account identifier scheme                          | IBAN, BBAN, OTHER            | ISO20022                 |
| creditor_account_id      | String (34)      | Y               | AE170540123456789012345 | Beneficiary account number                         | Bank defined                 | IBAN                     |
| creditor_bank_id_scheme  | Enum             | Y               | BIC                     | Bank identifier scheme                             | BIC, LOCAL                   | ISO20022                 |
| creditor_bank_id         | String (11)      | Y               | EBILAEAD                | Beneficiary bank identifier                        | SWIFT/BIC                    | SWIFT BIC                |
| intermediary_bank_bic    | String (11)      | N               | DEUTDEFF                | Intermediary bank for cross-border payments        | Valid BIC                    | SWIFT                    |
| remittance_unstructured  | String (140)     | N               | Invoice 7843            | Free text payment description                      | Any text                     | ISO20022 Remittance      |
| remittance_reference     | String (35)      | N               | INV-7843                | Structured reference number                        | Invoice / reference ID       | ISO20022                 |

### Sample CSV Row

```csv
transfer_id,transfer_type,transaction_datetime,requested_execution_date,amount,currency,purpose_code,charge_bearer,exchange_rate,debtor_name,debtor_country,debtor_account_scheme,debtor_account_id,debtor_bank_id_scheme,debtor_bank_id,creditor_name,creditor_country,creditor_account_scheme,creditor_account_id,creditor_bank_id_scheme,creditor_bank_id,intermediary_bank_bic,remittance_unstructured,remittance_reference
PTX-0000001,DOMESTIC,2026-03-03T10:15:30Z,2026-03-04,2500.00,AED,SUPP,SHAR,,Sharjah Trading LLC,AE,IBAN,AE070331234567890123456,BIC,SIBUAEAD,Desert Supplies FZC,AE,IBAN,AE170540123456789012345,BIC,EBILAEAD,,Invoice 7843 - office supplies,INV-7843
PTX-0000002,CROSS_BORDER,2026-03-03T10:20:00Z,2026-03-04,1200.00,USD,INVC,SHAR,3.6725,Sharjah Trading LLC,AE,IBAN,AE070331234567890123456,BIC,SIBUAEAD,Global Parts Ltd,GB,IBAN,GB33BUKB20201555555555,BIC,BUKBGB22,DEUTDEFF,Invoice 9912 - spare parts,INV-9912
```

### Table Structure

``` sql
DROP TABLE IF EXISTS oftl_fwcsv_registry;
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
            );
DROP TABLE IF EXISTS  oftl_fwcsv_checkpoint;
CREATE TABLE IF NOT EXISTS oftl_fwcsv_checkpoint (
    file_id VARCHAR(128) PRIMARY KEY,
    row_number BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS   oftl_fwcsv_row_dispatch;
CREATE TABLE IF NOT EXISTS oftl_fwcsv_row_dispatch (
     transfer_id VARCHAR(36) PRIMARY KEY,
     file_id VARCHAR(128) NOT NULL,
     row_number BIGINT NOT NULL,
     request_queue TEXT NOT NULL,
     status VARCHAR(32) NOT NULL,
     published_at TIMESTAMPTZ NULL,
     error_message TEXT NULL,
     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
 );

 CREATE INDEX IF NOT EXISTS idx_oftl_fwcsv_row_dispatch_file_id
    ON oftl_fwcsv_row_dispatch (file_id);

CREATE INDEX IF NOT EXISTS idx_oftl_fwcsv_row_dispatch_status
    ON oftl_fwcsv_row_dispatch (status);

CREATE INDEX IF NOT EXISTS idx_oftl_fwcsv_row_dispatch_updated_at
    ON oftl_fwcsv_row_dispatch (updated_at);

ALTER TABLE oftl_fwcsv_row_dispatch
    ADD CONSTRAINT chk_oftl_fwcsv_row_dispatch_status
    CHECK (status IN ('PUBLISHED', 'FAILED', 'PROCESSED'));

```

## Major Libraries Used

- `environs`
- `sqlalchemy`
- `psycopg2-binary`
- `jsonschema`
- `watchfiles`
- `pika`
- `pytest`
