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
  domain/                   # Placeholder for domain models, schemas, business logic
    PaymentModel.py         # Pydantic model for payment data (not currently used in processing)
    PaymentProcessor.py     # Domain class for payment processing logic (called from FileWatcher)
  utilities/
    ConfigLoader.py        # OFTL_* configuration discovery and access
    Logging.py             # Logging bootstrap and helper methods
    DBHelper.py            # Singleton DB engine/session helper + CRUD execution helpers
    FileWatcher.py         # CSV watcher/scanner and processing pipeline
    RabbitMQHelper.py      # RabbitMQ queue/exchange messaging helper
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
- `OFTL_RABITMQ_CONNECTION_ATTEMPTS` (default: `3`)
- `OFTL_RABITMQ_RETRY_DELAY` (default: `2`)
- `OFTL_RABITMQ_SOCKET_TIMEOUT` (default: `5`)
- `OFTL_RABITMQ_QUEUE_DURABLE` (default: `true`)
- `OFTL_RABITMQ_EXCHANGE_DURABLE` (default: `true`)
- `OFTL_RABITMQ_EXCHANGE_TYPE` (default: `direct`)
- `OFTL_RABITMQ_MESSAGE_PERSISTENT` (default: `true`)
- `OFTL_RABITMQ_PUBLISH_MANDATORY` (default: `false`)

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

The helper validates required DB environment variables, defaults `OFTL_POSTGRESDB_NAME` and `OFTL_POSTGRESDB_SCHEMA` to `public`, sets the PostgreSQL `search_path` from `OFTL_POSTGRESDB_SCHEMA`, and performs a connectivity check (`SELECT 1`) during initialization.

## Testing

Run tests with:

```bash
uv run pytest tests -v
```

At present, `test_config_loader.py` aligns with the current utility code. `test_routes.py` is a legacy file that still assumes a FastAPI app exists.

## CSV File Format

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
  CREATE TABLE IF NOT EXISTS oftl_fwcsv_checkpoint (
      file_id VARCHAR(128) PRIMARY KEY,
      row_number BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
```

## Major Libraries Used

- `environs`
- `sqlalchemy`
- `psycopg2-binary`
- `watchfiles`
- `pika`
- `pytest`
