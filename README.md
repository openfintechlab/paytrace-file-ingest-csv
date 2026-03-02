# PayTrace File Ingest CSV

## Introduction

This project currently provides reusable core utilities and a simple startup script for PayTrace services:

- Environment/configuration loading from `.env` and process environment (`ConfigLoader`)
- Centralized application logging (`Logging`)
- PostgreSQL connection and query helpers using SQLAlchemy (`DBHelper`)
- A lightweight startup entrypoint (`src/main.py`) that logs service banner information

This repository is **not** currently exposing FastAPI routes.

## Project Structure

```text
src/
  main.py                  # Startup script (banner + startup logging)
  utilities/
    ConfigLoader.py        # OFTL_* configuration discovery and access
    Logging.py             # Logging bootstrap and helper methods
    DBHelper.py            # Singleton DB engine/session helper + CRUD execution helpers
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

### 3. Run the startup script

```bash
uv run python src/main.py
```

## Configuration Reference

### Logging

- `OFTL_LOG_LEVEL` (default: `INFO`)
- `OFTL_LOG_FORMAT` (default: `[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s`)

### Service Metadata (used by `displayBanner`)

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
- `pytest`
