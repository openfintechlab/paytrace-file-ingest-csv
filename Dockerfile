# syntax=docker/dockerfile:1
# 
# Copyright 2026-2030 Openfintechlab, Inc. All rights reserved.
# Description: 
# Generic Dockerfile for python application
# Ref: https://hub.docker.com/hardened-images/catalog/dhi/python


## -----------------------------------------------------
## Build stage (use tag with -dev suffix: e.g. 3.9.23-debian13-fips-dev)
FROM dhi.io/python:3-debian13-sfw-dev AS builder

ENV PATH="/app/venv/bin:$PATH" \
    APP_HOME=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR ${APP_HOME}

# Install uv through pip as requested.
RUN python -m pip install --upgrade pip uv

# Copy the full project into /app for development/build stage.
COPY . ${APP_HOME}

# Create and populate project virtual environment from lockfile.
RUN uv sync --frozen

# Persist uv executable for the runtime image.
RUN cp "$(command -v uv)" /app/uv


FROM dhi.io/python:3 AS runtime

LABEL "authur"="openfintechlab.com" \
      "base-image-repo"="dhi.io/python:3/" \
      "copyright"="Copyright 2026-2030 OpenFintechlab, Inc. All rights reserved."


ENV APP_HOME=/app \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:/usr/local/bin:/root/.local/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR ${APP_HOME}

# Copy only runtime essentials from builder.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/uv /usr/local/bin/uv

EXPOSE 8081

# Start the service through uv.
CMD ["/usr/local/bin/uv", "run", "python", "src/main.py"]
