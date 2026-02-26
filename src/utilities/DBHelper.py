# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Database helper utility for PostgreSQL using SQLAlchemy.
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/12
"""

from __future__ import annotations

from threading import Lock
from typing import Any, ClassVar
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from utilities.ConfigLoader import ConfigLoader
from utilities.Logging import Logging


class DBHelper:
    """Singleton helper for pooled SQLAlchemy database access."""

    _instance: ClassVar["DBHelper | None"] = None
    _lock: ClassVar[Lock] = Lock()
    _engine: ClassVar[Engine | None] = None
    _session_factory: ClassVar[sessionmaker | None] = None
    _DEFAULT_POOL_SIZE: ClassVar[int] = 10

    def __new__(cls) -> "DBHelper":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "DBHelper":
        return cls()

    @classmethod
    def _build_connection_url(cls) -> str:
        username    = ConfigLoader.get("OFTL_POSTGRESDB_USERNAME")
        password    = ConfigLoader.get("OFTL_POSTGRESDB_PASSWORD")
        host        = ConfigLoader.get("OFTL_POSTGRESDB_HOST")
        port_value  = ConfigLoader.get("OFTL_POSTGRESDB_PORT")
        db_name     = ConfigLoader.get("OFTL_POSTGRESDB_NAME")

        missing = [
            key
            for key, value in {
                "OFTL_POSTGRESDB_USERNAME": username,
                "OFTL_POSTGRESDB_PASSWORD": password,
                "OFTL_POSTGRESDB_HOST": host,
                "OFTL_POSTGRESDB_PORT": port_value,
                "OFTL_POSTGRESDB_NAME": db_name,
            }.items()
            if not value
        ]

        if missing:
            missing_values = ", ".join(missing)
            raise ValueError(f"Missing DB configuration values: {missing_values}")

        try:
            port = int(str(port_value))
        except (TypeError, ValueError):
            raise ValueError("Invalid OFTL_POSTGRESDB_PORT; expected a numeric value.")

        encoded_password = quote_plus(str(password))
        return (
            f"postgresql+psycopg2://{username}:{encoded_password}@"
            f"{host}:{port}/{db_name}"
        )

    @classmethod
    def initialize_connection(cls) -> bool:
        """Initialize engine/session factory once with connection pooling."""
        if cls._engine is not None and cls._session_factory is not None:
            return True

        with cls._lock:
            if cls._engine is not None and cls._session_factory is not None:
                return True

            try:
                connection_url = cls._build_connection_url()
            except ValueError as exc:
                Logging.warning(f"Database initialization skipped: {exc}")
                return False

            raw_pool_size = ConfigLoader.get(
                "OFTL_POSTGRESDB_POOLSIZE",
                cls._DEFAULT_POOL_SIZE,
            )
            try:
                pool_size = int(raw_pool_size)
            except (TypeError, ValueError):
                pool_size = cls._DEFAULT_POOL_SIZE

            cls._engine = create_engine(
                connection_url,
                pool_size=pool_size,
                max_overflow=0,
                pool_pre_ping=True,
                pool_recycle=1800,
            )
            cls._session_factory = sessionmaker(
                bind=cls._engine,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )

            try:
                with cls._engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                Logging.info("Database connection initialized successfully.")
                return True
            except SQLAlchemyError as exc:
                Logging.error(f"Database connectivity check failed: {exc}")
                cls.dispose_connection()
                return False

    @classmethod
    def dispose_connection(cls) -> None:
        """Dispose active engine and reset singleton DB state."""
        if cls._engine is not None:
            cls._engine.dispose()
        cls._engine = None
        cls._session_factory = None

    @classmethod
    def _get_session(cls):
        if cls._session_factory is None and not cls.initialize_connection():
            raise RuntimeError("Database is not initialized. Check DB configuration.")
        return cls._session_factory()

    @classmethod
    def execute_select(
        cls,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute SELECT query and return rows as dictionaries."""
        session = cls._get_session()
        try:
            result = session.execute(text(query), params or {})
            return [dict(row._mapping) for row in result.fetchall()]
        finally:
            session.close()

    @classmethod
    def execute_insert(
        cls,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute INSERT query and return affected row count."""
        session = cls._get_session()
        try:
            result = session.execute(text(query), params or {})
            session.commit()
            return result.rowcount or 0
        except SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()

    @classmethod
    def execute_update(
        cls,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute UPDATE query and return affected row count."""
        session = cls._get_session()
        try:
            result = session.execute(text(query), params or {})
            session.commit()
            return result.rowcount or 0
        except SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()

    @classmethod
    def execute_delete(
        cls,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute DELETE query and return affected row count."""
        session = cls._get_session()
        try:
            result = session.execute(text(query), params or {})
            session.commit()
            return result.rowcount or 0
        except SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()
