# -*- coding: utf-8 -*-
"""
Copyright 2026-2028 openfintechlab.com, Inc. All rights reserved.
Licenses: LICENSE.md
Description: Routes boilerplate for PayTrace SCA Service.
Reference: https://github.com/openfintechlab/pytrace-backlogs/issues/12
"""

from __future__ import annotations

from fastapi import APIRouter

from utilities import ConfigLoader


class Routes:
    """Defines base routes for the service."""

    def __init__(self) -> None:
        self.prefix = self._build_prefix()
        self.router = APIRouter(prefix=self.prefix)
        self.public_router = APIRouter()
        self._register_routes()

    @staticmethod
    def _normalize_segment(value: str) -> str:
        if not value:
            return ""
        value = value.strip()
        if not value:
            return ""
        if not value.startswith("/"):
            value = "/" + value
        return value.rstrip("/")

    @classmethod
    def _build_prefix(cls) -> str:
        context_root = ConfigLoader.get("OFTL_SCA_CONTEXT_ROOT")
        version = ConfigLoader.get("OFTL_SCA_VERSION")
        context_root = cls._normalize_segment(str(context_root)) if context_root else ""
        version = str(version).strip() if version else ""
        version_segment = f"/v{version}" if version else ""

        return f"{context_root}{version_segment}"

    def _register_routes(self) -> None:
        @self.router.get("/")
        async def root() -> dict[str, str]:
            return {"status": "ok"}

        @self.public_router.get("/_healthz")
        async def healthz() -> dict[str, str]:
            return self.route_get_healthz()

        @self.public_router.get("/_probe")
        async def probe() -> dict[str, str]:
            return self.route_get_probe()

    @staticmethod
    def route_get_healthz() -> dict[str, str]:
        """Returns the health status."""
        # TODO! Write code to perform health check of the solution
        return {"status": "ok"}

    @staticmethod
    def route_get_probe() -> dict[str, str]:
        """Returns the probe status."""
        # TODO! Write code to perform probe check of the solution
        return {"status": "ok"}
