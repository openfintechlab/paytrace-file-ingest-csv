from __future__ import annotations

from collections.abc import Callable

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


class HeaderValidationMiddleware(BaseHTTPMiddleware):
    """Validate mandatory PayTrace headers for incoming API requests."""

    _BASE_REQUIRED_HEADERS: tuple[str, ...] = (
        "authorization",
        "x-transaction-id",
        "x-correlation-id",
        "accept-language",
        "accept",
    )
    _STATE_CHANGING_REQUIRED_HEADERS: tuple[str, ...] = (
        "idempotency-key",
        "content-type",
    )
    _STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH"}
    _EXEMPT_PATHS = {
        "/_healthz",
        "/_probe",
        "/docs",
        "/openapi.json",
        "/redoc",
    }

    async def dispatch(self, request: Request, call_next: Callable):
        if self._is_exempt_request(request):
            return await call_next(request)

        missing_headers = self._find_missing_headers(request)
        if missing_headers:
            return JSONResponse(status_code=400, content=self._error_payload(missing_headers))

        return await call_next(request)

    @classmethod
    def _is_exempt_request(cls, request: Request) -> bool:
        path = request.url.path
        if request.method.upper() == "OPTIONS":
            return True
        return path in cls._EXEMPT_PATHS

    @classmethod
    def _find_missing_headers(cls, request: Request) -> list[str]:
        required: list[str] = list(cls._BASE_REQUIRED_HEADERS)
        if request.method.upper() in cls._STATE_CHANGING_METHODS:
            required.extend(cls._STATE_CHANGING_REQUIRED_HEADERS)

        return [header for header in required if not request.headers.get(header)]

    @staticmethod
    def _error_payload(missing_headers: list[str]) -> dict:
        formatted_headers = ", ".join(missing_headers)
        return {
            "result": {
                "code": "PT-1401",
                "description": f"Missing required header(s): {formatted_headers}",
            },
            "errors": [
                {
                    "code": "PT-VAL-0001",
                    "description": f"Missing required header '{header}'",
                    "field": f"headers.{header}",
                    "severity": "ERROR",
                }
                for header in missing_headers
            ],
        }
