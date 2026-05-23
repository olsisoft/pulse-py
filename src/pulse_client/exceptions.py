"""Typed exception hierarchy for the Pulse client.

Every HTTP error from the server is translated into one of these. They all
inherit from :class:`PulseClientError` so callers can catch the entire
client-side family in one block.
"""

from __future__ import annotations

from typing import Any


class PulseClientError(Exception):
    """Base class for every exception raised by this library."""


class PulseAPIError(PulseClientError):
    """A non-2xx response from the Pulse server.

    Carries the status code, the parsed error body (if JSON), and the
    request path so log lines + bug reports are actionable.
    """

    def __init__(
        self,
        status_code: int,
        path: str,
        body: dict[str, Any] | str | None = None,
    ) -> None:
        self.status_code = status_code
        self.path = path
        self.body = body
        message = self._format_message(status_code, path, body)
        super().__init__(message)

    @staticmethod
    def _format_message(
        status_code: int, path: str, body: dict[str, Any] | str | None
    ) -> str:
        msg = f"HTTP {status_code} from {path}"
        if isinstance(body, dict):
            err = body.get("error") or body.get("errorMessage") or body.get("message")
            if err:
                msg += f" — {err}"
        elif isinstance(body, str) and body:
            msg += f" — {body[:200]}"
        return msg


class PulseAuthError(PulseAPIError):
    """The server returned 401 — invalid / expired / missing JWT."""


class PulseNotFoundError(PulseAPIError):
    """The server returned 404 — the resource does not exist."""


class PulseValidationError(PulseAPIError):
    """The server returned 400 — the request body is malformed."""


class PulseRateLimitError(PulseAPIError):
    """The server returned 429 — per-user or per-IP rate limit hit.

    Carries the ``retry_after_seconds`` value the server advises waiting
    before retrying (parsed from the JSON body or the ``Retry-After`` header).
    """

    def __init__(
        self,
        status_code: int,
        path: str,
        body: dict[str, Any] | str | None,
        retry_after_seconds: int | None,
    ) -> None:
        super().__init__(status_code, path, body)
        self.retry_after_seconds = retry_after_seconds
