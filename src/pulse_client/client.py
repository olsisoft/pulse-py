"""The main ``PulseClient`` and its sub-resource accessors.

Design: every API surface (auth, pipelines, agents, etc.) is its own small
class accessed via an attribute on the client (``client.pipelines``,
``client.agents``). The classes share the HTTP transport via composition
rather than inheritance — keeps each resource focused.

The wire format is the Pulse REST surface described by
``streamflow-pulse/src/main/resources/openapi/openapi.yaml`` (B-103). When
a new endpoint lands in the spec, add a method here and a matching test.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from types import TracebackType
from typing import Any, cast

import httpx

from pulse_client.exceptions import (
    PulseAPIError,
    PulseAuthError,
    PulseNotFoundError,
    PulseRateLimitError,
    PulseValidationError,
)

DEFAULT_TIMEOUT = 30.0
USER_AGENT = "pulse-client-python/2.5.8"


class PulseClient:
    """Synchronous HTTP client for the Pulse REST API.

    Args:
        base_url: The Pulse server URL (e.g. ``http://localhost:9090``).
        token: Optional JWT to attach as ``Authorization: Bearer <token>``
            on every request. If omitted, call :meth:`auth.login` first.
        timeout: Per-request timeout in seconds. Default 30.
        verify: TLS verification (passed through to httpx). Set to ``False``
            for self-signed certs in dev.

    Example:
        >>> from pulse_client import PulseClient
        >>> with PulseClient("http://localhost:9090") as client:
        ...     client.auth.login("alice", "secret")
        ...     for pipeline in client.pipelines.list():
        ...         print(pipeline["name"])
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        verify: bool | str = True,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._http = httpx.Client(
            base_url=self._base_url,
            timeout=timeout,
            verify=verify,
            headers={"User-Agent": USER_AGENT},
        )
        # Resource accessors — each one shares the same transport.
        self.auth = _AuthResource(self)
        self.pipelines = _PipelinesResource(self)
        self.agents = _AgentsResource(self)
        self.templates = _TemplatesResource(self)
        self.users = _UsersResource(self)
        self.events = _EventsResource(self)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> PulseClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    # ------------------------------------------------------------------
    # Public API: top-level endpoints
    # ------------------------------------------------------------------
    def version(self) -> dict[str, Any]:
        """Returns the Pulse server's build + version metadata.

        This is a public endpoint — no JWT required.
        """
        return cast(
            "dict[str, Any]",
            self._request("GET", "/api/pulse/version", authenticated=False),
        )

    @property
    def token(self) -> str | None:
        """The currently-set bearer token, if any."""
        return self._token

    @token.setter
    def token(self, value: str | None) -> None:
        self._token = value

    # ------------------------------------------------------------------
    # Internal: request execution + error translation
    # ------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> Any:
        """Issues an HTTP request and translates errors to typed exceptions.

        Returns the parsed JSON body for 2xx responses, or ``None`` for
        204 No Content.
        """
        headers: dict[str, str] = {}
        if authenticated:
            if not self._token:
                raise PulseAuthError(
                    status_code=401,
                    path=path,
                    body={"error": "No token set. Call client.auth.login() first or pass token=..."},
                )
            headers["Authorization"] = f"Bearer {self._token}"

        response = self._http.request(
            method,
            path,
            json=json,
            params=params,
            headers=headers,
        )

        if response.status_code == 204:
            return None

        if 200 <= response.status_code < 300:
            if not response.content:
                return None
            try:
                return response.json()
            except ValueError:
                return response.text

        # Translate error → typed exception
        self._raise_for_error(response, path)
        # _raise_for_error always raises; this is for type-checker happiness
        raise PulseAPIError(response.status_code, path)

    @staticmethod
    def _raise_for_error(response: httpx.Response, path: str) -> None:
        body: dict[str, Any] | str | None
        try:
            body = response.json()
        except ValueError:
            body = response.text or None

        if response.status_code == 401:
            raise PulseAuthError(response.status_code, path, body)
        if response.status_code == 404:
            raise PulseNotFoundError(response.status_code, path, body)
        if response.status_code == 400:
            raise PulseValidationError(response.status_code, path, body)
        if response.status_code == 429:
            retry_after: int | None = None
            if isinstance(body, dict):
                value = body.get("retryAfterSeconds")
                if isinstance(value, (int, float)):
                    retry_after = int(value)
            if retry_after is None:
                header = response.headers.get("Retry-After")
                if header is not None:
                    try:
                        retry_after = int(header)
                    except ValueError:
                        retry_after = None
            raise PulseRateLimitError(response.status_code, path, body, retry_after)
        raise PulseAPIError(response.status_code, path, body)


# ----------------------------------------------------------------------
# Resource classes — one per OpenAPI tag.
# ----------------------------------------------------------------------


class _Resource:
    """Shared base — holds a back-reference to the parent client."""

    def __init__(self, client: PulseClient) -> None:
        self._client = client


class _AuthResource(_Resource):
    """``client.auth`` — authentication + session management."""

    def login(self, username: str, password: str) -> dict[str, Any]:
        """POST /api/auth/login — exchanges credentials for a JWT.

        On success, the returned token is cached on the parent client so
        subsequent calls authenticate automatically. The full response
        (including ``refreshToken`` and ``activeOrg``) is returned to the
        caller for downstream use.
        """
        response = cast(
            "dict[str, Any]",
            self._client._request(
                "POST",
                "/api/auth/login",
                json={"username": username, "password": password},
                authenticated=False,
            ),
        )
        token = response.get("token") if isinstance(response, dict) else None
        if token:
            self._client.token = token
        return response

    def refresh(self, refresh_token: str) -> dict[str, Any]:
        """POST /api/auth/refresh — exchanges a refresh token for a fresh JWT.

        The new ``token`` is cached on the parent client.
        """
        response = cast(
            "dict[str, Any]",
            self._client._request(
                "POST",
                "/api/auth/refresh",
                json={"refreshToken": refresh_token},
                authenticated=False,
            ),
        )
        token = response.get("token") if isinstance(response, dict) else None
        if token:
            self._client.token = token
        return response

    def organizations(self) -> list[dict[str, Any]]:
        """GET /api/auth/organizations — orgs the current user is a member of."""
        result = self._client._request("GET", "/api/auth/organizations")
        if isinstance(result, dict):
            orgs = result.get("organizations", [])
            if isinstance(orgs, list):
                return cast("list[dict[str, Any]]", orgs)
        return []

    def switch_org(self, org_id: str) -> dict[str, Any]:
        """POST /api/auth/switch-org — switches the active org.

        The new JWT (with updated ``orgId`` claim) is cached on the parent client.
        """
        response = cast(
            "dict[str, Any]",
            self._client._request(
                "POST",
                "/api/auth/switch-org",
                json={"orgId": org_id},
            ),
        )
        token = response.get("token") if isinstance(response, dict) else None
        if token:
            self._client.token = token
        return response


class _PipelinesResource(_Resource):
    """``client.pipelines`` — create / list / inspect / delete pipelines."""

    def list(self) -> list[dict[str, Any]]:
        """GET /api/pulse/pipelines — every pipeline in the current org."""
        result = self._client._request("GET", "/api/pulse/pipelines")
        if isinstance(result, dict):
            pipelines = result.get("pipelines", [])
            if isinstance(pipelines, list):
                return cast("list[dict[str, Any]]", pipelines)
        return []

    def get(self, pipeline_id: str) -> dict[str, Any]:
        """GET /api/pulse/pipelines/{id} — one pipeline by id."""
        return cast(
            "dict[str, Any]",
            self._client._request("GET", f"/api/pulse/pipelines/{pipeline_id}"),
        )

    def create(self, definition: dict[str, Any]) -> dict[str, Any]:
        """POST /api/pulse/pipelines — creates + deploys a new pipeline.

        ``definition`` must follow the CreatePipelineRequest schema
        (see /openapi.yaml). At minimum: ``name`` + ``nodes``.
        """
        return cast(
            "dict[str, Any]",
            self._client._request("POST", "/api/pulse/pipelines", json=definition),
        )

    def delete(self, pipeline_id: str) -> None:
        """DELETE /api/pulse/pipelines/{id} — tears down the pipeline."""
        self._client._request("DELETE", f"/api/pulse/pipelines/{pipeline_id}")


class _AgentsResource(_Resource):
    """``client.agents`` — inspect deployed agents (read-only)."""

    def list(self) -> list[dict[str, Any]]:
        """GET /api/pulse/agents — every deployed agent in the current org."""
        result = self._client._request("GET", "/api/pulse/agents")
        if isinstance(result, dict):
            agents = result.get("agents", [])
            if isinstance(agents, list):
                return cast("list[dict[str, Any]]", agents)
        return []

    def get(self, agent_id: str) -> dict[str, Any]:
        """GET /api/pulse/agents/{id} — one agent by id."""
        return cast(
            "dict[str, Any]",
            self._client._request("GET", f"/api/pulse/agents/{agent_id}"),
        )


class _TemplatesResource(_Resource):
    """``client.templates`` — first-party pipeline template catalog."""

    def list(self) -> list[dict[str, Any]]:
        """GET /api/pulse/templates — the 223+ first-party templates."""
        result = self._client._request("GET", "/api/pulse/templates")
        if isinstance(result, dict):
            templates = result.get("templates", [])
            if isinstance(templates, list):
                return cast("list[dict[str, Any]]", templates)
        return []


class _UsersResource(_Resource):
    """``client.users`` — user management (admin only)."""

    def list(self) -> list[dict[str, Any]]:
        """GET /api/pulse/users — every user in the current org.

        Requires the caller to have the ``USERS_LIST`` permission atom (Owner
        and Platform Admin personas by default — see B-105).
        """
        result = self._client._request("GET", "/api/pulse/users")
        if isinstance(result, dict):
            users = result.get("users", [])
            if isinstance(users, list):
                return cast("list[dict[str, Any]]", users)
        return []


class _EventsResource(_Resource):
    """``client.events`` — live SSE stream of events flowing through the engine.

    Usage:

        >>> with PulseClient("http://localhost:9090", token=token) as client:
        ...     for event in client.events.stream():
        ...         print(event["type"], event.get("payload"))

    The stream blocks the calling thread (synchronous generator). For an
    async iterator using ``httpx.AsyncClient``, see ``pulse_client.async_events``
    (planned for v3.0 alongside ``AsyncPulseClient``).

    Cancellation: break out of the for-loop. The underlying HTTP connection
    is closed by the generator's ``__exit__``.
    """

    def stream(
        self,
        *,
        timeout: float | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Subscribes to ``GET /api/pulse/events/stream`` and yields events.

        Args:
            timeout: Per-read timeout in seconds. ``None`` disables (the
                connection stays open until the server closes it or the
                caller breaks out of the loop). Default: inherit the client
                timeout (which itself defaults to 30s — likely too short
                for long-running streams; override here).

        Yields:
            One dict per event, parsed from the SSE ``data: ...`` line.
            Heartbeat lines, comments (``:keep-alive``), and SSE-control
            fields (``event:``, ``id:``, ``retry:``) are silently
            consumed but not yielded.

        Raises:
            PulseAuthError: If no token is set or the server rejects it.
            PulseAPIError: If the server returns a non-2xx response.
        """
        client = self._client
        if not client._token:
            raise PulseAuthError(
                status_code=401,
                path="/api/pulse/events/stream",
                body={"error": "No token set for SSE stream"},
            )
        headers = {
            "Authorization": f"Bearer {client._token}",
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        }
        with client._http.stream(
            "GET",
            "/api/pulse/events/stream",
            headers=headers,
            timeout=timeout if timeout is not None else client._http.timeout,
        ) as response:
            if response.status_code >= 400:
                response.read()
                client._raise_for_error(response, "/api/pulse/events/stream")
                return

            # SSE parser — accumulate `data:` lines per event, dispatch on
            # blank line. See https://html.spec.whatwg.org/multipage/server-sent-events.html
            data_lines: list[str] = []
            for raw_line in response.iter_lines():
                if raw_line is None:
                    continue
                if raw_line == "":
                    # Event boundary — assemble and yield
                    if data_lines:
                        payload = "\n".join(data_lines)
                        data_lines = []
                        try:
                            yield json.loads(payload)
                        except ValueError:
                            # Non-JSON event payload — surface as raw string
                            yield {"data": payload}
                    continue
                if raw_line.startswith(":"):
                    continue  # SSE comment / keep-alive
                if raw_line.startswith("data:"):
                    data_lines.append(raw_line[5:].lstrip())
                # Other SSE fields (`event:`, `id:`, `retry:`) are
                # consumed but not surfaced — Pulse's server doesn't use
                # them today. Add explicit dispatch when it does.
