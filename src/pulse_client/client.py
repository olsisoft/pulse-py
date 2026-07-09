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

import builtins
import json
import random
import time
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
USER_AGENT = "pulse-client-python/2.7.5"

# HTTP methods that are safe to retry on a transient 5xx / transport error
# (the request either has no side effect or is idempotent). 429 (rate-limited)
# is always safe to retry regardless of method — the request was rejected, not
# processed — so it is handled separately.
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})


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
        max_retries: int = 0,
        retry_backoff: float = 0.2,
        retry_max_backoff: float = 10.0,
        retry_on_status: tuple[int, ...] = (502, 503, 504),
        retry_idempotent_only: bool = True,
    ) -> None:
        """Construct a client.

        Retries are **opt-in and off by default** (``max_retries=0``). When
        ``max_retries > 0`` the client retries transient failures with bounded,
        full-jitter exponential backoff:

        * **429 (rate limited)** is always retried (the request was rejected,
          never processed) and honours ``retryAfterSeconds`` / the ``Retry-After``
          header before falling back to backoff;
        * ``retry_on_status`` 5xx and transport (connect/read) errors are retried
          only for idempotent methods (GET/HEAD/PUT/DELETE/OPTIONS) unless
          ``retry_idempotent_only=False`` — so a POST create is never silently
          duplicated by a retry.
        """
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._max_retries = max(0, max_retries)
        self._retry_backoff = retry_backoff
        self._retry_max_backoff = retry_max_backoff
        self._retry_on_status = tuple(retry_on_status)
        self._retry_idempotent_only = retry_idempotent_only
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
        self.iq = _IQResource(self)
        self.models = _ModelsResource(self)
        self.wasm = _WasmResource(self)
        self.connectors = _ConnectorsResource(self)
        # Imported locally to avoid an import cycle (streams imports
        # PulseClient only at type-check time via TYPE_CHECKING).
        from pulse_client.streams import StreamsResource

        self.streams = StreamsResource(self)

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

    def duplex(self, agent_id: str, *, ws_url: str | None = None) -> Any:
        """B-114 — open a bidirectional duplex channel to an agent.

        Returns an async context manager (:class:`~pulse_client._duplex.DuplexChannel`)
        that streams events IN and receives the agent's correlated outputs OUT
        on a single WebSocket — the synchronous-decision path (fraud, pricing,
        A/B assignment). Requires the ``[duplex]`` extra
        (``pip install streamflow-pulse-client[duplex]``).

        The endpoint runs on the Pulse WebSocket port (REST port + 1); pass
        ``ws_url`` to override the derived URL.

        Example::

            async with client.duplex("fraud-detector") as ch:
                await ch.send({"amount": 5000}, correlation_id="tx-1")
                signal = await ch.recv()
                # signal["correlation_id"] == "tx-1"
        """
        if not isinstance(agent_id, str) or not agent_id.strip():
            raise ValueError("agent_id must be a non-empty string")
        from pulse_client._duplex import DuplexChannel, derive_ws_url

        url = ws_url or derive_ws_url(self._base_url, agent_id, self._token)
        return DuplexChannel(url)

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
    def _backoff_delay(self, attempt: int) -> float:
        """Full-jitter exponential backoff: uniform(0, min(max, base * 2**attempt))."""
        ceiling = min(self._retry_max_backoff, self._retry_backoff * (2 ** attempt))
        return random.uniform(0.0, max(0.0, ceiling))

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> Any:
        """Opt-in retry wrapper around :meth:`_send_once` (off by default).

        See :meth:`__init__` for the policy. A no-retry client (``max_retries=0``)
        makes exactly one attempt — identical to the pre-retry behaviour.
        """
        idempotent = method.upper() in _IDEMPOTENT_METHODS
        attempt = 0
        while True:
            try:
                return self._send_once(
                    method, path, json=json, params=params,
                    files=files, data=data, authenticated=authenticated,
                )
            except PulseRateLimitError as exc:
                # 429: rejected, never processed → always safe to retry; honour Retry-After.
                if attempt >= self._max_retries:
                    raise
                delay = (
                    float(exc.retry_after_seconds)
                    if exc.retry_after_seconds is not None
                    else self._backoff_delay(attempt)
                )
                time.sleep(max(0.0, delay))
            except PulseAPIError as exc:
                # Transient 5xx (PulseRateLimitError already handled above) — retry only
                # for idempotent methods unless explicitly opted out. 401/404/400 have
                # statuses outside retry_on_status → re-raised here.
                retryable = (
                    attempt < self._max_retries
                    and exc.status_code in self._retry_on_status
                    and (idempotent or not self._retry_idempotent_only)
                )
                if not retryable:
                    raise
                time.sleep(self._backoff_delay(attempt))
            except httpx.TransportError:
                retryable = (
                    attempt < self._max_retries
                    and (idempotent or not self._retry_idempotent_only)
                )
                if not retryable:
                    raise
                time.sleep(self._backoff_delay(attempt))
            attempt += 1

    def _send_once(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> Any:
        """Issues a single HTTP request and translates errors to typed exceptions.

        Returns the parsed JSON body for 2xx responses, or ``None`` for
        204 No Content. When ``files`` is given the request is sent as
        ``multipart/form-data`` (with optional ``data`` form fields) instead of
        a JSON body — used by the model-upload endpoint.
        """
        headers: dict[str, str] = {}
        if authenticated:
            if not self._token:
                raise PulseAuthError(
                    status_code=401,
                    path=path,
                    body={
                        "error": "No token set. Call client.auth.login() first or pass token=..."
                    },
                )
            headers["Authorization"] = f"Bearer {self._token}"

        response = self._http.request(
            method,
            path,
            json=json if files is None else None,
            params=params,
            files=files,
            data=data,
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
        token = (response.get("accessToken") or response.get("token")) if isinstance(response, dict) else None
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
        token = (response.get("accessToken") or response.get("token")) if isinstance(response, dict) else None
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
        token = (response.get("accessToken") or response.get("token")) if isinstance(response, dict) else None
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
            self._client._request("GET", f"/api/pulse/pipelines/{_encode_path_segment(pipeline_id)}"),
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
        self._client._request("DELETE", f"/api/pulse/pipelines/{_encode_path_segment(pipeline_id)}")


class _AgentsResource(_Resource):
    """``client.agents`` — list / get / update / delete deployed agents."""

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
            self._client._request("GET", f"/api/pulse/agents/{_encode_path_segment(agent_id)}"),
        )

    def update(self, agent_id: str, config: dict[str, Any]) -> dict[str, Any]:
        """B-115 Phase 1 — PUT /api/pulse/agents/{id}: replace the agent's config.

        ``config`` is the FULL agent config (not a partial merge) — at minimum
        ``name``; optional ``description`` / ``engineType`` / ``inputTopic`` /
        ``outputTopic`` / ``instances`` / ``monthlyBudget`` / ``config`` fall
        back to safe defaults when omitted. See the UpdateAgentRequest schema
        in openapi.yaml.

        Today this triggers a full stop + persist + start cycle on the engine
        side — the agent is briefly unavailable while the swap happens.
        Existing state in the agent's keyed store is preserved (the swap is
        config-only). Phase 2 (B-115-engine) will add atomic event-boundary
        swap so hot-reloadable changes apply with no downtime.

        Returns the post-update agent snapshot (same shape as :meth:`get`).
        Raises :class:`PulseValidationError` on a bad config (self-loop,
        invalid streaming operators), :class:`PulseNotFoundError` if the
        agent doesn't exist.
        """
        return cast(
            "dict[str, Any]",
            self._client._request("PUT", f"/api/pulse/agents/{_encode_path_segment(agent_id)}", json=config),
        )

    def delete(self, agent_id: str) -> None:
        """DELETE /api/pulse/agents/{id} — stop the agent + remove its config row.

        The agent's keyed state store is also dropped. Requires the
        ``AGENT_DELETE`` permission.
        """
        self._client._request("DELETE", f"/api/pulse/agents/{_encode_path_segment(agent_id)}")


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


class _ConnectorsResource(_Resource):
    """``client.connectors`` — the connector catalogue (the B-093 analytics
    family + every other native / bridged connector), the same list the Pipeline
    Studio palette and ``pulse connectors list`` show.

    Each entry is ``{"subType", "displayName", "configFields": [...]}``. Use the
    ``subType`` as a sink/source node ``type`` in a pipeline definition passed to
    ``client.pipelines.deploy``. Bridged connectors (Kafka, JDBC, S3, Segment,
    Amplitude, GA4, …) only appear when the enterprise bridge JAR is on the
    server's classpath.
    """

    def list(self) -> dict[str, Any]:
        """GET /api/pulse/connectors — ``{"sources": [...], "sinks": [...]}``."""
        result = self._client._request("GET", "/api/pulse/connectors")
        if isinstance(result, dict):
            return cast("dict[str, Any]", result)
        return {"sources": [], "sinks": []}

    # NB: this class defines a ``list`` method, which shadows the builtin
    # ``list`` inside class-scoped (PEP 563) annotations — so these three
    # qualify it as ``builtins.list`` to stay unambiguous for the type checker
    # (ruff UP006 only rewrites ``typing.List``, so this stays clean).
    def sinks(self) -> builtins.list[dict[str, Any]]:
        """Just the sink connectors."""
        return self._entries("sinks")

    def sources(self) -> builtins.list[dict[str, Any]]:
        """Just the source connectors."""
        return self._entries("sources")

    def _entries(self, kind: str) -> builtins.list[dict[str, Any]]:
        entries = self.list().get(kind, [])
        return cast("list[dict[str, Any]]", entries) if isinstance(entries, list) else []


class _ModelsResource(_Resource):
    """``client.models`` — B-112 embedded ML model registry.

    Upload ONNX models that the streaming ``ml_predict`` operator scores events
    against, in-process on the Pulse engine (no model-server hop). Models are
    org-scoped; upload / delete require the ADMIN role.

    Example:
        >>> client.models.upload(
        ...     name="fraud-classifier",
        ...     path="./model.onnx",
        ...     input_schema={"amount": "float", "country": "string"},
        ...     output_schema={"fraud_score": "float", "label": "string"},
        ... )
        >>> builder.from_topic("transactions").ml_predict(
        ...     model="fraud-classifier",
        ...     input_fields=["amount", "country"],
        ...     output_field="prediction",
        ... ).to_topic("scored")
    """

    def upload(
        self,
        *,
        name: str,
        path: str | None = None,
        data: bytes | None = None,
        runtime: str = "onnx",
        input_schema: dict[str, str] | None = None,
        output_schema: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """POST /api/pulse/ml-models — upload (or replace) a model.

        Supply the model either by file ``path`` or raw ``data`` bytes.
        ``input_schema`` (feature-name → type, in the model's input order) is
        used to pack features into the model's input tensor; ``output_schema``
        is informational. Replacing an existing name hot-swaps the model with
        no agent restart.

        Args:
            name: Model name referenced by ``ml_predict(model=...)``.
            path: Filesystem path to the ``.onnx`` file.
            data: Raw model bytes (alternative to ``path``).
            runtime: Model runtime — only ``"onnx"`` is supported today.
            input_schema: Ordered feature-name → type map.
            output_schema: Output-name → type map (informational).

        Returns:
            The persisted model metadata (name, runtime, sha256, version, …).
        """
        _require_nonblank_models("name", name)
        if (path is None) == (data is None):
            raise ValueError("provide exactly one of 'path' or 'data'")
        if path is not None:
            with open(path, "rb") as fh:
                blob = fh.read()
            filename = path.rsplit("/", 1)[-1]
        else:
            blob = data  # type: ignore[assignment]
            filename = f"{name}.onnx"
        if not blob:
            raise ValueError("model bytes are empty")

        form: dict[str, Any] = {"name": name, "runtime": runtime}
        if input_schema is not None:
            form["inputSchema"] = json.dumps(input_schema)
        if output_schema is not None:
            form["outputSchema"] = json.dumps(output_schema)
        files = {"model": (filename, blob, "application/octet-stream")}
        return cast(
            "dict[str, Any]",
            self._client._request(
                "POST", "/api/pulse/ml-models", files=files, data=form
            ),
        )

    def list(self) -> list[dict[str, Any]]:
        """GET /api/pulse/ml-models — models registered for the caller's org."""
        result = self._client._request("GET", "/api/pulse/ml-models")
        if isinstance(result, dict):
            models = result.get("models", [])
            if isinstance(models, list):
                return cast("list[dict[str, Any]]", models)
        return []

    def get(self, name: str) -> dict[str, Any]:
        """GET /api/pulse/ml-models/{name} — metadata for one model."""
        _require_nonblank_models("name", name)
        return cast(
            "dict[str, Any]",
            self._client._request("GET", f"/api/pulse/ml-models/{_encode_path_segment(name)}"),
        )

    def delete(self, name: str) -> None:
        """DELETE /api/pulse/ml-models/{name} — remove a model (ADMIN)."""
        _require_nonblank_models("name", name)
        self._client._request("DELETE", f"/api/pulse/ml-models/{_encode_path_segment(name)}")


def _require_nonblank_models(field: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")


def _read_uleb128(blob: bytes, pos: int) -> tuple[int, int]:
    """Decode an unsigned LEB128 integer from ``blob`` at ``pos``.

    Returns ``(value, next_pos)``. Raises ``ValueError`` on truncated input.
    """
    result = 0
    shift = 0
    while True:
        if pos >= len(blob):
            raise ValueError("malformed WASM module")
        byte = blob[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("malformed WASM module")


def _validate_wasm_module(blob: bytes) -> None:
    """Client-side pre-upload validation of a WASM module's bytes.

    Mirrors the server's ``ChicoryWasmRunner.validateModule`` checks so a
    non-conforming module is rejected locally with a clear message, without an
    HTTP round-trip. Inspects the binary; it does not execute it.

    Raises:
        ValueError: if the bytes are not a conforming sandbox module — too
            short, bad magic/version, importing host functions, malformed, or
            missing the required ``alloc`` / ``process`` / ``memory`` exports.
    """
    if not blob or len(blob) < 8:
        raise ValueError("not a WASM module: too short")
    if blob[0:4] != b"\x00asm" or blob[4:8] != b"\x01\x00\x00\x00":
        raise ValueError("not a WASM module (bad magic/version)")

    names: set[str] = set()
    pos = 8
    n = len(blob)
    while pos < n:
        section_id = blob[pos]
        pos += 1
        size, pos = _read_uleb128(blob, pos)
        payload_end = pos + size
        if payload_end > n:
            raise ValueError("malformed WASM module")
        if section_id == 2:  # imports
            count, p = _read_uleb128(blob, pos)
            if count > 0:
                raise ValueError(
                    "WASM module imports host functions; it must be a pure "
                    "sandbox (build with no WASI/host imports)"
                )
        elif section_id == 7:  # exports
            count, p = _read_uleb128(blob, pos)
            for _ in range(count):
                name_len, p = _read_uleb128(blob, p)
                name_end = p + name_len
                if name_end > payload_end:
                    raise ValueError("malformed WASM module")
                try:
                    names.add(blob[p:name_end].decode("utf-8"))
                except UnicodeDecodeError as exc:
                    raise ValueError("malformed WASM module") from exc
                p = name_end
                if p >= payload_end:
                    raise ValueError("malformed WASM module")
                p += 1  # kind byte
                _, p = _read_uleb128(blob, p)  # export index
        pos = payload_end

    required = {"alloc", "process", "memory"}
    if not required.issubset(names):
        raise ValueError("WASM module must export alloc, process and memory")


class _WasmResource(_Resource):
    """``client.wasm`` — B-110 sandboxed WASM module registry.

    Upload WebAssembly modules that the streaming ``wasm`` operator runs over
    events, sandboxed in pure-Java Chicory on the engine (no host syscalls).
    Modules are org-scoped; upload / delete require the ADMIN role.

    Example:
        >>> client.wasm.upload(name="pii-redactor", path="./redactor.wasm")
        >>> builder.from_topic("events").wasm(module="pii-redactor").to_topic("clean")
    """

    def upload(
        self,
        *,
        name: str,
        path: str | None = None,
        data: bytes | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """POST /api/pulse/wasm-modules — upload (or replace) a module.

        Supply the module by file ``path`` or raw ``data`` bytes. The module is
        validated (must parse, import no host functions, export
        alloc/process/memory) before persisting. Replacing a name hot-swaps it
        with no agent restart.

        Args:
            name: Module name referenced by ``wasm(module=...)``.
            path: Filesystem path to the ``.wasm`` file.
            data: Raw module bytes (alternative to ``path``).
            description: Optional human-readable description.

        Returns:
            The persisted module metadata (name, sha256, version, …).
        """
        _require_nonblank_models("name", name)
        if (path is None) == (data is None):
            raise ValueError("provide exactly one of 'path' or 'data'")
        if path is not None:
            with open(path, "rb") as fh:
                blob = fh.read()
            filename = path.rsplit("/", 1)[-1]
        else:
            blob = data  # type: ignore[assignment]
            filename = f"{name}.wasm"
        if not blob:
            raise ValueError("module bytes are empty")
        _validate_wasm_module(blob)
        form: dict[str, Any] = {"name": name}
        if description is not None:
            form["description"] = description
        files = {"module": (filename, blob, "application/wasm")}
        return cast(
            "dict[str, Any]",
            self._client._request(
                "POST", "/api/pulse/wasm-modules", files=files, data=form
            ),
        )

    def list(self) -> list[dict[str, Any]]:
        """GET /api/pulse/wasm-modules — modules registered for the caller's org."""
        result = self._client._request("GET", "/api/pulse/wasm-modules")
        if isinstance(result, dict):
            modules = result.get("modules", [])
            if isinstance(modules, list):
                return cast("list[dict[str, Any]]", modules)
        return []

    def get(self, name: str) -> dict[str, Any]:
        """GET /api/pulse/wasm-modules/{name} — metadata for one module."""
        _require_nonblank_models("name", name)
        return cast(
            "dict[str, Any]",
            self._client._request("GET", f"/api/pulse/wasm-modules/{_encode_path_segment(name)}"),
        )

    def delete(self, name: str) -> None:
        """DELETE /api/pulse/wasm-modules/{name} — remove a module (ADMIN)."""
        _require_nonblank_models("name", name)
        self._client._request("DELETE", f"/api/pulse/wasm-modules/{_encode_path_segment(name)}")


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


class _IQResource(_Resource):
    """``client.iq`` — B-106 Interactive Queries.

    Live state of streaming agents, queryable like a database from any
    microservice. Five operations against the engine's state store:

    * :meth:`summary` — headline (size, backend, last checkpoint).
    * :meth:`get` — point lookup at a key.
    * :meth:`scan` — paginated range scan returning key/value pairs.
    * :meth:`keys` — paginated range scan returning keys only.
    * :meth:`query` — filtered / projected / grouped query.

    The killer use case is a synchronous decision microservice (fraud,
    rate-limit, pricing) that calls :meth:`get` on every request and
    reads agent state from RAM with zero ingest-to-decision lag:

        >>> with PulseClient(url, token=jwt) as client:
        ...     state = client.iq.get("fraud-detector", "customer-42")
        ...     if state["value"]["tx_count_60s"] > 5:
        ...         deny_payment()

    All endpoints require the ``AGENT_READ`` permission (Owner, Platform
    Admin, Developer, Auditor personas by default — see B-105).

    Server responses for ``state`` / ``scan`` / ``keys`` / ``query`` are
    returned as raw dicts so callers can paginate, filter, and inspect
    response metadata (``truncated``, ``limitApplied``, ``totalScanned``)
    without going through a wrapper layer.
    """

    def summary(self, agent_id: str) -> dict[str, Any]:
        """``GET /api/pulse/iq/agents/{id}/state`` — headline state summary.

        Returns the IQSummary dict — fields ``agentId``, ``queryable``,
        ``backend``, ``hotSize``, ``hotBytes``, ``coldSize``, ``coldBytes``,
        ``lastCheckpointId``, ``totalSize``. All always present;
        ``queryable=False`` when the agent has no live streaming backend.
        """
        path = f"/api/pulse/iq/agents/{_encode_path_segment(agent_id)}/state"
        return cast("dict[str, Any]", self._client._request("GET", path))

    def get(self, agent_id: str, key: str, *, as_of: str | None = None) -> dict[str, Any]:
        """``GET /api/pulse/iq/agents/{id}/state/value/{key}`` — point lookup.

        Returns an IQValue dict with fields ``agentId``, ``key``, ``value``.
        ``value`` is the JSON-decoded payload; ``None`` is a legal value
        (the server distinguishes "key present with null" from "key absent",
        the latter raises :class:`PulseNotFoundError`).

        B-113 — pass ``as_of`` to read the value as it was at a past instant
        (time-travel) instead of the live value. Accepts ``now``, a relative
        offset (``-1h``, ``-30m``, ``-7d``), an ISO-8601 instant, or epoch
        millis. The response then also carries ``asOf`` (resolved epoch ms)::

            state_1h_ago = client.iq.get("user-sessions", "u42", as_of="-1h")

        Raises:
            PulseNotFoundError: key absent OR agent not queryable. Check
                ``e.body["error"]`` ("Key not found" vs "Agent has no
                queryable state") to distinguish, and ``e.body["reason"]``
                for the not-queryable cause.
        """
        path = (
            f"/api/pulse/iq/agents/{_encode_path_segment(agent_id)}"
            f"/state/value/{_encode_path_segment(key)}"
        )
        params = {"as_of": as_of} if as_of is not None else None
        return cast("dict[str, Any]", self._client._request("GET", path, params=params))

    def diff(self, agent_id: str, key: str, *, from_: str = "-1h", to: str = "now") -> dict[str, Any]:
        """``GET /api/pulse/iq/agents/{id}/state/diff/{key}`` — B-113 state diff.

        Field-level delta of ``key``'s state between two instants. ``from_``
        and ``to`` accept the same specs as ``as_of`` (default: last hour).
        Returns ``{from, to, fromTs, toTs, changes}`` where ``changes`` maps
        each changed field to ``{delta?, from, to}`` (``delta`` present for
        numeric fields), or ``{added}`` / ``{removed}``::

            d = client.iq.diff("user-sessions", "u42", from_="-1h", to="now")
            # d["changes"]["cart_value"] == {"delta": 70.0, "from": 0, "to": 70}
        """
        path = (
            f"/api/pulse/iq/agents/{_encode_path_segment(agent_id)}"
            f"/state/diff/{_encode_path_segment(key)}"
        )
        return cast(
            "dict[str, Any]",
            self._client._request("GET", path, params={"from": from_, "to": to}),
        )

    def scan(
        self,
        agent_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """``GET /api/pulse/iq/agents/{id}/state/scan`` — paginated range scan.

        Returns the IQScanResponse dict. Inspect ``truncated`` to decide if
        there's more — if so, paginate by setting ``start`` to the last
        returned key plus a sentinel suffix on the next call.

        Args:
            start: Inclusive lower bound on the key range; ``None`` = beginning.
            end: Exclusive upper bound; ``None`` = end.
            limit: Page size; server clamps to ``[1, 1000]``. Default 100.
                If exceeded, the response also carries the
                ``X-Pulse-Pagination-Clamped: true`` header (not surfaced
                in the body — read via the underlying response if needed).

        Raises:
            PulseNotFoundError: agent not queryable.
        """
        path = f"/api/pulse/iq/agents/{_encode_path_segment(agent_id)}/state/scan"
        return cast(
            "dict[str, Any]",
            self._client._request("GET", path, params=_iq_scan_params(start, end, limit)),
        )

    def list_keys(
        self,
        agent_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """``GET /api/pulse/iq/agents/{id}/state/keys`` — keys-only range scan.

        Same shape as :meth:`scan` minus the values. Returns the
        IQKeysResponse dict (``keys`` field is a list of strings).

        Named ``list_keys`` (not ``keys``) to avoid shadowing the builtin
        ``dict.keys`` semantics readers expect from the method name.
        """
        path = f"/api/pulse/iq/agents/{_encode_path_segment(agent_id)}/state/keys"
        return cast(
            "dict[str, Any]",
            self._client._request("GET", path, params=_iq_scan_params(start, end, limit)),
        )

    def query(
        self,
        agent_id: str,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
        filter: dict[str, Any] | None = None,  # noqa: A002 - shadows builtin intentionally
        projection: list[str] | None = None,
        group_by: str | None = None,
    ) -> dict[str, Any]:
        """``POST /api/pulse/iq/agents/{id}/state/query`` — filtered / grouped query.

        Args:
            start, end, limit: Key-range + page-size, same as :meth:`scan`.
            filter: Recursive filter expression. Leaf shape:
                ``{"field": "name", "op": "eq|neq|gt|gte|lt|lte|exists|notexists|contains|in", "value": ...}``.
                Compound: ``{"and": [...]}``, ``{"or": [...]}``, ``{"not": {...}}``.
                Use ``"$value"`` as field to test the value itself (scalar states).
                Each node must carry exactly ONE discriminator; mixing is a 400.
            projection: When supplied, returned entries contain only these
                fields (non-map values are returned unchanged).
            group_by: Group entries by this field; switches the response
                shape to IQQueryGroupedResponse (``groups`` array of
                ``{groupKey, count}`` pairs).

        Returns:
            IQQueryFlatResponse if ``group_by`` is ``None``, else
            IQQueryGroupedResponse. Inspect ``truncated`` + ``totalScanned``
            (grouped queries cap scan at 100_000 keys).

        Raises:
            PulseValidationError: invalid filter syntax (HTTP 400 from server).
            PulseNotFoundError: agent not queryable.
        """
        body: dict[str, Any] = {}
        if start is not None:
            body["start"] = start
        if end is not None:
            body["end"] = end
        if limit != 100:
            body["limit"] = limit
        if filter is not None:
            body["filter"] = filter
        if projection is not None:
            body["projection"] = projection
        if group_by is not None:
            body["groupBy"] = group_by
        path = f"/api/pulse/iq/agents/{_encode_path_segment(agent_id)}/state/query"
        return cast(
            "dict[str, Any]",
            self._client._request("POST", path, json=body if body else None),
        )


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

    def replay(
        self,
        *,
        affecting_state: str,
        key: str,
        from_: str = "-1h",
        to: str = "now",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """B-113 — the changes that touched a state key between two instants.

        ``affecting_state`` is the agent whose state store to inspect; ``key``
        is the state key. ``from_`` / ``to`` accept the same specs as
        ``iq.get(as_of=...)``. Returns the ordered list of changes, each with
        ``timestamp``, ``changeType`` (``PUT`` / ``DELETE``), the resulting
        ``value``, and ``eventId`` when known::

            changes = client.events.replay(
                affecting_state="user-sessions", key="u42",
                from_="2026-05-24T10:00:00Z", to="2026-05-24T11:00:00Z")
        """
        path = (
            f"/api/pulse/iq/agents/{_encode_path_segment(affecting_state)}"
            f"/state/replay/{_encode_path_segment(key)}"
        )
        result = self._client._request(
            "GET", path, params={"from": from_, "to": to, "limit": limit}
        )
        if isinstance(result, dict):
            changes = result.get("changes", [])
            if isinstance(changes, list):
                return cast("list[dict[str, Any]]", changes)
        return []


# ---------------------------------------------------------------------------
# Module-level helpers (not part of the public API).
# ---------------------------------------------------------------------------


def _encode_path_segment(segment: str) -> str:
    """URL-encodes a path segment so values containing ``/``, spaces, etc.
    survive the round-trip to the server intact.

    Used for agent ids + IQ keys. The server-side IQ handler explicitly
    URL-decodes the key segment so operators can query, e.g.,
    ``user:123/orders`` without the slash splitting the path.

    We use :func:`urllib.parse.quote` with ``safe=""`` so that every
    character outside the unreserved set is percent-encoded — including
    ``/``, which is the whole point.
    """
    from urllib.parse import quote

    return quote(segment, safe="")


def _iq_scan_params(start: str | None, end: str | None, limit: int) -> dict[str, Any]:
    """Builds the ``?start=&end=&limit=`` query dict for IQ scan/keys.

    Skips keys that are ``None`` so the URL stays clean (httpx omits
    None-valued params). ``limit`` is always sent (default 100, server
    clamps to ``[1, 1000]``).
    """
    params: dict[str, Any] = {"limit": limit}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    return params
