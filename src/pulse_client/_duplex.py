"""B-114 — bidirectional duplex channel for synchronous decision agents.

Opens ONE WebSocket to ``/api/pulse/agents/{id}/duplex``: events are streamed
IN and the agent's correlated outputs come back OUT on the same connection,
matched by a correlation id. Eliminates the 2-connection publish-then-poll
pattern for decision microservices (fraud, pricing, A/B assignment).

The duplex endpoint runs on the Pulse WebSocket port (REST port + 1 by
convention); :func:`derive_ws_url` derives it from the client's ``base_url``.

WebSocket support is an optional dependency — install with
``pip install streamflow-pulse-client[duplex]``.

Example::

    async with client.duplex("fraud-detector") as ch:
        for tx in incoming:
            await ch.send(tx, correlation_id=tx["id"])
            signal = await ch.recv()        # the agent's output for THIS input
            if signal["payload"]["decision"] == "DENY":
                ...
"""

from __future__ import annotations

import json
import uuid
from types import TracebackType
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

from pulse_client.exceptions import PulseAPIError

_INSTALL_HINT = (
    "duplex requires the 'websockets' package — install with "
    "`pip install streamflow-pulse-client[duplex]`"
)


def derive_ws_url(base_url: str, agent_id: str, token: str | None) -> str:
    """Builds the duplex WebSocket URL from the client's REST ``base_url``.

    http→ws / https→wss, host unchanged, port → REST port + 1 (the Pulse
    WebSocket server convention). The JWT, when set, rides as a ``token`` query
    param (the server reads it from the upgrade request line).
    """
    parts = urlsplit(base_url)
    scheme = "wss" if parts.scheme == "https" else "ws"
    host = parts.hostname or "localhost"
    netloc = host if parts.port is None else f"{host}:{parts.port + 1}"
    path = f"/api/pulse/agents/{quote(agent_id, safe='')}/duplex"
    query = f"token={quote(token)}" if token else ""
    return urlunsplit((scheme, netloc, path, query, ""))


class DuplexChannel:
    """An open duplex session. Use as an async context manager.

    :meth:`send` publishes an event to the agent's input topic and returns its
    correlation id; :meth:`recv` returns the next output event the agent
    produced (each carries a ``correlation_id`` matching the input that caused
    it). Acknowledgement / keep-alive frames are consumed transparently.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._ws: Any = None

    async def __aenter__(self) -> DuplexChannel:
        try:
            import websockets  # noqa: PLC0415 — optional dep, imported lazily
        except ImportError as exc:  # pragma: no cover - exercised via env without extra
            raise RuntimeError(_INSTALL_HINT) from exc
        self._ws = await websockets.connect(self._url)
        # The server sends a 'connected' frame first (or 'error' + close for an
        # unknown agent / disabled duplex). Surface the error eagerly.
        first = json.loads(await self._ws.recv())
        if first.get("type") == "error":
            await self._close()
            raise PulseAPIError(400, self._url, first)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._close()

    async def send(self, payload: dict[str, Any], *, correlation_id: str | None = None) -> str:
        """Publish ``payload`` to the agent's input topic.

        Returns the correlation id (generated when not supplied) that the
        matching output will carry.
        """
        if self._ws is None:
            raise RuntimeError("duplex channel is not open")
        cid = correlation_id or str(uuid.uuid4())
        await self._ws.send(
            json.dumps({"type": "send", "correlationId": cid, "payload": payload})
        )
        return cid

    async def recv(self) -> dict[str, Any]:
        """Return the next agent output event (skips ack / pong frames).

        The returned dict is the agent's output event (``id`` / ``topic`` /
        ``type`` / ``key`` / ``payload``) plus a ``correlation_id`` field
        identifying the input that produced it.
        """
        if self._ws is None:
            raise RuntimeError("duplex channel is not open")
        while True:
            msg = json.loads(await self._ws.recv())
            kind = msg.get("type")
            if kind == "output":
                event = msg.get("event")
                event = dict(event) if isinstance(event, dict) else {"value": event}
                event["correlation_id"] = msg.get("correlationId")
                return event
            if kind == "error":
                raise PulseAPIError(400, self._url, msg)
            # ack / pong / connected → transparently skipped

    async def _close(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            finally:
                self._ws = None
