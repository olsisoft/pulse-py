"""B-114 — tests for the duplex WebSocket client.

The URL-derivation tests are pure-sync. The round-trip tests run a local
``websockets`` server that speaks the Pulse duplex protocol (connected → ack →
output / error), exercising the real client send/recv loop without the Java
server (which is covered by AgentDuplexWebSocketTest on the server side).
"""

from __future__ import annotations

import json

import pytest
import websockets

from pulse_client import PulseAPIError, PulseClient
from pulse_client._duplex import DuplexChannel, derive_ws_url


class TestDeriveWsUrl:
    def test_http_port_plus_one(self) -> None:
        url = derive_ws_url("http://localhost:9090", "fraud", None)
        assert url == "ws://localhost:9090".replace(":9090", ":9091") + "/api/pulse/agents/fraud/duplex"

    def test_https_becomes_wss(self) -> None:
        url = derive_ws_url("https://pulse.example.com:8443", "agent-x", None)
        assert url.startswith("wss://pulse.example.com:8444/api/pulse/agents/agent-x/duplex")

    def test_token_in_query(self) -> None:
        url = derive_ws_url("http://h:9090", "a", "jwt.tok en")
        assert "/api/pulse/agents/a/duplex?token=jwt.tok%20en" in url

    def test_agent_id_is_url_encoded(self) -> None:
        url = derive_ws_url("http://h:9090", "team/agent", None)
        assert "agents/team%2Fagent/duplex" in url

    def test_client_duplex_rejects_blank_agent(self) -> None:
        c = PulseClient("http://h:9090")
        try:
            with pytest.raises(ValueError, match="agent_id"):
                c.duplex("  ")
        finally:
            c.close()


# ── round-trip against a local protocol-speaking stub ──────────────


async def _decision_stub(ws) -> None:
    """A stub agent: connected → for each send, ack then a correlated output."""
    await ws.send(json.dumps({"type": "connected", "mode": "duplex", "agentId": "fraud"}))
    async for raw in ws:
        msg = json.loads(raw)
        if msg.get("type") == "send":
            cid = msg["correlationId"]
            await ws.send(json.dumps({"type": "ack", "correlationId": cid, "eventId": "e-" + cid}))
            decision = "DENY" if msg["payload"].get("amount", 0) > 1000 else "APPROVE"
            await ws.send(json.dumps({
                "type": "output",
                "correlationId": cid,
                "event": {"id": "e-" + cid, "type": "signal", "payload": {"decision": decision}},
            }))


async def _error_stub(ws) -> None:
    await ws.send(json.dumps({"type": "error", "error": "unknown agent or agent has no input/output topic"}))


async def test_duplex_round_trip_correlates() -> None:
    async with websockets.serve(_decision_stub, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}/api/pulse/agents/fraud/duplex"
        async with DuplexChannel(url) as ch:
            cid = await ch.send({"amount": 5000}, correlation_id="tx-1")
            assert cid == "tx-1"
            out = await ch.recv()
            assert out["correlation_id"] == "tx-1"
            assert out["payload"]["decision"] == "DENY"


async def test_duplex_generates_correlation_id_and_pipelines() -> None:
    async with websockets.serve(_decision_stub, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}/api/pulse/agents/fraud/duplex"
        async with DuplexChannel(url) as ch:
            c1 = await ch.send({"amount": 50})    # APPROVE, generated id
            c2 = await ch.send({"amount": 9000})  # DENY, generated id
            assert c1 != c2
            # outputs arrive in order; each carries its own correlation id
            first = await ch.recv()
            second = await ch.recv()
            by_id = {first["correlation_id"]: first, second["correlation_id"]: second}
            assert by_id[c1]["payload"]["decision"] == "APPROVE"
            assert by_id[c2]["payload"]["decision"] == "DENY"


async def test_duplex_error_frame_on_open_raises() -> None:
    async with websockets.serve(_error_stub, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}/api/pulse/agents/ghost/duplex"
        with pytest.raises(PulseAPIError):
            async with DuplexChannel(url):
                pass
