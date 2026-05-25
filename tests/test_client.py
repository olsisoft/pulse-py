"""Smoke tests for the PulseClient surface.

Every test is offline — `respx` intercepts httpx calls and returns canned
responses. The point is to pin the wire format the client speaks, not to
exercise the real server (that's the in-tree E2E harness on the server side).
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from pulse_client import (
    PulseAPIError,
    PulseAuthError,
    PulseClient,
    PulseNotFoundError,
    PulseRateLimitError,
    PulseValidationError,
)


class TestClientLifecycle:
    def test_close_is_idempotent(self, client: PulseClient) -> None:
        client.close()
        client.close()  # should not raise

    def test_context_manager_closes(self, base_url: str) -> None:
        with PulseClient(base_url) as c:
            assert c.token is None
        # context manager exit closes the http pool — subsequent close() is a no-op

    def test_token_setter(self, client: PulseClient) -> None:
        assert client.token is None
        client.token = "abc"
        assert client.token == "abc"
        client.token = None
        assert client.token is None

    def test_base_url_trailing_slash_normalised(self) -> None:
        c = PulseClient("http://pulse.test:9090/")
        try:
            assert c._base_url == "http://pulse.test:9090"
        finally:
            c.close()


class TestVersion:
    @respx.mock
    def test_version_is_public_and_returns_metadata(
        self, client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/version").mock(
            return_value=httpx.Response(200, json={"version": "2.6.0", "edition": "desktop"})
        )
        result = client.version()
        assert result == {"version": "2.6.0", "edition": "desktop"}

    @respx.mock
    def test_version_works_without_token(self, client: PulseClient, base_url: str) -> None:
        # /api/pulse/version is in the spec's public list (security: [])
        respx.get(f"{base_url}/api/pulse/version").mock(
            return_value=httpx.Response(200, json={"version": "2.6.0"})
        )
        assert client.token is None
        result = client.version()
        assert result["version"] == "2.6.0"


class TestAuth:
    @respx.mock
    def test_login_caches_token_on_client(self, client: PulseClient, base_url: str) -> None:
        respx.post(f"{base_url}/api/auth/login").mock(
            return_value=httpx.Response(
                200,
                json={
                    "token": "new.jwt.token",
                    "refreshToken": "refresh.token",
                    "activeOrg": {"id": "org1", "name": "Acme"},
                },
            )
        )
        result = client.auth.login("alice", "secret")
        assert client.token == "new.jwt.token"
        assert result["refreshToken"] == "refresh.token"

    @respx.mock
    def test_login_failure_raises_auth_error(self, client: PulseClient, base_url: str) -> None:
        respx.post(f"{base_url}/api/auth/login").mock(
            return_value=httpx.Response(401, json={"error": "Invalid credentials"})
        )
        with pytest.raises(PulseAuthError) as exc:
            client.auth.login("alice", "wrong")
        assert exc.value.status_code == 401
        assert "Invalid credentials" in str(exc.value)
        # token NOT cached on failure
        assert client.token is None

    @respx.mock
    def test_refresh_caches_new_token(self, client: PulseClient, base_url: str) -> None:
        respx.post(f"{base_url}/api/auth/refresh").mock(
            return_value=httpx.Response(200, json={"token": "refreshed.jwt"})
        )
        result = client.auth.refresh("some-refresh-token")
        assert client.token == "refreshed.jwt"
        assert result["token"] == "refreshed.jwt"

    @respx.mock
    def test_organizations_unwraps_envelope(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/auth/organizations").mock(
            return_value=httpx.Response(
                200,
                json={"organizations": [{"id": "o1", "name": "Acme"}]},
            )
        )
        orgs = authed_client.auth.organizations()
        assert orgs == [{"id": "o1", "name": "Acme"}]

    @respx.mock
    def test_switch_org_caches_new_token(self, authed_client: PulseClient, base_url: str) -> None:
        respx.post(f"{base_url}/api/auth/switch-org").mock(
            return_value=httpx.Response(200, json={"token": "switched.jwt"})
        )
        authed_client.auth.switch_org("org2")
        assert authed_client.token == "switched.jwt"


class TestPipelines:
    @respx.mock
    def test_list_unwraps_envelope(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pipelines": [
                        {"id": "p1", "name": "demo", "nodes": []},
                        {"id": "p2", "name": "fraud", "nodes": []},
                    ]
                },
            )
        )
        pipelines = authed_client.pipelines.list()
        assert len(pipelines) == 2
        assert pipelines[0]["id"] == "p1"

    @respx.mock
    def test_list_returns_empty_on_missing_envelope(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines").mock(return_value=httpx.Response(200, json={}))
        assert authed_client.pipelines.list() == []

    @respx.mock
    def test_get_returns_one_pipeline(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines/p1").mock(
            return_value=httpx.Response(200, json={"id": "p1", "name": "demo", "nodes": []})
        )
        result = authed_client.pipelines.get("p1")
        assert result["id"] == "p1"

    @respx.mock
    def test_get_missing_raises_not_found(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines/nope").mock(
            return_value=httpx.Response(404, json={"error": "not found"})
        )
        with pytest.raises(PulseNotFoundError):
            authed_client.pipelines.get("nope")

    @respx.mock
    def test_create_returns_created_pipeline(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.post(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(201, json={"id": "p3", "name": "new", "nodes": []})
        )
        result = authed_client.pipelines.create(
            {"name": "new", "nodes": [{"id": "n1", "type": "source"}]}
        )
        assert result["id"] == "p3"

    @respx.mock
    def test_create_validation_error_raises(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.post(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(400, json={"error": "Missing required field: nodes"})
        )
        with pytest.raises(PulseValidationError) as exc:
            authed_client.pipelines.create({"name": "bad"})
        assert "Missing required field" in str(exc.value)

    @respx.mock
    def test_delete_returns_none_on_204(self, authed_client: PulseClient, base_url: str) -> None:
        respx.delete(f"{base_url}/api/pulse/pipelines/p1").mock(return_value=httpx.Response(204))
        assert authed_client.pipelines.delete("p1") is None


class TestAgents:
    @respx.mock
    def test_list_unwraps_envelope(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/agents").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agents": [{"id": "a1", "name": "fraud-detector", "engineType": "streaming"}]
                },
            )
        )
        agents = authed_client.agents.list()
        assert agents[0]["engineType"] == "streaming"

    @respx.mock
    def test_get_returns_one_agent(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/agents/a1").mock(
            return_value=httpx.Response(
                200,
                json={"id": "a1", "name": "fraud-detector", "engineType": "streaming"},
            )
        )
        result = authed_client.agents.get("a1")
        assert result["id"] == "a1"

    @respx.mock
    def test_update_puts_full_config_and_returns_fresh_snapshot(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.put(f"{base_url}/api/pulse/agents/a1").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "a1",
                    "name": "fraud-detector-v2",
                    "engineType": "rule-based",
                    "status": "running",
                },
            )
        )
        new_config = {
            "name": "fraud-detector-v2",
            "engineType": "rule-based",
            "config": {"rules": [{"if": "amount > 5000", "then": "block"}]},
        }
        result = authed_client.agents.update("a1", new_config)
        assert result["name"] == "fraud-detector-v2"
        assert route.called
        sent = json.loads(route.calls.last.request.content)
        assert sent == new_config

    @respx.mock
    def test_update_raises_validation_on_self_loop_400(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.put(f"{base_url}/api/pulse/agents/a1").mock(
            return_value=httpx.Response(
                400,
                json={
                    "error": "Agent would self-loop: outputTopic == inputTopic",
                    "unsafeFields": ["outputTopic"],
                    "suggestedOutputTopic": "pulse.x.out",
                },
            )
        )
        with pytest.raises(PulseValidationError) as exc:
            authed_client.agents.update("a1", {"name": "x", "inputTopic": "t", "outputTopic": "t"})
        assert "self-loop" in str(exc.value)

    @respx.mock
    def test_update_raises_not_found_on_missing_agent(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.put(f"{base_url}/api/pulse/agents/missing").mock(
            return_value=httpx.Response(404, json={"error": "Agent not found: missing"})
        )
        with pytest.raises(PulseNotFoundError):
            authed_client.agents.update("missing", {"name": "x"})

    @respx.mock
    def test_delete_204(self, authed_client: PulseClient, base_url: str) -> None:
        route = respx.delete(f"{base_url}/api/pulse/agents/a1").mock(
            return_value=httpx.Response(204)
        )
        authed_client.agents.delete("a1")  # no raise
        assert route.called

    @respx.mock
    def test_delete_raises_not_found(self, authed_client: PulseClient, base_url: str) -> None:
        respx.delete(f"{base_url}/api/pulse/agents/missing").mock(
            return_value=httpx.Response(404, json={"error": "Agent not found"})
        )
        with pytest.raises(PulseNotFoundError):
            authed_client.agents.delete("missing")

    def test_update_without_token_raises_auth_error_synchronously(
        self, client: PulseClient
    ) -> None:
        # No respx route — verify the no-token check fires before any HTTP call.
        with pytest.raises(PulseAuthError):
            client.agents.update("a1", {"name": "x"})

    def test_delete_without_token_raises_auth_error_synchronously(
        self, client: PulseClient
    ) -> None:
        with pytest.raises(PulseAuthError):
            client.agents.delete("a1")


class TestTemplates:
    @respx.mock
    def test_list_unwraps_envelope(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/templates").mock(
            return_value=httpx.Response(
                200,
                json={"templates": [{"id": "fraud-detection", "name": "Fraud Detection"}]},
            )
        )
        templates = authed_client.templates.list()
        assert templates[0]["id"] == "fraud-detection"


class TestModels:
    """B-112 — client.models (embedded ML model registry)."""

    @respx.mock
    def test_upload_from_bytes_sends_multipart(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.post(f"{base_url}/api/pulse/ml-models").mock(
            return_value=httpx.Response(
                201,
                json={"name": "fraud", "runtime": "onnx", "version": 1, "sizeBytes": 5},
            )
        )
        meta = authed_client.models.upload(
            name="fraud",
            data=b"\x08\x09onnx",
            input_schema={"amount": "float"},
            output_schema={"score": "float"},
        )
        assert meta["name"] == "fraud"
        assert route.called
        sent = route.calls.last.request
        body = sent.content
        # multipart body carries the form fields + the file part
        assert b"multipart/form-data" in sent.headers["content-type"].encode()
        assert b'name="name"' in body
        assert b"fraud" in body
        assert b'name="inputSchema"' in body
        assert b'name="model"' in body  # the file part

    @respx.mock
    def test_upload_from_path(
        self, authed_client: PulseClient, base_url: str, tmp_path
    ) -> None:
        model_file = tmp_path / "m.onnx"
        model_file.write_bytes(b"onnxbytes")
        respx.post(f"{base_url}/api/pulse/ml-models").mock(
            return_value=httpx.Response(201, json={"name": "m", "version": 1})
        )
        meta = authed_client.models.upload(name="m", path=str(model_file))
        assert meta["name"] == "m"

    def test_upload_requires_exactly_one_source(self, authed_client: PulseClient) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            authed_client.models.upload(name="m")  # neither path nor data
        with pytest.raises(ValueError, match="exactly one"):
            authed_client.models.upload(name="m", path="x", data=b"y")

    def test_upload_rejects_empty_bytes(self, authed_client: PulseClient) -> None:
        with pytest.raises(ValueError, match="empty"):
            authed_client.models.upload(name="m", data=b"")

    def test_upload_rejects_blank_name(self, authed_client: PulseClient) -> None:
        with pytest.raises(ValueError, match="name"):
            authed_client.models.upload(name="  ", data=b"x")

    @respx.mock
    def test_list_unwraps_envelope(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/ml-models").mock(
            return_value=httpx.Response(200, json={"models": [{"name": "fraud"}]})
        )
        models = authed_client.models.list()
        assert models[0]["name"] == "fraud"

    @respx.mock
    def test_get_returns_metadata(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/ml-models/fraud").mock(
            return_value=httpx.Response(200, json={"name": "fraud", "version": 2})
        )
        assert authed_client.models.get("fraud")["version"] == 2

    @respx.mock
    def test_delete(self, authed_client: PulseClient, base_url: str) -> None:
        route = respx.delete(f"{base_url}/api/pulse/ml-models/fraud").mock(
            return_value=httpx.Response(200, json={"deleted": "fraud"})
        )
        authed_client.models.delete("fraud")
        assert route.called


class TestEventsStream:
    """B-098 Phase 7 — SSE event-stream consumer."""

    @respx.mock
    def test_stream_yields_parsed_events(self, authed_client: PulseClient, base_url: str) -> None:
        sse_body = (
            'data: {"type":"fraud_signal","payload":{"customerId":"c1"}}\n\n'
            'data: {"type":"heartbeat"}\n\n'
        )
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                content=sse_body,
            )
        )
        events = list(authed_client.events.stream())
        assert len(events) == 2
        assert events[0]["type"] == "fraud_signal"
        assert events[0]["payload"]["customerId"] == "c1"
        assert events[1]["type"] == "heartbeat"

    @respx.mock
    def test_stream_skips_comments_and_heartbeats(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # SSE allows comment lines starting with ':' for keep-alives.
        sse_body = (
            ': keep-alive\n\ndata: {"type":"a"}\n\n: another keep-alive\n\ndata: {"type":"b"}\n\n'
        )
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, content=sse_body
            )
        )
        events = list(authed_client.events.stream())
        assert [e["type"] for e in events] == ["a", "b"]

    @respx.mock
    def test_stream_handles_multiline_data(self, authed_client: PulseClient, base_url: str) -> None:
        # SSE concatenates multiple data: lines with \n
        sse_body = 'data: {"type":"multi",\ndata:   "value":42}\n\n'
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, content=sse_body
            )
        )
        events = list(authed_client.events.stream())
        assert len(events) == 1
        assert events[0]["value"] == 42

    @respx.mock
    def test_stream_yields_raw_payload_when_not_json(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        sse_body = "data: this-is-not-json\n\n"
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, content=sse_body
            )
        )
        events = list(authed_client.events.stream())
        assert events == [{"data": "this-is-not-json"}]

    def test_stream_without_token_raises_immediately(self, client: PulseClient) -> None:
        # No HTTP mock — if the client reached the wire we'd hit a real
        # connection. The token check fires before that.
        with pytest.raises(PulseAuthError):
            list(client.events.stream())

    @respx.mock
    def test_stream_raises_on_401(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(401, json={"error": "expired"})
        )
        with pytest.raises(PulseAuthError):
            list(authed_client.events.stream())


class TestIQ:
    """B-106 Interactive Queries — full coverage of the 5 IQ methods."""

    # ---- summary ----
    @respx.mock
    def test_summary_returns_state_metadata(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/iq/agents/fraud-detector/state").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "fraud-detector",
                    "queryable": True,
                    "backend": "rocksdb",
                    "hotSize": 1500,
                    "hotBytes": 32768,
                    "coldSize": 50000,
                    "coldBytes": 4194304,
                    "lastCheckpointId": 42,
                    "totalSize": 51500,
                },
            )
        )
        summary = authed_client.iq.summary("fraud-detector")
        assert summary["queryable"] is True
        assert summary["backend"] == "rocksdb"
        assert summary["totalSize"] == 51500

    @respx.mock
    def test_summary_returns_non_queryable_shape(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # Non-streaming agent — server returns queryable=False, backend='none',
        # lastCheckpointId=-1, sizes all 0
        respx.get(f"{base_url}/api/pulse/iq/agents/rule-agent/state").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "rule-agent",
                    "queryable": False,
                    "backend": "none",
                    "hotSize": 0,
                    "hotBytes": 0,
                    "coldSize": 0,
                    "coldBytes": 0,
                    "lastCheckpointId": -1,
                    "totalSize": 0,
                },
            )
        )
        summary = authed_client.iq.summary("rule-agent")
        assert summary["queryable"] is False
        assert summary["lastCheckpointId"] == -1

    @respx.mock
    def test_summary_url_encodes_agent_id_with_special_chars(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # Agent id with '/' (real case: multi-tenant agents named "tenant/agent")
        # The path segment must be URL-encoded so server matches /agents/tenant%2Fagent/state
        respx.get(f"{base_url}/api/pulse/iq/agents/tenant%2Fagent/state").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "tenant/agent",
                    "queryable": True,
                    "backend": "rocksdb",
                    "hotSize": 0,
                    "hotBytes": 0,
                    "coldSize": 0,
                    "coldBytes": 0,
                    "lastCheckpointId": 0,
                    "totalSize": 0,
                },
            )
        )
        result = authed_client.iq.summary("tenant/agent")
        assert result["agentId"] == "tenant/agent"

    # ---- get ----
    @respx.mock
    def test_get_returns_value_at_key(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/iq/agents/fraud-detector/state/value/customer-42").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "fraud-detector",
                    "key": "customer-42",
                    "value": {"tx_count_60s": 7, "total_amount_60s": 12500},
                },
            )
        )
        result = authed_client.iq.get("fraud-detector", "customer-42")
        assert result["key"] == "customer-42"
        assert result["value"]["tx_count_60s"] == 7

    @respx.mock
    def test_get_url_encodes_key_with_slash(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # Key with '/' — legitimate per server design (URLDecoder used)
        respx.get(f"{base_url}/api/pulse/iq/agents/sessions/state/value/user%3A123%2Forders").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "sessions",
                    "key": "user:123/orders",
                    "value": ["o1", "o2", "o3"],
                },
            )
        )
        result = authed_client.iq.get("sessions", "user:123/orders")
        assert result["value"] == ["o1", "o2", "o3"]

    @respx.mock
    def test_get_returns_null_value_when_present(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # Server distinguishes 'key present with null' (200, value=null) from
        # 'key absent' (404). The client should pass null through unchanged.
        respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/value/k1").mock(
            return_value=httpx.Response(200, json={"agentId": "a1", "key": "k1", "value": None})
        )
        result = authed_client.iq.get("a1", "k1")
        assert result["value"] is None

    @respx.mock
    def test_get_404_key_not_found_raises_not_found(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/value/missing-key").mock(
            return_value=httpx.Response(
                404,
                json={
                    "error": "Key not found",
                    "agentId": "a1",
                    "key": "missing-key",
                },
            )
        )
        with pytest.raises(PulseNotFoundError) as exc:
            authed_client.iq.get("a1", "missing-key")
        # Body must carry the 'Key not found' marker so callers can
        # distinguish from agent-not-queryable
        assert exc.value.body["error"] == "Key not found"
        assert exc.value.body["key"] == "missing-key"

    @respx.mock
    def test_get_404_agent_not_queryable_raises_with_reason(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/value/k1").mock(
            return_value=httpx.Response(
                404,
                json={
                    "error": "Agent has no queryable state",
                    "agentId": "a1",
                    "reason": "non-streaming or stopped",
                },
            )
        )
        with pytest.raises(PulseNotFoundError) as exc:
            authed_client.iq.get("a1", "k1")
        assert "no queryable state" in exc.value.body["error"]
        assert exc.value.body["reason"] == "non-streaming or stopped"

    # ---- scan ----
    @respx.mock
    def test_scan_returns_entries_with_pagination_metadata(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/scan").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "a1",
                    "entries": [
                        {"key": "k1", "value": 1},
                        {"key": "k2", "value": 2},
                    ],
                    "count": 2,
                    "truncated": False,
                    "limitApplied": 100,
                },
            )
        )
        result = authed_client.iq.scan("a1")
        assert len(result["entries"]) == 2
        assert result["truncated"] is False
        # Default params: limit=100 always sent, start/end omitted when None
        call = route.calls.last
        assert call.request.url.params["limit"] == "100"
        assert "start" not in call.request.url.params
        assert "end" not in call.request.url.params

    @respx.mock
    def test_scan_passes_through_range_params(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/scan").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "a1",
                    "entries": [],
                    "count": 0,
                    "truncated": False,
                    "limitApplied": 50,
                    "start": "alice",
                    "end": "bob",
                },
            )
        )
        authed_client.iq.scan("a1", start="alice", end="bob", limit=50)
        call = route.calls.last
        assert call.request.url.params["start"] == "alice"
        assert call.request.url.params["end"] == "bob"
        assert call.request.url.params["limit"] == "50"

    @respx.mock
    def test_scan_404_agent_not_queryable_raises(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/scan").mock(
            return_value=httpx.Response(
                404,
                json={
                    "error": "Agent has no queryable state",
                    "agentId": "a1",
                    "reason": "non-streaming or stopped",
                },
            )
        )
        with pytest.raises(PulseNotFoundError):
            authed_client.iq.scan("a1")

    # ---- list_keys ----
    @respx.mock
    def test_list_keys_returns_keys_array(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/keys").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "a1",
                    "keys": ["alpha", "beta", "gamma"],
                    "count": 3,
                    "truncated": False,
                    "limitApplied": 100,
                },
            )
        )
        result = authed_client.iq.list_keys("a1")
        assert result["keys"] == ["alpha", "beta", "gamma"]

    @respx.mock
    def test_list_keys_with_range(self, authed_client: PulseClient, base_url: str) -> None:
        route = respx.get(f"{base_url}/api/pulse/iq/agents/a1/state/keys").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "a1",
                    "keys": ["b1"],
                    "count": 1,
                    "truncated": True,
                    "limitApplied": 1,
                    "start": "b",
                    "end": "c",
                },
            )
        )
        result = authed_client.iq.list_keys("a1", start="b", end="c", limit=1)
        assert result["truncated"] is True
        call = route.calls.last
        assert call.request.url.params["limit"] == "1"

    # ---- query ----
    @respx.mock
    def test_query_flat_with_filter_returns_entries(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.post(f"{base_url}/api/pulse/iq/agents/fraud-detector/state/query").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "fraud-detector",
                    "entries": [
                        {"key": "c1", "value": {"tx_count_60s": 8}},
                        {"key": "c5", "value": {"tx_count_60s": 12}},
                    ],
                    "count": 2,
                    "totalScanned": 1500,
                    "matchedCount": 2,
                    "truncated": False,
                    "limitApplied": 100,
                },
            )
        )
        result = authed_client.iq.query(
            "fraud-detector",
            filter={"field": "tx_count_60s", "op": "gt", "value": 5},
        )
        assert result["count"] == 2
        # Verify body shape
        body = json.loads(route.calls.last.request.content)
        assert body["filter"]["field"] == "tx_count_60s"
        assert body["filter"]["op"] == "gt"

    @respx.mock
    def test_query_grouped_returns_groups(self, authed_client: PulseClient, base_url: str) -> None:
        route = respx.post(f"{base_url}/api/pulse/iq/agents/users/state/query").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "users",
                    "groups": [
                        {"groupKey": "free", "count": 8420},
                        {"groupKey": "pro", "count": 312},
                        {"groupKey": "enterprise", "count": 47},
                    ],
                    "groupCount": 3,
                    "totalScanned": 8779,
                    "matchedCount": 8779,
                    "truncated": False,
                    "limitApplied": 100,
                },
            )
        )
        result = authed_client.iq.query("users", group_by="plan")
        assert "groups" in result
        assert result["groupCount"] == 3
        body = json.loads(route.calls.last.request.content)
        assert body["groupBy"] == "plan"

    @respx.mock
    def test_query_with_compound_filter_and_projection(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.post(f"{base_url}/api/pulse/iq/agents/a1/state/query").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "a1",
                    "entries": [],
                    "count": 0,
                    "totalScanned": 100,
                    "matchedCount": 0,
                    "truncated": False,
                    "limitApplied": 100,
                },
            )
        )
        authed_client.iq.query(
            "a1",
            filter={
                "and": [
                    {"field": "country", "op": "eq", "value": "US"},
                    {"field": "amount", "op": "gt", "value": 1000},
                ]
            },
            projection=["customer_id", "amount"],
            start="2026-01-01",
            end="2026-12-31",
            limit=50,
        )
        body = json.loads(route.calls.last.request.content)
        assert body["filter"]["and"][0]["field"] == "country"
        assert body["projection"] == ["customer_id", "amount"]
        assert body["start"] == "2026-01-01"
        assert body["limit"] == 50

    @respx.mock
    def test_query_empty_body_is_valid(self, authed_client: PulseClient, base_url: str) -> None:
        # No filter / projection / group — server returns full scan
        route = respx.post(f"{base_url}/api/pulse/iq/agents/a1/state/query").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agentId": "a1",
                    "entries": [],
                    "count": 0,
                    "totalScanned": 0,
                    "matchedCount": 0,
                    "truncated": False,
                    "limitApplied": 100,
                },
            )
        )
        authed_client.iq.query("a1")
        # Verify body is null/empty (we send None when no params)
        # httpx sends None as no body or empty bytes
        sent_body = route.calls.last.request.content
        assert sent_body in (b"", b"null")

    @respx.mock
    def test_query_400_invalid_filter_raises_validation_error(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.post(f"{base_url}/api/pulse/iq/agents/a1/state/query").mock(
            return_value=httpx.Response(
                400,
                json={
                    "error": "filter cannot mix discriminators (field/and/or/not) at the same level"
                },
            )
        )
        with pytest.raises(PulseValidationError) as exc:
            authed_client.iq.query(
                "a1",
                filter={
                    "field": "a",
                    "and": [{"field": "b", "op": "eq", "value": 1}],
                },
            )
        assert "discriminator" in exc.value.body["error"]

    @respx.mock
    def test_query_404_agent_not_queryable(self, authed_client: PulseClient, base_url: str) -> None:
        respx.post(f"{base_url}/api/pulse/iq/agents/a1/state/query").mock(
            return_value=httpx.Response(
                404,
                json={
                    "error": "Agent has no queryable state",
                    "agentId": "a1",
                    "reason": "non-streaming or stopped",
                },
            )
        )
        with pytest.raises(PulseNotFoundError):
            authed_client.iq.query("a1", filter={"field": "x", "op": "exists"})

    # ---- auth gating ----
    def test_summary_without_token_raises_auth_error(self, client: PulseClient) -> None:
        with pytest.raises(PulseAuthError):
            client.iq.summary("a1")


class TestErrorHandling:
    @respx.mock
    def test_no_token_set_raises_auth_error_without_calling_server(
        self, client: PulseClient, base_url: str
    ) -> None:
        # Note: no respx mock set — if the client incorrectly tried the request
        # respx would raise. We assert it raises BEFORE making the call.
        with pytest.raises(PulseAuthError) as exc:
            client.pipelines.list()
        assert "No token set" in str(exc.value)

    @respx.mock
    def test_rate_limit_parses_retry_after_from_body(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(
                429,
                json={
                    "error": "Rate limit exceeded",
                    "errorCode": "RATE_LIMITED",
                    "retryAfterSeconds": 60,
                    "limit": 120,
                    "remaining": 0,
                },
            )
        )
        with pytest.raises(PulseRateLimitError) as exc:
            authed_client.pipelines.list()
        assert exc.value.retry_after_seconds == 60

    @respx.mock
    def test_rate_limit_falls_back_to_retry_after_header(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(
                429,
                headers={"Retry-After": "30"},
                text="Too Many Requests",
            )
        )
        with pytest.raises(PulseRateLimitError) as exc:
            authed_client.pipelines.list()
        assert exc.value.retry_after_seconds == 30

    @respx.mock
    def test_unknown_5xx_raises_generic_api_error(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(
                500,
                json={"error": "Internal", "errorClass": "NPE"},
            )
        )
        with pytest.raises(PulseAPIError) as exc:
            authed_client.pipelines.list()
        # not auth / not-found / not validation / not rate-limit
        assert not isinstance(
            exc.value,
            (PulseAuthError, PulseNotFoundError, PulseValidationError, PulseRateLimitError),
        )
        assert exc.value.status_code == 500

    @respx.mock
    def test_bearer_token_attached_to_request(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(200, json={"pipelines": []})
        )
        authed_client.pipelines.list()
        call = route.calls.last
        assert call.request.headers["Authorization"] == "Bearer fake.jwt.token"

    @respx.mock
    def test_user_agent_header_is_set(self, authed_client: PulseClient, base_url: str) -> None:
        route = respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(200, json={"pipelines": []})
        )
        authed_client.pipelines.list()
        call = route.calls.last
        assert "pulse-client-python" in call.request.headers["User-Agent"]
