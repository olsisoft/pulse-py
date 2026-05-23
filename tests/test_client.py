"""Smoke tests for the PulseClient surface.

Every test is offline — `respx` intercepts httpx calls and returns canned
responses. The point is to pin the wire format the client speaks, not to
exercise the real server (that's the in-tree E2E harness on the server side).
"""

from __future__ import annotations

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
            return_value=httpx.Response(
                200, json={"version": "2.5.8", "edition": "desktop"}
            )
        )
        result = client.version()
        assert result == {"version": "2.5.8", "edition": "desktop"}

    @respx.mock
    def test_version_works_without_token(
        self, client: PulseClient, base_url: str
    ) -> None:
        # /api/pulse/version is in the spec's public list (security: [])
        respx.get(f"{base_url}/api/pulse/version").mock(
            return_value=httpx.Response(200, json={"version": "2.5.8"})
        )
        assert client.token is None
        result = client.version()
        assert result["version"] == "2.5.8"


class TestAuth:
    @respx.mock
    def test_login_caches_token_on_client(
        self, client: PulseClient, base_url: str
    ) -> None:
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
    def test_login_failure_raises_auth_error(
        self, client: PulseClient, base_url: str
    ) -> None:
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
    def test_refresh_caches_new_token(
        self, client: PulseClient, base_url: str
    ) -> None:
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
    def test_switch_org_caches_new_token(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.post(f"{base_url}/api/auth/switch-org").mock(
            return_value=httpx.Response(200, json={"token": "switched.jwt"})
        )
        authed_client.auth.switch_org("org2")
        assert authed_client.token == "switched.jwt"


class TestPipelines:
    @respx.mock
    def test_list_unwraps_envelope(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
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
        respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(200, json={})
        )
        assert authed_client.pipelines.list() == []

    @respx.mock
    def test_get_returns_one_pipeline(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pipelines/p1").mock(
            return_value=httpx.Response(
                200, json={"id": "p1", "name": "demo", "nodes": []}
            )
        )
        result = authed_client.pipelines.get("p1")
        assert result["id"] == "p1"

    @respx.mock
    def test_get_missing_raises_not_found(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
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
            return_value=httpx.Response(
                201, json={"id": "p3", "name": "new", "nodes": []}
            )
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
            return_value=httpx.Response(
                400, json={"error": "Missing required field: nodes"}
            )
        )
        with pytest.raises(PulseValidationError) as exc:
            authed_client.pipelines.create({"name": "bad"})
        assert "Missing required field" in str(exc.value)

    @respx.mock
    def test_delete_returns_none_on_204(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.delete(f"{base_url}/api/pulse/pipelines/p1").mock(
            return_value=httpx.Response(204)
        )
        assert authed_client.pipelines.delete("p1") is None


class TestAgents:
    @respx.mock
    def test_list_unwraps_envelope(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/agents").mock(
            return_value=httpx.Response(
                200,
                json={
                    "agents": [
                        {"id": "a1", "name": "fraud-detector", "engineType": "streaming"}
                    ]
                },
            )
        )
        agents = authed_client.agents.list()
        assert agents[0]["engineType"] == "streaming"

    @respx.mock
    def test_get_returns_one_agent(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/agents/a1").mock(
            return_value=httpx.Response(
                200,
                json={"id": "a1", "name": "fraud-detector", "engineType": "streaming"},
            )
        )
        result = authed_client.agents.get("a1")
        assert result["id"] == "a1"


class TestTemplates:
    @respx.mock
    def test_list_unwraps_envelope(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/templates").mock(
            return_value=httpx.Response(
                200,
                json={
                    "templates": [
                        {"id": "fraud-detection", "name": "Fraud Detection"}
                    ]
                },
            )
        )
        templates = authed_client.templates.list()
        assert templates[0]["id"] == "fraud-detection"


class TestEventsStream:
    """B-098 Phase 7 — SSE event-stream consumer."""

    @respx.mock
    def test_stream_yields_parsed_events(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        sse_body = (
            "data: {\"type\":\"fraud_signal\",\"payload\":{\"customerId\":\"c1\"}}\n\n"
            "data: {\"type\":\"heartbeat\"}\n\n"
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
            ": keep-alive\n\n"
            "data: {\"type\":\"a\"}\n\n"
            ": another keep-alive\n\n"
            "data: {\"type\":\"b\"}\n\n"
        )
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, content=sse_body
            )
        )
        events = list(authed_client.events.stream())
        assert [e["type"] for e in events] == ["a", "b"]

    @respx.mock
    def test_stream_handles_multiline_data(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # SSE concatenates multiple data: lines with \n
        sse_body = "data: {\"type\":\"multi\",\n" "data:   \"value\":42}\n\n"
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

    def test_stream_without_token_raises_immediately(
        self, client: PulseClient
    ) -> None:
        # No HTTP mock — if the client reached the wire we'd hit a real
        # connection. The token check fires before that.
        with pytest.raises(PulseAuthError):
            list(client.events.stream())

    @respx.mock
    def test_stream_raises_on_401(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/events/stream").mock(
            return_value=httpx.Response(401, json={"error": "expired"})
        )
        with pytest.raises(PulseAuthError):
            list(authed_client.events.stream())


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
            exc.value, (PulseAuthError, PulseNotFoundError, PulseValidationError, PulseRateLimitError)
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
    def test_user_agent_header_is_set(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.get(f"{base_url}/api/pulse/pipelines").mock(
            return_value=httpx.Response(200, json={"pipelines": []})
        )
        authed_client.pipelines.list()
        call = route.calls.last
        assert "pulse-client-python" in call.request.headers["User-Agent"]
