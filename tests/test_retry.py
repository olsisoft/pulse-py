"""B-170 / GA-gate #312 — opt-in retry policy (off by default).

Verifies the bounded, full-jitter, idempotency-aware retry wrapper around the
client's request path: disabled by default; 429 retried for any method honouring
Retry-After; transient 5xx / transport errors retried only for idempotent
methods (unless opted out); terminal 4xx never retried; retries are bounded.
"""
from __future__ import annotations

import httpx
import pytest
import respx

from pulse_client import PulseClient
from pulse_client.exceptions import (
    PulseAPIError,
    PulseNotFoundError,
)

BASE = "http://pulse.test:9090"


def _client(**kw) -> PulseClient:
    # Tiny backoff so the (real) sleeps are negligible in the test.
    kw.setdefault("retry_backoff", 0.001)
    kw.setdefault("retry_max_backoff", 0.002)
    return PulseClient(BASE, token="t", **kw)


@respx.mock
def test_no_retry_by_default():
    route = respx.get(f"{BASE}/api/pulse/version").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json={"ok": True})]
    )
    c = PulseClient(BASE, token="t")  # max_retries defaults to 0
    with pytest.raises(PulseAPIError) as ei:
        c.version()
    assert ei.value.status_code == 503
    assert route.call_count == 1  # exactly one attempt — unchanged legacy behaviour
    c.close()


@respx.mock
def test_idempotent_get_retried_on_5xx_then_succeeds():
    route = respx.get(f"{BASE}/api/pulse/version").mock(
        side_effect=[httpx.Response(503), httpx.Response(502), httpx.Response(200, json={"v": 1})]
    )
    c = _client(max_retries=2)
    assert c.version() == {"v": 1}
    assert route.call_count == 3
    c.close()


@respx.mock
def test_exhausts_retries_then_raises():
    route = respx.get(f"{BASE}/api/pulse/version").mock(return_value=httpx.Response(503))
    c = _client(max_retries=2)
    with pytest.raises(PulseAPIError) as ei:
        c.version()
    assert ei.value.status_code == 503
    assert route.call_count == 3  # initial + 2 retries
    c.close()


@respx.mock
def test_429_retried_for_non_idempotent_post_honouring_retry_after():
    # POST is non-idempotent, but a 429 means the request was rejected, never
    # processed → safe to retry. retryAfterSeconds=0 keeps the test instant.
    route = respx.post(f"{BASE}/api/pulse/pipelines").mock(
        side_effect=[
            httpx.Response(429, json={"retryAfterSeconds": 0}),
            httpx.Response(201, json={"id": "p1"}),
        ]
    )
    c = _client(max_retries=1)
    assert c.pipelines.create({"name": "x"}) == {"id": "p1"}
    assert route.call_count == 2
    c.close()


@respx.mock
def test_post_5xx_not_retried_when_idempotent_only_default():
    route = respx.post(f"{BASE}/api/pulse/pipelines").mock(
        side_effect=[httpx.Response(503), httpx.Response(201, json={"id": "p1"})]
    )
    c = _client(max_retries=3)  # retry_idempotent_only=True (default)
    with pytest.raises(PulseAPIError) as ei:
        c.pipelines.create({"name": "x"})
    assert ei.value.status_code == 503
    assert route.call_count == 1  # POST not retried on a transient 5xx
    c.close()


@respx.mock
def test_post_5xx_retried_when_opted_in():
    route = respx.post(f"{BASE}/api/pulse/pipelines").mock(
        side_effect=[httpx.Response(503), httpx.Response(201, json={"id": "p1"})]
    )
    c = _client(max_retries=2, retry_idempotent_only=False)
    assert c.pipelines.create({"name": "x"}) == {"id": "p1"}
    assert route.call_count == 2
    c.close()


@respx.mock
def test_transport_error_retried_for_idempotent_get():
    route = respx.get(f"{BASE}/api/pulse/pipelines").mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200, json={"pipelines": []})]
    )
    c = _client(max_retries=1)
    c.pipelines.list()
    assert route.call_count == 2
    c.close()


@respx.mock
def test_terminal_4xx_never_retried():
    route = respx.get(f"{BASE}/api/pulse/pipelines/nope").mock(return_value=httpx.Response(404))
    c = _client(max_retries=3)
    with pytest.raises(PulseNotFoundError):
        c.pipelines.get("nope")
    assert route.call_count == 1  # 404 is terminal
    c.close()


def test_backoff_is_bounded_and_nonnegative():
    c = PulseClient(BASE, token="t", retry_backoff=0.5, retry_max_backoff=4.0)
    for attempt in range(8):
        delay = c._backoff_delay(attempt)
        assert 0.0 <= delay <= 4.0  # full jitter within the capped ceiling
    c.close()
