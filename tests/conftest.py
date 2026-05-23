"""Shared pytest fixtures for the pulse-client test suite."""

from __future__ import annotations

import pytest

from pulse_client import PulseClient

BASE_URL = "http://pulse.test:9090"


@pytest.fixture
def client() -> PulseClient:
    """An unauthenticated client pointed at a fake test host."""
    c = PulseClient(BASE_URL)
    yield c
    c.close()


@pytest.fixture
def authed_client() -> PulseClient:
    """A client with a fake bearer token set."""
    c = PulseClient(BASE_URL, token="fake.jwt.token")
    yield c
    c.close()


@pytest.fixture
def base_url() -> str:
    return BASE_URL
