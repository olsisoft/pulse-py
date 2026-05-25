# streamflow-pulse-client — Python SDK for StreamFlow Pulse

Official Python client for the [Pulse](https://github.com/olsisoft/pulse-py) AI Agent Platform.

**Distribution name** on PyPI is `streamflow-pulse-client`; **import statement** stays the natural `from pulse_client import ...` (same convention as `python-dateutil` → `import dateutil`).

```python
from pulse_client import PulseClient

with PulseClient("http://localhost:9090") as client:
    client.auth.login("alice", "secret")
    for pipeline in client.pipelines.list():
        print(pipeline["name"])
```

## Install

```bash
pip install streamflow-pulse-client
```

Requires Python **3.10+**. Pure Python — only depends on `httpx`.

## Why pulse-client

- **Pythonic** — context-manager friendly, typed exceptions, attribute-style resource access (`client.pipelines.list()`).
- **Lightweight** — single dependency (`httpx`), <500 LoC, no generated bloat.
- **Spec-aligned** — every method corresponds 1:1 to an endpoint in the [Pulse OpenAPI 3.1 spec](../streamflow-pulse/src/main/resources/openapi/openapi.yaml). Drift is caught at PR time by the in-tree spec invariant tests (B-103).
- **Async-ready** — the sync client ships today; an `AsyncPulseClient` (same surface, `await` everywhere) will follow in v3.0.

## Quick start

```python
from pulse_client import PulseClient, PulseAuthError

client = PulseClient("http://localhost:9090")

# Authenticate — the returned JWT is cached on the client automatically
try:
    response = client.auth.login("alice", "secret")
    print(f"Logged in as {response['user']['username']}")
except PulseAuthError as e:
    print(f"Login failed: {e}")

# List + inspect resources
for pipeline in client.pipelines.list():
    print(pipeline["name"], pipeline["status"])

# Create a pipeline from a template
new_pipeline = client.pipelines.create({
    "name": "my-fraud-detector",
    "templateId": "fintech-fraud-detection-realtime",
    "nodes": [
        {"id": "source", "type": "source", "subType": "kafka-source"},
        {"id": "agent", "type": "agent", "subType": "streaming"},
        {"id": "sink", "type": "sink", "subType": "telegram"},
    ],
})

# Inspect deployed agents
for agent in client.agents.list():
    print(f"  {agent['name']} — {agent['engineType']} — {agent['status']}")

client.close()
```

## Supported surfaces (v2.6.0)

| Resource | Methods | Notes |
|---|---|---|
| `client.auth` | `login()`, `refresh()`, `organizations()`, `switch_org()` | Auto-caches JWT on the client after `login` / `refresh` / `switch_org`. |
| `client.pipelines` | `list()`, `get(id)`, `create(definition)`, `delete(id)` | `definition` follows the `CreatePipelineRequest` schema (see OpenAPI spec). |
| `client.agents` | `list()`, `get(id)` | Read-only — agents are owned by pipelines. |
| `client.templates` | `list()` | The 223+ first-party templates. |
| `client.users` | `list()` | Requires `USERS_LIST` permission (Owner / Platform Admin personas). |
| `client.version()` | top-level | Public — no JWT required. |

The full ~112-endpoint surface (admin, audit, backups, chat, workspace, etc.) is documented in the OpenAPI spec at `<pulse-server>/api-docs`. SDK methods for those land opportunistically as user-facing demand surfaces.

## Embedded ML inference & duplex

Score events with an uploaded ONNX model in-process (B-112), and open a
bidirectional duplex channel for synchronous decisions (B-114). Full guide:
[ML inference & duplex](https://github.com/olsisoft/pulse-py/blob/dev/docs/SDK-ML-INFERENCE-AND-DUPLEX.md).

```python
# Upload + score with an ONNX model (no model-server hop)
client.models.upload(name="fraud", path="./fraud.onnx",
                     input_schema={"amount": "float", "country": "float"})
builder.from_topic("transactions").ml_predict(
    model="fraud", input_fields=["amount", "country"], output_field="prediction"
).filter("prediction.fraud_score > 0.8").to_topic("flagged")

# Duplex: send in, receive the correlated output on one connection
# (pip install streamflow-pulse-client[duplex])
async with client.duplex("fraud-detector") as ch:
    await ch.send({"amount": 5000}, correlation_id="tx-1")
    signal = await ch.recv()        # signal["correlation_id"] == "tx-1"
```

## Authentication

Three patterns, pick what fits:

```python
# 1. Username + password (interactive / CLI tools)
client = PulseClient("http://localhost:9090")
client.auth.login("alice", "secret")

# 2. Pre-minted JWT (CI / service accounts)
client = PulseClient("http://localhost:9090", token="ey...")

# 3. JWT from environment (12-factor apps)
import os
client = PulseClient(os.environ["PULSE_URL"], token=os.environ["PULSE_TOKEN"])
```

For long-running daemons, store the `refreshToken` from `login()` and call `client.auth.refresh(refresh_token)` when the JWT nears expiry (default 1 h TTL).

## Error handling

Every server error becomes a typed exception you can catch precisely:

```python
from pulse_client import (
    PulseClientError,   # base — catches every client-side error
    PulseAuthError,     # 401 — invalid / missing / expired JWT
    PulseNotFoundError, # 404
    PulseValidationError, # 400 — malformed request
    PulseRateLimitError,  # 429 — carries .retry_after_seconds
    PulseAPIError,      # everything else (5xx, etc.)
)

try:
    client.pipelines.get("nope")
except PulseNotFoundError:
    print("Doesn't exist — fine")
except PulseRateLimitError as e:
    print(f"Backing off {e.retry_after_seconds}s")
    time.sleep(e.retry_after_seconds or 60)
except PulseClientError as e:
    print(f"Something else went wrong: {e}")
```

Every exception carries `.status_code`, `.path`, and `.body` so log lines + bug reports are actionable.

## Development

```bash
git clone https://github.com/olsisoft/pulse-py.git
cd pulse-py

# Install in editable mode with dev deps
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src tests
mypy src
```

CI runs the same on every push touching `pulse-py/` — see `.github/workflows/pulse-py.yaml`.

## Roadmap

- **v2.5.x** — current sync API, 5 core resources (auth, pipelines, agents, templates, users), `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — `AsyncPulseClient` with `async def` everywhere; same surface; one library, two clients.
- **B-098 satellite** — once `olsisoft/pulse-py` exists as its own repo, this in-tree code lifts out wholesale. Pip-install will switch to the satellite; in-tree continues to mirror for one release cycle so the migration is non-breaking.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
