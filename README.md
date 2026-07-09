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

## Supported surfaces (v2.7.x)

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

# Upload + run a sandboxed WASM module over each event (B-110, pure-Java
# Chicory on the engine — no host syscalls). Authored from any wasm32
# toolchain (Rust, TinyGo, AssemblyScript, C).
client.wasm.upload(name="pii-redactor", path="./redactor.wasm")
builder.from_topic("events").wasm(module="pii-redactor").to_topic("clean")

# Duplex: send in, receive the correlated output on one connection
# (pip install streamflow-pulse-client[duplex])
async with client.duplex("fraud-detector") as ch:
    await ch.send({"amount": 5000}, correlation_id="tx-1")
    signal = await ch.recv()        # signal["correlation_id"] == "tx-1"
```

**Legacy formats & protocols — the headline use case.** Compile *any* existing
parser to `wasm32` and drop it in as a single-message transform to bring legacy
data into the pipeline — **COBOL copybooks**, FIX, HL7, EDI X12, ASN.1, Modbus, …
You don't rewrite the parser, you wrap it (see the `pulse-wasm-guest` guest SDK for
the Rust/TinyGo/AssemblyScript/C operator ABI). Pair it with `.ml_predict()` (ONNX
above) to parse *and* score each event in-stream, with no external service.

## Authentication

### Where credentials come from

The SDK authenticates as a **Pulse user** — there are no separate API keys to
provision. A username + password (or a JWT minted from them) is all you need,
and they live in **your own Pulse instance**, not on streamflowmesh.io.

1. **First run → bootstrap admin.** The very first account is created either by
   the first-run screen of the Pulse web/desktop app, or by a single
   *unauthenticated* `POST /api/auth/register` with a `{"username","password"}`
   body **while no user exists yet**. That first user is granted **ADMIN**. As
   soon as any user exists, `/api/auth/register` locks down and requires an admin
   JWT — so the open bootstrap can only ever mint the very first account.
2. **Additional users.** An admin creates more accounts from **Settings → Users**
   in the Pulse UI (or an admin-authenticated `register` call). Give each CI job
   or service integration its own dedicated user rather than sharing the admin.
3. **Exchange for a token.** `login(username, password)` returns a short-lived
   **access JWT** (~1 h TTL) plus a **refresh token**; the client caches the
   access token automatically. In CI, either call `login` at startup, or pass a
   pre-minted JWT (pattern 2 below) and refresh it before it expires.

`base_url` / the first positional arg points at *your* Pulse server —
`http://localhost:9090` for a local `pulse --headless` or desktop install, or
your deployed Pulse URL.

### Passing the token to the client

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

## Automatic retry (opt-in)

By default the client makes exactly one attempt per request and surfaces the
typed error — retries are **off** so nothing is retried behind your back. Opt in
with `max_retries`:

```python
client = PulseClient(
    "http://localhost:9090",
    token="ey...",
    max_retries=3,                 # 0 = off (default)
    retry_backoff=0.2,             # base seconds; full-jitter exponential backoff
    retry_max_backoff=10.0,        # cap per attempt
    retry_on_status=(502, 503, 504),
    retry_idempotent_only=True,    # don't retry POST on 5xx (default)
)
```

Policy:

* **429 (rate limited)** is retried for **any** method (the request was rejected,
  never processed) and honours `retryAfterSeconds` / the `Retry-After` header
  before falling back to backoff;
* `retry_on_status` 5xx and transport (connect/read) errors are retried only for
  **idempotent** methods (GET/HEAD/PUT/DELETE) unless `retry_idempotent_only=False`
  — so a POST create is never silently duplicated;
* terminal 4xx (400/401/404) are never retried; retries are bounded by `max_retries`.

> This opt-in retry policy currently ships in the **Python SDK** as the reference
> implementation. Rolling the same policy out to the Rust / Go / JS / Java SDKs is
> tracked as **B-170** (issue #312); those SDKs surface `retryAfter` on the
> rate-limit error today so callers can retry manually.

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

## Local pipeline simulation (Python-exclusive)

The streams DSL is **client-side declaration, server-side execution** — but the
Python SDK additionally ships a local, in-process executor: the moral equivalent
of Kafka Streams' `TopologyTestDriver`, with **no server and no JVM**. Run your
pipeline over sample events to see what would reach the sink, before you deploy:

```python
from pulse_client.streams import StreamBuilder, windows, aggs

builder = (
    StreamBuilder("card-velocity-60s")
    .from_topic("card-authorizations")
    .key_by("cardId")
    .window(windows.tumbling("60s"), aggregations={"txCount": aggs.count()})
    .filter("txCount > 5")
    .to_topic("fraud-alerts")
)

# Feed synthetic events through the SAME operator chain deploy() would POST:
survivors = builder.simulate([
    {"cardId": "card-7", "amount": 10, "_ts": 1_000},
    # … more events in the same 60s window …
    {"cardId": "card-7", "amount": 10, "_ts": 70_000},  # advances the watermark, closes the window
])
print(survivors)  # the window emissions that crossed txCount > 5
```

`simulate()` supports `filter` / `map` / `flat_map` / `key_by` / `window`
(tumbling + global) and all 7 aggregators, with a safe `ast`-based expression
evaluator (no `eval`). Engine-bound operators (`enrich`, `cep`, joins, LLM/MCP/ML)
raise `NotImplementedError` — deploy server-side for those.

> This is **unique to the Python SDK today**. The Rust / Go / JS / Java SDKs
> declare client-side (`compile`) and execute server-side (`deploy`) but have no
> in-process simulator. Cross-language parity is tracked as **B-169** (issue #311).

## Streaming SQL (compile SQL → pipeline) — B-097

Write a streaming pipeline as SQL; it compiles client-side to the same
`StreamBuilder` pipeline (a KSQL/Flink-SQL-flavoured subset):

```python
from pulse_client import PulseClient, compile_sql

builder = compile_sql(
    """SELECT count(*) AS cnt, sum(amount) AS total
       FROM payments
       WHERE amount > 1000
       GROUP BY customer_id
       WINDOW TUMBLING(60s)
       HAVING cnt > 5
       INTO fraud-alerts""",
    name="fraud-detector",
)

with PulseClient(url, token=jwt) as client:
    client.streams.deploy(builder)          # or: client.streams.from_sql(sql, name=...)
```

Supported: `SELECT` aggregates (`count(*)`, `sum/avg/min/max(f)`,
`distinct_count(f)`, `collect_list(f)`) with `AS` aliases, `*`, and plain-column
projection (→ a `map`); `WHERE`/`HAVING` (SQL `=`/`<>`/`AND`/`OR` translated to
`==`/`!=`/`&&`/`||`); `GROUP BY`; `WINDOW TUMBLING/SLIDING/SESSION/COUNT/GLOBAL`;
`INTO`. Inspect the result with `builder.build()` or run it with
`builder.simulate(events)` before deploying.

## Roadmap

- **v2.5.x** — current sync API, 5 core resources (auth, pipelines, agents, templates, users), `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — `AsyncPulseClient` with `async def` everywhere; same surface; one library, two clients.
- **B-098 satellite** — once `olsisoft/pulse-py` exists as its own repo, this in-tree code lifts out wholesale. Pip-install will switch to the satellite; in-tree continues to mirror for one release cycle so the migration is non-breaking.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
