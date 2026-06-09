# Pulse Python SDK — Examples

Five runnable examples showing how an application drives the **StreamFlow event
mesh** through Pulse. The SDK *declares* the work; Pulse runs it on the cluster
(sharded, replicated) — `app → SDK → Pulse API → bridge → mesh`.

## Use cases

| # | File | What it shows |
|---|------|---------------|
| 1 | [`realtime_windowed_aggregation.py`](realtime_windowed_aggregation.py) | Per-merchant 1-minute tumbling-window rollup (`count`/`sum`/`avg`/`max`) → topic |
| 2 | [`events_live_and_replay.py`](events_live_and_replay.py) | Tail the live event stream **and** replay a key's committed state history (time-travel) |
| 3 | [`interactive_query.py`](interactive_query.py) | Interactive Query — `summary` / point `get` / bounded `scan` / filtered + grouped `query` |
| 4 | [`ai_enrichment_pipeline.py`](ai_enrichment_pipeline.py) | Agentic stream — LLM sentiment → `extract` structured fields → MCP CRM lookup |
| 5 | [`stream_to_connector.py`](stream_to_connector.py) | Discover sink connectors, then `filter` → sink a stream to a ClickHouse connector |

## Prerequisites

- **Python 3.9+** and the SDK installed: `pip install streamflow-pulse-client`
  (or `pip install -e .` from the repo).
- A reachable **Pulse** instance — running its embedded mesh, or attached to a
  StreamFlow cluster (Settings → Data Plane → REMOTE).

## Run

```bash
export PULSE_URL=http://localhost:9090      # your Pulse base URL
export PULSE_TOKEN=...                       # only if your Pulse requires auth

python examples/realtime_windowed_aggregation.py
python examples/events_live_and_replay.py
python examples/interactive_query.py
python examples/ai_enrichment_pipeline.py
python examples/stream_to_connector.py
```

Each pipeline example prints the compiled pipeline spec before deploying, so you
can inspect the declaration even without a live Pulse.

## Use-case ladder (simplest → most complex)

A graduated 5-step ladder over **one** domain — card-payments fraud monitoring
(topic `card-authorizations`, events `{cardId, merchantId, amount, ts}`; fraud
rule = more than 5 authorizations on one card in a 60s tumbling window). Each
step adds one capability on top of the previous.

| # | File | What it shows | Run command |
|---|------|---------------|-------------|
| 1 | [`usecase_1_connect_and_list.py`](usecase_1_connect_and_list.py) | Connect, read `version()`, log in if creds are set (degrade gracefully), list pipelines + connectors | `python examples/usecase_1_connect_and_list.py` |
| 2 | [`usecase_2_deploy_velocity_pipeline.py`](usecase_2_deploy_velocity_pipeline.py) | Build `card-velocity-60s` with the streams DSL, compile offline, deploy — plus a local `.simulate()` test-driver | `python examples/usecase_2_deploy_velocity_pipeline.py` |
| 3 | [`usecase_3_interactive_query.py`](usecase_3_interactive_query.py) | Interactive Query the agent's live state: `summary`, filtered `txCount > 5` query, point `get` — with a caller-side 429 retry | `python examples/usecase_3_interactive_query.py` |
| 4 | [`usecase_4_events_and_replay.py`](usecase_4_events_and_replay.py) | Tail live `fraud-alert` events (SSE), then replay one card's committed state history (time-travel) | `python examples/usecase_4_events_and_replay.py` |
| 5 | [`usecase_5_synchronous_decision.py`](usecase_5_synchronous_decision.py) | Open an async duplex channel to `fraud-decider`, send charges, receive correlated ALLOW/DENY (B-114) | `python examples/usecase_5_synchronous_decision.py` |

All five talk to a live Pulse at `PULSE_URL` (default `http://localhost:9090`;
set `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD`, for auth). Two things run
with **no** server: use-case 2's offline `.simulate()` demo — a local test-driver
unique to the Python SDK that runs the operator chain in-process — and the
offline `compile()` print. Use-case 5 needs the optional duplex extra:
`pip install streamflow-pulse-client[duplex]`.
