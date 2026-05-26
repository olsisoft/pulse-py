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
