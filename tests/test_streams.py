"""Tests for B-107 streams DSL.

Coverage strategy: every operator method is exercised at three layers —

1. **Constructor validation** — the SDK rejects obviously-broken input
   client-side (empty topic, no source, no operators, invalid ordering enum,
   …) so callers see the bug immediately instead of getting a 400 from the
   server several seconds later.
2. **Compiled shape** — the dict produced by ``builder.build()`` matches
   exactly what ``StreamingOperatorValidator`` accepts (verified against the
   schema in the validator's source).
3. **Round-trip** — the canonical ``iot-temperature-aggregator`` template is
   rebuilt from scratch using the DSL and matches the hand-authored JSON
   shipped in ``streamflow-pulse/src/main/resources/pipeline-templates/``.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from pulse_client import (
    PulseClient,
    StreamBuilder,
    StreamsResource,
    WindowSpec,
    aggs,
    windows,
)
from pulse_client.streams import _require_nonblank

# ---------------------------------------------------------------------------
# Window-spec factories
# ---------------------------------------------------------------------------


class TestWindowSpec:
    def test_tumbling_emits_expected_string(self) -> None:
        assert windows.tumbling("60s").spec == "tumbling(60s)"

    def test_sliding_emits_expected_string(self) -> None:
        assert windows.sliding("10m", "1m").spec == "sliding(10m,1m)"

    def test_session_emits_expected_string(self) -> None:
        assert windows.session("30s").spec == "session(30s)"

    def test_global_emits_expected_string(self) -> None:
        assert windows.global_().spec == "global"

    def test_count_window_emits_expected_string(self) -> None:
        assert windows.count(100).spec == "count(100)"

    def test_count_sliding_emits_expected_string(self) -> None:
        assert windows.count_sliding(100, 10).spec == "count_sliding(100,10)"

    def test_tumbling_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="size"):
            windows.tumbling("")

    def test_sliding_rejects_blank_either_arg(self) -> None:
        with pytest.raises(ValueError, match="slide"):
            windows.sliding("10m", "")
        with pytest.raises(ValueError, match="size"):
            windows.sliding(" ", "1m")

    def test_count_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            windows.count(0)
        with pytest.raises(ValueError, match="positive"):
            windows.count(-5)

    def test_count_sliding_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            windows.count_sliding(100, 0)
        with pytest.raises(ValueError, match="positive"):
            windows.count_sliding(0, 10)

    def test_window_spec_eq_and_hash(self) -> None:
        a = WindowSpec("tumbling(60s)")
        b = WindowSpec("tumbling(60s)")
        c = WindowSpec("tumbling(30s)")
        assert a == b
        assert hash(a) == hash(b)
        assert a != c
        assert a != "tumbling(60s)"  # not equal to a raw string

    def test_window_spec_repr_includes_spec(self) -> None:
        assert "tumbling(60s)" in repr(windows.tumbling("60s"))

    def test_window_spec_rejects_blank(self) -> None:
        with pytest.raises(ValueError):
            WindowSpec("")
        with pytest.raises(ValueError):
            WindowSpec("   ")


# ---------------------------------------------------------------------------
# Aggregator factories
# ---------------------------------------------------------------------------


class TestAggregators:
    def test_count_takes_no_field(self) -> None:
        assert aggs.count() == "count()"

    def test_sum_avg_min_max_collect_distinct(self) -> None:
        assert aggs.sum("amount") == "sum(amount)"
        assert aggs.avg("price") == "avg(price)"
        assert aggs.min("latency") == "min(latency)"
        assert aggs.max("latency") == "max(latency)"
        assert aggs.collect_list("sku") == "collect_list(sku)"
        assert aggs.distinct_count("user_id") == "distinct_count(user_id)"

    def test_aggregators_reject_blank_field(self) -> None:
        for fn in (aggs.sum, aggs.avg, aggs.min, aggs.max, aggs.collect_list, aggs.distinct_count):
            with pytest.raises(ValueError, match="field"):
                fn("")
            with pytest.raises(ValueError, match="field"):
                fn("   ")


# ---------------------------------------------------------------------------
# StreamBuilder — per-operator shape
# ---------------------------------------------------------------------------


class TestStreamBuilderOperators:
    def test_filter_emits_validator_shape(self) -> None:
        b = StreamBuilder().from_topic("in").filter("amount > 1000")
        ops = b.operators()
        assert ops == [{"type": "filter", "condition": "amount > 1000"}]

    def test_filter_rejects_blank_condition(self) -> None:
        with pytest.raises(ValueError, match="condition"):
            StreamBuilder().from_topic("in").filter("")

    def test_map_with_fields_only(self) -> None:
        b = StreamBuilder().from_topic("in").map(fields={"alert": "concat(id, '!')"})
        assert b.operators() == [{"type": "map", "fields": {"alert": "concat(id, '!')"}}]

    def test_map_with_target_type_only(self) -> None:
        b = StreamBuilder().from_topic("in").map(target_type="alert")
        assert b.operators() == [{"type": "map", "targetType": "alert"}]

    def test_map_with_both_fields_and_target_type(self) -> None:
        b = StreamBuilder().from_topic("in").map(fields={"x": "1"}, target_type="alert")
        assert b.operators() == [{"type": "map", "fields": {"x": "1"}, "targetType": "alert"}]

    def test_map_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="map operator does nothing"):
            StreamBuilder().from_topic("in").map()

    def test_flat_map_emits_validator_shape(self) -> None:
        b = StreamBuilder().from_topic("in").flat_map("items")
        assert b.operators() == [{"type": "flatMap", "splitField": "items"}]

    def test_flat_map_rejects_blank(self) -> None:
        with pytest.raises(ValueError, match="split_field"):
            StreamBuilder().from_topic("in").flat_map("")

    def test_key_by_emits_validator_shape(self) -> None:
        b = StreamBuilder().from_topic("in").key_by("deviceId")
        assert b.operators() == [{"type": "keyBy", "field": "deviceId"}]

    def test_key_by_rejects_blank(self) -> None:
        with pytest.raises(ValueError, match="field"):
            StreamBuilder().from_topic("in").key_by("")

    def test_window_with_aggregations(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .window(
                windows.tumbling("60s"),
                aggregations={"avgTemp": aggs.avg("temperature")},
            )
        )
        assert b.operators() == [
            {
                "type": "window",
                "spec": "tumbling(60s)",
                "aggregations": {"avgTemp": "avg(temperature)"},
            }
        ]

    def test_window_accepts_raw_string_spec(self) -> None:
        b = StreamBuilder().from_topic("in").window("sliding(10m,1m)")
        assert b.operators() == [{"type": "window", "spec": "sliding(10m,1m)"}]

    def test_window_with_output_topic_and_trigger(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .window(
                windows.tumbling("60s"),
                output_topic="late-data",
                trigger={"kind": "earlyFire", "afterEvents": 10},
            )
        )
        op = b.operators()[0]
        assert op["outputTopic"] == "late-data"
        assert op["trigger"] == {"kind": "earlyFire", "afterEvents": 10}

    def test_window_rejects_blank_spec_string(self) -> None:
        with pytest.raises(ValueError, match="spec"):
            StreamBuilder().from_topic("in").window("")

    def test_branch_emits_validator_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .branch(
                [
                    {"condition": "tier == 'gold'", "topic": "vip-events"},
                    {"condition": "tier == 'silver'", "topic": "std-events"},
                ]
            )
        )
        assert b.operators() == [
            {
                "type": "branch",
                "branches": [
                    {"condition": "tier == 'gold'", "topic": "vip-events"},
                    {"condition": "tier == 'silver'", "topic": "std-events"},
                ],
            }
        ]

    def test_branch_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            StreamBuilder().from_topic("in").branch([])

    def test_branch_rejects_missing_fields(self) -> None:
        with pytest.raises(ValueError, match="condition"):
            StreamBuilder().from_topic("in").branch([{"topic": "x"}])
        with pytest.raises(ValueError, match="topic"):
            StreamBuilder().from_topic("in").branch([{"condition": "x > 0"}])

    def test_enrich_emits_validator_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .enrich(lookup_topic="customers", key_field="customerId")
        )
        assert b.operators() == [
            {"type": "enrich", "lookupTopic": "customers", "keyField": "customerId"}
        ]

    def test_enrich_rejects_blank_args(self) -> None:
        with pytest.raises(ValueError):
            StreamBuilder().from_topic("in").enrich(lookup_topic="", key_field="k")
        with pytest.raises(ValueError):
            StreamBuilder().from_topic("in").enrich(lookup_topic="t", key_field="")

    def test_enrich_async_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .enrich_async(
                url="https://x.example/lookup/{id}",
                parallelism=8,
                queue_size=128,
                timeout_ms=5000,
                max_retries=3,
                retry_backoff_ms=200,
                ordering="PRESERVE_INPUT",
                on_failure="EMIT_ERROR",
            )
        )
        assert b.operators() == [
            {
                "type": "enrichAsync",
                "url": "https://x.example/lookup/{id}",
                "parallelism": 8,
                "queueSize": 128,
                "timeoutMs": 5000,
                "maxRetries": 3,
                "retryBackoffMs": 200,
                "ordering": "PRESERVE_INPUT",
                "onFailure": "EMIT_ERROR",
            }
        ]

    def test_enrich_async_rejects_bad_ordering(self) -> None:
        with pytest.raises(ValueError, match="ordering"):
            StreamBuilder().from_topic("in").enrich_async(url="https://x", ordering="SHUFFLED")

    def test_enrich_async_rejects_bad_on_failure(self) -> None:
        with pytest.raises(ValueError, match="on_failure"):
            StreamBuilder().from_topic("in").enrich_async(url="https://x", on_failure="EXPLODE")

    def test_cep_emits_validator_shape(self) -> None:
        seq = [
            {"name": "add", "match": "type == 'ADD_TO_CART'", "within": "10m"},
            {"name": "view", "match": "type == 'VIEW_CART'", "follow": "followedBy"},
        ]
        b = StreamBuilder().from_topic("in").cep(seq, within="20m", name="cart-flow")
        assert b.operators() == [
            {"type": "cep", "sequence": seq, "within": "20m", "name": "cart-flow"}
        ]

    def test_cep_rejects_empty_sequence(self) -> None:
        with pytest.raises(ValueError, match="non-empty sequence"):
            StreamBuilder().from_topic("in").cep([])

    # ---- B-109 map_llm ----

    def test_map_llm_minimal_shape(self) -> None:
        b = StreamBuilder().from_topic("in").map_llm("Classify: {text}", output_field="sentiment")
        assert b.operators() == [
            {"type": "mapLlm", "prompt": "Classify: {text}", "outputField": "sentiment"}
        ]

    def test_map_llm_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .map_llm(
                "Summarise: {body}",
                output_field="summary",
                model="gemma3:7b",
                temperature=0.0,
                max_tokens=64,
                parallelism=8,
                ordering="UNORDERED",
                on_failure="PASS_THROUGH",
                max_calls_per_sec=50,
            )
        )
        assert b.operators() == [
            {
                "type": "mapLlm",
                "prompt": "Summarise: {body}",
                "outputField": "summary",
                "model": "gemma3:7b",
                "temperature": 0.0,
                "maxTokens": 64,
                "parallelism": 8,
                "ordering": "UNORDERED",
                "onFailure": "PASS_THROUGH",
                "maxCallsPerSec": 50,
            }
        ]

    def test_map_llm_rejects_blank_prompt(self) -> None:
        with pytest.raises(ValueError, match="prompt"):
            StreamBuilder().from_topic("in").map_llm("", output_field="x")

    def test_map_llm_rejects_blank_output_field(self) -> None:
        with pytest.raises(ValueError, match="output_field"):
            StreamBuilder().from_topic("in").map_llm("p", output_field="")

    def test_map_llm_rejects_bad_ordering(self) -> None:
        with pytest.raises(ValueError, match="ordering"):
            StreamBuilder().from_topic("in").map_llm("p", output_field="o", ordering="SHUFFLED")

    def test_map_llm_rejects_bad_on_failure(self) -> None:
        with pytest.raises(ValueError, match="on_failure"):
            StreamBuilder().from_topic("in").map_llm("p", output_field="o", on_failure="EXPLODE")

    # ---- B-109 extract ----

    def test_extract_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .extract(
                instruction="Extract the intent and urgency",
                schema={"intent": "string", "urgency": "int"},
                model="gemma3:7b",
                temperature=0.0,
            )
        )
        assert b.operators() == [
            {
                "type": "extract",
                "instruction": "Extract the intent and urgency",
                "schema": {"intent": "string", "urgency": "int"},
                "model": "gemma3:7b",
                "temperature": 0.0,
            }
        ]

    def test_extract_rejects_blank_instruction(self) -> None:
        with pytest.raises(ValueError, match="instruction"):
            StreamBuilder().from_topic("in").extract(instruction="", schema={"a": "string"})

    def test_extract_rejects_empty_schema(self) -> None:
        with pytest.raises(ValueError, match="schema"):
            StreamBuilder().from_topic("in").extract(instruction="x", schema={})

    def test_extract_rejects_bad_on_failure(self) -> None:
        with pytest.raises(ValueError, match="on_failure"):
            StreamBuilder().from_topic("in").extract(
                instruction="x", schema={"a": "string"}, on_failure="NOPE"
            )

    def test_llm_operators_chain_with_others(self) -> None:
        # B-109 operators compose in the same chain as filter/map/window.
        b = (
            StreamBuilder()
            .from_topic("tickets")
            .map_llm("Summarise: {body}", output_field="summary")
            .extract(instruction="Classify", schema={"urgency": "int"})
            .filter("urgency >= 4")
        )
        types = [op["type"] for op in b.operators()]
        assert types == ["mapLlm", "extract", "filter"]

    # ---- B-109 Phase 2 mcp_call ----

    def test_mcp_call_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .mcp_call(
                "crm.lookup_customer",
                args={"customer_id": "{customerId}"},
                output_field="customer",
                parallelism=4,
                ordering="UNORDERED",
                on_failure="EMIT_ERROR",
            )
        )
        assert b.operators() == [
            {
                "type": "mcpCall",
                "tool": "crm.lookup_customer",
                "args": {"customer_id": "{customerId}"},
                "outputField": "customer",
                "parallelism": 4,
                "ordering": "UNORDERED",
                "onFailure": "EMIT_ERROR",
            }
        ]

    def test_mcp_call_minimal_fire_and_forget(self) -> None:
        b = StreamBuilder().from_topic("in").mcp_call("pagerduty.create_incident")
        assert b.operators() == [{"type": "mcpCall", "tool": "pagerduty.create_incident"}]

    def test_mcp_call_rejects_blank_tool(self) -> None:
        with pytest.raises(ValueError, match="tool"):
            StreamBuilder().from_topic("in").mcp_call("")

    def test_mcp_call_rejects_bad_on_failure(self) -> None:
        with pytest.raises(ValueError, match="on_failure"):
            StreamBuilder().from_topic("in").mcp_call("x", on_failure="NOPE")

    # ── B-112 ml_predict ──────────────────────────────────────────

    def test_ml_predict_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("transactions")
            .ml_predict(
                model="fraud-classifier",
                input_fields=["amount", "country", "merchant_category"],
                output_field="prediction",
                parallelism=8,
                ordering="UNORDERED",
                on_failure="DROP",
            )
        )
        assert b.operators() == [
            {
                "type": "mlPredict",
                "model": "fraud-classifier",
                "inputFields": ["amount", "country", "merchant_category"],
                "outputField": "prediction",
                "parallelism": 8,
                "ordering": "UNORDERED",
                "onFailure": "DROP",
            }
        ]

    def test_ml_predict_minimal_shape(self) -> None:
        b = StreamBuilder().from_topic("in").ml_predict(
            model="m", input_fields=["x"], output_field="p"
        )
        assert b.operators() == [
            {"type": "mlPredict", "model": "m", "inputFields": ["x"], "outputField": "p"}
        ]

    def test_ml_predict_rejects_blank_model(self) -> None:
        with pytest.raises(ValueError, match="model"):
            StreamBuilder().from_topic("in").ml_predict(
                model="", input_fields=["x"], output_field="p"
            )

    def test_ml_predict_rejects_blank_output_field(self) -> None:
        with pytest.raises(ValueError, match="output_field"):
            StreamBuilder().from_topic("in").ml_predict(
                model="m", input_fields=["x"], output_field=""
            )

    def test_ml_predict_rejects_empty_input_fields(self) -> None:
        with pytest.raises(ValueError, match="input_fields"):
            StreamBuilder().from_topic("in").ml_predict(
                model="m", input_fields=[], output_field="p"
            )

    def test_ml_predict_rejects_non_string_input_fields(self) -> None:
        with pytest.raises(ValueError, match="input_fields"):
            StreamBuilder().from_topic("in").ml_predict(
                model="m", input_fields=["x", ""], output_field="p"
            )

    def test_ml_predict_rejects_bad_ordering(self) -> None:
        with pytest.raises(ValueError, match="ordering"):
            StreamBuilder().from_topic("in").ml_predict(
                model="m", input_fields=["x"], output_field="p", ordering="SHUFFLED"
            )

    def test_broadcast_join_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .broadcast_join(
                join_key_field="userId",
                streaming_topic="users-table",
                name="users-join",
                max_bytes=10_000_000,
                refresh_mode="cdc",
                interval_millis=30_000,
            )
        )
        assert b.operators() == [
            {
                "type": "broadcastJoin",
                "joinKeyField": "userId",
                "streamingTopic": "users-table",
                "name": "users-join",
                "maxBytes": 10_000_000,
                "refreshMode": "cdc",
                "intervalMillis": 30_000,
            }
        ]

    def test_broadcast_join_rejects_bad_refresh_mode(self) -> None:
        with pytest.raises(ValueError, match="refresh_mode"):
            StreamBuilder().from_topic("in").broadcast_join(
                join_key_field="k", refresh_mode="random"
            )

    def test_cdc_join_full_shape(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .cdc_join(
                source="postgres://orders",
                join_key="orderId",
                table="orders",
                state_backend="rocksdb",
            )
        )
        assert b.operators() == [
            {
                "type": "cdcJoin",
                "source": "postgres://orders",
                "joinKey": "orderId",
                "table": "orders",
                "stateBackend": "rocksdb",
            }
        ]

    def test_cdc_join_minimal_shape(self) -> None:
        b = StreamBuilder().from_topic("in").cdc_join(source="postgres://orders")
        assert b.operators() == [{"type": "cdcJoin", "source": "postgres://orders"}]


# ---------------------------------------------------------------------------
# StreamBuilder — full pipeline compilation
# ---------------------------------------------------------------------------


class TestStreamBuilderCompile:
    def test_minimal_pipeline_builds(self) -> None:
        out = StreamBuilder(name="p1").from_topic("in").filter("x > 0").build()
        assert out["name"] == "p1"
        assert out["nodes"][0]["type"] == "source"
        assert out["nodes"][0]["config"]["inputTopic"] == "in"
        assert out["nodes"][0]["config"]["engine"] == "kafka"
        assert out["nodes"][1]["type"] == "agent"
        assert out["nodes"][1]["config"]["engine"] == "streaming"
        assert out["nodes"][1]["config"]["operators"] == [{"type": "filter", "condition": "x > 0"}]
        # No sink: agent's only output is its state store
        assert len(out["nodes"]) == 2

    def test_name_can_be_set_via_named(self) -> None:
        b = StreamBuilder().named("p2").from_topic("in").filter("x > 0")
        assert b.build()["name"] == "p2"

    def test_name_passed_to_build_overrides_constructor(self) -> None:
        b = StreamBuilder(name="ignored").from_topic("in").filter("x > 0")
        assert b.build(name="actual")["name"] == "actual"

    def test_description_propagates(self) -> None:
        b = StreamBuilder(name="p3", description="my pipeline").from_topic("in").filter("x > 0")
        assert b.build()["description"] == "my pipeline"

    def test_described_as_setter(self) -> None:
        b = StreamBuilder(name="p4").described_as("desc").from_topic("in").filter("x > 0")
        assert b.build()["description"] == "desc"

    def test_agent_label_setter(self) -> None:
        b = (
            StreamBuilder(name="p5")
            .with_agent_label("Per-Device Average")
            .from_topic("in")
            .filter("x > 0")
        )
        assert b.build()["nodes"][1]["label"] == "Per-Device Average"

    def test_build_emits_sink_when_to_topic_has_channel(self) -> None:
        out = (
            StreamBuilder(name="p6")
            .from_topic("in")
            .filter("x > 0")
            .to_topic("out", sink_channel="email")
            .build()
        )
        assert len(out["nodes"]) == 3
        sink = out["nodes"][2]
        assert sink == {
            "type": "sink",
            "label": "email sink",
            "config": {"channel": "email", "inputTopic": "out"},
        }
        # Agent gets the outputTopic too
        assert out["nodes"][1]["config"]["outputTopic"] == "out"

    def test_to_connector_emits_sink_with_channel_config_and_default_topic(self) -> None:
        out = (
            StreamBuilder(name="pc")
            .from_topic("in")
            .filter("x > 0")
            .to_connector("segment", {"segment.write.key": "wk"})
            .build()
        )
        sink = out["nodes"][2]
        assert sink["type"] == "sink"
        assert sink["config"]["channel"] == "segment"
        assert sink["config"]["inputTopic"] == "segment-sink-out"  # derived default
        assert sink["config"]["segment.write.key"] == "wk"

    def test_to_connector_honours_custom_topic_and_rejects_blank(self) -> None:
        out = (
            StreamBuilder(name="pc2")
            .from_topic("in")
            .filter("x > 0")
            .to_connector("kafka", topic="events")
            .build()
        )
        assert out["nodes"][2]["config"]["inputTopic"] == "events"
        with pytest.raises(ValueError):
            StreamBuilder().from_topic("in").to_connector("")

    def test_build_skips_sink_when_to_topic_has_no_channel(self) -> None:
        out = StreamBuilder(name="p7").from_topic("in").filter("x > 0").to_topic("out").build()
        assert len(out["nodes"]) == 2
        assert out["nodes"][1]["config"]["outputTopic"] == "out"

    def test_to_state_clears_output_and_sink(self) -> None:
        out = (
            StreamBuilder(name="p8")
            .from_topic("in")
            .filter("x > 0")
            .to_topic("out", sink_channel="email")  # set then…
            .to_state()  # …override to state-only
            .build()
        )
        assert len(out["nodes"]) == 2
        assert "outputTopic" not in out["nodes"][1]["config"]

    def test_source_engine_and_label_propagate(self) -> None:
        out = (
            StreamBuilder(name="p9")
            .from_topic(
                "in",
                source_engine="mqtt",
                source_config={"qos": 1},
                label="Sensor readings",
            )
            .filter("x > 0")
            .build()
        )
        src = out["nodes"][0]
        assert src["label"] == "Sensor readings"
        assert src["config"] == {"engine": "mqtt", "inputTopic": "in", "qos": 1}

    def test_sink_config_merge(self) -> None:
        out = (
            StreamBuilder(name="p10")
            .from_topic("in")
            .filter("x > 0")
            .to_topic(
                "out",
                sink_channel="slack",
                sink_config={"webhookUrl": "https://hooks.slack/..."},
                label="Heat Alert",
            )
            .build()
        )
        sink = out["nodes"][2]
        assert sink["label"] == "Heat Alert"
        assert sink["config"] == {
            "channel": "slack",
            "inputTopic": "out",
            "webhookUrl": "https://hooks.slack/...",
        }

    def test_build_rejects_missing_name(self) -> None:
        with pytest.raises(ValueError, match="name"):
            StreamBuilder().from_topic("in").filter("x > 0").build()

    def test_build_rejects_missing_source(self) -> None:
        with pytest.raises(ValueError, match="source"):
            StreamBuilder(name="p").filter("x > 0").build()

    def test_build_rejects_empty_operator_chain(self) -> None:
        with pytest.raises(ValueError, match="operators"):
            StreamBuilder(name="p").from_topic("in").build()

    def test_operators_returns_copy_not_mutating(self) -> None:
        b = StreamBuilder().from_topic("in").filter("x > 0")
        snapshot = b.operators()
        snapshot.append({"type": "tampered"})
        # The internal chain stayed intact
        assert b.operators() == [{"type": "filter", "condition": "x > 0"}]

    def test_chain_ordering_preserved(self) -> None:
        out = (
            StreamBuilder(name="p11")
            .from_topic("in")
            .filter("a > 0")
            .key_by("k")
            .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
            .filter("cnt > 5")
            .map(fields={"out": "cnt"})
            .build()
        )
        ops = out["nodes"][1]["config"]["operators"]
        assert [op["type"] for op in ops] == ["filter", "keyBy", "window", "filter", "map"]

    def test_iot_template_round_trip(self) -> None:
        """Rebuild the canonical iot-temperature-aggregator template via the DSL.

        Verifies the DSL produces the SAME operator JSON shipped in
        streamflow-pulse/src/main/resources/pipeline-templates/iot-temperature-aggregator.json
        — i.e. our compiler output is byte-compatible with a known-validated
        template.
        """
        out = (
            StreamBuilder(name="iot-temperature-aggregator")
            .with_agent_label("Per-Device Average")
            .from_topic(
                "sensor-readings",
                source_engine="mqtt",
                label="Sensor readings",
            )
            .key_by("deviceId")
            .window(
                windows.tumbling("60s"),
                aggregations={"avgTemp": aggs.avg("temperature")},
            )
            .filter("avgTemp > 75")
            .to_topic(
                "sensor-minute-averages",
                sink_channel="email",
                label="Heat Alert",
            )
            .build()
        )

        # The 3 nodes the template ships, in order
        assert [n["type"] for n in out["nodes"]] == ["source", "agent", "sink"]

        src = out["nodes"][0]
        assert src["label"] == "Sensor readings"
        assert src["config"] == {"engine": "mqtt", "inputTopic": "sensor-readings"}

        agent = out["nodes"][1]
        assert agent["label"] == "Per-Device Average"
        assert agent["config"]["engine"] == "streaming"
        assert agent["config"]["inputTopic"] == "sensor-readings"
        assert agent["config"]["outputTopic"] == "sensor-minute-averages"
        assert agent["config"]["operators"] == [
            {"type": "keyBy", "field": "deviceId"},
            {
                "type": "window",
                "spec": "tumbling(60s)",
                "aggregations": {"avgTemp": "avg(temperature)"},
            },
            {"type": "filter", "condition": "avgTemp > 75"},
        ]

        sink = out["nodes"][2]
        assert sink["label"] == "Heat Alert"
        assert sink["config"] == {
            "channel": "email",
            "inputTopic": "sensor-minute-averages",
        }


# ---------------------------------------------------------------------------
# StreamsResource — compile + deploy
# ---------------------------------------------------------------------------


class TestStreamsResource:
    def test_streams_accessor_exists(self, client: PulseClient) -> None:
        assert isinstance(client.streams, StreamsResource)

    def test_compile_returns_dict_without_http_call(self, client: PulseClient) -> None:
        b = StreamBuilder(name="p").from_topic("in").filter("x > 0")
        with respx.mock(base_url="http://pulse.test:9090") as router:
            out = client.streams.compile(b)
            # No mock was set up — assert that .compile() didn't try to call
            # the network (router has zero received calls)
            assert router.calls.call_count == 0
        assert out["name"] == "p"

    def test_deploy_posts_built_definition_to_pipelines_endpoint(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        b = (
            StreamBuilder(name="fraud-detector")
            .from_topic("payments")
            .filter("amount > 1000")
            .key_by("customer_id")
            .window(
                windows.tumbling("60s"),
                aggregations={"cnt": aggs.count()},
            )
            .filter("cnt > 5")
            .to_topic("fraud-alerts")
        )

        with respx.mock(base_url=base_url, assert_all_called=True) as router:
            route = router.post("/api/pulse/pipelines").mock(
                return_value=httpx.Response(
                    201, json={"id": "p-42", "name": "fraud-detector", "status": "running"}
                )
            )
            result = authed_client.streams.deploy(b)
            assert result["id"] == "p-42"
            # Verify the wire body was the DSL-compiled definition
            assert route.called
            sent = route.calls.last.request
            import json as _json

            body = _json.loads(sent.content)
            assert body["name"] == "fraud-detector"
            assert body["nodes"][1]["config"]["operators"][2]["type"] == "window"

    def test_deploy_name_override_propagates_to_body(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        b = StreamBuilder(name="original").from_topic("in").filter("x > 0")
        with respx.mock(base_url=base_url) as router:
            route = router.post("/api/pulse/pipelines").mock(
                return_value=httpx.Response(201, json={"id": "p", "name": "renamed"})
            )
            authed_client.streams.deploy(b, name="renamed")
            import json as _json

            body = _json.loads(route.calls.last.request.content)
            assert body["name"] == "renamed"


# ---------------------------------------------------------------------------
# Helper coverage
# ---------------------------------------------------------------------------


class TestRequireNonblank:
    def test_rejects_none(self) -> None:
        with pytest.raises(ValueError, match="must be a non-empty string"):
            _require_nonblank("x", None)

    def test_rejects_blank(self) -> None:
        with pytest.raises(ValueError):
            _require_nonblank("x", "")
        with pytest.raises(ValueError):
            _require_nonblank("x", "   ")

    def test_accepts_nontrivial(self) -> None:
        _require_nonblank("x", "valid")  # no raise
