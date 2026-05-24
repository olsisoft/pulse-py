"""Tests for B-111 local pipeline simulation.

Coverage strategy:
1. **Expression evaluator** — every comparison, boolean, arithmetic, literal
   exercised plus the unsupported-construct guardrails.
2. **Per-operator semantics** — filter / map / flat_map / key_by / window
   each tested in isolation against hand-computed expected output.
3. **End-to-end pipelines** — multi-operator chains (the iot-temperature use
   case + a fraud-detector use case) matched against expected emission count
   and values.
4. **Error path** — unsupported operators raise NotImplementedError with the
   operator name in the message.
"""

from __future__ import annotations

import pytest

from pulse_client import StreamBuilder, aggs, windows
from pulse_client._simulator import (
    _eval_expr,
    _parse_agg,
    _parse_duration_ms,
    _parse_window_spec,
)

# ---------------------------------------------------------------------------
# Expression evaluator
# ---------------------------------------------------------------------------


class TestEvalExpr:
    def test_field_reference(self) -> None:
        assert _eval_expr("amount", {"amount": 42}) == 42

    def test_missing_field_returns_none(self) -> None:
        assert _eval_expr("missing", {"amount": 42}) is None

    def test_string_literal(self) -> None:
        assert _eval_expr("'gold'", {}) == "gold"

    def test_int_literal(self) -> None:
        assert _eval_expr("42", {}) == 42

    def test_float_literal(self) -> None:
        assert _eval_expr("3.14", {}) == 3.14

    def test_bool_literal(self) -> None:
        assert _eval_expr("True", {}) is True
        assert _eval_expr("False", {}) is False

    def test_none_literal(self) -> None:
        assert _eval_expr("None", {}) is None

    def test_greater_than(self) -> None:
        assert _eval_expr("amount > 100", {"amount": 200}) is True
        assert _eval_expr("amount > 100", {"amount": 50}) is False

    def test_greater_equal(self) -> None:
        assert _eval_expr("x >= 5", {"x": 5}) is True
        assert _eval_expr("x >= 5", {"x": 4}) is False

    def test_less_than(self) -> None:
        assert _eval_expr("x < 10", {"x": 5}) is True

    def test_less_equal(self) -> None:
        assert _eval_expr("x <= 10", {"x": 10}) is True

    def test_equal(self) -> None:
        assert _eval_expr("tier == 'gold'", {"tier": "gold"}) is True
        assert _eval_expr("tier == 'gold'", {"tier": "silver"}) is False

    def test_not_equal(self) -> None:
        assert _eval_expr("tier != 'free'", {"tier": "pro"}) is True

    def test_chained_compare(self) -> None:
        # Python chains: 1 < x < 10 means 1<x and x<10
        assert _eval_expr("1 < x < 10", {"x": 5}) is True
        assert _eval_expr("1 < x < 10", {"x": 15}) is False

    def test_and_operator(self) -> None:
        assert _eval_expr("a > 0 and b > 0", {"a": 1, "b": 2}) is True
        assert _eval_expr("a > 0 and b > 0", {"a": 1, "b": -1}) is False

    def test_or_operator(self) -> None:
        assert _eval_expr("a > 0 or b > 0", {"a": -1, "b": 2}) is True
        assert _eval_expr("a > 0 or b > 0", {"a": -1, "b": -1}) is False

    def test_not_operator(self) -> None:
        assert _eval_expr("not active", {"active": False}) is True
        assert _eval_expr("not active", {"active": True}) is False

    def test_arithmetic(self) -> None:
        assert _eval_expr("a + b", {"a": 1, "b": 2}) == 3
        assert _eval_expr("a - b", {"a": 5, "b": 2}) == 3
        assert _eval_expr("a * b", {"a": 4, "b": 3}) == 12
        assert _eval_expr("a / b", {"a": 10, "b": 4}) == 2.5
        assert _eval_expr("a % b", {"a": 10, "b": 3}) == 1
        assert _eval_expr("a // b", {"a": 10, "b": 3}) == 3

    def test_parentheses(self) -> None:
        assert _eval_expr("(a + b) * c", {"a": 1, "b": 2, "c": 4}) == 12

    def test_unary_minus(self) -> None:
        assert _eval_expr("-x", {"x": 5}) == -5

    def test_invalid_syntax_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="invalid expression"):
            _eval_expr("amount >", {})

    def test_function_call_not_supported(self) -> None:
        # Calls / attribute access / lambda etc. are out of scope
        with pytest.raises(NotImplementedError, match="expression node"):
            _eval_expr("concat(a, b)", {"a": 1, "b": 2})

    def test_subscript_not_supported(self) -> None:
        with pytest.raises(NotImplementedError, match="expression node"):
            _eval_expr("payload[0]", {"payload": [1, 2, 3]})


# ---------------------------------------------------------------------------
# Aggregator parsing + accumulators
# ---------------------------------------------------------------------------


class TestParseAgg:
    def test_count(self) -> None:
        assert _parse_agg("count()") == ("count", None)

    def test_sum(self) -> None:
        assert _parse_agg("sum(amount)") == ("sum", "amount")

    def test_avg_whitespace_tolerated(self) -> None:
        assert _parse_agg(" avg( temperature ) ") == ("avg", "temperature")

    def test_invalid_form_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid aggregator"):
            _parse_agg("just_a_name")
        with pytest.raises(ValueError, match="invalid aggregator"):
            _parse_agg("count(")


# ---------------------------------------------------------------------------
# Window-spec + duration parsing
# ---------------------------------------------------------------------------


class TestParseWindowSpec:
    def test_tumbling(self) -> None:
        assert _parse_window_spec("tumbling(60s)") == ("tumbling", {"size_ms": 60_000})

    def test_global(self) -> None:
        assert _parse_window_spec("global") == ("global", {})

    def test_sliding_not_supported(self) -> None:
        with pytest.raises(NotImplementedError, match="sliding"):
            _parse_window_spec("sliding(10m,1m)")

    def test_session_not_supported(self) -> None:
        with pytest.raises(NotImplementedError, match="session"):
            _parse_window_spec("session(30s)")

    def test_count_not_supported(self) -> None:
        with pytest.raises(NotImplementedError, match="count"):
            _parse_window_spec("count(100)")

    def test_unknown_form_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown window spec"):
            _parse_window_spec("hopping(5m)")


class TestParseDuration:
    def test_ms(self) -> None:
        assert _parse_duration_ms("500ms") == 500

    def test_s(self) -> None:
        assert _parse_duration_ms("60s") == 60_000

    def test_m(self) -> None:
        assert _parse_duration_ms("5m") == 300_000

    def test_h(self) -> None:
        assert _parse_duration_ms("2h") == 7_200_000

    def test_d(self) -> None:
        assert _parse_duration_ms("1d") == 86_400_000

    def test_invalid_unit_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid duration"):
            _parse_duration_ms("60min")

    def test_non_positive_raises(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            _parse_duration_ms("0s")
        with pytest.raises(ValueError, match="positive"):
            _parse_duration_ms("-5m")

    def test_non_numeric_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid duration"):
            _parse_duration_ms("xxs")


# ---------------------------------------------------------------------------
# Filter operator
# ---------------------------------------------------------------------------


class TestFilterOperator:
    def test_filter_keeps_passing_events(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .filter("amount > 100")
            .simulate(
                [
                    {"amount": 200, "id": "a"},
                    {"amount": 50, "id": "b"},
                    {"amount": 300, "id": "c"},
                ]
            )
        )
        assert [e["id"] for e in out] == ["a", "c"]

    def test_filter_with_and(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .filter("a > 0 and b < 10")
            .simulate(
                [
                    {"a": 1, "b": 5, "ok": 1},
                    {"a": 1, "b": 15, "ok": 0},
                    {"a": -1, "b": 5, "ok": 0},
                ]
            )
        )
        assert [e["ok"] for e in out] == [1]

    def test_filter_empty_input(self) -> None:
        out = StreamBuilder().from_topic("in").filter("x > 0").simulate([])
        assert out == []


# ---------------------------------------------------------------------------
# Map operator
# ---------------------------------------------------------------------------


class TestMapOperator:
    def test_map_adds_fields(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .map(fields={"double": "x * 2", "incr": "x + 1"})
            .simulate([{"x": 5}, {"x": 10}])
        )
        assert out == [
            {"x": 5, "double": 10, "incr": 6},
            {"x": 10, "double": 20, "incr": 11},
        ]

    def test_map_target_type(self) -> None:
        out = StreamBuilder().from_topic("in").map(target_type="alert").simulate([{"x": 5}])
        assert out == [{"x": 5, "type": "alert"}]

    def test_map_fields_and_target_type(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .map(fields={"y": "x"}, target_type="rewritten")
            .simulate([{"x": 1}])
        )
        assert out == [{"x": 1, "y": 1, "type": "rewritten"}]


# ---------------------------------------------------------------------------
# Flat-map operator
# ---------------------------------------------------------------------------


class TestFlatMapOperator:
    def test_flat_map_explodes_array(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .flat_map("items")
            .simulate([{"orderId": "o1", "items": ["a", "b", "c"]}])
        )
        assert [e["items"] for e in out] == ["a", "b", "c"]
        assert all(e["orderId"] == "o1" for e in out)

    def test_flat_map_dict_items_merge_into_event(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .flat_map("items")
            .simulate(
                [
                    {
                        "orderId": "o1",
                        "items": [{"sku": "x", "qty": 1}, {"sku": "y", "qty": 2}],
                    }
                ]
            )
        )
        # Each output event has the merged keys
        assert out[0]["sku"] == "x"
        assert out[0]["qty"] == 1
        assert out[1]["sku"] == "y"

    def test_flat_map_skips_non_arrays(self) -> None:
        out = (
            StreamBuilder().from_topic("in").flat_map("items").simulate([{"items": "not-an-array"}])
        )
        assert out == []


# ---------------------------------------------------------------------------
# Key-by operator
# ---------------------------------------------------------------------------


class TestKeyByOperator:
    def test_key_by_attaches_internal_key(self) -> None:
        # key_by sets a `_key` field on every event so downstream stateful
        # operators (window, aggregate) can group on it. The field is the
        # group identity — visible in the output so callers can inspect it
        # the same way they'd inspect a Kafka message key.
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("customer_id")
            .simulate([{"customer_id": "c1", "amount": 100}])
        )
        assert out == [{"_key": "c1", "customer_id": "c1", "amount": 100}]


# ---------------------------------------------------------------------------
# Window operator (tumbling + global)
# ---------------------------------------------------------------------------


class TestTumblingWindow:
    def test_tumbling_count_single_bucket(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("customer_id")
            .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
            .simulate(
                [
                    {"customer_id": "c1", "_ts": 0},
                    {"customer_id": "c1", "_ts": 10_000},
                    {"customer_id": "c1", "_ts": 30_000},
                    # Next event lands in the next 60s bucket → closes the first
                    {"customer_id": "c1", "_ts": 60_000},
                ]
            )
        )
        # First bucket closed by the 60_000 event → 3 events for c1
        # Second bucket flushed at end → 1 event for c1
        assert len(out) == 2
        assert out[0]["cnt"] == 3
        assert out[0]["_window_start"] == 0
        assert out[0]["_window_end"] == 60_000
        assert out[1]["cnt"] == 1
        assert out[1]["_window_start"] == 60_000

    def test_tumbling_avg_aggregator(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("deviceId")
            .window(
                windows.tumbling("60s"),
                aggregations={"avgTemp": aggs.avg("temperature")},
            )
            .simulate(
                [
                    {"deviceId": "d1", "temperature": 80.0, "_ts": 0},
                    {"deviceId": "d1", "temperature": 82.0, "_ts": 20_000},
                    {"deviceId": "d1", "temperature": 84.0, "_ts": 40_000},
                ]
            )
        )
        # No event past 60_000 → window flushes at end-of-input
        assert len(out) == 1
        assert out[0]["avgTemp"] == 82.0

    def test_tumbling_separate_keys(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
            .simulate(
                [
                    {"k": "a", "_ts": 0},
                    {"k": "b", "_ts": 0},
                    {"k": "a", "_ts": 10_000},
                ]
            )
        )
        # Single bucket, two keys, flushed at end
        assert len(out) == 2
        by_key = {e["_key"]: e["cnt"] for e in out}
        assert by_key == {"a": 2, "b": 1}

    def test_tumbling_sum(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(windows.tumbling("60s"), aggregations={"total": aggs.sum("amount")})
            .simulate(
                [
                    {"k": "a", "amount": 10, "_ts": 0},
                    {"k": "a", "amount": 20, "_ts": 5_000},
                    {"k": "a", "amount": 30, "_ts": 10_000},
                ]
            )
        )
        assert out[0]["total"] == 60

    def test_tumbling_min_max(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(
                windows.tumbling("60s"),
                aggregations={"lo": aggs.min("v"), "hi": aggs.max("v")},
            )
            .simulate(
                [
                    {"k": "a", "v": 5, "_ts": 0},
                    {"k": "a", "v": 2, "_ts": 1_000},
                    {"k": "a", "v": 9, "_ts": 2_000},
                ]
            )
        )
        assert out[0]["lo"] == 2
        assert out[0]["hi"] == 9

    def test_tumbling_collect_list(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(
                windows.tumbling("60s"),
                aggregations={"all": aggs.collect_list("v")},
            )
            .simulate(
                [
                    {"k": "a", "v": "x", "_ts": 0},
                    {"k": "a", "v": "y", "_ts": 1_000},
                ]
            )
        )
        assert out[0]["all"] == ["x", "y"]

    def test_tumbling_distinct_count(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(
                windows.tumbling("60s"),
                aggregations={"uniq": aggs.distinct_count("v")},
            )
            .simulate(
                [
                    {"k": "a", "v": "x", "_ts": 0},
                    {"k": "a", "v": "y", "_ts": 1_000},
                    {"k": "a", "v": "x", "_ts": 2_000},  # duplicate
                ]
            )
        )
        assert out[0]["uniq"] == 2

    def test_tumbling_nulls_skipped(self) -> None:
        # Server semantics: null values are skipped for numeric aggs.
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(
                windows.tumbling("60s"),
                aggregations={"s": aggs.sum("v"), "c": aggs.count()},
            )
            .simulate(
                [
                    {"k": "a", "v": 1, "_ts": 0},
                    {"k": "a", "v": None, "_ts": 1_000},
                    {"k": "a", "v": 2, "_ts": 2_000},
                ]
            )
        )
        # sum skips None → 3, count includes all → 3
        assert out[0]["s"] == 3
        assert out[0]["c"] == 3


class TestGlobalWindow:
    def test_global_aggregates_everything_at_end(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(windows.global_(), aggregations={"cnt": aggs.count()})
            .simulate(
                [
                    {"k": "a", "_ts": 0},
                    {"k": "a", "_ts": 1_000_000},
                    {"k": "b", "_ts": 2_000_000},
                ]
            )
        )
        assert len(out) == 2
        by_key = {e["_key"]: e["cnt"] for e in out}
        assert by_key == {"a": 2, "b": 1}

    def test_global_no_window_markers(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(windows.global_(), aggregations={"cnt": aggs.count()})
            .simulate([{"k": "a", "_ts": 0}])
        )
        # Global windows don't carry _window_start/_window_end
        assert "_window_start" not in out[0]
        assert "_window_end" not in out[0]


class TestWindowRequiresAggregations:
    def test_window_without_aggregations_raises(self) -> None:
        # The build()-side schema is permissive (the server validates), but
        # simulate() needs aggregations to produce output
        b = StreamBuilder().from_topic("in").key_by("k").window(windows.tumbling("60s"))
        with pytest.raises(ValueError, match="aggregations"):
            b.simulate([{"k": "a", "_ts": 0}])


# ---------------------------------------------------------------------------
# End-to-end pipelines
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_iot_temperature_aggregator(self) -> None:
        # Mirrors the canonical iot-temperature-aggregator template: alert
        # when the per-device minute-average temperature exceeds 75°C.
        events = [
            # rack-A is hot for the first minute → average above 75 → alert
            {"deviceId": "rack-A", "temperature": 80.0, "_ts": 0},
            {"deviceId": "rack-A", "temperature": 82.0, "_ts": 10_000},
            {"deviceId": "rack-A", "temperature": 78.0, "_ts": 20_000},
            # rack-B stays cool the whole minute → average below 75 → no alert
            {"deviceId": "rack-B", "temperature": 22.0, "_ts": 0},
            {"deviceId": "rack-B", "temperature": 90.0, "_ts": 10_000},
            {"deviceId": "rack-B", "temperature": 23.0, "_ts": 20_000},
        ]
        alerts = (
            StreamBuilder()
            .from_topic("sensor-readings", source_engine="mqtt")
            .key_by("deviceId")
            .window(
                windows.tumbling("60s"),
                aggregations={"avgTemp": aggs.avg("temperature")},
            )
            .filter("avgTemp > 75")
        ).simulate(events)

        assert len(alerts) == 1
        assert alerts[0]["_key"] == "rack-A"
        assert alerts[0]["avgTemp"] == 80.0

    def test_fraud_velocity_detector(self) -> None:
        # Mirrors the B-107-spec fraud-detector example: count high-value
        # transactions per customer per minute; alert when >5 in a window.
        events = [
            {"customer_id": "c1", "amount": 5000, "_ts": t * 1_000}
            for t in range(6)  # 6 events in the first 60s for c1
        ] + [
            {"customer_id": "c2", "amount": 5000, "_ts": 10_000},  # only 1
            {"customer_id": "c1", "amount": 100, "_ts": 30_000},  # filtered out
        ]
        alerts = (
            StreamBuilder()
            .from_topic("payments")
            .filter("amount > 1000")
            .key_by("customer_id")
            .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
            .filter("cnt > 5")
        ).simulate(events)

        assert len(alerts) == 1
        assert alerts[0]["_key"] == "c1"
        assert alerts[0]["cnt"] == 6

    def test_map_after_window_renames_keys(self) -> None:
        out = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
            .map(fields={"alert_count": "cnt"}, target_type="signal")
        ).simulate([{"k": "x", "_ts": 0}])

        assert out[0]["alert_count"] == 1
        assert out[0]["type"] == "signal"

    def test_simulate_empty_input_returns_empty(self) -> None:
        out = (StreamBuilder().from_topic("in").filter("x > 0")).simulate([])
        assert out == []

    def test_simulate_event_must_be_dict(self) -> None:
        b = StreamBuilder().from_topic("in").filter("x > 0")
        with pytest.raises(TypeError, match="dict"):
            b.simulate([1, 2, 3])  # type: ignore[list-item]


# ---------------------------------------------------------------------------
# Error / unsupported paths
# ---------------------------------------------------------------------------


class TestUnsupportedOperators:
    def test_branch_raises(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .branch(
                [
                    {"condition": "x > 0", "topic": "pos"},
                    {"condition": "x < 0", "topic": "neg"},
                ]
            )
        )
        with pytest.raises(NotImplementedError, match="branch"):
            b.simulate([{"x": 1}])

    def test_enrich_raises(self) -> None:
        b = StreamBuilder().from_topic("in").enrich(lookup_topic="customers", key_field="id")
        with pytest.raises(NotImplementedError, match="enrich"):
            b.simulate([{"id": "c1"}])

    def test_enrich_async_raises(self) -> None:
        b = StreamBuilder().from_topic("in").enrich_async(url="https://x")
        with pytest.raises(NotImplementedError, match="enrich"):
            b.simulate([{"x": 1}])

    def test_cep_raises(self) -> None:
        b = StreamBuilder().from_topic("in").cep([{"match": "x > 0"}])
        with pytest.raises(NotImplementedError, match="cep"):
            b.simulate([{"x": 1}])

    def test_broadcast_join_raises(self) -> None:
        b = StreamBuilder().from_topic("in").broadcast_join(join_key_field="k")
        with pytest.raises(NotImplementedError, match="broadcastJoin"):
            b.simulate([{"k": "x"}])

    def test_cdc_join_raises(self) -> None:
        b = StreamBuilder().from_topic("in").cdc_join(source="postgres://x")
        with pytest.raises(NotImplementedError, match="cdcJoin"):
            b.simulate([{"x": 1}])

    def test_sliding_window_raises(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(
                windows.sliding("10m", "1m"),
                aggregations={"cnt": aggs.count()},
            )
        )
        with pytest.raises(NotImplementedError, match="sliding"):
            b.simulate([{"k": "a", "_ts": 0}])

    def test_session_window_raises(self) -> None:
        b = (
            StreamBuilder()
            .from_topic("in")
            .key_by("k")
            .window(windows.session("30s"), aggregations={"cnt": aggs.count()})
        )
        with pytest.raises(NotImplementedError, match="session"):
            b.simulate([{"k": "a", "_ts": 0}])


class TestSimulateGuardrails:
    def test_simulate_rejects_empty_chain(self) -> None:
        with pytest.raises(ValueError, match="operators"):
            StreamBuilder().from_topic("in").simulate([{"x": 1}])
