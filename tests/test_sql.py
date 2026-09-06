import pytest

from pulse_client import compile_sql


def _agent_ops(builder):
    nodes = builder.build(name="t")["nodes"]
    agent = next(n for n in nodes if n["type"] == "agent")
    return agent["config"]["operators"], agent["config"], nodes


def test_full_windowed_aggregate_query():
    b = compile_sql(
        """SELECT count(*) AS cnt, sum(amount) AS total
           FROM payments
           WHERE amount > 1000
           GROUP BY customer_id
           WINDOW TUMBLING(60s)
           HAVING cnt > 5
           INTO fraud-alerts""",
        name="fraud",
    )
    ops, agent, nodes = _agent_ops(b)
    assert ops == [
        {"type": "filter", "condition": "amount > 1000"},
        {"type": "keyBy", "field": "customer_id"},
        {"type": "window", "spec": "tumbling(60s)",
         "aggregations": {"cnt": "count()", "total": "sum(amount)"}},
        {"type": "filter", "condition": "cnt > 5"},
    ]
    assert agent["outputTopic"] == "fraud-alerts"
    source = next(n for n in nodes if n["type"] == "source")
    assert source["config"]["inputTopic"] == "payments"


def test_where_operators_translated_to_engine_syntax():
    b = compile_sql("SELECT * FROM t WHERE a = 1 AND b <> 2 OR c = 3 INTO o")
    ops, _, _ = _agent_ops(b)
    assert ops[0] == {"type": "filter", "condition": "a == 1 && b != 2 || c == 3"}


def test_comparison_operators_preserved():
    b = compile_sql("SELECT * FROM t WHERE x >= 1 AND y <= 2 AND z != 3 INTO o")
    assert _agent_ops(b)[0][0]["condition"] == "x >= 1 && y <= 2 && z != 3"


def test_projection_emits_map():
    b = compile_sql("SELECT a, b AS bb FROM t WHERE x = 1 INTO out")
    ops, _, _ = _agent_ops(b)
    assert ops == [
        {"type": "filter", "condition": "x == 1"},
        {"type": "map", "fields": {"a": "a", "bb": "b"}},
    ]


def test_default_aggregate_aliases():
    b = compile_sql("SELECT count(*), sum(amount), avg(price) FROM t GROUP BY k WINDOW TUMBLING(1m)")
    win = _agent_ops(b)[0][-1]
    assert win["aggregations"] == {"count": "count()", "sum_amount": "sum(amount)", "avg_price": "avg(price)"}


@pytest.mark.parametrize("clause,spec", [
    ("WINDOW TUMBLING(60s)", "tumbling(60s)"),
    ("WINDOW SLIDING(10m, 1m)", "sliding(10m,1m)"),
    ("WINDOW SESSION(30s)", "session(30s)"),
    ("WINDOW COUNT(100)", "count(100)"),
    ("WINDOW GLOBAL", "global"),
])
def test_window_variants(clause, spec):
    b = compile_sql(f"SELECT count(*) AS c FROM t GROUP BY k {clause} INTO o")
    assert _agent_ops(b)[0][-1]["spec"] == spec


def test_aggregate_without_window_uses_global():
    b = compile_sql("SELECT count(*) AS c FROM t")
    assert _agent_ops(b)[0][-1] == {"type": "window", "spec": "global", "aggregations": {"c": "count()"}}


def test_no_sink_when_into_omitted():
    b = compile_sql("SELECT * FROM t WHERE x = 1")
    _, _, nodes = _agent_ops(b)
    assert [n["type"] for n in nodes] == ["source", "agent"]  # no sink node


@pytest.mark.parametrize("sql", [
    "SELECT * FROM t WINDOW TUMBLING(60s) INTO o",   # window without aggregate
    "SELECT * FROM t HAVING cnt > 5 INTO o",          # having without aggregate
    "SELECT * FROM t INTO o",                          # no operators
    "SELECT count(*) FROM t WINDOW HOPPING(1m) GROUP BY k",  # unknown window
    "DELETE FROM t",                                   # not a SELECT
    "SELECT foo(bar) FROM t GROUP BY k",               # unsupported function
    "SELECT sum(*) FROM t GROUP BY k WINDOW GLOBAL",   # sum(*) invalid
])
def test_invalid_queries_raise(sql):
    with pytest.raises(ValueError):
        compile_sql(sql)


def test_via_streams_resource(monkeypatch):
    from pulse_client import PulseClient

    client = PulseClient(base_url="http://localhost:9090")
    builder = client.streams.from_sql("SELECT count(*) AS c FROM t GROUP BY k WINDOW TUMBLING(1m) INTO o",
                                      name="agg")
    assert builder.build()["name"] == "agg"
