"""A small Streaming-SQL → :class:`~pulse_client.streams.StreamBuilder` compiler
(B-097, first slice).

Compiles a bounded, KSQL/Flink-SQL-flavoured subset to a Pulse streaming
pipeline — client-side, like the rest of the DSL:

    SELECT <select-list>
    FROM   <topic>
    [WHERE  <condition>]
    [GROUP BY <field>]
    [WINDOW <window>]          -- TUMBLING(60s) | SLIDING(10m,1m) | SESSION(30s) | COUNT(100) | GLOBAL
    [HAVING <condition>]       -- filter on the windowed result
    [INTO   <topic>]

Example::

    from pulse_client import compile_sql
    builder = compile_sql(
        '''SELECT count(*) AS cnt, sum(amount) AS total
           FROM payments
           WHERE amount > 1000
           GROUP BY customer_id
           WINDOW TUMBLING(60s)
           HAVING cnt > 5
           INTO fraud-alerts''',
        name="fraud-detector",
    )
    client.streams.deploy(builder)

SELECT supports aggregate functions (``count(*)``, ``sum/avg/min/max(field)``,
``distinct_count(field)``, ``collect_list(field)``) with optional ``AS alias``,
``*`` (pass-through), and plain columns (projected via a ``map`` operator).
``=`` / ``<>`` / ``AND`` / ``OR`` are translated to the engine's expression
syntax (``==`` / ``!=`` / ``&&`` / ``||``).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pulse_client.streams import StreamBuilder, aggs, windows

if TYPE_CHECKING:
    from pulse_client.streams import WindowSpec

__all__ = ["compile_sql"]

_QUERY_RE = re.compile(
    r"""^\s*
        SELECT\s+(?P<select>.+?)
        \s+FROM\s+(?P<from>[\w.\-]+)
        (?:\s+WHERE\s+(?P<where>.+?))?
        (?:\s+GROUP\s+BY\s+(?P<group>[\w.]+))?
        (?:\s+WINDOW\s+(?P<window>\w+\s*\([^)]*\)|GLOBAL))?
        (?:\s+HAVING\s+(?P<having>.+?))?
        (?:\s+INTO\s+(?P<into>[\w.\-]+))?
        \s*;?\s*$""",
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

_AGG_RE = re.compile(
    r"""^(?P<func>count|sum|avg|min|max|distinct_count|collect_list)
        \s*\(\s*(?P<arg>\*|[\w.]+)\s*\)
        (?:\s+AS\s+(?P<alias>[\w]+))?$""",
    re.IGNORECASE | re.VERBOSE,
)

_COL_RE = re.compile(r"^(?P<col>[\w.]+)(?:\s+AS\s+(?P<alias>[\w]+))?$", re.IGNORECASE)

_WINDOW_RE = re.compile(r"^(?P<func>\w+)\s*\((?P<args>.*)\)$", re.DOTALL)


def compile_sql(sql: str, *, name: str | None = None) -> StreamBuilder:
    """Compile a Streaming-SQL string into a :class:`StreamBuilder`."""
    if not sql or not sql.strip():
        raise ValueError("empty SQL")
    m = _QUERY_RE.match(" ".join(sql.split()))
    if not m:
        raise ValueError("could not parse SQL — expected SELECT ... FROM ... [WHERE/GROUP BY/WINDOW/HAVING/INTO]")

    select = m.group("select").strip()
    from_topic = m.group("from")
    where = m.group("where")
    group = m.group("group")
    window = m.group("window")
    having = m.group("having")
    into = m.group("into")

    agg_items: list[tuple[str, str]] = []
    projection: dict[str, str] = {}
    select_all = False

    for raw in _split_top_level(select):
        item = raw.strip()
        if item == "*":
            select_all = True
            continue
        agg = _AGG_RE.match(item)
        if agg:
            agg_items.append(_aggregation(agg))
            continue
        col = _COL_RE.match(item)
        if not col:
            raise ValueError(f"unsupported SELECT item: {item!r}")
        projection[col.group("alias") or col.group("col")] = col.group("col")

    builder = StreamBuilder(name=name).from_topic(from_topic)

    if where:
        builder.filter(_translate(where))
    if group:
        builder.key_by(group)

    if agg_items:
        spec = _window_spec(window) if window else windows.global_()
        builder.window(spec, aggregations=dict(agg_items))
    elif window:
        raise ValueError("WINDOW requires aggregate functions in SELECT")
    elif projection:
        builder.map(fields=projection)

    if having:
        if not agg_items:
            raise ValueError("HAVING requires aggregate functions in SELECT")
        builder.filter(_translate(having))

    if into:
        builder.to_topic(into)

    if not (where or group or agg_items or projection or having):
        raise ValueError(
            "query has no operators — add a WHERE, GROUP BY, an aggregate, or a column projection "
            "(a bare 'SELECT * FROM t' is an empty pipeline)"
        )
    _ = select_all  # `*` alone contributes no operator; documented above.
    return builder


def _aggregation(m: re.Match[str]) -> tuple[str, str]:
    func = m.group("func").lower()
    arg = m.group("arg")
    alias = m.group("alias")
    if func == "count":
        return (alias or "count", aggs.count())
    if arg == "*":
        raise ValueError(f"{func}(*) is not valid — {func} needs a field")
    agg_str = getattr(aggs, func)(arg)
    return (alias or f"{func}_{arg}".replace(".", "_"), agg_str)


def _window_spec(window: str) -> "WindowSpec":
    if window.strip().upper() == "GLOBAL":
        return windows.global_()
    m = _WINDOW_RE.match(window.strip())
    if not m:
        raise ValueError(f"unsupported WINDOW: {window!r}")
    func = m.group("func").lower()
    args = [a.strip() for a in m.group("args").split(",") if a.strip()]
    if func == "tumbling" and len(args) == 1:
        return windows.tumbling(args[0])
    if func == "sliding" and len(args) == 2:
        return windows.sliding(args[0], args[1])
    if func == "session" and len(args) == 1:
        return windows.session(args[0])
    if func == "count" and len(args) == 1:
        return windows.count(int(args[0]))
    raise ValueError(f"unsupported WINDOW: {window!r}")


def _translate(condition: str) -> str:
    """SQL boolean/comparison syntax → the engine's CEL-like expression syntax."""
    c = condition.strip()
    c = c.replace("<>", "!=")
    c = re.sub(r"\bAND\b", "&&", c, flags=re.IGNORECASE)
    c = re.sub(r"\bOR\b", "||", c, flags=re.IGNORECASE)
    # standalone '=' (not part of ==, >=, <=, !=) → ==
    c = re.sub(r"(?<![<>=!])=(?!=)", "==", c)
    return c.strip()


def _split_top_level(s: str, sep: str = ",") -> list[str]:
    """Split on ``sep`` at paren-depth 0 (so ``sum(a), avg(b)`` splits cleanly)."""
    out: list[str] = []
    depth = 0
    cur: list[str] = []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == sep and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        out.append("".join(cur))
    return [p for p in (x.strip() for x in out) if p]
