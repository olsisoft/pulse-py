"""B-111 — local pipeline simulation. Runs the StreamBuilder's operator chain
in-process against sample events, no server required.

This is the "test before deploy" surface — the local moral equivalent of Kafka
Streams' ``TopologyTestDriver`` or Flink's ``MiniCluster``, except it runs in
a Python interpreter with zero startup cost and no JVM.

Scope notes (Phase 1):

Supported operators: ``filter``, ``map``, ``flat_map``, ``key_by``,
``window`` (with ``tumbling(...)`` and ``global`` specs).
Unsupported operators (``branch``, ``enrich``, ``enrich_async``, ``cep``,
``broadcast_join``, ``cdc_join``) and window specs (``sliding``, ``session``,
``count``, ``count_sliding``) raise :class:`NotImplementedError` with a
clear message — they need cross-resource state machines / external lookups
that don't fit a pure-Python local executor.

Supported aggregators inside windows: ``count``, ``sum``, ``avg``, ``min``,
``max``, ``collect_list``, ``distinct_count``.

Conditions and map-field expressions are evaluated by a small safe AST walker
that supports comparisons, boolean operators, arithmetic, field references,
and literals — NOT arbitrary function calls (`concat()`, `now()`, etc.).
Production runs use the server's full CEL evaluator; simulate is a
unit-test feedback loop, not a perfect emulator.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from typing import Any


def _expression_unsupported(node: ast.AST) -> NotImplementedError:
    return NotImplementedError(
        f"simulate doesn't support expression node {type(node).__name__!s} "
        f"(simulate evaluates a strict subset of CEL: comparisons, booleans, "
        f"arithmetic, field refs, literals). Deploy server-side for full CEL."
    )


def _eval_expr(expr: str, event: dict[str, Any]) -> Any:
    """Evaluate ``expr`` against ``event`` using a safe AST walker.

    Supports a strict subset of CEL: comparisons (``>`` ``>=`` ``<`` ``<=``
    ``==`` ``!=``), boolean operators (``and`` ``or`` ``not``), arithmetic
    (``+`` ``-`` ``*`` ``/`` ``%``), bare names (interpreted as field
    references on ``event``), and literals (numbers, strings, booleans,
    ``None``).

    Raises :class:`NotImplementedError` for unsupported constructs.
    """
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"simulate: invalid expression {expr!r}: {e}") from e
    return _eval_node(tree.body, event)


def _eval_node(node: ast.AST, event: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return event.get(node.id)
    if isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand, event)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.UAdd):
            return +operand
        raise _expression_unsupported(node.op)
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(_eval_node(v, event) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(_eval_node(v, event) for v in node.values)
        raise _expression_unsupported(node.op)
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, event)
        for op, comparator in zip(node.ops, node.comparators, strict=False):
            right = _eval_node(comparator, event)
            if not _apply_cmp(op, left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, event)
        right = _eval_node(node.right, event)
        return _apply_binop(node.op, left, right)
    raise _expression_unsupported(node)


def _apply_cmp(op: ast.cmpop, left: Any, right: Any) -> bool:
    if isinstance(op, ast.Eq):
        return bool(left == right)
    if isinstance(op, ast.NotEq):
        return bool(left != right)
    if isinstance(op, ast.Gt):
        return bool(left > right)
    if isinstance(op, ast.GtE):
        return bool(left >= right)
    if isinstance(op, ast.Lt):
        return bool(left < right)
    if isinstance(op, ast.LtE):
        return bool(left <= right)
    raise _expression_unsupported(op)


def _apply_binop(op: ast.operator, left: Any, right: Any) -> Any:
    if isinstance(op, ast.Add):
        return left + right
    if isinstance(op, ast.Sub):
        return left - right
    if isinstance(op, ast.Mult):
        return left * right
    if isinstance(op, ast.Div):
        return left / right
    if isinstance(op, ast.Mod):
        return left % right
    if isinstance(op, ast.FloorDiv):
        return left // right
    raise _expression_unsupported(op)


# ---------------------------------------------------------------------------
# Aggregator template parsing — turns "avg(temperature)" into a callable.
# ---------------------------------------------------------------------------


def _parse_agg(template: str) -> tuple[str, str | None]:
    """``"avg(temperature)"`` → ``("avg", "temperature")``; ``"count()"`` → ``("count", None)``."""
    template = template.strip()
    if "(" not in template or not template.endswith(")"):
        raise ValueError(
            f"simulate: invalid aggregator template {template!r}; expected 'fn(field)' form"
        )
    fn = template[: template.index("(")].strip()
    field = template[template.index("(") + 1 : -1].strip()
    return fn, (field or None)


def _new_accumulator(fn: str) -> dict[str, Any]:
    if fn == "count":
        return {"fn": fn, "n": 0}
    if fn in ("sum", "min", "max"):
        return {"fn": fn, "value": None}
    if fn == "avg":
        return {"fn": fn, "sum": 0.0, "n": 0}
    if fn == "collect_list":
        return {"fn": fn, "values": []}
    if fn == "distinct_count":
        return {"fn": fn, "seen": set()}
    raise NotImplementedError(
        f"simulate: aggregator {fn!r} not supported (valid: count, sum, avg, min, max, "
        f"collect_list, distinct_count)"
    )


def _update_accumulator(acc: dict[str, Any], field: str | None, event: dict[str, Any]) -> None:
    fn = acc["fn"]
    if fn == "count":
        acc["n"] += 1
        return
    if field is None:
        raise ValueError(f"simulate: aggregator {fn!r} requires a field name")
    value = event.get(field)
    if value is None:
        return  # skip — matches the server's null-tolerant semantics
    if fn == "sum":
        acc["value"] = (acc["value"] or 0) + value
    elif fn == "min":
        acc["value"] = value if acc["value"] is None else min(acc["value"], value)
    elif fn == "max":
        acc["value"] = value if acc["value"] is None else max(acc["value"], value)
    elif fn == "avg":
        acc["sum"] += value
        acc["n"] += 1
    elif fn == "collect_list":
        acc["values"].append(value)
    elif fn == "distinct_count":
        acc["seen"].add(value)


def _finalise_accumulator(acc: dict[str, Any]) -> Any:
    fn = acc["fn"]
    if fn == "count":
        return acc["n"]
    if fn in ("sum", "min", "max"):
        return acc["value"]
    if fn == "avg":
        return (acc["sum"] / acc["n"]) if acc["n"] else None
    if fn == "collect_list":
        return list(acc["values"])
    if fn == "distinct_count":
        return len(acc["seen"])
    raise AssertionError(f"unreachable: unknown fn {fn!r}")


# ---------------------------------------------------------------------------
# Window-spec parsing — turns "tumbling(60s)" into milliseconds.
# ---------------------------------------------------------------------------


_DURATION_UNITS = {
    "ms": 1,
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}


def _parse_duration_ms(value: str) -> int:
    """``"60s"`` → ``60_000``. Accepts the same units WindowEngine.parseSpec does."""
    value = value.strip()
    # Find the unit suffix (greedy: longest first so 'ms' matches before 's')
    for suffix in ("ms", "s", "m", "h", "d"):
        if value.endswith(suffix):
            num_part = value[: -len(suffix)].strip()
            try:
                n = int(num_part)
            except ValueError as e:
                raise ValueError(
                    f"simulate: invalid duration {value!r}; expected e.g. '60s', '5m', '500ms'"
                ) from e
            if n <= 0:
                raise ValueError(f"simulate: duration must be positive, got {value!r}")
            return n * _DURATION_UNITS[suffix]
    raise ValueError(f"simulate: invalid duration {value!r}; expected unit one of ms/s/m/h/d")


def _parse_window_spec(spec: str) -> tuple[str, dict[str, Any]]:
    """``"tumbling(60s)"`` → ``("tumbling", {"size_ms": 60000})``.

    Only ``tumbling(...)`` and ``global`` are supported in Phase 1.
    """
    spec = spec.strip()
    if spec == "global":
        return ("global", {})
    if not spec.startswith("tumbling(") or not spec.endswith(")"):
        if (
            spec.startswith("sliding(")
            or spec.startswith("session(")
            or spec.startswith("count(")
            or spec.startswith("count_sliding(")
        ):
            kind = spec.split("(", 1)[0]
            raise NotImplementedError(
                f"simulate: window spec {kind!r} not supported yet (Phase 2). "
                f"Supported in Phase 1: tumbling(...), global."
            )
        raise ValueError(
            f"simulate: unknown window spec {spec!r}; expected 'tumbling(d)' or 'global'"
        )
    inner = spec[len("tumbling(") : -1].strip()
    size_ms = _parse_duration_ms(inner)
    return ("tumbling", {"size_ms": size_ms})


# ---------------------------------------------------------------------------
# The simulator itself
# ---------------------------------------------------------------------------


# Sentinel inserted on every event so stateful operators can read it back as
# the key. Falls through transparently when no key_by has run.
_NO_KEY = object()


def simulate(
    operators: list[dict[str, Any]], events: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Run ``operators`` against ``events`` in order. Returns the emissions.

    Args:
        operators: The operator chain as produced by :meth:`StreamBuilder.operators`.
        events: Iterable of input event dicts. Events should carry a ``_ts``
            field (epoch milliseconds) for windowed operators to bucket
            correctly; absent ``_ts`` defaults to 0 (all events in the same
            tumbling bucket).

    Returns:
        A list of dicts representing the events that would be emitted to the
        downstream sink. For windowed operators, each emitted event carries
        ``_window_start`` and ``_window_end`` keys plus the configured
        aggregation fields plus the key field. For non-windowed pipelines,
        emitted events match the upstream operator's output shape.

    Raises:
        NotImplementedError: When the chain uses an operator or window spec
            not supported by the Phase 1 simulator.
    """
    # Single pass: hand each event through the operator chain. For stateful
    # operators (window), accumulate; flush at end (and on time advance).
    state = _ChainState(operators)
    out: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            raise TypeError(f"simulate: each input must be a dict, got {type(event).__name__}")
        out.extend(state.feed(event))
    out.extend(state.flush())
    return out


class _ChainState:
    """Holds per-operator state and routes events through the chain."""

    def __init__(self, operators: list[dict[str, Any]]) -> None:
        self.operators = operators
        # Per-operator state. Currently only `window` operators have state;
        # everything else is stateless. We key by operator index.
        self.windows: dict[int, _WindowState] = {}
        for i, op in enumerate(operators):
            if op["type"] == "window":
                self.windows[i] = _WindowState(op)

    def feed(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        # An event may produce zero (filter), one (most ops), or many
        # (flat_map, window-close-emission) events. We carry a list through
        # the chain.
        current: list[dict[str, Any]] = [{"_key": _NO_KEY, **event}]
        for i, op in enumerate(self.operators):
            current = _apply_op(op, i, current, self.windows)
            if not current:
                return []
        # Strip the internal _key sentinel before exposing to caller
        return [_strip_internals(e) for e in current]

    def flush(self) -> list[dict[str, Any]]:
        """Drain any windows that are still open at end-of-input."""
        if not self.windows:
            return []
        # Find the operator index of each window so we can re-feed its output
        # through the rest of the chain.
        out: list[dict[str, Any]] = []
        for i, window_state in self.windows.items():
            emitted = window_state.flush()
            # Send the window-close events through the downstream operators
            for emit in emitted:
                downstream = self._apply_downstream(emit, start_idx=i + 1)
                out.extend(_strip_internals(e) for e in downstream)
        return out

    def _apply_downstream(self, event: dict[str, Any], start_idx: int) -> list[dict[str, Any]]:
        current: list[dict[str, Any]] = [event]
        for j in range(start_idx, len(self.operators)):
            op = self.operators[j]
            current = _apply_op(op, j, current, self.windows)
            if not current:
                return []
        return current


def _strip_internals(event: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in event.items() if not (k == "_key" and v is _NO_KEY)}


def _apply_op(
    op: dict[str, Any],
    op_index: int,
    events: list[dict[str, Any]],
    windows: dict[int, _WindowState],
) -> list[dict[str, Any]]:
    kind = op.get("type")
    if kind == "filter":
        return [e for e in events if _eval_expr(op["condition"], e)]
    if kind == "map":
        return [_apply_map(op, e) for e in events]
    if kind == "flatMap":
        out: list[dict[str, Any]] = []
        split_field = op["splitField"]
        for e in events:
            arr = e.get(split_field)
            if not isinstance(arr, list):
                continue  # mirrors the server: non-array → drop
            for item in arr:
                new_event = dict(e)
                # Each item replaces the split_field with itself if dict, else
                # surfaces under split_field as the scalar value
                if isinstance(item, dict):
                    new_event.update(item)
                new_event[split_field] = item
                out.append(new_event)
        return out
    if kind == "keyBy":
        field = op["field"]
        return [{**e, "_key": e.get(field)} for e in events]
    if kind == "window":
        return windows[op_index].feed(events)
    if kind in (
        "branch",
        "enrich",
        "enrichAsync",
        "cep",
        "broadcastJoin",
        "cdcJoin",
        "mapLlm",
        "extract",
        "mcpCall",
        "mlPredict",
    ):
        raise NotImplementedError(
            f"simulate: operator {kind!r} needs the live engine (LLM / MCP / ML / "
            f"external lookup) and cannot run in local simulation. Local sim supports "
            f"filter, map, flat_map, key_by, window."
        )
    raise ValueError(f"simulate: unknown operator type {kind!r}")


def _apply_map(op: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    out = dict(event)
    if "fields" in op:
        for out_name, expr in op["fields"].items():
            out[out_name] = _eval_expr(expr, event)
    if "targetType" in op:
        out["type"] = op["targetType"]
    return out


class _WindowState:
    """Per-window operator accumulator. Buckets events, emits on close + flush."""

    def __init__(self, op: dict[str, Any]) -> None:
        self.kind, self.params = _parse_window_spec(op["spec"])
        self.aggregations: dict[str, str] = dict(op.get("aggregations", {}))
        if not self.aggregations:
            raise ValueError("simulate: window operator requires `aggregations` to produce output")
        # Pre-parse each aggregation template
        self.agg_specs: list[tuple[str, str, str | None]] = []
        for name, template in self.aggregations.items():
            fn, field = _parse_agg(template)
            self.agg_specs.append((name, fn, field))
        # State: bucket → key → {agg_name: acc}
        self.buckets: dict[tuple[int, int], dict[Any, dict[str, dict[str, Any]]]] = {}
        # Highest watermark seen so far (max _ts processed)
        self.watermark_ms: int = -1

    def feed(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for e in events:
            ts = int(e.get("_ts", 0))
            key = e.get("_key", _NO_KEY)
            bucket_start, bucket_end = self._bucket_for(ts)
            keymap = self.buckets.setdefault((bucket_start, bucket_end), {})
            accs = keymap.setdefault(key, self._fresh_accs())
            for name, _fn, field in self.agg_specs:
                _update_accumulator(accs[name], field, e)
            # Time advances — close buckets older than the watermark.
            if self.kind == "tumbling":
                if ts > self.watermark_ms:
                    self.watermark_ms = ts
                    out.extend(self._close_buckets_before(ts))
        return out

    def flush(self) -> list[dict[str, Any]]:
        # Emit every remaining bucket, oldest first
        out: list[dict[str, Any]] = []
        for start, end in sorted(self.buckets.keys()):
            out.extend(self._emit_bucket(start, end))
        self.buckets.clear()
        return out

    # ---- helpers ----

    def _bucket_for(self, ts: int) -> tuple[int, int]:
        if self.kind == "global":
            return (0, 0)
        size = self.params["size_ms"]
        start = (ts // size) * size
        return (start, start + size)

    def _fresh_accs(self) -> dict[str, dict[str, Any]]:
        return {name: _new_accumulator(fn) for (name, fn, _field) in self.agg_specs}

    def _close_buckets_before(self, ts: int) -> list[dict[str, Any]]:
        """Emit + drop every bucket whose end is ≤ ts (i.e. fully past)."""
        out: list[dict[str, Any]] = []
        # Iterate over a snapshot of keys because we mutate the dict
        for bucket in sorted(self.buckets.keys()):
            start, end = bucket
            if end <= ts:
                out.extend(self._emit_bucket(start, end))
                del self.buckets[bucket]
        return out

    def _emit_bucket(self, start: int, end: int) -> list[dict[str, Any]]:
        keymap = self.buckets.get((start, end), {})
        out: list[dict[str, Any]] = []
        for key, accs in keymap.items():
            emit: dict[str, Any] = {"_key": key}
            for name, _fn, _field in self.agg_specs:
                emit[name] = _finalise_accumulator(accs[name])
            if self.kind == "tumbling":
                emit["_window_start"] = start
                emit["_window_end"] = end
            out.append(emit)
        return out
