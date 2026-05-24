"""B-107 — Kafka-Streams-like declarative DSL that compiles to a Pulse pipeline.

The DSL is **server-side execution, client-side declaration**: the operator
chain is built in Python, compiled to the JSON pipeline shape that the Pulse
server's ``StreamingOperatorValidator`` accepts, and POSTed to
``/api/pulse/pipelines``. Stream processing then runs on the Pulse engine
(3.6 M evt/s native throughput), not in the client process.

This is the opposite of Kafka Streams (which runs in the caller's JVM). The
trade-off: you can't do microsecond client-side compute, but you get
infinite-scale stateful streaming, durable replicated state queryable via
B-106 IQ, and the same DSL works from any of the 5 Pulse SDKs.

Quick start::

    from pulse_client import PulseClient
    from pulse_client.streams import StreamBuilder, windows, aggs

    builder = (
        StreamBuilder(name="iot-temperature-aggregator")
        .from_topic("sensor-readings", source_engine="mqtt")
        .key_by("deviceId")
        .window(
            windows.tumbling("60s"),
            aggregations={"avgTemp": aggs.avg("temperature")},
        )
        .filter("avgTemp > 75")
        .to_topic("sensor-minute-averages", sink_channel="email")
    )

    with PulseClient("http://localhost:9090", token="ey...") as client:
        deployed = client.streams.deploy(builder)
        print(deployed["id"])

Supported operators (mirror the 11 validated by
``com.streamflow.pulse.streaming.StreamingOperatorValidator``):
``filter``, ``map``, ``flat_map``, ``key_by``, ``window``, ``branch``,
``enrich``, ``enrich_async``, ``cep``, ``broadcast_join``, ``cdc_join``.

Supported window specs: ``tumbling``, ``sliding``, ``session``, ``global``,
``count``, ``count_sliding``.

Supported aggregators: ``count``, ``sum``, ``avg``, ``min``, ``max``,
``collect_list``, ``distinct_count``.

Conditions and field-expressions are passed as **strings** (the Pulse
streaming runtime parses them server-side). Lambdas / closures are NOT
supported because they cannot be serialised to JSON.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pulse_client.client import PulseClient


# ---------------------------------------------------------------------------
# Window specs — typed wrappers that compile to the string form the server
# parser (``WindowEngine.parseSpec``) accepts.
# ---------------------------------------------------------------------------


class WindowSpec:
    """A window specification. Compiled to the string form the server expects.

    Construct via the :data:`windows` helpers — never instantiate directly
    unless you've already validated the raw string against
    ``WindowEngine.parseSpec``.
    """

    __slots__ = ("spec",)

    def __init__(self, spec: str) -> None:
        if not spec or not spec.strip():
            raise ValueError("WindowSpec requires a non-empty spec string")
        self.spec = spec

    def __repr__(self) -> str:
        return f"WindowSpec({self.spec!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, WindowSpec) and self.spec == other.spec

    def __hash__(self) -> int:
        return hash(self.spec)


class _Windows:
    """Factory for the 6 window kinds the server understands."""

    @staticmethod
    def tumbling(size: str) -> WindowSpec:
        """Non-overlapping fixed windows: ``tumbling("60s")``, ``tumbling("5m")``."""
        _require_nonblank("size", size)
        return WindowSpec(f"tumbling({size})")

    @staticmethod
    def sliding(size: str, slide: str) -> WindowSpec:
        """Overlapping windows: ``sliding("10m", "1m")`` = size, slide step."""
        _require_nonblank("size", size)
        _require_nonblank("slide", slide)
        return WindowSpec(f"sliding({size},{slide})")

    @staticmethod
    def session(timeout: str) -> WindowSpec:
        """Inactivity-bounded windows: ``session("30s")``."""
        _require_nonblank("timeout", timeout)
        return WindowSpec(f"session({timeout})")

    @staticmethod
    def global_() -> WindowSpec:
        """Single unbounded window. Use for global aggregates."""
        return WindowSpec("global")

    @staticmethod
    def count(n: int) -> WindowSpec:
        """Event-count tumbling: closes after ``n`` events. ``count(100)``."""
        if n <= 0:
            raise ValueError(f"count window size must be positive, got {n}")
        return WindowSpec(f"count({n})")

    @staticmethod
    def count_sliding(size: int, slide: int) -> WindowSpec:
        """Event-count sliding: ``count_sliding(100, 10)`` = window, slide."""
        if size <= 0 or slide <= 0:
            raise ValueError(f"count_sliding requires positive size and slide, got {size}, {slide}")
        return WindowSpec(f"count_sliding({size},{slide})")


windows = _Windows()
"""Singleton namespace for window-spec factories (``windows.tumbling("60s")``)."""


# ---------------------------------------------------------------------------
# Aggregators — string-template builders that compile to the form the
# server parser (``Aggregators.parse``) accepts inside ``window.aggregations``.
# ---------------------------------------------------------------------------


class _Aggs:
    """Factory for the 7 aggregation functions the server understands.

    Each returns the string template the server parses (``"avg(temperature)"``).
    Use inside a window's ``aggregations={}`` map::

        windows.tumbling("60s"), aggregations={"avgTemp": aggs.avg("temperature")}
    """

    @staticmethod
    def count() -> str:
        """Event count — no field required."""
        return "count()"

    @staticmethod
    def sum(field: str) -> str:
        """Sum of a numeric field: ``aggs.sum("amount")``."""
        _require_nonblank("field", field)
        return f"sum({field})"

    @staticmethod
    def avg(field: str) -> str:
        """Average of a numeric field: ``aggs.avg("price")``."""
        _require_nonblank("field", field)
        return f"avg({field})"

    @staticmethod
    def min(field: str) -> str:
        """Minimum value of a numeric field."""
        _require_nonblank("field", field)
        return f"min({field})"

    @staticmethod
    def max(field: str) -> str:
        """Maximum value of a numeric field."""
        _require_nonblank("field", field)
        return f"max({field})"

    @staticmethod
    def collect_list(field: str) -> str:
        """Collect every value of ``field`` into a list."""
        _require_nonblank("field", field)
        return f"collect_list({field})"

    @staticmethod
    def distinct_count(field: str) -> str:
        """Cardinality of distinct values of ``field``."""
        _require_nonblank("field", field)
        return f"distinct_count({field})"


aggs = _Aggs()
"""Singleton namespace for aggregator factories (``aggs.avg("temperature")``)."""


# ---------------------------------------------------------------------------
# StreamBuilder — fluent operator-chain → pipeline-JSON compiler.
# ---------------------------------------------------------------------------


class StreamBuilder:
    """Fluent builder for a Pulse streaming pipeline.

    The chain order matters: operators are emitted in the order the methods
    are called. Each method returns ``self`` so calls chain naturally::

        builder = (
            StreamBuilder(name="fraud-detector")
            .from_topic("payments")
            .filter("amount > 1000")
            .key_by("customer_id")
            .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
            .filter("cnt > 5")
            .to_topic("fraud-alerts")
        )

    Compile via :meth:`build` (returns the dict), or
    deploy via :meth:`PulseClient.streams.deploy` (POSTs to the server).

    Args:
        name: Pipeline name. Required at build time — pass it at construction
            or at ``.build(name=...)``.
        description: Optional pipeline description.
    """

    def __init__(self, name: str | None = None, *, description: str | None = None) -> None:
        self._name: str | None = name
        self._description: str | None = description
        self._input_topic: str | None = None
        self._source_engine: str | None = None
        self._source_config: dict[str, Any] = {}
        self._source_label: str | None = None
        self._output_topic: str | None = None
        self._sink_channel: str | None = None
        self._sink_config: dict[str, Any] = {}
        self._sink_label: str | None = None
        self._agent_label: str | None = None
        self._operators: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------

    def from_topic(
        self,
        topic: str,
        *,
        source_engine: str = "kafka",
        source_config: dict[str, Any] | None = None,
        label: str | None = None,
    ) -> StreamBuilder:
        """Sets the input topic + the source node config.

        Args:
            topic: The topic name the streaming agent will consume from.
            source_engine: The source subType (``"kafka"``, ``"mqtt"``,
                ``"webhook"``, …). Defaults to ``"kafka"``.
            source_config: Extra config merged into the source node's
                ``config`` dict (e.g. ``{"bootstrap_servers": "..."}``).
            label: Display label for the source node.
        """
        _require_nonblank("topic", topic)
        self._input_topic = topic
        self._source_engine = source_engine
        self._source_config = dict(source_config or {})
        self._source_label = label
        return self

    # ------------------------------------------------------------------
    # Operators — each appends one entry to self._operators
    # ------------------------------------------------------------------

    def filter(self, condition: str) -> StreamBuilder:
        """Filter operator. ``condition`` is a CEL-like expression string.

        Example: ``.filter("amount > 1000")``.
        """
        _require_nonblank("condition", condition)
        self._operators.append({"type": "filter", "condition": condition})
        return self

    def map(
        self,
        *,
        fields: dict[str, str] | None = None,
        target_type: str | None = None,
    ) -> StreamBuilder:
        """Map operator. Produces an output event from the input.

        At least one of ``fields`` or ``target_type`` is required (server
        rejects a map that does nothing).

        Args:
            fields: Mapping of output-field-name → source-expression string.
                Example: ``{"alert_msg": "concat(deviceId, ': hot!')"}``.
            target_type: Tag the output event with a ``type`` field.
        """
        if fields is None and target_type is None:
            raise ValueError("map operator does nothing — provide `fields` or `target_type`")
        op: dict[str, Any] = {"type": "map"}
        if fields is not None:
            op["fields"] = dict(fields)
        if target_type is not None:
            op["targetType"] = target_type
        self._operators.append(op)
        return self

    def flat_map(self, split_field: str) -> StreamBuilder:
        """Flat-map: explode an array-valued field into one event per element.

        Example: ``.flat_map("items")`` — one event per item in ``items[]``.
        """
        _require_nonblank("split_field", split_field)
        self._operators.append({"type": "flatMap", "splitField": split_field})
        return self

    def key_by(self, field: str) -> StreamBuilder:
        """Group the stream by a top-level field value.

        Required before stateful operators (window, aggregate). Equivalent
        to Kafka Streams' ``groupBy(key_selector)``.
        """
        _require_nonblank("field", field)
        self._operators.append({"type": "keyBy", "field": field})
        return self

    def window(
        self,
        spec: WindowSpec | str,
        *,
        aggregations: dict[str, str] | None = None,
        output_topic: str | None = None,
        trigger: Any = None,
    ) -> StreamBuilder:
        """Window operator. Aggregates events inside a window.

        Args:
            spec: A :class:`WindowSpec` built via :data:`windows`, or the
                raw string form (``"tumbling(60s)"``).
            aggregations: Map of output-field → aggregator-string. Use
                :data:`aggs` to build the right-hand side
                (``{"cnt": aggs.count()}``).
            output_topic: Optional override for where window results go.
            trigger: Server-side trigger config (passed through opaquely).
        """
        spec_str = spec.spec if isinstance(spec, WindowSpec) else spec
        _require_nonblank("spec", spec_str)
        op: dict[str, Any] = {"type": "window", "spec": spec_str}
        if aggregations is not None:
            op["aggregations"] = dict(aggregations)
        if output_topic is not None:
            op["outputTopic"] = output_topic
        if trigger is not None:
            op["trigger"] = trigger
        self._operators.append(op)
        return self

    def branch(self, branches: list[dict[str, str]]) -> StreamBuilder:
        """Branch operator: route events to different topics by condition.

        Args:
            branches: List of ``{"condition": "...", "topic": "..."}`` dicts.
                Each event is sent to the FIRST branch whose condition matches.
        """
        if not branches:
            raise ValueError("branch operator requires at least one branch")
        normalised: list[dict[str, str]] = []
        for i, b in enumerate(branches):
            cond = b.get("condition")
            topic = b.get("topic")
            if not isinstance(cond, str) or not cond.strip():
                raise ValueError(f"branch[{i}] requires a non-empty 'condition'")
            if not isinstance(topic, str) or not topic.strip():
                raise ValueError(f"branch[{i}] requires a non-empty 'topic'")
            normalised.append({"condition": cond, "topic": topic})
        self._operators.append({"type": "branch", "branches": normalised})
        return self

    def enrich(self, *, lookup_topic: str, key_field: str) -> StreamBuilder:
        """Synchronous enrichment: join the stream against a state-store topic.

        Args:
            lookup_topic: Topic backing the state store to join against.
            key_field: Field on the input event that maps to the state key.
        """
        _require_nonblank("lookup_topic", lookup_topic)
        _require_nonblank("key_field", key_field)
        self._operators.append(
            {
                "type": "enrich",
                "lookupTopic": lookup_topic,
                "keyField": key_field,
            }
        )
        return self

    def enrich_async(
        self,
        *,
        url: str,
        parallelism: int | None = None,
        queue_size: int | None = None,
        timeout_ms: int | None = None,
        max_retries: int | None = None,
        retry_backoff_ms: int | None = None,
        ordering: str | None = None,
        on_failure: str | None = None,
    ) -> StreamBuilder:
        """Asynchronous HTTP enrichment.

        ``url`` supports ``{field}`` placeholders that get substituted from
        the event payload before the call. ``ordering`` ∈
        {``"PRESERVE_INPUT"``, ``"UNORDERED"``}; ``on_failure`` ∈
        {``"EMIT_ERROR"``, ``"DROP"``, ``"PASS_THROUGH"``}.
        """
        _require_nonblank("url", url)
        if ordering is not None and ordering not in ("PRESERVE_INPUT", "UNORDERED"):
            raise ValueError(f"ordering must be PRESERVE_INPUT or UNORDERED, got {ordering!r}")
        if on_failure is not None and on_failure not in (
            "EMIT_ERROR",
            "DROP",
            "PASS_THROUGH",
        ):
            raise ValueError(
                f"on_failure must be EMIT_ERROR, DROP, or PASS_THROUGH, got {on_failure!r}"
            )
        op: dict[str, Any] = {"type": "enrichAsync", "url": url}
        if parallelism is not None:
            op["parallelism"] = parallelism
        if queue_size is not None:
            op["queueSize"] = queue_size
        if timeout_ms is not None:
            op["timeoutMs"] = timeout_ms
        if max_retries is not None:
            op["maxRetries"] = max_retries
        if retry_backoff_ms is not None:
            op["retryBackoffMs"] = retry_backoff_ms
        if ordering is not None:
            op["ordering"] = ordering
        if on_failure is not None:
            op["onFailure"] = on_failure
        self._operators.append(op)
        return self

    def cep(
        self,
        sequence: list[dict[str, Any]],
        *,
        within: str | None = None,
        name: str | None = None,
    ) -> StreamBuilder:
        """Complex Event Processing: match a sequence of conditions.

        ``sequence`` is a list of step dicts; each step supports ``match``
        (condition string), ``follow`` (``followedBy`` / ``followedByAny``
        / ``notFollowedBy``), ``times`` (repeat count), ``within`` (per-step
        window), ``name`` (step id).
        """
        if not sequence:
            raise ValueError("cep operator requires a non-empty sequence")
        op: dict[str, Any] = {"type": "cep", "sequence": list(sequence)}
        if within is not None:
            op["within"] = within
        if name is not None:
            op["name"] = name
        self._operators.append(op)
        return self

    def broadcast_join(
        self,
        *,
        join_key_field: str,
        streaming_topic: str | None = None,
        name: str | None = None,
        max_bytes: int | None = None,
        refresh_mode: str | None = None,
        interval_millis: int | None = None,
    ) -> StreamBuilder:
        """Broadcast join: enrich the stream against a fully-replicated table.

        ``refresh_mode`` ∈ {``"cdc"``, ``"periodic"``, ``"explicit"``}.
        """
        _require_nonblank("join_key_field", join_key_field)
        if refresh_mode is not None and refresh_mode not in (
            "cdc",
            "periodic",
            "explicit",
        ):
            raise ValueError(
                f"refresh_mode must be cdc, periodic, or explicit, got {refresh_mode!r}"
            )
        op: dict[str, Any] = {"type": "broadcastJoin", "joinKeyField": join_key_field}
        if streaming_topic is not None:
            op["streamingTopic"] = streaming_topic
        if name is not None:
            op["name"] = name
        if max_bytes is not None:
            op["maxBytes"] = max_bytes
        if refresh_mode is not None:
            op["refreshMode"] = refresh_mode
        if interval_millis is not None:
            op["intervalMillis"] = interval_millis
        self._operators.append(op)
        return self

    def cdc_join(
        self,
        *,
        source: str,
        join_key: str | None = None,
        table: str | None = None,
        state_backend: str | None = None,
    ) -> StreamBuilder:
        """CDC join: stream-table join against a CDC-fed state table."""
        _require_nonblank("source", source)
        op: dict[str, Any] = {"type": "cdcJoin", "source": source}
        if join_key is not None:
            op["joinKey"] = join_key
        if table is not None:
            op["table"] = table
        if state_backend is not None:
            op["stateBackend"] = state_backend
        self._operators.append(op)
        return self

    # ------------------------------------------------------------------
    # Sink
    # ------------------------------------------------------------------

    def to_topic(
        self,
        topic: str,
        *,
        sink_channel: str | None = None,
        sink_config: dict[str, Any] | None = None,
        label: str | None = None,
    ) -> StreamBuilder:
        """Sets the output topic + optional sink node config.

        If ``sink_channel`` is None, no sink node is emitted — the stream
        terminates at ``output_topic`` and downstream consumers can subscribe
        themselves.

        Args:
            topic: Output topic for the agent's emissions.
            sink_channel: Sink subType (``"kafka"``, ``"telegram"``,
                ``"email"``, ``"slack"``, …). Omit to skip the sink node.
            sink_config: Extra config merged into the sink node's config.
            label: Display label for the sink node.
        """
        _require_nonblank("topic", topic)
        self._output_topic = topic
        self._sink_channel = sink_channel
        self._sink_config = dict(sink_config or {})
        self._sink_label = label
        return self

    def to_state(self) -> StreamBuilder:
        """Terminate the stream in the agent's state store.

        State stays queryable via the B-106 Interactive Queries surface
        (``client.iq.get(agent_id, key)``). No sink node is emitted and no
        output topic is set — downstream callers query the agent directly.
        """
        self._output_topic = None
        self._sink_channel = None
        self._sink_config = {}
        self._sink_label = None
        return self

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def named(self, name: str) -> StreamBuilder:
        """Sets / overrides the pipeline name."""
        _require_nonblank("name", name)
        self._name = name
        return self

    def described_as(self, description: str) -> StreamBuilder:
        """Sets the pipeline description."""
        self._description = description
        return self

    def with_agent_label(self, label: str) -> StreamBuilder:
        """Sets the display label for the streaming agent node."""
        _require_nonblank("label", label)
        self._agent_label = label
        return self

    # ------------------------------------------------------------------
    # Compilation
    # ------------------------------------------------------------------

    def operators(self) -> list[dict[str, Any]]:
        """Returns a copy of the recorded operator chain (read-only view)."""
        return [dict(op) for op in self._operators]

    def simulate(self, events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """B-111 — run the operator chain locally against ``events``.

        Returns the events that would be emitted to the downstream sink, in
        order. Mirrors :meth:`build` semantically but executes in-process
        with no server deploy — the "test before deploy" feedback loop.

        Events should carry a ``_ts`` field (epoch milliseconds) for
        windowed operators to bucket correctly; absent ``_ts`` defaults to
        ``0`` (all events fall into the same tumbling bucket).

        Example::

            results = (
                StreamBuilder()
                .from_topic("payments")
                .filter("amount > 1000")
                .key_by("customer_id")
                .window(windows.tumbling("60s"), aggregations={"cnt": aggs.count()})
                .filter("cnt > 5")
            ).simulate([
                {"customer_id": "c1", "amount": 5000, "_ts": 0},
                # ... 5 more events for c1 within the 60s window ...
                {"customer_id": "c1", "amount": 5000, "_ts": 70_000},  # closes window
            ])

            assert len(results) == 1
            assert results[0]["cnt"] >= 6

        Scope (Phase 1):
          - Operators: ``filter``, ``map``, ``flat_map``, ``key_by``, ``window``.
          - Window specs: ``tumbling(...)``, ``global``.
          - Aggregators: ``count``, ``sum``, ``avg``, ``min``, ``max``,
            ``collect_list``, ``distinct_count``.
          - Expressions: comparisons, booleans, arithmetic, field refs, literals.
            NO function calls (use the server for full CEL evaluation).

        Unsupported operators (``branch``, ``enrich``, ``enrich_async``, ``cep``,
        ``broadcast_join``, ``cdc_join``) and window specs (``sliding``,
        ``session``, ``count``, ``count_sliding``) raise
        :class:`NotImplementedError` with a clear message.

        Raises:
            ValueError: If no operators have been chained yet.
            NotImplementedError: If the chain uses unsupported operators.
        """
        if not self._operators:
            raise ValueError(
                "no operators — chain at least one of .filter/.map/.key_by/... before .simulate()"
            )
        # Lazy import to keep the streams module free of simulator code at
        # the public surface.
        from pulse_client._simulator import simulate as _simulate

        return _simulate(self._operators, events)

    def build(self, *, name: str | None = None) -> dict[str, Any]:
        """Compile the chain into a Pulse pipeline dict ready for POST.

        Args:
            name: Override the pipeline name. Falls back to the value set at
                construction or via :meth:`named`.

        Raises:
            ValueError: If required pieces (name, source topic, operators)
                are missing.

        Returns:
            A JSON-serialisable dict matching the CreatePipelineRequest schema.
            Pass it to :meth:`PulseClient.pipelines.create` or
            :meth:`PulseClient.streams.deploy`.
        """
        pipeline_name = name or self._name
        if not pipeline_name:
            raise ValueError(
                "pipeline name required — pass to StreamBuilder(name=...) or .build(name=...)"
            )
        if not self._input_topic:
            raise ValueError("no source — call .from_topic(...) before .build()")
        if not self._operators:
            raise ValueError(
                "no operators — chain at least one of .filter/.map/.key_by/... before .build()"
            )

        nodes: list[dict[str, Any]] = []

        # Source node
        source_config = {
            "engine": self._source_engine,
            "inputTopic": self._input_topic,
            **self._source_config,
        }
        nodes.append(
            {
                "type": "source",
                "label": self._source_label or f"{self._source_engine} source",
                "config": source_config,
            }
        )

        # Agent node (the streaming processor)
        agent_config: dict[str, Any] = {
            "engine": "streaming",
            "inputTopic": self._input_topic,
            "operators": [dict(op) for op in self._operators],
        }
        if self._output_topic is not None:
            agent_config["outputTopic"] = self._output_topic
        nodes.append(
            {
                "type": "agent",
                "label": self._agent_label or pipeline_name,
                "config": agent_config,
            }
        )

        # Sink node — only when both an output topic AND a sink channel are set
        if self._output_topic is not None and self._sink_channel is not None:
            sink_config = {
                "channel": self._sink_channel,
                "inputTopic": self._output_topic,
                **self._sink_config,
            }
            nodes.append(
                {
                    "type": "sink",
                    "label": self._sink_label or f"{self._sink_channel} sink",
                    "config": sink_config,
                }
            )

        pipeline: dict[str, Any] = {"name": pipeline_name, "nodes": nodes}
        if self._description is not None:
            pipeline["description"] = self._description
        return pipeline


# ---------------------------------------------------------------------------
# StreamsResource — the client.streams accessor.
# ---------------------------------------------------------------------------


class StreamsResource:
    """``client.streams`` — compile + deploy :class:`StreamBuilder` pipelines.

    Just sugar over :meth:`PulseClient.pipelines.create` — the compilation
    happens client-side, the deploy is the same POST. Useful when you want
    to keep DSL plumbing out of business code::

        deployed = client.streams.deploy(builder)
    """

    def __init__(self, client: PulseClient) -> None:
        self._client = client

    def compile(self, builder: StreamBuilder, *, name: str | None = None) -> dict[str, Any]:
        """Compile the builder to a pipeline dict WITHOUT deploying."""
        return builder.build(name=name)

    def deploy(self, builder: StreamBuilder, *, name: str | None = None) -> dict[str, Any]:
        """Compile + POST to ``/api/pulse/pipelines``. Returns the server response."""
        definition = builder.build(name=name)
        return self._client.pipelines.create(definition)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_nonblank(name: str, value: str | None) -> None:
    if value is None or not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string, got {value!r}")
