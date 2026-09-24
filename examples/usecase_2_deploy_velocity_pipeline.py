"""Use-case ladder 2/5 — deploy the card-velocity fraud pipeline.

What it shows:
  Build the "card-velocity-60s" streaming pipeline with the streams DSL — a
  60s tumbling-window velocity rule that flags any card with more than 5
  authorizations in the window — print the compiled spec offline, then deploy
  it. As a Python-only bonus it also runs the builder's local ``.simulate()``
  test-driver over ~7 synthetic authorizations to show which events survive the
  ``txCount > 5`` filter without ever hitting the server.

Domain: card-payments fraud monitoring (topic ``card-authorizations``,
events ``{cardId, merchantId, amount, ts}``). Fraud rule: > 5 authorizations on
one card in a 60s tumbling window.

Prerequisites:
  * A reachable Pulse at ``PULSE_URL`` (default ``http://localhost:9090``).
  * Auth: set ``PULSE_TOKEN``, or ``PULSE_USER`` + ``PULSE_PASSWORD`` (deploy is authenticated).
  * The offline compile + ``.simulate()`` demo need no server at all.

Run:
  python examples/usecase_2_deploy_velocity_pipeline.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient
from pulse_client.streams import StreamBuilder, aggs, windows


def build_pipeline() -> StreamBuilder:
    """Pure declaration of the velocity rule (no network) — easy to inspect/test."""
    return (
        StreamBuilder(
            "card-velocity-60s",
            description="Flag cards with > 5 authorizations in a 60s tumbling window",
        )
        .from_topic("card-authorizations")
        .filter("amount > 0")
        .key_by("cardId")
        .window(
            windows.tumbling("60s"),
            aggregations={
                "txCount": aggs.count(),
                "totalAmount": aggs.sum("amount"),
                "maxAmount": aggs.max("amount"),
            },
        )
        .filter("txCount > 5")
        .to_topic("fraud-alerts", sink_channel="dashboard")
    )


def local_simulation(builder: StreamBuilder) -> None:
    """Python-only local test-driver — run the chain in-process, no deploy.

    Feeds ~7 synthetic authorizations for "card-007" inside one 60s window and
    prints which window emissions survive past the ``txCount > 5`` filter. This
    is the SDK's ``.simulate()`` feedback loop (unique to the Python client) —
    the moral equivalent of Kafka Streams' TopologyTestDriver.
    """
    # 7 charges on card-007 within the same 60s tumbling bucket (_ts in ms),
    # plus one trailing event to advance time and close the window.
    events = [
        {"cardId": "card-007", "merchantId": "m-1", "amount": 12.0, "_ts": 1_000},
        {"cardId": "card-007", "merchantId": "m-2", "amount": 8.5, "_ts": 5_000},
        {"cardId": "card-007", "merchantId": "m-3", "amount": 99.0, "_ts": 11_000},
        {"cardId": "card-007", "merchantId": "m-4", "amount": 3.0, "_ts": 17_000},
        {"cardId": "card-007", "merchantId": "m-5", "amount": 45.0, "_ts": 23_000},
        {"cardId": "card-007", "merchantId": "m-6", "amount": 7.0, "_ts": 29_000},
        {"cardId": "card-007", "merchantId": "m-7", "amount": 21.0, "_ts": 35_000},
        # Trailing event in the NEXT window advances the watermark → closes the
        # first bucket so its aggregate is emitted.
        {"cardId": "card-007", "merchantId": "m-8", "amount": 1.0, "_ts": 70_000},
    ]
    print("\n[python-only] Local .simulate() over 7 synthetic 'card-007' authorizations:")
    survivors = builder.simulate(events)
    if not survivors:
        print("  no window emission crossed txCount > 5 (no fraud flagged)")
    for emit in survivors:
        print(
            f"  FRAUD: card={emit.get('_key')} "
            f"txCount={emit.get('txCount')} totalAmount={emit.get('totalAmount')} "
            f"maxAmount={emit.get('maxAmount')}"
        )


def main() -> None:
    builder = build_pipeline()

    # Compile offline (no network) and print the spec the server would receive.
    url = os.environ.get("PULSE_URL", "http://localhost:9090")
    token = os.environ.get("PULSE_TOKEN")
    with PulseClient(url, token=token) as client:
        # A pre-minted PULSE_TOKEN authenticates directly; otherwise log in with
        # PULSE_USER + PULSE_PASSWORD (deploying a pipeline is an authenticated call).
        if not client.token:
            user, password = os.environ.get("PULSE_USER"), os.environ.get("PULSE_PASSWORD")
            if user and password:
                client.auth.login(user, password)

        print("Compiled pipeline:", client.streams.compile(builder))

        # Bonus: the Python-only local test-driver — no deploy required.
        local_simulation(builder)

        # Deploy to the cluster.
        print("\nDeploying card-velocity-60s …")
        deployed = client.streams.deploy(builder)
        print("Deployed:", deployed)


if __name__ == "__main__":
    main()
