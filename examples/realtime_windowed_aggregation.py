"""Use case 1 — real-time windowed aggregation on the event mesh.

Declares a streaming pipeline that rolls up payment transactions per merchant
in 1-minute tumbling windows and writes the rollups back to a mesh topic. When
deployed to a Pulse instance attached to a StreamFlow cluster, this runs on the
mesh (sharded, replicated) — the SDK only declares the flow.

Run against your Pulse:  PULSE_URL=http://localhost:9090 python realtime_windowed_aggregation.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient
from pulse_client.streams import StreamBuilder, aggs, windows


def build_pipeline() -> StreamBuilder:
    """Pure: the flow declaration (no network) — handy to unit-test / inspect."""
    return (
        StreamBuilder("merchant-rollups-1m", description="Per-merchant 1m transaction rollups")
        .from_topic("transactions")
        .filter("amount > 0")
        .key_by("merchant_id")
        .window(
            windows.tumbling("1m"),
            aggregations={
                "txn_count": aggs.count(),
                "total_amount": aggs.sum("amount"),
                "avg_amount": aggs.avg("amount"),
                "max_amount": aggs.max("amount"),
            },
        )
        .to_topic("merchant-rollups-1m")
    )


def main() -> None:
    builder = build_pipeline()
    print("Pipeline spec:", builder.build())

    with PulseClient(os.environ.get("PULSE_URL", "http://localhost:9090"),
                     token=os.environ.get("PULSE_TOKEN")) as client:
        deployed = client.streams.deploy(builder)
        print("Deployed:", deployed)


if __name__ == "__main__":
    main()
