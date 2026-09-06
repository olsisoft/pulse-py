"""Use case 5 — sink a mesh stream to an external connector.

Discovers the available sink connectors, then declares a stream that delivers
the per-merchant rollups to a ClickHouse warehouse via a connector sink. The
connector runs through the bridge against the cluster.

Run against your Pulse:  PULSE_URL=http://localhost:9090 python stream_to_connector.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient
from pulse_client.streams import StreamBuilder


def build_pipeline() -> StreamBuilder:
    return (
        StreamBuilder("rollups-to-warehouse", description="Ship 1m rollups to ClickHouse")
        .from_topic("merchant-rollups-1m")
        .filter("total_amount > 0")
        .to_connector(
            "clickhouse",
            {"url": "http://clickhouse:8123", "table": "merchant_rollups"},
        )
    )


def main() -> None:
    with PulseClient(os.environ.get("PULSE_URL", "http://localhost:9090"),
                     token=os.environ.get("PULSE_TOKEN")) as client:
        sinks = client.connectors.sinks()
        print(f"{len(sinks)} sink connector(s) available")

        builder = build_pipeline()
        print("Pipeline spec:", builder.build())
        print("Deployed:", client.streams.deploy(builder))


if __name__ == "__main__":
    main()
