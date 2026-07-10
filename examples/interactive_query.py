"""Use case 3 — Interactive Query over mesh-materialized agent state.

Reads the live, queryable state an agent maintains on the mesh: a summary, a
point lookup by key, a bounded scan, and a filtered/grouped query. This is the
"query the stream's current state without re-deriving it" path.

Run against your Pulse:  PULSE_URL=http://localhost:9090 python interactive_query.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient

AGENT_ID = "merchant-rollups-1m"


def main() -> None:
    with PulseClient(os.environ.get("PULSE_URL", "http://localhost:9090"),
                     token=os.environ.get("PULSE_TOKEN")) as client:
        print("Summary:", client.iq.summary(AGENT_ID))

        # Point lookup for one merchant's current rollup.
        print("merchant-7:", client.iq.get(AGENT_ID, key="merchant-7"))

        # Bounded scan of the keyspace.
        scan = client.iq.scan(AGENT_ID, limit=10)
        print("Scan (first 10):", scan)

        # Filtered + grouped query.
        result = client.iq.query(
            AGENT_ID,
            filter="total_amount > 1000",
            group_by="region",
            limit=20,
        )
        print("High-volume merchants by region:", result)


if __name__ == "__main__":
    main()
