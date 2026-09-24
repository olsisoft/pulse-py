"""Use case 2 — consume live mesh events and replay state history.

Two mesh-backed event operations:
  * tail the live event stream (SSE) — bounded here to the first few events;
  * replay the committed state-change history for one key (time-travel, B-113).

Run against your Pulse:  PULSE_URL=http://localhost:9090 python events_live_and_replay.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient


def main() -> None:
    with PulseClient(os.environ.get("PULSE_URL", "http://localhost:9090"),
                     token=os.environ.get("PULSE_TOKEN")) as client:
        # Replay the last hour of committed state changes for one account key.
        changes = client.events.replay(
            affecting_state="balance",
            key="acct-42",
            from_="-1h",
            to="now",
            limit=50,
        )
        print(f"Replayed {len(changes)} state change(s) for acct-42")
        for change in changes[:5]:
            print("  ", change)

        # Tail the live event stream — stop after the first 10 events.
        print("Tailing live events (first 10)…")
        for i, event in enumerate(client.events.stream(timeout=30.0)):
            print("  event:", event)
            if i >= 9:
                break


if __name__ == "__main__":
    main()
