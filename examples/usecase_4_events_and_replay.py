"""Use-case ladder 4/5 — tail live fraud alerts and replay a card's history.

What it shows:
  Subscribe to the live event stream (SSE) and print a handful of fraud-alert
  events (bounded by count + a short read timeout so the demo always returns),
  then replay one card's committed state-change history (time-travel, B-113) and
  print it.

Domain: card-payments fraud monitoring. The "card-velocity-60s" agent emits to
``fraud-alerts`` and keeps per-``cardId`` window state we can replay.

Prerequisites:
  * A reachable Pulse at ``PULSE_URL`` (default ``http://localhost:9090``).
  * Auth: set ``PULSE_TOKEN``, or ``PULSE_USER`` + ``PULSE_PASSWORD`` (stream + replay need a JWT).
  * The "card-velocity-60s" agent from use-case 2 deployed and emitting.

Run:
  python examples/usecase_4_events_and_replay.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient

AGENT_ID = "card-velocity-60s"
MAX_EVENTS = 5


def main() -> None:
    url = os.environ.get("PULSE_URL", "http://localhost:9090")
    with PulseClient(url, token=os.environ.get("PULSE_TOKEN")) as client:
        # A pre-minted PULSE_TOKEN authenticates directly; otherwise log in with
        # PULSE_USER + PULSE_PASSWORD (the SSE stream + replay require a JWT).
        if not client.token:
            user, password = os.environ.get("PULSE_USER"), os.environ.get("PULSE_PASSWORD")
            if user and password:
                client.auth.login(user, password)

        # Tail the live event stream — stop after a handful of fraud alerts.
        # timeout bounds each read so the demo returns even on a quiet stream.
        print(f"Tailing live events (first {MAX_EVENTS} fraud-alerts)…")
        seen = 0
        for event in client.events.stream(timeout=15.0):
            payload = event.get("payload", event)
            topic = event.get("topic")
            event_type = event.get("type")
            if topic == "fraud-alerts" or event_type == "fraud-alert":
                print("  fraud-alert:", payload)
                seen += 1
                if seen >= MAX_EVENTS:
                    break

        # Replay the committed state-change history for one card (time-travel).
        print("\nReplaying card-007 state history (last hour)…")
        changes = client.events.replay(
            affecting_state=AGENT_ID,
            key="card-007",
            from_="-1h",
            to="now",
            limit=50,
        )
        print(f"Replayed {len(changes)} state change(s) for card-007")
        for change in changes[:5]:
            print("  ", change)


if __name__ == "__main__":
    main()
