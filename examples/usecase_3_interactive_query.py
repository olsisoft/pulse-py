"""Use-case ladder 3/5 — Interactive Query the velocity agent's live state.

What it shows:
  Query the live, materialized state of the "card-velocity-60s" agent like a
  database: a headline summary, a filtered query for cards whose ``txCount > 5``
  (built with the IQ filter leaf using the ``gt`` op), and a point ``get`` for
  "card-007". Because the client never auto-retries, the query is wrapped in a
  small caller-side retry that honours ``retry_after_seconds`` on a 429.

Domain: card-payments fraud monitoring. The agent maintains, per ``cardId``,
the running 60s window state (``txCount`` / ``totalAmount`` / ``maxAmount``).

Prerequisites:
  * A reachable Pulse at ``PULSE_URL`` (default ``http://localhost:9090``).
  * Auth: set ``PULSE_TOKEN``, or ``PULSE_USER`` + ``PULSE_PASSWORD`` (IQ requires AGENT_READ).
  * The "card-velocity-60s" agent from use-case 2 deployed and queryable.

Run:
  python examples/usecase_3_interactive_query.py
"""

from __future__ import annotations

import os
import time

from pulse_client import PulseClient, PulseRateLimitError

AGENT_ID = "card-velocity-60s"


def query_with_retry(client: PulseClient, *, max_attempts: int = 3):
    """Run the txCount>5 IQ query, retrying on 429 — the SDK never auto-retries.

    PulseRateLimitError carries ``retry_after_seconds`` (parsed from the body or
    the ``Retry-After`` header); we sleep that long before retrying, capped at
    ``max_attempts``.
    """
    # IQ filter leaf: {"field", "op", "value"} — op ∈ eq/neq/gt/gte/lt/lte/...
    fraud_filter = {"field": "txCount", "op": "gt", "value": 5}
    for attempt in range(1, max_attempts + 1):
        try:
            return client.iq.query(AGENT_ID, filter=fraud_filter, limit=50)
        except PulseRateLimitError as exc:
            wait = exc.retry_after_seconds or 1
            if attempt == max_attempts:
                raise
            print(f"  rate-limited (429); retrying in {wait}s "
                  f"(attempt {attempt}/{max_attempts})")
            time.sleep(wait)
    return None  # unreachable — loop either returns or raises


def main() -> None:
    url = os.environ.get("PULSE_URL", "http://localhost:9090")
    with PulseClient(url, token=os.environ.get("PULSE_TOKEN")) as client:
        # A pre-minted PULSE_TOKEN authenticates directly; otherwise log in with
        # PULSE_USER + PULSE_PASSWORD (IQ requires the AGENT_READ permission).
        if not client.token:
            user, password = os.environ.get("PULSE_USER"), os.environ.get("PULSE_PASSWORD")
            if user and password:
                client.auth.login(user, password)

        print("Summary:", client.iq.summary(AGENT_ID))

        print("\nCards with txCount > 5 (fraud candidates):")
        result = query_with_retry(client)
        for entry in (result or {}).get("entries", []):
            print("  ", entry)

        # Point lookup for one card's current window state.
        print("\ncard-007 current state:", client.iq.get(AGENT_ID, key="card-007"))


if __name__ == "__main__":
    main()
