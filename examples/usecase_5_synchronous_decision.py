"""Use-case ladder 5/5 — synchronous ALLOW/DENY over a duplex channel (B-114).

What it shows:
  Open a bidirectional duplex channel to the "fraud-decider" agent and, on ONE
  WebSocket, send a couple of charges IN and receive the agent's correlated
  decision OUT — the synchronous-decision path. One hot card is expected to come
  back DENY, one fresh card ALLOW; each decision carries the correlation id of
  the charge that produced it.

Domain: card-payments fraud monitoring. The "fraud-decider" agent reads each
charge ``{cardId, amount}`` against the live velocity state and answers
ALLOW / DENY.

Prerequisites:
  * A reachable Pulse at ``PULSE_URL`` (default ``http://localhost:9090``).
  * Auth: set ``PULSE_TOKEN``.
  * Duplex is async and needs the optional WebSocket extra:
        pip install streamflow-pulse-client[duplex]
  * A "fraud-decider" agent deployed that emits a decision per input charge.

Run:
  python examples/usecase_5_synchronous_decision.py
"""

from __future__ import annotations

import asyncio
import os

from pulse_client import PulseClient

AGENT_ID = "fraud-decider"

# One hot card (likely DENY) and one fresh card (likely ALLOW).
CHARGES = [
    {"cardId": "card-007", "amount": 250.0},  # hot card → expect DENY
    {"cardId": "card-999", "amount": 12.0},   # fresh card → expect ALLOW
]


async def decide(client: PulseClient) -> None:
    # client.duplex(...) returns an async context manager (DuplexChannel).
    async with client.duplex(AGENT_ID) as channel:
        for charge in CHARGES:
            cid = await channel.send(charge, correlation_id=charge["cardId"])
            signal = await channel.recv()  # the output correlated to THIS input
            payload = signal.get("payload", signal)
            decision = payload.get("decision") if isinstance(payload, dict) else payload
            print(
                f"charge {charge} → {decision} "
                f"(correlation_id={signal.get('correlation_id')}, sent_cid={cid})"
            )


def main() -> None:
    url = os.environ.get("PULSE_URL", "http://localhost:9090")
    with PulseClient(url, token=os.environ.get("PULSE_TOKEN")) as client:
        try:
            asyncio.run(decide(client))
        except RuntimeError as exc:
            # DuplexChannel raises RuntimeError with an install hint when the
            # optional 'websockets' extra isn't present.
            print(f"Duplex unavailable: {exc}")


if __name__ == "__main__":
    main()
