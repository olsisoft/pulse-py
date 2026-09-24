"""Use-case ladder 1/5 — connect & list (hello-world / connectivity check).

What it shows:
  Construct the client, read the server version (public, no auth), log in if
  credentials are present (degrading gracefully when they aren't), then list the
  pipelines and connectors the org can see and print them.

Domain: card-payments fraud monitoring (topic ``card-authorizations``,
events ``{cardId, merchantId, amount, ts}``).

Prerequisites:
  * A reachable Pulse at ``PULSE_URL`` (default ``http://localhost:9090``).
  * Optional auth: set ``PULSE_TOKEN``, or ``PULSE_USER`` + ``PULSE_PASSWORD``.
    Without auth this still prints the version; the authenticated lists are
    skipped with a clear message.

Run:
  python examples/usecase_1_connect_and_list.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient, PulseAPIError


def main() -> None:
    url = os.environ.get("PULSE_URL", "http://localhost:9090")
    token = os.environ.get("PULSE_TOKEN")
    user = os.environ.get("PULSE_USER")
    password = os.environ.get("PULSE_PASSWORD")

    with PulseClient(url, token=token) as client:
        # version() is a public endpoint — no JWT required.
        print("Connected to Pulse:", client.version())

        # Authenticate if we can. Either PULSE_TOKEN was passed at construction,
        # or PULSE_USER/PULSE_PASSWORD let us log in for a fresh JWT.
        if client.token is None and user and password:
            try:
                session = client.auth.login(user, password)
                print(f"Logged in as {user} (org={session.get('activeOrg')})")
            except PulseAPIError as exc:
                print(f"Login failed ({exc}) — continuing unauthenticated")

        if client.token is None:
            print(
                "No token set (pass PULSE_TOKEN or PULSE_USER/PULSE_PASSWORD) — "
                "skipping the authenticated pipeline/connector listings."
            )
            return

        # Authenticated listings.
        try:
            pipelines = client.pipelines.list()
            print(f"\nPipelines ({len(pipelines)}):")
            for p in pipelines:
                print(f"  - {p.get('name')} (id={p.get('id')}, status={p.get('status')})")

            connectors = client.connectors.list()
            sources = connectors.get("sources", [])
            sinks = connectors.get("sinks", [])
            print(f"\nConnectors: {len(sources)} source(s), {len(sinks)} sink(s)")
            for c in sources[:5]:
                print(f"  source: {c.get('subType')} — {c.get('displayName')}")
            for c in sinks[:5]:
                print(f"  sink:   {c.get('subType')} — {c.get('displayName')}")
        except PulseAPIError as exc:
            print(f"Could not list pipelines/connectors: {exc}")


if __name__ == "__main__":
    main()
