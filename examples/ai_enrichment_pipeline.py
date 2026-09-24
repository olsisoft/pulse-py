"""Use case 4 — agentic enrichment pipeline (LLM + extract + MCP) on the mesh.

Enriches support tickets streaming through the mesh: classify sentiment with an
LLM, pull structured fields out of free text, then call an MCP tool to look the
customer up — all as a declarative stream that runs on the cluster.

Run against your Pulse:  PULSE_URL=http://localhost:9090 python ai_enrichment_pipeline.py
"""

from __future__ import annotations

import os

from pulse_client import PulseClient
from pulse_client.streams import StreamBuilder


def build_pipeline() -> StreamBuilder:
    return (
        StreamBuilder("ticket-enrichment", description="LLM + MCP enrichment of support tickets")
        .from_topic("support-tickets")
        .filter("priority != 'spam'")
        .map_llm(
            prompt="Classify the ticket sentiment as positive, neutral, or negative.",
            output_field="sentiment",
        )
        .extract(
            instruction="Extract the product name and the customer's requested action.",
            schema={"product": "string", "requested_action": "string"},
        )
        .mcp_call(
            "crm.lookup_customer",
            args={"email": "${customer_email}"},
            output_field="customer",
        )
        .to_topic("tickets-enriched")
    )


def main() -> None:
    builder = build_pipeline()
    print("Pipeline spec:", builder.build())

    with PulseClient(os.environ.get("PULSE_URL", "http://localhost:9090"),
                     token=os.environ.get("PULSE_TOKEN")) as client:
        print("Deployed:", client.streams.deploy(builder))


if __name__ == "__main__":
    main()
