"""PVSC and eval suites over the wire.

The five Pulse SDKs had no PVSC surface at all — ``grep -r pvsc`` across
pulse-js / pulse-py / pulse-rs / pulse-go / pulse-java returned nothing.
Anything an operator could do to a topic contract, an arbitration policy or an
eval suite was reachable only from the browser.

Offline throughout: `respx` intercepts httpx and returns canned responses. The
point is to pin the wire format, not to exercise a server.
"""

from __future__ import annotations

import json

import httpx
import respx

from pulse_client import PulseClient


class TestTopicContracts:
    @respx.mock
    def test_schemas_unwraps_the_envelope(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/pvsc/schemas").mock(
            return_value=httpx.Response(200, json={"schemas": [{"topic": "quotes"}], "count": 1})
        )
        assert authed_client.pvsc.schemas() == [{"topic": "quotes"}]

    @respx.mock
    def test_grounding_policy_reaches_the_wire(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # A grounding policy could be written from Java and from nowhere else
        # until recently. An SDK that dropped it would be the next place it was
        # unreachable from.
        route = respx.put(f"{base_url}/api/pulse/pvsc/schemas").mock(
            return_value=httpx.Response(200, json={"status": "saved", "topic": "quotes"})
        )
        authed_client.pvsc.save_schema(
            {
                "topic": "quotes",
                "requiredFields": {},
                "optionalFields": {"price": {"type": "number", "grounding": "required"}},
                "allowExtraFields": True,
            }
        )
        sent = json.loads(route.calls[0].request.content)
        assert sent["optionalFields"]["price"]["grounding"] == "required"

    @respx.mock
    def test_delete_sends_the_topic_in_the_body(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.delete(f"{base_url}/api/pulse/pvsc/schemas").mock(
            return_value=httpx.Response(200, json={"status": "deleted"})
        )
        authed_client.pvsc.delete_schema("quotes")
        assert json.loads(route.calls[0].request.content) == {"topic": "quotes"}


class TestArbitration:
    @respx.mock
    def test_config_carries_the_stances(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pvsc/config").mock(
            return_value=httpx.Response(
                200,
                json={
                    "arbitration": {
                        "stances": [{"domain": "legal", "precedence": 1, "veto": True}],
                        "stanceCount": 1,
                        "enabled": True,
                    }
                },
            )
        )
        assert authed_client.pvsc.config()["arbitration"]["enabled"] is True

    @respx.mock
    def test_set_stances_writes_through_the_config_route(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        route = respx.put(f"{base_url}/api/pulse/pvsc/config").mock(
            return_value=httpx.Response(200, json={"status": "updated"})
        )
        authed_client.pvsc.set_stances([{"domain": "legal", "precedence": 1, "veto": True}])
        assert json.loads(route.calls[0].request.content) == {
            "arbitrationStances": [{"domain": "legal", "precedence": 1, "veto": True}]
        }

    @respx.mock
    def test_empty_list_is_a_real_value(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # Clearing the stances disables arbitration. It must go over the wire
        # as [], not be skipped as falsy.
        route = respx.put(f"{base_url}/api/pulse/pvsc/config").mock(
            return_value=httpx.Response(200, json={"status": "updated"})
        )
        authed_client.pvsc.set_stances([])
        assert json.loads(route.calls[0].request.content) == {"arbitrationStances": []}


class TestMetricsAndDlq:
    @respx.mock
    def test_quorum_yield_is_surfaced(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pvsc/metrics").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pvscModeCount": 300,
                    "quorumInformationYield": 0.3333,
                    "quorumRedundantGuardianCalls": 400,
                },
            )
        )
        metrics = authed_client.pvsc.metrics()
        assert metrics["quorumRedundantGuardianCalls"] == 400

    @respx.mock
    def test_dlq_list_and_reinject(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        respx.get(f"{base_url}/api/pulse/pvsc/dlq").mock(
            return_value=httpx.Response(
                200,
                json={
                    "entries": [{"eventId": "e1", "rejectedBy": "pvsc-agent-gate"}],
                    "counts": {},
                    "total": 1,
                },
            )
        )
        route = respx.post(f"{base_url}/api/pulse/pvsc/dlq/reinject").mock(
            return_value=httpx.Response(200, json={"reinjected": True})
        )

        entries = authed_client.pvsc.dlq()
        assert entries[0]["rejectedBy"] == "pvsc-agent-gate"

        authed_client.pvsc.reinject("e1")
        assert json.loads(route.calls[0].request.content) == {"eventId": "e1"}


class TestEvals:
    @respx.mock
    def test_suites_and_cases(self, authed_client: PulseClient, base_url: str) -> None:
        respx.get(f"{base_url}/api/pulse/evals").mock(
            return_value=httpx.Response(200, json={"suites": ["pricing"], "count": 1})
        )
        route = respx.get(f"{base_url}/api/pulse/evals/cases").mock(
            return_value=httpx.Response(200, json={"cases": [{"caseId": "c1"}], "count": 1})
        )

        assert authed_client.evals.suites() == ["pricing"]
        assert authed_client.evals.cases("pricing")[0]["caseId"] == "c1"
        assert route.calls[0].request.url.params["suite"] == "pricing"

    @respx.mock
    def test_regression_is_a_verdict_not_a_raised_error(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        # The server answers 200 with blocksRelease=True because the run
        # succeeded. A caller in CI branches on blocksRelease; if this raised,
        # "the suite regressed" and "the call broke" would be the same event.
        respx.post(f"{base_url}/api/pulse/evals/run").mock(
            return_value=httpx.Response(
                200,
                json={
                    "suiteId": "pricing",
                    "total": 10,
                    "passing": 7,
                    "failing": 3,
                    "baseline": 9,
                    "gate": "REGRESSION",
                    "blocksRelease": True,
                    "summary": "suite 'pricing': 7/10 passing — REGRESSION",
                    "cases": [],
                },
            )
        )
        report = authed_client.evals.run("pricing")
        assert report["gate"] == "REGRESSION"
        assert report["blocksRelease"] is True

    @respx.mock
    def test_record_baseline_and_save_case(
        self, authed_client: PulseClient, base_url: str
    ) -> None:
        baseline = respx.post(f"{base_url}/api/pulse/evals/baseline").mock(
            return_value=httpx.Response(200, json={"suiteId": "pricing", "baseline": 7})
        )
        saved = respx.post(f"{base_url}/api/pulse/evals/cases").mock(
            return_value=httpx.Response(200, json={"caseId": "c9", "gradable": True})
        )

        authed_client.evals.record_baseline("pricing")
        assert json.loads(baseline.calls[0].request.content) == {"suiteId": "pricing"}

        authed_client.evals.save_case(
            {
                "suiteId": "pricing",
                "name": "grounded price",
                "agentKey": "quoter",
                "inputPayload": '{"montant":42.00}',
                "expectations": {"data.price": 42.0},
            }
        )
        assert json.loads(saved.calls[0].request.content)["agentKey"] == "quoter"
