from __future__ import annotations

from typing import Any

import pytest

from flashburst.adapters.runpod_flash import RunpodFlashAdapter, RunpodFlashJobError


class FakeEndpointJob:
    id = "remote-1"
    error = None

    def __init__(self, output: dict[str, Any]):
        self.output = output

    async def wait(self, timeout: float) -> None:
        return None


class FakeEndpoint:
    def __init__(self, output: dict[str, Any]):
        self.output = output
        self.payload: dict[str, Any] | None = None

    async def run(self, payload: dict[str, Any]) -> FakeEndpointJob:
        self.payload = payload
        return FakeEndpointJob(self.output)


class TimeoutEndpointJob:
    id = "remote-timeout"
    error = None
    output: dict[str, Any] = {}

    async def wait(self, timeout: float) -> None:
        raise TimeoutError("timed out")


class TimeoutEndpoint:
    async def run(self, payload: dict[str, Any]) -> TimeoutEndpointJob:
        return TimeoutEndpointJob()


@pytest.mark.asyncio
async def test_adapter_submits_inline_payload_and_returns_output_text() -> None:
    endpoint = FakeEndpoint(
        {
            "status": "succeeded",
            "output_text": '{"ok":true}\n',
            "output_media_type": "application/x-ndjson",
            "metrics": {"remote": True},
        }
    )
    adapter = RunpodFlashAdapter(
        endpoint_id="endpoint",
        endpoint_factory=lambda endpoint_id: endpoint,
    )

    remote_job_id, result = await adapter.run_payload(
        {"job_id": "job-1", "input": {"id": "a"}, "params": {}},
        timeout_seconds=1,
    )

    assert remote_job_id == "remote-1"
    assert result.status == "succeeded"
    assert result.output_text == '{"ok":true}\n'
    assert result.metrics == {"remote": True}
    assert endpoint.payload == {"job_id": "job-1", "input": {"id": "a"}, "params": {}}


@pytest.mark.asyncio
async def test_adapter_preserves_remote_job_id_when_wait_raises() -> None:
    adapter = RunpodFlashAdapter(
        endpoint_id="endpoint",
        endpoint_factory=lambda endpoint_id: TimeoutEndpoint(),
    )

    with pytest.raises(RunpodFlashJobError) as raised:
        await adapter.run_payload(
            {"job_id": "job-1", "input": {"id": "a"}, "params": {}},
            timeout_seconds=1,
        )

    assert raised.value.remote_job_id == "remote-timeout"
    assert "timed out" in str(raised.value)
