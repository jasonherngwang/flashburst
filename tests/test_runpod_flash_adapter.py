import pytest

from flashburst.adapters.runpod_flash import RunpodFlashAdapter
from flashburst.models import ExecutionEnvelope


class FakeJob:
    id = "remote_123"
    error = None
    output = {
        "status": "succeeded",
        "output_artifacts": [],
        "metrics": {"ok": True},
    }

    async def wait(self, timeout=None):
        return self


class FakeEndpoint:
    def __init__(self, endpoint_id: str):
        self.endpoint_id = endpoint_id
        self.input_data = None

    async def run(self, input_data):
        self.input_data = input_data
        return FakeJob()


@pytest.mark.asyncio
async def test_runpod_flash_adapter_parses_job_result() -> None:
    adapter = RunpodFlashAdapter(
        endpoint_id="endpoint_123",
        endpoint_factory=lambda endpoint_id: FakeEndpoint(endpoint_id),
    )
    remote_id, result = await adapter.run_envelope(
        ExecutionEnvelope(
            job_id="job_1",
            attempt_id="att_1",
            capability="embedding.fake-deterministic",
        )
    )
    assert remote_id == "remote_123"
    assert result.status == "succeeded"
    assert result.metrics["ok"] is True
