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


class FakeNoWaitJob(FakeJob):
    async def wait(self, timeout=None):
        raise AssertionError("status polling should avoid SDK wait")


class FakeEndpoint:
    def __init__(self, endpoint_id: str, *, job=None):
        self.endpoint_id = endpoint_id
        self.input_data = None
        self.job = job or FakeJob()

    async def run(self, input_data):
        self.input_data = input_data
        return self.job


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


@pytest.mark.asyncio
async def test_runpod_flash_adapter_polls_status_when_api_key_is_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNPOD_API_KEY", "test-key")
    payloads = [
        {"id": "remote_123", "status": "IN_QUEUE"},
        {"id": "remote_123", "status": "IN_PROGRESS"},
        {
            "id": "remote_123",
            "status": "COMPLETED",
            "output": {
                "status": "succeeded",
                "output_artifacts": [],
                "metrics": {"polled": True},
            },
        },
    ]
    seen_statuses: list[str] = []
    seen_remote_ids: list[str] = []

    def fetch_status(endpoint_id: str, job_id: str, api_key: str):
        assert endpoint_id == "endpoint_123"
        assert job_id == "remote_123"
        assert api_key == "test-key"
        return payloads.pop(0)

    adapter = RunpodFlashAdapter(
        endpoint_id="endpoint_123",
        endpoint_factory=lambda endpoint_id: FakeEndpoint(endpoint_id, job=FakeNoWaitJob()),
        status_fetcher=fetch_status,
        poll_interval_seconds=0,
    )
    remote_id, result = await adapter.run_envelope(
        ExecutionEnvelope(
            job_id="job_1",
            attempt_id="att_1",
            capability="embedding.fake-deterministic",
        ),
        on_remote_job_id=seen_remote_ids.append,
        on_status=lambda _remote_id, status, _payload: seen_statuses.append(status),
    )

    assert remote_id == "remote_123"
    assert seen_remote_ids == ["remote_123"]
    assert seen_statuses == ["IN_QUEUE", "IN_PROGRESS", "COMPLETED"]
    assert result.status == "succeeded"
    assert result.metrics["polled"] is True


@pytest.mark.asyncio
async def test_runpod_flash_adapter_converts_failed_status_to_job_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNPOD_API_KEY", "test-key")

    adapter = RunpodFlashAdapter(
        endpoint_id="endpoint_123",
        endpoint_factory=lambda endpoint_id: FakeEndpoint(endpoint_id, job=FakeNoWaitJob()),
        status_fetcher=lambda _endpoint_id, _job_id, _api_key: {
            "id": "remote_123",
            "status": "FAILED",
            "error": "worker failed",
        },
        poll_interval_seconds=0,
    )
    remote_id, result = await adapter.run_envelope(
        ExecutionEnvelope(
            job_id="job_1",
            attempt_id="att_1",
            capability="embedding.fake-deterministic",
        )
    )

    assert remote_id == "remote_123"
    assert result.status == "failed"
    assert result.error == "worker failed"
    assert result.metrics["runpod_status"] == "FAILED"
