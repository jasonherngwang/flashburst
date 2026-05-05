from pathlib import Path

import pytest

from flashburst.adapters.runpod_flash import RunpodFlashPlanRunner
from flashburst.db import FlashburstDB
from flashburst.workloads.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import (
    ArtifactGrant,
    ArtifactRef,
    CloudProfile,
    ExecutionEnvelope,
    JobResult,
    JobSpec,
    JobStatus,
)


class FakeS3Store:
    bucket = "bucket"

    def __init__(self):
        self.expiries: list[int] = []

    def upload_file(self, source: Path, uri: str, *, media_type: str) -> ArtifactRef:
        assert source.exists()
        return ArtifactRef(uri=uri, media_type=media_type, storage="s3")

    def presign_get(self, uri: str, *, expires_seconds: int = 3600) -> ArtifactGrant:
        self.expiries.append(expires_seconds)
        return ArtifactGrant(
            artifact_uri=uri,
            method="GET",
            url=f"https://example.test/get/{uri}",
            expires_at="2030-01-01T00:00:00Z",
        )

    def presign_put(
        self,
        uri: str,
        *,
        media_type: str = "application/octet-stream",
        expires_seconds: int = 3600,
    ) -> ArtifactGrant:
        self.expiries.append(expires_seconds)
        return ArtifactGrant(
            artifact_uri=uri,
            method="PUT",
            url=f"https://example.test/put/{uri}",
            expires_at="2030-01-01T00:00:00Z",
            content_type=media_type,
        )


class FakeAdapter:
    def __init__(self):
        self.timeout_seconds: float | None = None

    async def run_envelope(
        self,
        envelope: ExecutionEnvelope,
        *,
        timeout_seconds: float = 600,
        on_remote_job_id=None,
        on_status=None,
    ) -> tuple[str, JobResult]:
        self.timeout_seconds = timeout_seconds
        assert envelope.artifact_grants[0].method == "GET"
        assert envelope.artifact_grants[1].method == "PUT"
        if on_remote_job_id is not None:
            on_remote_job_id("remote_123")
        if on_status is not None:
            on_status("remote_123", "IN_PROGRESS", {"status": "IN_PROGRESS"})
        return (
            "remote_123",
            JobResult(
                status="succeeded",
                output_artifacts=[
                    ArtifactRef(
                        uri=envelope.output_artifacts[0].uri,
                        media_type="application/x-ndjson",
                        storage="s3",
                    )
                ],
                metrics={"remote": True},
            ),
        )


class FakeFailingAdapter:
    async def run_envelope(
        self,
        envelope: ExecutionEnvelope,
        *,
        timeout_seconds: float = 600,
        on_remote_job_id=None,
        on_status=None,
    ) -> tuple[str, JobResult]:
        if on_remote_job_id is not None:
            on_remote_job_id("remote_failed")
        return (
            "remote_failed",
            JobResult(
                status="failed",
                error="remote failed",
            ),
        )


@pytest.mark.asyncio
async def test_runpod_flash_plan_runner_completes_job(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.fake-deterministic",
        batch_size=1,
    )
    db = FlashburstDB(workspace / "flashburst.db")
    db.init_schema()
    with jobs_path.open("r", encoding="utf-8") as handle:
        job_id = db.insert_job(JobSpec.model_validate_json(handle.readline()))
    profile = CloudProfile(
        id="fake-burst",
        backend="runpod_flash",
        endpoint_id="rp_test",
        capability="embedding.fake-deterministic",
        config={
            "run_timeout_seconds": 1800,
            "artifact_grant_expires_seconds": 7200,
        },
    )
    claim = db.claim_next_cloud_job(
        worker_id="queue-cloud-0",
        capability=profile.capability,
        cloud_profile_id=profile.id,
        job_ids=[job_id],
    )
    assert claim is not None
    claimed_job_id, attempt_id = claim

    fake_s3 = FakeS3Store()
    fake_adapter = FakeAdapter()
    runner = RunpodFlashPlanRunner(
        db=db,
        workspace=workspace,
        s3_store=fake_s3,
        adapter_factory=lambda profile: fake_adapter,
    )

    assert await runner.run_claimed_job(
        job_id=claimed_job_id,
        attempt_id=attempt_id,
        profile=profile,
    )
    assert fake_adapter.timeout_seconds == 1800
    assert fake_s3.expiries == [7200, 7200]
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.SUCCEEDED.value
    attempts = db.list_attempts(job_id)
    assert attempts[0]["remote_job_id"] == "remote_123"


@pytest.mark.asyncio
async def test_runpod_flash_plan_runner_reports_remote_failure(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.fake-deterministic",
        batch_size=1,
    )
    db = FlashburstDB(workspace / "flashburst.db")
    db.init_schema()
    with jobs_path.open("r", encoding="utf-8") as handle:
        job_id = db.insert_job(JobSpec.model_validate_json(handle.readline()))
    profile = CloudProfile(
        id="fake-burst",
        backend="runpod_flash",
        endpoint_id="rp_test",
        capability="embedding.fake-deterministic",
    )
    claim = db.claim_next_cloud_job(
        worker_id="queue-cloud-0",
        capability=profile.capability,
        cloud_profile_id=profile.id,
        job_ids=[job_id],
    )
    assert claim is not None
    claimed_job_id, attempt_id = claim

    runner = RunpodFlashPlanRunner(
        db=db,
        workspace=workspace,
        s3_store=FakeS3Store(),
        adapter_factory=lambda profile: FakeFailingAdapter(),
    )

    assert not await runner.run_claimed_job(
        job_id=claimed_job_id,
        attempt_id=attempt_id,
        profile=profile,
    )
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.FAILED.value
    assert job["error"] == "remote failed"
