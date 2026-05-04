from decimal import Decimal
from pathlib import Path

import pytest

from flashburst.adapters.runpod_flash import RunpodFlashPlanRunner
from flashburst.db import FlashburstDB
from flashburst.examples.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import (
    ArtifactGrant,
    ArtifactRef,
    CloudProfile,
    ExecutionEnvelope,
    JobResult,
    JobSpec,
    JobStatus,
    PlacementKind,
    PlanItem,
)


class FakeS3Store:
    bucket = "bucket"

    def upload_file(self, source: Path, uri: str, *, media_type: str) -> ArtifactRef:
        assert source.exists()
        return ArtifactRef(uri=uri, media_type=media_type, storage="s3")

    def presign_get(self, uri: str, *, expires_seconds: int = 3600) -> ArtifactGrant:
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
        return ArtifactGrant(
            artifact_uri=uri,
            method="PUT",
            url=f"https://example.test/put/{uri}",
            expires_at="2030-01-01T00:00:00Z",
            content_type=media_type,
        )


class FakeAdapter:
    async def run_envelope(
        self,
        envelope: ExecutionEnvelope,
        *,
        timeout_seconds: float = 600,
    ) -> tuple[str, JobResult]:
        assert envelope.artifact_grants[0].method == "GET"
        assert envelope.artifact_grants[1].method == "PUT"
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
    ) -> tuple[str, JobResult]:
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
        capability="embedding.bge-small-en-v1.5",
        batch_size=1,
    )
    db = FlashburstDB(workspace / "flashburst.db")
    db.init_schema()
    with jobs_path.open("r", encoding="utf-8") as handle:
        job_id = db.insert_job(JobSpec.model_validate_json(handle.readline()))
    profile = CloudProfile(
        id="bge-small-burst",
        backend="runpod_flash",
        endpoint_id="rp_test",
        capability="embedding.bge-small-en-v1.5",
        estimated_cost_per_job_usd=Decimal("0.05"),
    )
    item = PlanItem(
        job_id=job_id,
        placement_kind=PlacementKind.RUNPOD_FLASH,
        cloud_profile_id=profile.id,
        estimated_cost_usd=Decimal("0.05"),
    )

    runner = RunpodFlashPlanRunner(
        db=db,
        workspace=workspace,
        s3_store=FakeS3Store(),
        adapter_factory=lambda profile: FakeAdapter(),
    )

    assert await runner.run_item(item=item, profile=profile)
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.SUCCEEDED.value


@pytest.mark.asyncio
async def test_runpod_flash_plan_runner_reports_remote_failure(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.bge-small-en-v1.5",
        batch_size=1,
    )
    db = FlashburstDB(workspace / "flashburst.db")
    db.init_schema()
    with jobs_path.open("r", encoding="utf-8") as handle:
        job_id = db.insert_job(JobSpec.model_validate_json(handle.readline()))
    profile = CloudProfile(
        id="bge-small-burst",
        backend="runpod_flash",
        endpoint_id="rp_test",
        capability="embedding.bge-small-en-v1.5",
        estimated_cost_per_job_usd=Decimal("0.05"),
    )
    item = PlanItem(
        job_id=job_id,
        placement_kind=PlacementKind.RUNPOD_FLASH,
        cloud_profile_id=profile.id,
        estimated_cost_usd=Decimal("0.05"),
    )

    runner = RunpodFlashPlanRunner(
        db=db,
        workspace=workspace,
        s3_store=FakeS3Store(),
        adapter_factory=lambda profile: FakeFailingAdapter(),
    )

    assert not await runner.run_item(item=item, profile=profile)
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.FAILED.value
    assert job["error"] == "remote failed"
