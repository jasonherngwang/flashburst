"""Runpod Flash adapters."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from flashburst.artifacts.local import LocalArtifactStore
from flashburst.artifacts.s3 import S3ArtifactStore
from flashburst.db import FlashburstDB
from flashburst.models import (
    ArtifactRef,
    AttemptStatus,
    CloudProfile,
    ExecutionEnvelope,
    JobResult,
    PlacementKind,
    PlanItem,
)


class RunpodFlashAdapter:
    def __init__(
        self,
        *,
        endpoint_id: str,
        endpoint_factory: Callable[[str], Any] | None = None,
    ):
        self.endpoint_id = endpoint_id
        self._endpoint_factory = endpoint_factory

    def _make_endpoint(self):
        if self._endpoint_factory is not None:
            return self._endpoint_factory(self.endpoint_id)
        from runpod_flash import Endpoint

        return Endpoint(id=self.endpoint_id)

    async def run_envelope(
        self,
        envelope: ExecutionEnvelope,
        *,
        timeout_seconds: float = 600,
    ) -> tuple[str, JobResult]:
        endpoint = self._make_endpoint()
        job = await endpoint.run(envelope.model_dump(mode="json"))
        await job.wait(timeout=timeout_seconds)
        if job.error:
            return job.id, JobResult(status="failed", error=str(job.error))
        output = job.output
        if isinstance(output, dict) and "output" in output and isinstance(output["output"], dict):
            output = output["output"]
        if not isinstance(output, dict):
            return job.id, JobResult(status="failed", error=f"unexpected Runpod output: {output!r}")
        return job.id, JobResult.model_validate(output)


class RunpodFlashPlanRunner:
    def __init__(
        self,
        *,
        db: FlashburstDB,
        workspace: Path,
        s3_store: S3ArtifactStore,
        adapter_factory: Callable[[CloudProfile], RunpodFlashAdapter] | None = None,
        timeout_seconds: float = 600,
        grant_expires_seconds: int = 3600,
    ):
        self.db = db
        self.workspace = workspace
        self.s3_store = s3_store
        self._adapter_factory = adapter_factory
        self.timeout_seconds = timeout_seconds
        self.grant_expires_seconds = grant_expires_seconds

    def _adapter_for(self, profile: CloudProfile) -> RunpodFlashAdapter:
        if self._adapter_factory is not None:
            return self._adapter_factory(profile)
        if profile.endpoint_id is None:
            raise ValueError(f"cloud profile {profile.id} has no endpoint_id")
        return RunpodFlashAdapter(endpoint_id=profile.endpoint_id)

    def _stage_input(self, *, job_id: str, input_ref: ArtifactRef) -> ArtifactRef:
        if input_ref.storage == "s3":
            return input_ref
        local_store = LocalArtifactStore(self.workspace / "artifacts")
        source = local_store.path_for_uri(input_ref.uri)
        destination = f"s3://{self.s3_store.bucket}/flashburst/inputs/{job_id}/{source.name}"
        staged = self.s3_store.upload_file(
            source,
            destination,
            media_type=input_ref.media_type,
        )
        self.db.record_artifact(staged)
        return staged

    def _output_ref(self, *, job_id: str, attempt_id: str) -> ArtifactRef:
        return ArtifactRef(
            uri=f"s3://{self.s3_store.bucket}/flashburst/outputs/{job_id}/{attempt_id}/result.jsonl",
            media_type="application/x-ndjson",
            storage="s3",
            producer_job_id=job_id,
        )

    async def run_item(self, *, item: PlanItem, profile: CloudProfile) -> bool:
        job = self.db.get_job(item.job_id)
        if job is None:
            raise KeyError(f"job not found: {item.job_id}")
        if job["status"] == "succeeded":
            return False
        if job["privacy"] == "local_only":
            return False

        attempt_id = self.db.create_attempt(
            job_id=item.job_id,
            placement_kind=PlacementKind.RUNPOD_FLASH,
            status=AttemptStatus.SUBMITTED,
            cloud_profile_id=profile.id,
            reserved_cost_usd=item.estimated_cost_usd,
        )
        try:
            inputs = self.db.get_job_input_artifacts(item.job_id)
            if len(inputs) != 1:
                raise ValueError("Runpod Flash runner currently expects exactly one input artifact")
            staged_input = self._stage_input(job_id=item.job_id, input_ref=inputs[0])
            output_ref = self._output_ref(job_id=item.job_id, attempt_id=attempt_id)
            read_grant = self.s3_store.presign_get(
                staged_input.uri,
                expires_seconds=self.grant_expires_seconds,
            )
            write_grant = self.s3_store.presign_put(
                output_ref.uri,
                media_type=output_ref.media_type,
                expires_seconds=self.grant_expires_seconds,
            )
            envelope = ExecutionEnvelope(
                job_id=item.job_id,
                attempt_id=attempt_id,
                capability=profile.capability,
                params=self.db.get_job_params(item.job_id),
                input_artifacts=[staged_input],
                output_artifacts=[output_ref],
                artifact_grants=[read_grant, write_grant],
            )
            remote_job_id, result = await self._adapter_for(profile).run_envelope(
                envelope,
                timeout_seconds=self.timeout_seconds,
            )
            self.db.update_attempt_remote_job(
                attempt_id=attempt_id,
                remote_job_id=remote_job_id,
            )
            self.db.record_backend_run(
                attempt_id=attempt_id,
                backend="runpod_flash",
                remote_job_id=remote_job_id,
                request=envelope.model_dump(mode="json"),
                response=result.model_dump(mode="json"),
            )
            if result.status == "succeeded":
                self.db.complete_attempt(
                    job_id=item.job_id,
                    attempt_id=attempt_id,
                    result=result,
                )
                return True
            self.db.fail_attempt(
                job_id=item.job_id,
                attempt_id=attempt_id,
                error=result.error or "Runpod Flash job failed",
            )
            return False
        except Exception as exc:
            self.db.fail_attempt(job_id=item.job_id, attempt_id=attempt_id, error=str(exc))
            raise
