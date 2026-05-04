"""Mock cloud adapter for deterministic scheduler tests."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from flashburst.artifacts.local import LocalArtifactStore, sha256_file
from flashburst.capabilities.registry import get_capability
from flashburst.db import FlashburstDB
from flashburst.ids import new_id
from flashburst.models import ArtifactRef, AttemptStatus, JobResult, PlacementKind, PlanItem
from flashburst.time import utc_now


class MockCloudAdapter:
    def __init__(self, *, db: FlashburstDB, workspace: Path):
        self.db = db
        self.workspace = workspace

    def run_item(self, item: PlanItem) -> bool:
        job = self.db.get_job(item.job_id)
        if job is None:
            raise KeyError(f"job not found: {item.job_id}")
        if job["status"] == "succeeded":
            return False

        capability = get_capability(str(job["required_capability"]))
        if capability.local_runner is None:
            raise ValueError(f"capability has no runner for mock cloud: {capability.spec.name}")

        if not self.db.reserve_budget(
            plan_id=item.cloud_profile_id or "mock",
            limit_usd=item.estimated_cost_usd,
            amount_usd=Decimal("0"),
        ):
            return False

        attempt_id = self.db.create_attempt(
            job_id=item.job_id,
            placement_kind=PlacementKind.MOCK_CLOUD,
            status=AttemptStatus.RUNNING,
            cloud_profile_id=item.cloud_profile_id,
            remote_job_id=new_id("mockrun"),
            reserved_cost_usd=item.estimated_cost_usd,
        )
        store = LocalArtifactStore(self.workspace / "artifacts")
        inputs = self.db.get_job_input_artifacts(item.job_id)
        params = self.db.get_job_params(item.job_id)
        if len(inputs) != 1:
            self.db.fail_attempt(
                job_id=item.job_id,
                attempt_id=attempt_id,
                error="mock cloud currently expects exactly one input artifact",
            )
            return True
        input_path = store.path_for_uri(inputs[0].uri)
        relative_output = f"mock_outputs/{item.job_id}/{attempt_id}/result.jsonl"
        output_path = store.ensure_parent_for_uri(f"local://{relative_output}")
        try:
            result = capability.local_runner(input_path, output_path, params)
            output_ref = ArtifactRef(
                uri=f"local://{relative_output}",
                media_type="application/x-ndjson",
                storage="local",
                sha256=sha256_file(output_path),
                size_bytes=output_path.stat().st_size,
                producer_job_id=item.job_id,
                created_at=utc_now(),
            )
            self.db.complete_attempt(
                job_id=item.job_id,
                attempt_id=attempt_id,
                result=JobResult(
                    status=result.status,
                    output_artifacts=[output_ref],
                    metrics={
                        **result.metrics,
                        "mock_remote": True,
                    },
                    logs_uri=result.logs_uri,
                    error=result.error,
                ),
            )
        except Exception as exc:
            self.db.fail_attempt(job_id=item.job_id, attempt_id=attempt_id, error=str(exc))
            raise
        return True
