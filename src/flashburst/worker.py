"""Local worker execution."""

from __future__ import annotations

from pathlib import Path

from flashburst.artifacts.local import LocalArtifactStore, sha256_file
from flashburst.capabilities.registry import get_capability
from flashburst.db import FlashburstDB
from flashburst.models import ArtifactRef, JobResult
from flashburst.time import utc_now


def run_once(
    *,
    db: FlashburstDB,
    workspace: Path,
    worker_id: str,
    capability_name: str,
) -> bool:
    capability = get_capability(capability_name)
    if capability.local_runner is None:
        raise ValueError(f"capability does not support local execution: {capability_name}")
    claim = db.claim_next_local_job(worker_id=worker_id, capability=capability_name)
    if claim is None:
        return False

    store = LocalArtifactStore(workspace / "artifacts")
    input_artifacts = db.get_job_input_artifacts(claim.job_id)
    params = db.get_job_params(claim.job_id)
    if len(input_artifacts) != 1:
        db.fail_attempt(
            job_id=claim.job_id,
            attempt_id=claim.attempt_id,
            error="fake local worker currently expects exactly one input artifact",
        )
        return True

    input_path = store.path_for_uri(input_artifacts[0].uri)
    relative_output = f"outputs/{claim.job_id}/{claim.attempt_id}/result.jsonl"
    output_path = store.ensure_parent_for_uri(f"local://{relative_output}")
    try:
        result = capability.local_runner(input_path, output_path, params)
        output_ref = ArtifactRef(
            uri=f"local://{relative_output}",
            media_type="application/x-ndjson",
            storage="local",
            sha256=sha256_file(output_path),
            size_bytes=output_path.stat().st_size,
            producer_job_id=claim.job_id,
            created_at=utc_now(),
        )
        result = JobResult(
            status=result.status,
            output_artifacts=[output_ref],
            metrics=result.metrics,
            logs_uri=result.logs_uri,
            error=result.error,
        )
        db.complete_attempt(job_id=claim.job_id, attempt_id=claim.attempt_id, result=result)
    except Exception as exc:
        db.fail_attempt(job_id=claim.job_id, attempt_id=claim.attempt_id, error=str(exc))
        raise
    return True
