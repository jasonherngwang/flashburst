"""Prepare embedding jobs from text input."""

from __future__ import annotations

import json
from pathlib import Path

from flashburst.artifacts.local import LocalArtifactStore
from flashburst.ids import compute_idempotency_key
from flashburst.models import JobSpec, Privacy


def _parse_text_line(line: str, index: int) -> dict:
    stripped = line.strip()
    if not stripped:
        raise ValueError("empty line")
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return {"id": f"text-{index}", "text": stripped}
    if isinstance(parsed, str):
        return {"id": f"text-{index}", "text": parsed}
    if isinstance(parsed, dict) and "text" in parsed:
        return {"id": parsed.get("id", f"text-{index}"), "text": parsed["text"]}
    raise ValueError(f"line {index + 1} must be text, a JSON string, or an object with text")


def load_texts(path: Path) -> list[dict]:
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if line.strip():
                records.append(_parse_text_line(line, index))
    return records


def prepare_embedding_jobs(
    *,
    input_path: Path,
    workspace: Path,
    capability: str,
    batch_size: int,
    params: dict | None = None,
) -> Path:
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    store = LocalArtifactStore(workspace / "artifacts")
    jobs_dir = workspace / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    job_path = jobs_dir / "embeddings.jsonl"
    records = load_texts(input_path)
    job_params = params or {}
    with job_path.open("w", encoding="utf-8") as jobs_file:
        for batch_index, start in enumerate(range(0, len(records), batch_size)):
            batch = records[start : start + batch_size]
            relative = f"inputs/embedding-batch-{batch_index:04d}.jsonl"
            artifact_path = store.ensure_parent_for_uri(f"local://{relative}")
            with artifact_path.open("w", encoding="utf-8") as batch_file:
                for item in batch:
                    batch_file.write(json.dumps(item, sort_keys=True) + "\n")
            artifact = store.ref_for_path(relative, media_type="application/x-ndjson")
            idempotency_key = compute_idempotency_key(
                job_type="embedding.embed_text_batch",
                required_capability=capability,
                input_artifacts=[artifact.model_dump(mode="json")],
                params=job_params,
            )
            spec = JobSpec(
                job_type="embedding.embed_text_batch",
                required_capability=capability,
                input_artifacts=[artifact],
                params=job_params,
                privacy=Privacy.CLOUD_OK,
                idempotency_key=idempotency_key,
            )
            jobs_file.write(spec.model_dump_json() + "\n")
    return job_path
