"""Deterministic embedding capability for local and mock testing."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from time import perf_counter

from flashburst.capabilities.registry import Capability
from flashburst.models import ArtifactRef, CapabilitySpec, JobResult
from flashburst.time import utc_now

VECTOR_DIM = 8


def embed_text(text: str, *, dim: int = VECTOR_DIM) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values = []
    for idx in range(dim):
        raw = int.from_bytes(digest[idx * 2 : idx * 2 + 2], "big")
        values.append(round((raw / 65535.0) * 2.0 - 1.0, 6))
    return values


def run(input_path: Path, output_path: Path, params: dict) -> JobResult:
    started = perf_counter()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with (
        input_path.open("r", encoding="utf-8") as source,
        output_path.open("w", encoding="utf-8") as sink,
    ):
        for line in source:
            if not line.strip():
                continue
            item = json.loads(line)
            text = str(item["text"])
            record = {
                "id": item.get("id", f"text-{count}"),
                "text": text if params.get("include_text", False) else None,
                "embedding": embed_text(text),
            }
            sink.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    elapsed = perf_counter() - started
    artifact = ArtifactRef(
        uri=f"local://{output_path.name}",
        media_type="application/x-ndjson",
        storage="local",
        created_at=utc_now(),
    )
    return JobResult(
        status="succeeded",
        output_artifacts=[artifact],
        metrics={
            "model_name": "fake-deterministic",
            "input_count": count,
            "vector_dim": VECTOR_DIM,
            "embedding_seconds": elapsed,
        },
    )


capability = Capability(
    spec=CapabilitySpec(
        name="embedding.fake-deterministic",
        job_type="embedding.embed_text_batch",
        version="v1",
        supports_local=True,
        supports_runpod_flash=False,
    ),
    local_runner=run,
)
