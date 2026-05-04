"""Sentence Transformers embedding capability for the tracer bullet."""

from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter
from typing import Any

from flashburst.capabilities.registry import Capability
from flashburst.models import ArtifactRef, CapabilitySpec, JobResult
from flashburst.time import utc_now

DEFAULT_MODEL_NAME = "BAAI/bge-small-en-v1.5"
CAPABILITY_NAME = "embedding.bge-small-en-v1.5"


def _load_model(model_name: str) -> Any:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "embedding.bge-small-en-v1.5 requires the embeddings extra: "
            "`uv sync --extra embeddings --extra dev`"
        ) from exc
    return SentenceTransformer(model_name)


def _device_name() -> str:
    try:
        import torch

        if torch.cuda.is_available():
            return torch.cuda.get_device_name(0)
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            return "mps"
        return "cpu"
    except Exception:
        return "unknown"


def _as_list(vector: Any) -> list[float]:
    if hasattr(vector, "tolist"):
        return list(vector.tolist())
    return list(vector)


def _read_records(input_path: Path) -> tuple[list[str], list[str]]:
    ids: list[str] = []
    texts: list[str] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if not line.strip():
                continue
            item = json.loads(line)
            ids.append(str(item.get("id", f"text-{index}")))
            texts.append(str(item["text"]))
    return ids, texts


def run(input_path: Path, output_path: Path, params: dict) -> JobResult:
    started = perf_counter()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    model_name = str(params.get("model_name") or DEFAULT_MODEL_NAME)
    batch_size = int(params.get("batch_size") or 32)
    normalize_embeddings = bool(params.get("normalize_embeddings", True))

    ids, texts = _read_records(input_path)

    model_started = perf_counter()
    model = _load_model(model_name)
    model_load_seconds = perf_counter() - model_started

    embed_started = perf_counter()
    vectors = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=normalize_embeddings,
        show_progress_bar=False,
    )
    embedding_seconds = perf_counter() - embed_started

    vector_dim = 0
    with output_path.open("w", encoding="utf-8") as handle:
        for item_id, vector in zip(ids, vectors, strict=True):
            values = _as_list(vector)
            vector_dim = len(values)
            handle.write(
                json.dumps(
                    {"id": item_id, "embedding": values},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )

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
            "model_name": model_name,
            "device": _device_name(),
            "input_count": len(texts),
            "vector_dim": vector_dim,
            "model_load_seconds": model_load_seconds,
            "embedding_seconds": embedding_seconds,
            "total_seconds": perf_counter() - started,
        },
    )


capability = Capability(
    spec=CapabilitySpec(
        name=CAPABILITY_NAME,
        job_type="embedding.embed_text_batch",
        version="v1",
        supports_local=True,
        supports_mock_cloud=False,
        supports_runpod_flash=True,
        min_vram_gb=4,
    ),
    local_runner=run,
)
