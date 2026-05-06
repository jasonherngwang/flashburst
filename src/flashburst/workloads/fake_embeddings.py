"""Deterministic file-based workload used by tests and local smoke checks."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from time import perf_counter
from typing import Any

VECTOR_DIM = 8


def embed_text(text: str, *, dim: int = VECTOR_DIM) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values = []
    for idx in range(dim):
        raw = int.from_bytes(digest[idx * 2 : idx * 2 + 2], "big")
        values.append(round((raw / 65535.0) * 2.0 - 1.0, 6))
    return values


def run_job(input_path: Path, output_path: Path, params: dict[str, Any]) -> dict[str, Any]:
    started = perf_counter()
    record = json.loads(input_path.read_text(encoding="utf-8"))
    text = str(record.get("text") or record.get("source") or "")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {
                "id": record.get("id", input_path.stem),
                "text": text if params.get("include_text", False) else None,
                "embedding": embed_text(text),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "status": "succeeded",
        "metrics": {
            "model_name": "fake-deterministic",
            "input_count": 1,
            "vector_dim": VECTOR_DIM,
            "embedding_seconds": perf_counter() - started,
        },
    }
