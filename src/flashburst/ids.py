"""Identifier and idempotency helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from uuid import uuid4


STABLE_ARTIFACT_IDENTITY_KEYS = (
    "uri",
    "storage",
    "media_type",
    "sha256",
    "size_bytes",
    "producer_job_id",
)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def compute_idempotency_key(
    *,
    job_type: str,
    required_capability: str,
    input_artifacts: list[dict[str, Any]],
    params: dict[str, Any],
    runner_version: str = "v1",
    output_schema_version: str = "v1",
) -> str:
    stable_artifacts = [
        {key: artifact.get(key) for key in STABLE_ARTIFACT_IDENTITY_KEYS if key in artifact}
        for artifact in input_artifacts
    ]
    payload = {
        "job_type": job_type,
        "required_capability": required_capability,
        "input_artifacts": stable_artifacts,
        "params": params,
        "runner_version": runner_version,
        "output_schema_version": output_schema_version,
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return f"idem_{digest}"
