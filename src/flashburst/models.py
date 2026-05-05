"""Core Flashburst data contracts."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptStatus(str, Enum):
    LEASED = "leased"
    SUBMITTED = "submitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXPIRED = "expired"


class PlacementKind(str, Enum):
    LOCAL = "local"
    RUNPOD_FLASH = "runpod_flash"


class Privacy(str, Enum):
    LOCAL_ONLY = "local_only"
    CLOUD_OK = "cloud_ok"


class ArtifactRef(StrictModel):
    uri: str
    media_type: str
    storage: Literal["local", "s3"]
    sha256: str | None = None
    size_bytes: int | None = None
    producer_job_id: str | None = None
    created_at: datetime | None = None


class ArtifactGrant(StrictModel):
    artifact_uri: str
    method: Literal["GET", "PUT"]
    url: str
    expires_at: datetime
    content_type: str | None = None


class JobSpec(StrictModel):
    job_type: str
    required_capability: str
    input_artifacts: list[ArtifactRef]
    params: dict[str, Any] = Field(default_factory=dict)
    privacy: Privacy
    idempotency_key: str


class JobResult(StrictModel):
    status: Literal["succeeded", "failed"]
    output_artifacts: list[ArtifactRef] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    logs_uri: str | None = None
    error: str | None = None


class CapabilitySpec(StrictModel):
    name: str
    job_type: str
    version: str = "v1"
    supports_local: bool = True
    supports_runpod_flash: bool = False
    min_vram_gb: int | None = None


class ExecutionEnvelope(StrictModel):
    schema_version: str = "1"
    job_id: str
    attempt_id: str
    capability: str
    params: dict[str, Any] = Field(default_factory=dict)
    input_artifacts: list[ArtifactRef] = Field(default_factory=list)
    output_artifacts: list[ArtifactRef] = Field(default_factory=list)
    artifact_grants: list[ArtifactGrant] = Field(default_factory=list)


class CloudProfile(StrictModel):
    id: str
    backend: Literal["runpod_flash"]
    capability: str
    max_concurrent_jobs: int = 1
    endpoint_id: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
