"""Core Flashburst data contracts."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
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
    CREATED = "created"
    LEASED = "leased"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class PlacementKind(str, Enum):
    LOCAL = "local"
    MOCK_CLOUD = "mock_cloud"
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
    max_cost_usd: Decimal | None = None


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
    supports_mock_cloud: bool = True
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


class WorkerSpec(StrictModel):
    id: str
    capabilities: list[str]
    placement_kind: Literal["local"] = "local"
    gpu_device: str | None = None
    gpu_name: str | None = None
    vram_gb: int | None = None


class CloudProfile(StrictModel):
    id: str
    backend: Literal["mock_cloud", "runpod_flash"]
    capability: str
    estimated_cost_per_job_usd: Decimal = Decimal("0")
    max_concurrent_jobs: int = 1
    endpoint_id: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)


class Attempt(StrictModel):
    id: str
    job_id: str
    placement_kind: PlacementKind
    status: AttemptStatus
    worker_id: str | None = None
    cloud_profile_id: str | None = None
    remote_job_id: str | None = None
    reserved_cost_usd: Decimal | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime


class Lease(StrictModel):
    id: str
    job_id: str
    attempt_id: str
    worker_id: str
    expires_at: datetime
    heartbeat_at: datetime
    created_at: datetime


class BudgetLedger(StrictModel):
    id: str
    plan_id: str
    limit_usd: Decimal
    reserved_usd: Decimal = Decimal("0")
    status: Literal["open", "exhausted", "closed"] = "open"
    created_at: datetime
    updated_at: datetime


class PlanItem(StrictModel):
    job_id: str
    placement_kind: PlacementKind
    worker_id: str | None = None
    cloud_profile_id: str | None = None
    estimated_cost_usd: Decimal = Decimal("0")


class Plan(StrictModel):
    id: str
    items: list[PlanItem]
    budget_limit_usd: Decimal | None = None
    approved: bool = False
    created_at: datetime
