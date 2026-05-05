"""Small public data contracts for Flashburst runs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


PlacementKind = Literal["local", "flash"]
RunStatus = Literal["succeeded", "failed", "skipped"]


class JobResult(StrictModel):
    status: Literal["succeeded", "failed"]
    output_text: str | None = None
    output_media_type: str = "application/x-ndjson"
    metrics: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


class WorkItem(StrictModel):
    id: str
    input: dict[str, Any]
    params: dict[str, Any] = Field(default_factory=dict)
    flash_ok: bool = False
    input_path: str


class RunRecord(StrictModel):
    job_id: str
    status: RunStatus
    placement: PlacementKind | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    input_path: str | None = None
    input_artifacts: list[dict[str, Any]] = Field(default_factory=list)
    output_path: str | None = None
    output_media_type: str | None = None
    output_artifact: dict[str, Any] | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    remote_job_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
