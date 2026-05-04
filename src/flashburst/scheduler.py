"""Planning and plan-file helpers."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from flashburst.db import FlashburstDB
from flashburst.ids import new_id
from flashburst.models import JobSpec, PlacementKind, Plan, PlanItem, Privacy
from flashburst.time import utc_now


def plans_dir(workspace: Path) -> Path:
    path = workspace / "plans"
    path.mkdir(parents=True, exist_ok=True)
    return path


def plan_path(workspace: Path, plan_id: str) -> Path:
    return plans_dir(workspace) / f"{plan_id}.json"


def save_plan(workspace: Path, plan: Plan) -> Path:
    path = plan_path(workspace, plan.id)
    path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    return path


def load_plan(workspace: Path, plan_id: str) -> Plan:
    path = plan_path(workspace, plan_id)
    if not path.exists():
        raise FileNotFoundError(f"plan not found: {plan_id}")
    return Plan.model_validate_json(path.read_text(encoding="utf-8"))


def approve_plan(workspace: Path, plan_id: str) -> Plan:
    plan = load_plan(workspace, plan_id)
    approved = Plan(
        id=plan.id,
        items=plan.items,
        budget_limit_usd=plan.budget_limit_usd,
        approved=True,
        created_at=plan.created_at,
    )
    save_plan(workspace, approved)
    return approved


def create_plan_from_jobs_file(
    *,
    db: FlashburstDB,
    workspace: Path,
    jobs_file: Path,
    allow_cloud: bool,
    backend: str | None,
    budget_usd: Decimal | None,
    profile_id: str | None = None,
    estimated_cost_per_job_usd: Decimal = Decimal("0.05"),
) -> Plan:
    items: list[PlanItem] = []
    profile = db.get_cloud_profile(profile_id) if profile_id is not None else None
    if profile_id is not None and profile is None:
        raise ValueError(f"cloud profile not found: {profile_id}")
    with jobs_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            spec = JobSpec.model_validate_json(line)
            job_id = db.insert_job(spec)
            job = db.get_job(job_id)
            if job and job["status"] == "succeeded":
                continue
            can_use_cloud = allow_cloud and spec.privacy == Privacy.CLOUD_OK
            if (
                can_use_cloud
                and profile is not None
                and profile.backend == "runpod_flash"
                and spec.required_capability == profile.capability
            ):
                items.append(
                    PlanItem(
                        job_id=job_id,
                        placement_kind=PlacementKind.RUNPOD_FLASH,
                        cloud_profile_id=profile.id,
                        estimated_cost_usd=profile.estimated_cost_per_job_usd,
                    )
                )
            elif can_use_cloud and backend == "mock":
                items.append(
                    PlanItem(
                        job_id=job_id,
                        placement_kind=PlacementKind.MOCK_CLOUD,
                        cloud_profile_id="mock",
                        estimated_cost_usd=estimated_cost_per_job_usd,
                    )
                )
            else:
                items.append(
                    PlanItem(
                        job_id=job_id,
                        placement_kind=PlacementKind.LOCAL,
                        estimated_cost_usd=Decimal("0"),
                    )
                )
    plan = Plan(
        id=new_id("plan"),
        items=items,
        budget_limit_usd=budget_usd,
        approved=False,
        created_at=utc_now(),
    )
    save_plan(workspace, plan)
    return plan
