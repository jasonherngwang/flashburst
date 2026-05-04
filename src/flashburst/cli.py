"""Flashburst command-line interface."""

from __future__ import annotations

import asyncio
import json
import os
from decimal import Decimal
from pathlib import Path

import typer
from rich.console import Console

from flashburst.config import default_db_path, default_workspace_dir
from flashburst.config import configure_s3_store, get_artifact_store_config
from flashburst.capabilities.registry import default_capabilities
from flashburst.db import FlashburstDB
from flashburst.artifacts.s3 import S3ArtifactStore
from flashburst.examples.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import JobSpec
from flashburst.models import CloudProfile
from flashburst.scheduler import approve_plan, create_plan_from_jobs_file, load_plan
from flashburst.worker import run_once
from flashburst.adapters.mock_cloud import MockCloudAdapter
from flashburst.adapters.runpod_flash import RunpodFlashPlanRunner

app = typer.Typer(help="Local-first GPU job distribution with explicit cloud burst.")
console = Console()
worker_app = typer.Typer(help="Run local workers.")
examples_app = typer.Typer(help="Prepare example workloads.")
embeddings_app = typer.Typer(help="Embedding examples.")
storage_app = typer.Typer(help="Configure artifact storage.")
storage_configure_app = typer.Typer(help="Configure artifact storage backends.")
artifacts_app = typer.Typer(help="Inspect and move artifacts.")
cloud_app = typer.Typer(help="Configure cloud execution profiles.")
cloud_profile_app = typer.Typer(help="Configure cloud profiles.")
inspect_app = typer.Typer(help="Inspect completed work.")
leases_app = typer.Typer(help="Lease maintenance.")
configure_app = typer.Typer(help="Friendly configuration commands.")
prepare_app = typer.Typer(help="Friendly workload preparation commands.")
app.add_typer(worker_app, name="worker", hidden=True)
app.add_typer(examples_app, name="examples", hidden=True)
app.add_typer(storage_app, name="storage", hidden=True)
app.add_typer(artifacts_app, name="artifacts", hidden=True)
app.add_typer(cloud_app, name="cloud", hidden=True)
app.add_typer(inspect_app, name="inspect", hidden=True)
app.add_typer(leases_app, name="leases", hidden=True)
app.add_typer(configure_app, name="configure")
app.add_typer(prepare_app, name="prepare")
examples_app.add_typer(embeddings_app, name="embeddings")
storage_app.add_typer(storage_configure_app, name="configure")
cloud_app.add_typer(cloud_profile_app, name="profile")


def _print_check(label: str, ok: bool, detail: str = "") -> bool:
    status = "ok" if ok else "fail"
    color = "green" if ok else "red"
    suffix = f" - {detail}" if detail else ""
    console.print(f"[{color}]{status}[/{color}] {label}{suffix}")
    return ok


def _env_any(*names: str) -> bool:
    return any(bool(os.getenv(name)) for name in names)


def _run_doctor(*, cloud: bool, workspace: Path, db: Path) -> int:
    failures = 0
    if not _print_check("workspace directory", workspace.exists(), str(workspace)):
        failures += 1
    if not _print_check("database", db.exists(), str(db)):
        failures += 1

    if db.exists():
        try:
            database = FlashburstDB(db)
            jobs = database.list_jobs()
            _print_check("database schema", True, f"{len(jobs)} jobs recorded")
        except Exception as exc:
            _print_check("database schema", False, str(exc))
            failures += 1

    caps = default_capabilities()
    _print_check("capability registry", bool(caps), f"{len(caps)} capabilities")

    if cloud:
        try:
            store = get_artifact_store_config(workspace)
            configured = store.get("type") == "s3" and bool(store.get("bucket"))
            if not _print_check("S3/R2 artifact store", configured, store.get("bucket", "")):
                failures += 1
        except Exception as exc:
            _print_check("S3/R2 artifact store", False, str(exc))
            failures += 1

        r2_key = _env_any("R2_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")
        r2_secret = _env_any("R2_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")
        r2_endpoint = _env_any("R2_ENDPOINT_URL", "AWS_ENDPOINT_URL")
        if not _print_check("R2 access key env", r2_key, "R2_* or AWS_*"):
            failures += 1
        if not _print_check("R2 secret env", r2_secret, "R2_* or AWS_*"):
            failures += 1
        if not _print_check("R2 endpoint env", r2_endpoint, "R2_ENDPOINT_URL or AWS_ENDPOINT_URL"):
            failures += 1

        runpod_config = Path.home() / ".runpod" / "config.toml"
        runpod_auth = bool(os.getenv("RUNPOD_API_KEY")) or runpod_config.exists()
        if not _print_check("Runpod auth presence", runpod_auth, "RUNPOD_API_KEY or flash login"):
            failures += 1

        if db.exists():
            profiles = FlashburstDB(db).list_cloud_profiles()
            if not _print_check("cloud profiles", bool(profiles), f"{len(profiles)} configured"):
                failures += 1
    return failures


def _print_results(*, database: FlashburstDB, json_output: bool = False) -> None:
    results = []
    for job in database.list_jobs():
        if not job["result_json"]:
            continue
        result = json.loads(job["result_json"])
        results.append(
            {
                "job_id": job["id"],
                "status": job["status"],
                "required_capability": job["required_capability"],
                "output_artifacts": result.get("output_artifacts", []),
                "metrics": result.get("metrics", {}),
            }
        )
    if json_output:
        typer.echo(json.dumps(results, indent=2, sort_keys=True))
        return
    if not results:
        console.print("No completed results.")
        return
    for item in results:
        outputs = ", ".join(a["uri"] for a in item["output_artifacts"]) or "-"
        console.print(
            f"{item['job_id']} {item['status']} {item['required_capability']} -> {outputs}"
        )


def _pull_s3_artifacts(*, database: FlashburstDB, workspace: Path, missing: bool) -> tuple[int, int]:
    store = S3ArtifactStore.from_config(get_artifact_store_config(workspace))
    pulled = 0
    skipped = 0
    for artifact in database.list_artifacts():
        if artifact["storage"] != "s3":
            continue
        relative = artifact["uri"].removeprefix("s3://")
        destination = workspace / "artifacts" / "pulled" / relative
        if missing and destination.exists():
            skipped += 1
            continue
        store.download_file(artifact["uri"], destination)
        pulled += 1
    return pulled, skipped


def _save_cloud_profile(
    *,
    profile_id: str,
    endpoint_id: str,
    capability: str,
    estimated_cost_per_job_usd: str,
    max_concurrent_jobs: int,
    db: Path,
) -> None:
    database = FlashburstDB(db)
    database.init_schema()
    profile = CloudProfile(
        id=profile_id,
        backend="runpod_flash",
        endpoint_id=endpoint_id,
        capability=capability,
        estimated_cost_per_job_usd=Decimal(estimated_cost_per_job_usd),
        max_concurrent_jobs=max_concurrent_jobs,
    )
    database.upsert_cloud_profile(profile)
    console.print(f"Saved cloud profile [bold]{profile_id}[/bold].")


def _run_plan_by_id(*, plan_id: str, workspace: Path, db: Path) -> tuple[int, int]:
    database = FlashburstDB(db)
    database.init_schema()
    plan_model = load_plan(workspace, plan_id)
    if not plan_model.approved:
        console.print(f"Plan {plan_id} is not approved.")
        raise typer.Exit(code=1)
    mock = MockCloudAdapter(db=database, workspace=workspace)
    runpod_runner: RunpodFlashPlanRunner | None = None
    completed = 0
    skipped = 0
    for item in plan_model.items:
        if item.placement_kind == "mock_cloud":
            if plan_model.budget_limit_usd is not None:
                ok = database.reserve_budget(
                    plan_id=plan_model.id,
                    limit_usd=plan_model.budget_limit_usd,
                    amount_usd=item.estimated_cost_usd,
                )
                if not ok:
                    console.print(f"Budget blocked job {item.job_id}.")
                    skipped += 1
                    continue
            if mock.run_item(item):
                completed += 1
            else:
                skipped += 1
        elif item.placement_kind == "runpod_flash":
            if item.cloud_profile_id is None:
                console.print(f"Runpod Flash item for job {item.job_id} has no profile.")
                skipped += 1
                continue
            profile = database.get_cloud_profile(item.cloud_profile_id)
            if profile is None:
                console.print(f"Cloud profile {item.cloud_profile_id} was not found.")
                skipped += 1
                continue
            if runpod_runner is None:
                try:
                    runpod_runner = RunpodFlashPlanRunner(
                        db=database,
                        workspace=workspace,
                        s3_store=S3ArtifactStore.from_config(get_artifact_store_config(workspace)),
                    )
                except ValueError as exc:
                    console.print(str(exc))
                    raise typer.Exit(code=1) from exc
            if plan_model.budget_limit_usd is not None:
                ok = database.reserve_budget(
                    plan_id=plan_model.id,
                    limit_usd=plan_model.budget_limit_usd,
                    amount_usd=item.estimated_cost_usd,
                )
                if not ok:
                    console.print(f"Budget blocked job {item.job_id}.")
                    skipped += 1
                    continue
            if runpod_runner is None:
                raise RuntimeError("Runpod Flash runner was not initialized")
            if asyncio.run(runpod_runner.run_item(item=item, profile=profile)):
                completed += 1
            else:
                skipped += 1
        else:
            skipped += 1
    console.print(f"Plan run complete: {completed} completed, {skipped} skipped.")
    return completed, skipped


@app.command()
def init(workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w")) -> None:
    """Initialize a local Flashburst workspace."""
    workspace.mkdir(parents=True, exist_ok=True)
    db_path = workspace / "flashburst.db"
    FlashburstDB(db_path).init_schema()
    console.print(f"Initialized Flashburst workspace at [bold]{workspace}[/bold]")


@app.command()
def status(
    results: bool = typer.Option(False, "--results", help="Also show completed results."),
    pull: bool = typer.Option(False, "--pull", help="Pull S3/R2 artifacts before showing results."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Show current job status counts."""
    if not db.exists():
        console.print("No Flashburst database found. Run `flashburst init` first.")
        raise typer.Exit(code=1)
    database = FlashburstDB(db)
    jobs = database.list_jobs()
    counts: dict[str, int] = {}
    for job in jobs:
        counts[job["status"]] = counts.get(job["status"], 0) + 1
    if not counts:
        console.print("No jobs.")
    else:
        for status_name, count in sorted(counts.items()):
            console.print(f"{status_name}: {count}")
    if pull:
        pulled, skipped = _pull_s3_artifacts(database=database, workspace=workspace, missing=True)
        console.print(f"Pulled {pulled} artifacts ({skipped} skipped).")
        results = True
    if results:
        _print_results(database=database)


@app.command(hidden=True)
def doctor(
    cloud: bool = typer.Option(False, "--cloud", help="Also check cloud/R2 prerequisites."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Check local configuration without starting paid cloud work."""
    if _run_doctor(cloud=cloud, workspace=workspace, db=db):
        raise typer.Exit(code=1)


@app.command()
def check(
    cloud: bool = typer.Option(False, "--cloud", help="Also check cloud/R2 prerequisites."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Friendly alias for preflight checks."""
    if _run_doctor(cloud=cloud, workspace=workspace, db=db):
        raise typer.Exit(code=1)


@app.command("capabilities", hidden=True)
def capabilities() -> None:
    """List MVP in-repo capabilities."""
    for name in sorted(default_capabilities()):
        console.print(name)


@app.command(hidden=True)
def submit(jobs_file: Path, db: Path = typer.Option(default_db_path(), "--db")) -> None:
    """Submit jobs from a JSONL file of JobSpec objects."""
    database = FlashburstDB(db)
    database.init_schema()
    inserted = 0
    seen = 0
    with jobs_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            seen += 1
            spec = JobSpec.model_validate_json(line)
            before_count = len(database.list_jobs())
            database.insert_job(spec)
            after_count = len(database.list_jobs())
            if after_count > before_count:
                inserted += 1
    console.print(f"Submitted {inserted} new jobs ({seen - inserted} duplicates skipped).")


@app.command(hidden=True)
def plan(
    jobs_file: Path,
    allow_cloud: bool = typer.Option(False, "--allow-cloud"),
    backend: str | None = typer.Option(None, "--backend"),
    profile: str | None = typer.Option(None, "--profile"),
    budget: str | None = typer.Option(None, "--budget"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Create a simple execution plan from a jobs JSONL file."""
    database = FlashburstDB(db)
    database.init_schema()
    created = create_plan_from_jobs_file(
        db=database,
        workspace=workspace,
        jobs_file=jobs_file,
        allow_cloud=allow_cloud,
        backend=backend,
        profile_id=profile,
        budget_usd=Decimal(budget) if budget is not None else None,
    )
    console.print(f"Created plan [bold]{created.id}[/bold] with {len(created.items)} items.")
    if created.budget_limit_usd is not None:
        console.print(f"Budget: ${created.budget_limit_usd}")


@app.command()
def preview(
    jobs_file: Path,
    cloud: bool = typer.Option(False, "--cloud", help="Allow cloud placement."),
    profile: str | None = typer.Option(None, "--profile"),
    backend: str | None = typer.Option(None, "--backend"),
    budget: str | None = typer.Option(None, "--budget"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Friendly alias for creating a reviewable execution plan."""
    database = FlashburstDB(db)
    database.init_schema()
    created = create_plan_from_jobs_file(
        db=database,
        workspace=workspace,
        jobs_file=jobs_file,
        allow_cloud=cloud,
        backend=backend,
        profile_id=profile,
        budget_usd=Decimal(budget) if budget is not None else None,
    )
    placements: dict[str, int] = {}
    for item in created.items:
        key = item.placement_kind.value if hasattr(item.placement_kind, "value") else item.placement_kind
        placements[key] = placements.get(key, 0) + 1
    console.print(f"Created plan [bold]{created.id}[/bold] with {len(created.items)} items.")
    for placement, count in sorted(placements.items()):
        console.print(f"{placement}: {count}")
    if created.budget_limit_usd is not None:
        console.print(f"Budget: ${created.budget_limit_usd}")
    console.print(f"Run with: flashburst execute {created.id} --approve")


@app.command(hidden=True)
def approve(
    plan_id: str,
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Approve a plan for execution."""
    approved = approve_plan(workspace, plan_id)
    console.print(f"Approved plan [bold]{approved.id}[/bold].")


@app.command("run", hidden=True)
def run_plan(
    plan_id: str,
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Run an approved plan."""
    _run_plan_by_id(plan_id=plan_id, workspace=workspace, db=db)


@app.command()
def execute(
    plan_id: str,
    approve: bool = typer.Option(False, "--approve", help="Approve and run the plan."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Approve and run a saved plan from the friendly workflow."""
    plan_model = load_plan(workspace, plan_id)
    if not plan_model.approved:
        if not approve:
            console.print(f"Plan {plan_id} is not approved. Re-run with --approve to execute it.")
            raise typer.Exit(code=1)
        approve_plan(workspace, plan_id)
        console.print(f"Approved plan [bold]{plan_id}[/bold].")
    _run_plan_by_id(plan_id=plan_id, workspace=workspace, db=db)


@storage_configure_app.command("s3")
def storage_configure_s3(
    bucket: str = typer.Option(..., "--bucket"),
    provider: str = typer.Option("r2", "--provider"),
    endpoint_url: str | None = typer.Option(None, "--endpoint-url"),
    region: str = typer.Option("auto", "--region"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Configure non-secret S3-compatible artifact store settings."""
    configure_s3_store(
        workspace=workspace,
        provider=provider,
        bucket=bucket,
        endpoint_url=endpoint_url,
        region=region,
    )
    console.print(f"Configured {provider} artifact store for bucket [bold]{bucket}[/bold].")
    console.print("Credentials are read from R2_* or AWS_* environment variables at runtime.")


@configure_app.command("r2")
def configure_r2(
    bucket: str = typer.Option(..., "--bucket"),
    endpoint_url: str | None = typer.Option(None, "--endpoint-url"),
    region: str = typer.Option("auto", "--region"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Friendly command for configuring Cloudflare R2 artifact storage."""
    configure_s3_store(
        workspace=workspace,
        provider="r2",
        bucket=bucket,
        endpoint_url=endpoint_url,
        region=region,
    )
    console.print(f"Configured r2 artifact store for bucket [bold]{bucket}[/bold].")
    console.print("Credentials are read from R2_* or AWS_* environment variables at runtime.")


@configure_app.command("runpod")
def configure_runpod(
    profile_id: str = typer.Option("bge-small-burst", "--profile"),
    endpoint_id: str = typer.Option(..., "--endpoint-id"),
    capability: str = typer.Option("embedding.bge-small-en-v1.5", "--capability"),
    estimated_cost_per_job_usd: str = typer.Option("0.05", "--estimated-cost-per-job-usd"),
    max_concurrent_jobs: int = typer.Option(1, "--max-concurrent-jobs"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Friendly command for saving a Runpod Flash profile."""
    _save_cloud_profile(
        profile_id=profile_id,
        endpoint_id=endpoint_id,
        capability=capability,
        estimated_cost_per_job_usd=estimated_cost_per_job_usd,
        max_concurrent_jobs=max_concurrent_jobs,
        db=db,
    )


@artifacts_app.command("inspect")
def artifacts_inspect(db: Path = typer.Option(default_db_path(), "--db")) -> None:
    """List artifacts recorded in local state."""
    database = FlashburstDB(db)
    if not db.exists():
        console.print("No Flashburst database found. Run `flashburst init` first.")
        raise typer.Exit(code=1)
    artifacts = database.list_artifacts()
    if not artifacts:
        console.print("No artifacts.")
        return
    for artifact in artifacts:
        console.print(
            f"{artifact['uri']} {artifact['media_type']} {artifact['size_bytes'] or '-'} bytes"
        )


@artifacts_app.command("put")
def artifacts_put(
    source: Path,
    uri: str,
    media_type: str = typer.Option("application/octet-stream", "--media-type"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Upload a local file to the configured S3-compatible artifact store."""
    store = S3ArtifactStore.from_config(get_artifact_store_config(workspace))
    ref = store.upload_file(source, uri, media_type=media_type)
    typer.echo(ref.model_dump_json(indent=2))


@artifacts_app.command("grant-read")
def artifacts_grant_read(
    uri: str,
    expires_seconds: int = typer.Option(3600, "--expires-seconds"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Create a presigned read grant for an S3 artifact."""
    store = S3ArtifactStore.from_config(get_artifact_store_config(workspace))
    grant = store.presign_get(uri, expires_seconds=expires_seconds)
    typer.echo(grant.model_dump_json(indent=2))


@artifacts_app.command("grant-write")
def artifacts_grant_write(
    uri: str,
    media_type: str = typer.Option("application/octet-stream", "--media-type"),
    expires_seconds: int = typer.Option(3600, "--expires-seconds"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Create a presigned write grant for an S3 artifact."""
    store = S3ArtifactStore.from_config(get_artifact_store_config(workspace))
    grant = store.presign_put(uri, media_type=media_type, expires_seconds=expires_seconds)
    typer.echo(grant.model_dump_json(indent=2))


@artifacts_app.command("pull")
def artifacts_pull(
    missing: bool = typer.Option(False, "--missing"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Pull recorded S3 artifacts into the local workspace."""
    database = FlashburstDB(db)
    pulled, skipped = _pull_s3_artifacts(database=database, workspace=workspace, missing=missing)
    console.print(f"Pulled {pulled} artifacts ({skipped} skipped).")


@inspect_app.command("results")
def inspect_results(
    db: Path = typer.Option(default_db_path(), "--db"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect completed job results."""
    database = FlashburstDB(db)
    if not db.exists():
        console.print("No Flashburst database found. Run `flashburst init` first.")
        raise typer.Exit(code=1)
    _print_results(database=database, json_output=json_output)


@inspect_app.command("plan")
def inspect_plan(
    plan_id: str,
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect a saved execution plan with current job and budget state."""
    database = FlashburstDB(db)
    if not db.exists():
        console.print("No Flashburst database found. Run `flashburst init` first.")
        raise typer.Exit(code=1)
    try:
        plan_model = load_plan(workspace, plan_id)
    except FileNotFoundError as exc:
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    items = []
    for item in plan_model.items:
        job = database.get_job(item.job_id)
        items.append(
            {
                "job_id": item.job_id,
                "placement_kind": item.placement_kind.value
                if hasattr(item.placement_kind, "value")
                else item.placement_kind,
                "cloud_profile_id": item.cloud_profile_id,
                "estimated_cost_usd": str(item.estimated_cost_usd),
                "job_status": job["status"] if job else "missing",
                "job_error": job["error"] if job else "job not found",
            }
        )
    payload = {
        "id": plan_model.id,
        "approved": plan_model.approved,
        "budget_limit_usd": str(plan_model.budget_limit_usd)
        if plan_model.budget_limit_usd is not None
        else None,
        "budget_ledger": database.get_budget_ledger(plan_model.id),
        "items": items,
    }

    if json_output:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return

    approved = "approved" if plan_model.approved else "not-approved"
    budget = payload["budget_limit_usd"] or "-"
    console.print(f"{plan_model.id} {approved} budget={budget} items={len(items)}")
    ledger = payload["budget_ledger"]
    if ledger:
        console.print(
            f"budget ledger: reserved={ledger['reserved_usd']} "
            f"limit={ledger['limit_usd']} status={ledger['status']}"
        )
    for row in items:
        error = f" error={row['job_error']}" if row["job_error"] else ""
        profile = row["cloud_profile_id"] or "-"
        console.print(
            f"{row['job_id']} {row['job_status']} {row['placement_kind']} "
            f"profile={profile} cost={row['estimated_cost_usd']}{error}"
        )


@inspect_app.command("attempts")
def inspect_attempts(
    job_id: str | None = typer.Option(None, "--job-id"),
    db: Path = typer.Option(default_db_path(), "--db"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Inspect local and cloud attempts."""
    database = FlashburstDB(db)
    if not db.exists():
        console.print("No Flashburst database found. Run `flashburst init` first.")
        raise typer.Exit(code=1)
    attempts = database.list_attempts(job_id=job_id)
    if json_output:
        typer.echo(json.dumps(attempts, indent=2, sort_keys=True, default=str))
        return
    if not attempts:
        console.print("No attempts.")
        return
    for attempt in attempts:
        remote = attempt["remote_job_id"] or "-"
        cost = attempt["reserved_cost_usd"] or "-"
        error = f" error={attempt['error']}" if attempt["error"] else ""
        console.print(
            f"{attempt['id']} job={attempt['job_id']} {attempt['status']} "
            f"{attempt['placement_kind']} remote={remote} cost={cost}{error}"
        )


@leases_app.command("retry-expired")
def leases_retry_expired(db: Path = typer.Option(default_db_path(), "--db")) -> None:
    """Mark expired local leases retryable."""
    database = FlashburstDB(db)
    if not db.exists():
        console.print("No Flashburst database found. Run `flashburst init` first.")
        raise typer.Exit(code=1)
    retried = database.retry_expired_leases()
    console.print(f"Retried {retried} expired leases.")


@cloud_profile_app.command("set")
def cloud_profile_set(
    profile_id: str,
    endpoint_id: str = typer.Option(..., "--endpoint-id"),
    capability: str = typer.Option(..., "--capability"),
    estimated_cost_per_job_usd: str = typer.Option("0.05", "--estimated-cost-per-job-usd"),
    max_concurrent_jobs: int = typer.Option(1, "--max-concurrent-jobs"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Create or update a Runpod Flash cloud profile."""
    _save_cloud_profile(
        profile_id=profile_id,
        endpoint_id=endpoint_id,
        capability=capability,
        estimated_cost_per_job_usd=estimated_cost_per_job_usd,
        max_concurrent_jobs=max_concurrent_jobs,
        db=db,
    )


@embeddings_app.command()
def prepare(
    input_path: Path,
    capability: str = typer.Option("embedding.fake-deterministic", "--capability"),
    batch_size: int = typer.Option(4, "--batch-size"),
    model_name: str | None = typer.Option(None, "--model-name"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Prepare embedding JobSpecs from text or JSONL input."""
    workspace.mkdir(parents=True, exist_ok=True)
    params = {"model_name": model_name} if model_name is not None else None
    job_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability=capability,
        batch_size=batch_size,
        params=params,
    )
    console.print(f"Wrote embedding jobs to [bold]{job_path}[/bold]")


@prepare_app.command("embeddings")
def prepare_embeddings_friendly(
    input_path: Path,
    capability: str = typer.Option("embedding.bge-small-en-v1.5", "--capability"),
    batch_size: int = typer.Option(1, "--batch-size"),
    model_name: str | None = typer.Option(
        "sentence-transformers/all-MiniLM-L6-v2",
        "--model-name",
    ),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Friendly command for preparing embedding jobs."""
    workspace.mkdir(parents=True, exist_ok=True)
    params = {"model_name": model_name} if model_name is not None else None
    job_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability=capability,
        batch_size=batch_size,
        params=params,
    )
    console.print(f"Wrote embedding jobs to [bold]{job_path}[/bold]")


@worker_app.command()
def run(
    id: str = typer.Option(..., "--id"),
    capability: str = typer.Option(..., "--capability"),
    once: bool = typer.Option(False, "--once"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Run a local worker."""
    database = FlashburstDB(db)
    database.init_schema()
    processed = 0
    while True:
        did_work = run_once(
            db=database,
            workspace=workspace,
            worker_id=id,
            capability_name=capability,
        )
        if not did_work:
            if processed == 0:
                console.print("No eligible jobs.")
            break
        processed += 1
        console.print(f"Processed job #{processed}")
        if once:
            break


if __name__ == "__main__":
    app()
