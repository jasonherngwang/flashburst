"""Flashburst command-line interface."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import typer
from rich.console import Console

from flashburst.adapters.runpod_flash import (
    DEFAULT_ARTIFACT_GRANT_EXPIRES_SECONDS,
    DEFAULT_RUNPOD_TIMEOUT_SECONDS,
    RunpodFlashPlanRunner,
)
from flashburst.artifacts.local import LocalArtifactStore, sha256_file
from flashburst.artifacts.s3 import S3ArtifactStore
from flashburst.capabilities.registry import all_capabilities, default_capabilities, get_capability
from flashburst.config import add_capability_import
from flashburst.config import default_db_path, default_workspace_dir
from flashburst.config import configure_s3_store, get_artifact_store_config
from flashburst.db import FlashburstDB
from flashburst.endpoint_scaffold import scaffold_runpod_endpoint
from flashburst.models import ArtifactRef, JobResult, JobSpec
from flashburst.models import CloudProfile, JobStatus
from flashburst.workload_scaffold import job_file_name_for, normalize_package_name
from flashburst.workload_scaffold import scaffold_workload_project
from flashburst.workloads.prepare_embeddings import prepare_embedding_jobs

app = typer.Typer(help="Local-first GPU job distribution with explicit cloud burst.")
console = Console()
leases_app = typer.Typer(help="Lease maintenance.")
configure_app = typer.Typer(help="Friendly configuration commands.")
prepare_app = typer.Typer(help="Friendly workload preparation commands.")
capability_app = typer.Typer(help="Register user-owned capabilities.")
endpoint_app = typer.Typer(help="Scaffold user-owned endpoint adapters.")
workload_app = typer.Typer(help="Scaffold custom workload projects.")
app.add_typer(leases_app, name="leases", hidden=True)
app.add_typer(configure_app, name="configure")
app.add_typer(prepare_app, name="prepare")
app.add_typer(capability_app, name="capability")
app.add_typer(endpoint_app, name="endpoint")
app.add_typer(workload_app, name="workload")


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

    try:
        caps = all_capabilities(workspace=workspace)
    except Exception as exc:
        caps = default_capabilities()
        _print_check("configured capabilities", False, str(exc))
        failures += 1
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


def _pull_s3_artifacts(
    *, database: FlashburstDB, workspace: Path, missing: bool
) -> tuple[int, int]:
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
    max_concurrent_jobs: int,
    run_timeout_seconds: int,
    artifact_grant_expires_seconds: int,
    db: Path,
) -> None:
    if run_timeout_seconds <= 0:
        raise typer.BadParameter("run timeout must be positive")
    if artifact_grant_expires_seconds <= 0:
        raise typer.BadParameter("artifact grant expiry must be positive")
    database = FlashburstDB(db)
    database.init_schema()
    profile = CloudProfile(
        id=profile_id,
        backend="runpod_flash",
        endpoint_id=endpoint_id,
        capability=capability,
        max_concurrent_jobs=max_concurrent_jobs,
        config={
            "run_timeout_seconds": run_timeout_seconds,
            "artifact_grant_expires_seconds": artifact_grant_expires_seconds,
        },
    )
    database.upsert_cloud_profile(profile)
    console.print(f"Saved cloud profile [bold]{profile_id}[/bold].")


def _run_local_attempt(
    *,
    database: FlashburstDB,
    workspace: Path,
    item_job_id: str,
    attempt_id: str,
) -> bool:
    job = database.get_job(item_job_id)
    if job is None:
        raise KeyError(f"job not found: {item_job_id}")

    capability = get_capability(job["required_capability"], workspace=workspace)
    if capability.local_runner is None:
        raise ValueError(
            f"capability does not support local execution: {job['required_capability']}"
        )

    store = LocalArtifactStore(workspace / "artifacts")
    inputs = database.get_job_input_artifacts(item_job_id)
    params = database.get_job_params(item_job_id)
    if len(inputs) != 1:
        database.fail_attempt(
            job_id=item_job_id,
            attempt_id=attempt_id,
            error="local queue runner currently expects exactly one input artifact",
        )
        return False

    input_path = store.path_for_uri(inputs[0].uri)
    relative_output = f"outputs/{item_job_id}/{attempt_id}/result.jsonl"
    output_path = store.ensure_parent_for_uri(f"local://{relative_output}")
    try:
        result = capability.local_runner(input_path, output_path, params)
        output_ref = ArtifactRef(
            uri=f"local://{relative_output}",
            media_type="application/x-ndjson",
            storage="local",
            sha256=sha256_file(output_path),
            size_bytes=output_path.stat().st_size,
            producer_job_id=item_job_id,
        )
        result = JobResult(
            status=result.status,
            output_artifacts=[output_ref],
            metrics=result.metrics,
            logs_uri=result.logs_uri,
            error=result.error,
        )
        database.complete_attempt(job_id=item_job_id, attempt_id=attempt_id, result=result)
        return True
    except Exception as exc:
        database.fail_attempt(job_id=item_job_id, attempt_id=attempt_id, error=str(exc))
        raise


def _import_jobs_file(database: FlashburstDB, jobs_file: Path) -> tuple[list[str], int, int]:
    job_ids: list[str] = []
    inserted = 0
    seen = 0
    with jobs_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            seen += 1
            spec = JobSpec.model_validate_json(line)
            before_count = len(database.list_jobs())
            job_id = database.insert_job(spec)
            after_count = len(database.list_jobs())
            if after_count > before_count:
                inserted += 1
            if job_id not in job_ids:
                job_ids.append(job_id)
    return job_ids, inserted, seen


def _has_unfinished_jobs(database: FlashburstDB, job_ids: list[str]) -> bool:
    unfinished = {JobStatus.QUEUED.value, JobStatus.RUNNING.value}
    for job_id in job_ids:
        job = database.get_job(job_id)
        if job is not None and job["status"] in unfinished:
            return True
    return False


async def _run_queue(
    *,
    database: FlashburstDB,
    workspace: Path,
    job_ids: list[str],
    local_slots: int,
    cloud_slots: int,
    profile: CloudProfile | None,
    poll_interval_seconds: float = 0.25,
    cloud_start_after_seconds: float = 0,
) -> tuple[int, int]:
    capabilities = all_capabilities(workspace=workspace)
    target_capabilities = {
        str(job["required_capability"])
        for job_id in job_ids
        if (job := database.get_job(job_id)) is not None
    }
    local_capabilities = [
        name
        for name in sorted(target_capabilities)
        if name in capabilities and capabilities[name].local_runner is not None
    ]
    if local_slots > 0 and not local_capabilities:
        console.print("No local-capable jobs are available for this queue run.")

    runpod_runner: RunpodFlashPlanRunner | None = None
    if cloud_slots > 0:
        if profile is None:
            raise typer.BadParameter("--profile is required when --cloud-slots is greater than 0")

        def print_remote_status(
            remote_job_id: str, status: str, payload: dict[str, object]
        ) -> None:
            console.print(f"Runpod Flash job {remote_job_id}: {status}")

        runpod_runner = RunpodFlashPlanRunner(
            db=database,
            workspace=workspace,
            s3_store=S3ArtifactStore.from_config(get_artifact_store_config(workspace)),
            status_callback=print_remote_status,
        )

    state_lock = asyncio.Lock()
    local_active = 0
    completed = 0
    skipped = 0

    async def record_result(ok: bool) -> None:
        nonlocal completed, skipped
        async with state_lock:
            if ok:
                completed += 1
            else:
                skipped += 1

    async def local_worker(slot: int) -> None:
        nonlocal local_active
        while True:
            claim = None
            for capability_name in local_capabilities:
                claim = database.claim_next_local_job(
                    worker_id=f"queue-local-{slot}",
                    capability=capability_name,
                    job_ids=job_ids,
                )
                if claim is not None:
                    break
            if claim is None:
                if not _has_unfinished_jobs(database, job_ids):
                    return
                await asyncio.sleep(poll_interval_seconds)
                continue

            async with state_lock:
                local_active += 1
            try:
                ok = await asyncio.to_thread(
                    _run_local_attempt,
                    database=database,
                    workspace=workspace,
                    item_job_id=claim.job_id,
                    attempt_id=claim.attempt_id,
                )
                await record_result(ok)
            finally:
                async with state_lock:
                    local_active -= 1

    async def cloud_worker(slot: int) -> None:
        if profile is None or runpod_runner is None:
            return
        if cloud_start_after_seconds > 0:
            await asyncio.sleep(cloud_start_after_seconds)
        while True:
            if local_slots > 0 and local_capabilities:
                async with state_lock:
                    active = local_active
                if active < local_slots:
                    if not _has_unfinished_jobs(database, job_ids):
                        return
                    await asyncio.sleep(poll_interval_seconds)
                    continue

            claim = database.claim_next_cloud_job(
                worker_id=f"queue-cloud-{slot}",
                capability=profile.capability,
                cloud_profile_id=profile.id,
                job_ids=job_ids,
            )
            if claim is None:
                if not _has_unfinished_jobs(database, job_ids):
                    return
                await asyncio.sleep(poll_interval_seconds)
                continue

            job_id, attempt_id = claim
            try:
                ok = await runpod_runner.run_claimed_job(
                    job_id=job_id,
                    attempt_id=attempt_id,
                    profile=profile,
                )
                await record_result(ok)
            except Exception as exc:
                console.print(f"Runpod Flash job {job_id} failed: {exc}")
                await record_result(False)

    tasks = [
        *(local_worker(slot) for slot in range(local_slots)),
        *(cloud_worker(slot) for slot in range(cloud_slots)),
    ]
    if not tasks:
        raise typer.BadParameter("at least one local or cloud slot is required")
    await asyncio.gather(*tasks)
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


@capability_app.command("add")
def capability_add(
    import_path: str,
    project_root: Path | None = typer.Option(None, "--project-root"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Register a user-owned capability import path for this workspace."""
    add_capability_import(
        workspace=workspace,
        import_path=import_path,
        project_root=str(project_root) if project_root is not None else None,
    )
    console.print(f"Registered capability import [bold]{import_path}[/bold].")


@capability_app.command("list")
def capability_list(
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """List built-in and workspace-registered capabilities."""
    for name in sorted(all_capabilities(workspace=workspace)):
        console.print(name)


@endpoint_app.command("scaffold")
def endpoint_scaffold(
    runner_import: str = typer.Option(..., "--runner-import"),
    output: Path = typer.Option(Path("endpoint.py"), "--output", "-o"),
    endpoint_name: str = typer.Option("flashburst-job", "--name"),
    gpu: str = typer.Option("AMPERE_24", "--gpu"),
    workers_min: int = typer.Option(0, "--workers-min"),
    workers_max: int = typer.Option(1, "--workers-max"),
    idle_timeout: int = typer.Option(30, "--idle-timeout"),
    dependency: list[str] = typer.Option(
        ["httpx>=0.27"],
        "--dependency",
        help="Pip dependency to include in the endpoint bundle. Repeatable.",
    ),
    system_dependency: list[str] = typer.Option(
        [],
        "--system-dependency",
        help="System package to install in the endpoint environment. Repeatable.",
    ),
) -> None:
    """Scaffold a Runpod Flash endpoint wrapper in the user project."""
    path = scaffold_runpod_endpoint(
        output=output,
        runner_import=runner_import,
        endpoint_name=endpoint_name,
        gpu=gpu,
        workers_min=workers_min,
        workers_max=workers_max,
        idle_timeout=idle_timeout,
        dependencies=dependency,
        system_dependencies=system_dependency,
    )
    console.print(f"Wrote Runpod Flash endpoint to [bold]{path}[/bold].")


@workload_app.command("scaffold")
def workload_scaffold(
    target: Path = typer.Argument(Path("."), help="Workload project directory."),
    package: str = typer.Option("jobs", "--package", help="Python package to create/use."),
    capability: str = typer.Option("custom.work", "--capability", help="Capability name."),
    job_type: str | None = typer.Option(
        None, "--job-type", help="Job type. Defaults to capability."
    ),
    runner_import: str | None = typer.Option(
        None,
        "--runner-import",
        help="Existing runner in module:function form. If omitted, a placeholder core.py is created.",
    ),
    runner_name: str = typer.Option("run_job", "--runner-name", help="Placeholder runner name."),
    supports_runpod_flash: bool = typer.Option(
        False,
        "--runpod/--no-runpod",
        help="Whether the generated capability allows Runpod Flash placement.",
    ),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite generated files."),
) -> None:
    """Scaffold the Flashburst adapter/prep files for a user workload."""
    generated = scaffold_workload_project(
        target=target,
        package=package,
        capability=capability,
        job_type=job_type or capability,
        runner_import=runner_import,
        runner_name=runner_name,
        supports_runpod_flash=supports_runpod_flash,
        overwrite=overwrite,
    )
    if generated:
        console.print("Generated workload files:")
        for path in generated:
            console.print(f"- {path}")
    else:
        console.print("No files changed. Re-run with --overwrite to replace existing files.")
    package_name = normalize_package_name(package)
    jobs_file = target / ".flashburst" / "jobs" / job_file_name_for(job_type or capability)
    console.print("")
    console.print("Next:")
    console.print(f"  uv run flashburst init --workspace {target / '.flashburst'}")
    console.print(
        "  uv run flashburst capability add "
        f"{package_name}.capabilities:capability --project-root {target}"
    )
    console.print(f"  uv run python {target / 'prepare_jobs.py'} <source>")
    console.print(f"  uv run flashburst run-queue {jobs_file} --local-slots 1")


@app.command("run-queue")
def run_queue(
    jobs_file: Path,
    local_slots: int = typer.Option(1, "--local-slots", help="Number of local workers."),
    cloud_slots: int = typer.Option(0, "--cloud-slots", help="Number of Runpod workers."),
    profile_id: str | None = typer.Option(None, "--profile"),
    approve_cloud: bool = typer.Option(
        False,
        "--approve-cloud",
        help="Required when cloud slots are enabled.",
    ),
    cloud_start_after_seconds: float = typer.Option(
        0,
        "--cloud-start-after-seconds",
        help="Delay before cloud workers may lease jobs.",
    ),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Run jobs from a shared local-first queue."""
    if local_slots < 0:
        raise typer.BadParameter("--local-slots must be zero or positive")
    if cloud_slots < 0:
        raise typer.BadParameter("--cloud-slots must be zero or positive")
    if cloud_start_after_seconds < 0:
        raise typer.BadParameter("--cloud-start-after-seconds must be zero or positive")
    if cloud_slots > 0 and not approve_cloud:
        console.print("Cloud slots require explicit --approve-cloud.")
        raise typer.Exit(code=1)

    database = FlashburstDB(db)
    database.init_schema()
    job_ids, inserted, seen = _import_jobs_file(database, jobs_file)
    if not job_ids:
        console.print("No jobs found.")
        return

    profile = None
    if cloud_slots > 0:
        if profile_id is None:
            console.print("--profile is required when --cloud-slots is greater than 0.")
            raise typer.Exit(code=1)
        profile = database.get_cloud_profile(profile_id)
        if profile is None:
            console.print(f"Cloud profile {profile_id} was not found.")
            raise typer.Exit(code=1)

    console.print(f"Queued {len(job_ids)} job(s): {inserted} new, {seen - inserted} existing.")
    console.print(f"local_slots={local_slots} cloud_slots={cloud_slots}")
    completed, skipped = asyncio.run(
        _run_queue(
            database=database,
            workspace=workspace,
            job_ids=job_ids,
            local_slots=local_slots,
            cloud_slots=cloud_slots,
            profile=profile,
            cloud_start_after_seconds=cloud_start_after_seconds,
        )
    )
    console.print(f"Queue run complete: {completed} completed, {skipped} skipped.")


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
    profile_id: str = typer.Option("runpod-burst", "--profile"),
    endpoint_id: str = typer.Option(..., "--endpoint-id"),
    capability: str = typer.Option(..., "--capability"),
    max_concurrent_jobs: int = typer.Option(1, "--max-concurrent-jobs"),
    run_timeout_seconds: int = typer.Option(
        DEFAULT_RUNPOD_TIMEOUT_SECONDS,
        "--run-timeout-seconds",
        help="How long Flashburst waits for each Runpod job.",
    ),
    artifact_grant_expires_seconds: int = typer.Option(
        DEFAULT_ARTIFACT_GRANT_EXPIRES_SECONDS,
        "--artifact-grant-expires-seconds",
        help="Lifetime for presigned input/output artifact grants.",
    ),
    db: Path = typer.Option(default_db_path(), "--db"),
) -> None:
    """Friendly command for saving a Runpod Flash profile."""
    _save_cloud_profile(
        profile_id=profile_id,
        endpoint_id=endpoint_id,
        capability=capability,
        max_concurrent_jobs=max_concurrent_jobs,
        run_timeout_seconds=run_timeout_seconds,
        artifact_grant_expires_seconds=artifact_grant_expires_seconds,
        db=db,
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


@prepare_app.command("embeddings")
def prepare_embeddings_friendly(
    input_path: Path,
    capability: str = typer.Option("embedding.fake-deterministic", "--capability"),
    batch_size: int = typer.Option(1, "--batch-size"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Prepare deterministic embedding smoke jobs."""
    workspace.mkdir(parents=True, exist_ok=True)
    job_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability=capability,
        batch_size=batch_size,
    )
    console.print(f"Wrote embedding jobs to [bold]{job_path}[/bold]")


if __name__ == "__main__":
    app()
