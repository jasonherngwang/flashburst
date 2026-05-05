"""Flashburst command-line interface."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

from flashburst.agent_context import (
    build_agent_context,
    discover_manifests,
    discover_project_dependencies,
    discover_workloads,
    inspect_manifest_file,
    inspect_workload_spec,
)
from flashburst.adapters.dbos_queue import FlashConfig, drain_items, inspect_queue_state
from flashburst.adapters.runpod_flash import DEFAULT_RUNPOD_TIMEOUT_SECONDS
from flashburst.config import (
    bind_project,
    configure_r2_store,
    configure_runpod_profile,
    default_workspace_dir,
    get_r2_config,
    get_runpod_profile,
    list_runpod_profiles,
    load_project_config,
)
from flashburst.endpoint_scaffold import parse_endpoint_env, scaffold_runpod_endpoint
from flashburst.workload import (
    final_run_records,
    latest_run_id,
    prepare_items,
    read_results,
    run_dir_for,
    utc_stamp,
)

app = typer.Typer(help="Durable local-first batch runner with optional Runpod Flash burst.")
configure_app = typer.Typer(help="Save local non-secret configuration.")
manifest_app = typer.Typer(help="Inspect and validate workload manifests.")
workload_app = typer.Typer(help="Inspect workload entrypoints.")
app.add_typer(configure_app, name="configure")
app.add_typer(manifest_app, name="manifest")
app.add_typer(workload_app, name="workload")
console = Console()


def _print_check(label: str, ok: bool, detail: str = "") -> bool:
    status = "ok" if ok else "fail"
    color = "green" if ok else "red"
    suffix = f" - {detail}" if detail else ""
    console.print(f"[{color}]{status}[/{color}] {label}{suffix}")
    return ok


def _echo_json(payload: dict | list) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


def _resolve_source(source: Path, project_root: Path) -> Path:
    return source if source.is_absolute() else project_root / source


def _valid_auto_stage_fields(manifest_info: dict) -> list[str]:
    fields: list[str] = []
    for field, status in manifest_info.get("stage_field_status", {}).items():
        if (
            status.get("present", 0) > 0
            and status.get("local_file", 0) > 0
            and status.get("missing", 0) == 0
            and status.get("non_string", 0) == 0
            and status.get("url", 0) == 0
            and status.get("missing_file", 0) == 0
        ):
            fields.append(str(field))
    return fields


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "-_" else "-" for char in value).strip("-")


def _select_workload(workload: str | None, project_root: Path) -> str:
    if workload:
        return workload
    candidates = discover_workloads(project_root)
    if not candidates:
        raise typer.BadParameter(
            "could not discover a workload. Add a function with "
            "`input_path, output_path, params` args or pass --workload."
        )
    return str(candidates[0]["spec"])


def _select_manifest(manifest: Path | None, project_root: Path) -> Path:
    if manifest is not None:
        return manifest
    candidates = discover_manifests(project_root)
    if not candidates:
        raise typer.BadParameter("could not discover a JSONL manifest. Pass --manifest.")
    return Path(str(candidates[0]["path"]))


def _endpoint_runner_import(workload_spec: str, project_root: Path) -> str:
    module_or_path, _, function_name = workload_spec.partition(":")
    if not function_name:
        raise typer.BadParameter("runner import must be path.py:function or module:function")
    path = Path(module_or_path)
    if path.suffix != ".py":
        return workload_spec
    if path.is_absolute():
        try:
            path = path.relative_to(project_root.resolve())
        except ValueError:
            pass
    module_name = ".".join(path.with_suffix("").parts)
    return f"{module_name}:{function_name}"


def _bind_project_defaults(
    *,
    workload: str | None,
    manifest: Path | None,
    params_json: str,
    stage_field: list[str],
    profile: str | None,
    project_root: Path,
    workspace: Path,
) -> dict[str, Any]:
    params = json.loads(params_json)
    if not isinstance(params, dict):
        raise typer.BadParameter("--params-json must decode to a JSON object")
    selected_workload = _select_workload(workload, project_root)
    selected_manifest = _select_manifest(manifest, project_root)
    workload_info = inspect_workload_spec(selected_workload, project_root=project_root)
    if not workload_info["valid"]:
        raise typer.BadParameter(f"invalid workload: {workload_info['error']}")
    manifest_path = _resolve_source(selected_manifest, project_root)
    manifest_info = inspect_manifest_file(
        manifest_path,
        project_root=project_root,
        stage_fields=stage_field,
    )
    if not manifest_info["exists"]:
        raise typer.BadParameter(f"manifest does not exist: {selected_manifest}")
    if not stage_field:
        candidate_fields = manifest_info.get("candidate_file_fields") or []
        if candidate_fields:
            candidate_info = inspect_manifest_file(
                manifest_path,
                project_root=project_root,
                stage_fields=[str(field) for field in candidate_fields],
            )
            stage_field = _valid_auto_stage_fields(candidate_info)
            manifest_info = inspect_manifest_file(
                manifest_path,
                project_root=project_root,
                stage_fields=stage_field,
            )
    if stage_field and not manifest_info["valid"]:
        raise typer.BadParameter("manifest stage-field validation failed")
    return bind_project(
        workspace=workspace,
        workload=selected_workload,
        manifest=selected_manifest.as_posix(),
        params=params,
        stage_fields=stage_field,
        runpod_profile=profile,
    )


def _scaffold_endpoint(
    *,
    runner_import: str | None,
    output: Path,
    endpoint_name: str | None,
    gpu: str,
    workers_min: int,
    workers_max: int,
    idle_timeout: int,
    dependency: list[str],
    system_dependency: list[str],
    endpoint_env: list[str],
    endpoint_env_from: list[str],
    project_root: Path,
    workspace: Path,
) -> Path:
    project_config = load_project_config(workspace)
    selected_runner = runner_import or str(project_config.get("workload") or "")
    if not selected_runner:
        selected_runner = _select_workload(None, project_root)
    selected_runner = _endpoint_runner_import(selected_runner, project_root)
    selected_name = endpoint_name or _safe_name(project_root.resolve().name) or "flashburst-job"
    dependencies = dependency or discover_project_dependencies(project_root)
    selected_output = output if output.is_absolute() else project_root / output
    try:
        env, env_from = parse_endpoint_env(endpoint_env, endpoint_env_from)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    return scaffold_runpod_endpoint(
        output=selected_output,
        runner_import=selected_runner,
        endpoint_name=selected_name,
        gpu=gpu,
        workers_min=workers_min,
        workers_max=workers_max,
        idle_timeout=idle_timeout,
        dependencies=dependencies,
        system_dependencies=system_dependency,
        env=env,
        env_from=env_from,
    )


@app.command(hidden=True)
def init(workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w")) -> None:
    """Create the local Flashburst workspace directory."""
    workspace.mkdir(parents=True, exist_ok=True)
    console.print(f"Initialized Flashburst workspace at [bold]{workspace}[/bold]")


@app.command()
def check(
    flash: bool = typer.Option(False, "--flash", help="Also check Runpod Flash settings."),
    profile: str = typer.Option("flash-burst", "--profile"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Check local DBOS and optional Runpod Flash configuration."""
    failures = 0
    if not _print_check("workspace directory", workspace.exists(), str(workspace)):
        failures += 1
    dbos_url = os.getenv("DBOS_SYSTEM_DATABASE_URL") or f"sqlite:///{workspace / 'dbos.sqlite'}"
    _print_check("DBOS system database", True, dbos_url)
    profiles = list_runpod_profiles(workspace)
    _print_check("Runpod Flash profiles", True, f"{len(profiles)} configured")
    if flash:
        try:
            selected = get_runpod_profile(workspace, profile)
            if not _print_check("Runpod Flash endpoint", bool(selected.get("endpoint_id"))):
                failures += 1
        except Exception as exc:
            _print_check("Runpod Flash endpoint", False, str(exc))
            failures += 1
        auth = (
            bool(os.getenv("RUNPOD_API_KEY")) or (Path.home() / ".runpod" / "config.toml").exists()
        )
        if not _print_check("Runpod auth presence", auth):
            failures += 1
        try:
            r2_config = get_r2_config(workspace)
            console.print(
                "[green]ok[/green] R2 artifact store - "
                f"{r2_config.get('bucket')} at {r2_config.get('endpoint_url')}"
            )
            r2_auth = bool(
                (os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID"))
                and (os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))
            )
            auth_detail = "env present" if r2_auth else "env missing"
            console.print(
                f"[{'green' if r2_auth else 'yellow'}]"
                f"{'ok' if r2_auth else 'warn'}"
                f"[/{'green' if r2_auth else 'yellow'}] "
                f"R2 credential env - {auth_detail}"
            )
        except KeyError:
            console.print(
                "[yellow]skip[/yellow] R2 artifact store - "
                "not configured; required only for --stage-field remote file staging"
            )
    if failures:
        raise typer.Exit(code=1)


@app.command("run")
def run_workload(
    workload: str | None = typer.Argument(
        None,
        help="Workload as path.py:function or module:function. Defaults to project binding.",
    ),
    source: Path | None = typer.Argument(
        None,
        help="JSONL input file. Text lines are accepted too. Defaults to project binding.",
    ),
    run_id: str | None = typer.Option(None, "--run-id", help="Run id. Defaults to UTC timestamp."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to each job."),
    limit: int | None = typer.Option(None, "--limit", help="Limit input records."),
    local_mode: bool = typer.Option(
        False,
        "--local",
        help="Use saved project defaults for a local-only smoke run.",
    ),
    hybrid_mode: bool = typer.Option(
        False,
        "--hybrid",
        help="Use saved project defaults for a mixed local/Runpod Flash run.",
    ),
    local_slots: int = typer.Option(1, "--local-slots", help="Number of local DBOS workers."),
    flash_slots: int = typer.Option(0, "--flash-slots", help="Number of Runpod Flash workers."),
    flash_ok: bool = typer.Option(False, "--flash-ok", help="Allow inputs to leave the machine."),
    approve_flash: bool = typer.Option(
        False, "--approve-flash", help="Required when --flash-slots is positive."
    ),
    profile: str = typer.Option("flash-burst", "--profile", help="Runpod Flash profile."),
    stage_field: list[str] = typer.Option(
        [],
        "--stage-field",
        help=(
            "Top-level input field containing a local file path to stage through R2 "
            "for flash jobs. Repeatable."
        ),
    ),
    artifact_url_ttl_seconds: int | None = typer.Option(
        None,
        "--artifact-url-ttl-seconds",
        help=(
            "Presigned R2 URL TTL for staged flash inputs/outputs. Defaults to the "
            "Runpod timeout plus margin, capped at 7 days."
        ),
    ),
    dbos_database_url: str | None = typer.Option(
        None,
        "--dbos-database-url",
        help="DBOS system database URL. Defaults to DBOS_SYSTEM_DATABASE_URL or workspace SQLite.",
    ),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Root added to sys.path."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Run a file-based Python workload through DBOS queues."""
    if local_mode and hybrid_mode:
        raise typer.BadParameter("choose either --local or --hybrid")
    project_config = load_project_config(workspace)
    project_stage_fields: list[str] = []
    using_project_defaults = local_mode or hybrid_mode or workload is None or source is None
    if using_project_defaults:
        if not project_config:
            raise typer.BadParameter(
                "no project binding found. Run `flashburst bind` or pass WORKLOAD and SOURCE."
            )
        workload = workload or str(project_config.get("workload") or "")
        manifest = project_config.get("manifest")
        source = source or Path(str(manifest or ""))
        if params_json == "{}" and isinstance(project_config.get("params"), dict):
            params_json = json.dumps(project_config["params"])
        if isinstance(project_config.get("stage_fields"), list):
            project_stage_fields = [str(field) for field in project_config["stage_fields"]]
        if profile == "flash-burst" and isinstance(project_config.get("runpod_profile"), str):
            profile = str(project_config["runpod_profile"])
    if not workload:
        raise typer.BadParameter("workload is required")
    if source is None or not str(source):
        raise typer.BadParameter("source manifest is required")
    if local_mode:
        local_slots = 1
        flash_slots = 0
        stage_field = []
    if hybrid_mode:
        local_slots = max(local_slots, 1)
        flash_slots = max(flash_slots, 1)
        flash_ok = True
    if flash_slots > 0 and not stage_field and project_stage_fields:
        stage_field = project_stage_fields
    if local_slots < 0:
        raise typer.BadParameter("--local-slots must be zero or positive")
    if flash_slots < 0:
        raise typer.BadParameter("--flash-slots must be zero or positive")
    if local_slots == 0 and flash_slots == 0:
        raise typer.BadParameter("at least one local or flash slot is required")
    if flash_slots > 0 and not approve_flash:
        console.print("Runpod Flash slots require explicit --approve-flash.")
        raise typer.Exit(code=1)
    if flash_slots > 0 and not flash_ok:
        console.print("Runpod Flash slots require --flash-ok so data movement is explicit.")
        raise typer.Exit(code=1)
    if stage_field and flash_slots == 0:
        raise typer.BadParameter("--stage-field requires --flash-slots")
    if artifact_url_ttl_seconds is not None and artifact_url_ttl_seconds <= 0:
        raise typer.BadParameter("--artifact-url-ttl-seconds must be positive")
    if artifact_url_ttl_seconds is not None and not stage_field:
        raise typer.BadParameter("--artifact-url-ttl-seconds only applies with --stage-field")

    params = json.loads(params_json)
    if not isinstance(params, dict):
        raise typer.BadParameter("--params-json must decode to a JSON object")
    selected_run_id = run_id or f"run-{utc_stamp()}"
    workspace.mkdir(parents=True, exist_ok=True)
    source_path = _resolve_source(source, project_root)
    items = prepare_items(
        source=source_path,
        workspace=workspace,
        run_id=selected_run_id,
        params=params,
        flash_ok=flash_ok,
        limit=limit,
    )

    flash_config = None
    if flash_slots:
        profile_config = get_runpod_profile(workspace, profile)
        unique_stage_fields = tuple(dict.fromkeys(stage_field))
        if unique_stage_fields:
            try:
                get_r2_config(workspace)
            except KeyError:
                console.print(
                    "--stage-field requires R2 configuration. Run "
                    "`flashburst configure r2 --bucket ... --account-id ...` first."
                )
                raise typer.Exit(code=1)
        flash_config = FlashConfig(
            endpoint_id=str(profile_config["endpoint_id"]),
            timeout_seconds=int(
                profile_config.get("timeout_seconds", DEFAULT_RUNPOD_TIMEOUT_SECONDS)
            ),
            stage_fields=unique_stage_fields,
            artifact_url_ttl_seconds=artifact_url_ttl_seconds,
        )

    console.print(
        f"Prepared {len(items)} item(s) for run [bold]{selected_run_id}[/bold]; "
        f"local_slots={local_slots} flash_slots={flash_slots}"
    )
    succeeded, failed, skipped_existing = drain_items(
        workload_spec=workload,
        items=items,
        workspace=workspace,
        run_id=selected_run_id,
        local_slots=local_slots,
        flash_slots=flash_slots,
        project_root=project_root,
        flash_config=flash_config,
        database_url=dbos_database_url,
    )
    console.print(
        "Run complete: "
        f"{succeeded} succeeded, {failed} failed, {skipped_existing} already complete."
    )


@app.command()
def status(
    run_id: str | None = typer.Option(None, "--run-id", help="Run id. Defaults to latest run."),
    results: bool = typer.Option(False, "--results", help="Also show result records."),
    json_output: bool = typer.Option(False, "--json", help="Emit final JSON result records."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Show run status counts."""
    selected_run_id = run_id or latest_run_id(workspace)
    if selected_run_id is None:
        console.print("No Flashburst run found.")
        raise typer.Exit(code=1)
    run_dir = run_dir_for(workspace, selected_run_id)
    records = read_results(run_dir)
    final_records = final_run_records(records)
    if json_output:
        typer.echo(
            json.dumps(
                [record.model_dump(mode="json") for record in final_records],
                indent=2,
                sort_keys=True,
            )
        )
        return
    console.print(f"run: {selected_run_id}")
    if not records:
        console.print("No results.")
        return
    summary: dict[str, int] = {}
    for record in final_records:
        summary[record.status] = summary.get(record.status, 0) + 1
    for status_name, count in sorted(summary.items()):
        console.print(f"{status_name}: {count}")
    if results:
        for record in final_records:
            output = record.output_path or "-"
            placement = record.placement or "-"
            console.print(f"{record.job_id} {record.status} {placement} -> {output}")


@app.command("queue")
def queue_status(
    run_id: str | None = typer.Option(None, "--run-id", help="Run id. Defaults to latest run."),
    all_runs: bool = typer.Option(False, "--all", help="Show all Flashburst DBOS queues."),
    details: bool = typer.Option(False, "--details", help="Show recent workflow rows."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
    limit: int = typer.Option(50, "--limit", help="Maximum workflow rows to include."),
    dbos_database_url: str | None = typer.Option(
        None,
        "--dbos-database-url",
        help="DBOS system database URL. Defaults to DBOS_SYSTEM_DATABASE_URL or workspace SQLite.",
    ),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Show DBOS queue workflow state."""
    if all_runs and run_id:
        raise typer.BadParameter("choose either --run-id or --all")
    selected_run_id = None
    if not all_runs:
        selected_run_id = run_id or latest_run_id(workspace)
        if selected_run_id is None:
            console.print("No Flashburst run found.")
            raise typer.Exit(code=1)
    try:
        payload = inspect_queue_state(
            workspace=workspace,
            run_id=selected_run_id,
            database_url=dbos_database_url,
            limit=limit,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if json_output:
        _echo_json(payload)
        return

    if selected_run_id:
        console.print(f"run: {selected_run_id}")
    else:
        console.print("run: all")
    console.print(f"database: {payload['database_url']}")
    summary = payload["summary"]
    if not summary:
        console.print("No DBOS queue workflows found.")
        return
    for item in summary:
        console.print(f"{item['queue_name']} {item['status']}: {item['count']}")
    if details:
        console.print("workflows:")
        for workflow in payload["workflows"]:
            queue_name = workflow.get("queue_name") or "-"
            status_name = workflow.get("status") or "-"
            name = workflow.get("name") or "-"
            workflow_id = workflow.get("workflow_id") or "-"
            dequeued_at = workflow.get("dequeued_at")
            queued = "queued" if dequeued_at is None else "dequeued"
            console.print(f"{workflow_id} {status_name} {queued} {queue_name} {name}")


@app.command("context")
def agent_context(
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit machine-readable JSON.",
        hidden=True,
    ),
    text: bool = typer.Option(False, "--text", help="Emit a concise human-readable summary."),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Workload project root."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Show agent-readable project state and suggested next actions."""
    payload = build_agent_context(workspace=workspace, project_root=project_root)
    if json_output or not text:
        _echo_json(payload)
        return
    console.print(f"workspace: {payload['workspace']}")
    project = payload.get("project") or {}
    if project:
        console.print(f"workload: {project.get('workload')}")
        console.print(f"manifest: {project.get('manifest')}")
        stage_fields = ", ".join(project.get("stage_fields") or []) or "-"
        console.print(f"stage_fields: {stage_fields}")
    else:
        console.print("project: not bound")
    latest = payload.get("latest_run")
    if latest:
        console.print(f"latest_run: {latest['id']} {latest['summary']}")
    suggestions = payload.get("suggested_next_actions") or []
    if suggestions:
        console.print("suggested_next_actions:")
        for action in suggestions:
            console.print(f"- {action}")


@app.command("bind")
def bind(
    workload: str | None = typer.Option(None, "--workload", help="Workload as path.py:function."),
    manifest: Path | None = typer.Option(None, "--manifest", help="Default JSONL manifest."),
    params_json: str = typer.Option("{}", "--params-json", help="Default params JSON object."),
    stage_field: list[str] = typer.Option(
        [],
        "--stage-field",
        help=(
            "Top-level local file field to stage for cloud jobs. Repeatable. "
            "If omitted, bind auto-detects existing local *_path fields."
        ),
    ),
    profile: str | None = typer.Option(None, "--profile", help="Default Runpod profile."),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Workload project root."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Discover and save workload defaults for short agent-native commands."""
    config = _bind_project_defaults(
        workload=workload,
        manifest=manifest,
        params_json=params_json,
        stage_field=stage_field,
        profile=profile,
        project_root=project_root,
        workspace=workspace,
    )
    console.print(
        f"Bound workload [bold]{config['workload']}[/bold] "
        f"to manifest [bold]{config['manifest']}[/bold]."
    )


@workload_app.command("inspect")
def workload_inspect(
    workload: str = typer.Argument(..., help="Workload as path.py:function."),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Workload project root."),
) -> None:
    """Inspect whether a workload entrypoint can be imported."""
    payload = inspect_workload_spec(workload, project_root=project_root)
    if json_output:
        _echo_json(payload)
        return
    status_text = "valid" if payload["valid"] else "invalid"
    console.print(f"{status_text}: {workload}")
    if payload.get("signature"):
        console.print(f"signature: {payload['signature']}")
    if payload.get("error"):
        console.print(f"error: {payload['error']}")


@manifest_app.command("inspect")
def manifest_inspect(
    source: Path = typer.Argument(..., help="JSONL manifest."),
    stage_field: list[str] = typer.Option(
        [],
        "--stage-field",
        help="Top-level local file field to validate. Repeatable.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Workload project root."),
) -> None:
    """Inspect manifest records, keys, and likely local file fields."""
    payload = inspect_manifest_file(
        _resolve_source(source, project_root),
        project_root=project_root,
        stage_fields=stage_field,
    )
    if json_output:
        _echo_json(payload)
        return
    console.print(f"manifest: {source}")
    console.print(f"records: {payload['records']}")
    console.print(f"candidate_file_fields: {', '.join(payload['candidate_file_fields']) or '-'}")
    if stage_field:
        console.print(f"valid: {payload['valid']}")


@manifest_app.command("validate")
def manifest_validate(
    source: Path = typer.Argument(..., help="JSONL manifest."),
    stage_field: list[str] = typer.Option(
        [],
        "--stage-field",
        help="Top-level local file field to validate. Repeatable.",
    ),
    json_output: bool = typer.Option(False, "--json", help="Emit machine-readable JSON."),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Workload project root."),
) -> None:
    """Validate manifest shape and optional staged local file fields."""
    payload = inspect_manifest_file(
        _resolve_source(source, project_root),
        project_root=project_root,
        stage_fields=stage_field,
    )
    if json_output:
        _echo_json(payload)
    if not payload["valid"]:
        if not json_output:
            console.print("manifest validation failed")
        raise typer.Exit(code=1)
    if not json_output:
        console.print("manifest validation passed")


@configure_app.command("runpod")
def configure_runpod(
    profile: str = typer.Option("flash-burst", "--profile"),
    endpoint_id: str = typer.Option(..., "--endpoint-id"),
    timeout_seconds: int = typer.Option(
        DEFAULT_RUNPOD_TIMEOUT_SECONDS,
        "--timeout-seconds",
    ),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Save a named Runpod Flash endpoint profile."""
    configure_runpod_profile(
        workspace=workspace,
        profile=profile,
        endpoint_id=endpoint_id,
        timeout_seconds=timeout_seconds,
    )
    console.print(f"Saved Runpod Flash profile [bold]{profile}[/bold].")


@configure_app.command("r2")
def configure_r2(
    bucket: str = typer.Option(..., "--bucket"),
    account_id: str | None = typer.Option(
        None,
        "--account-id",
        help="Cloudflare account id. Used to derive the R2 endpoint URL.",
    ),
    endpoint_url: str | None = typer.Option(
        None,
        "--endpoint-url",
        help="Explicit S3-compatible endpoint URL. Overrides --account-id.",
    ),
    region: str = typer.Option("auto", "--region"),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Save non-secret Cloudflare R2 artifact settings."""
    try:
        configure_r2_store(
            workspace=workspace,
            bucket=bucket,
            account_id=account_id,
            endpoint_url=endpoint_url,
            region=region,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(
        f"Saved R2 artifact store [bold]{bucket}[/bold]. "
        "Credentials are read from R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY or AWS_* env vars."
    )


@app.command("scaffold")
def scaffold(
    runner_import: str | None = typer.Option(None, "--runner-import"),
    output: Path = typer.Option(Path("endpoint.py"), "--output", "-o"),
    endpoint_name: str | None = typer.Option(None, "--name"),
    gpu: str = typer.Option("AMPERE_24", "--gpu"),
    workers_min: int = typer.Option(0, "--workers-min"),
    workers_max: int = typer.Option(1, "--workers-max"),
    idle_timeout: int = typer.Option(30, "--idle-timeout"),
    dependency: list[str] = typer.Option(
        [],
        "--dependency",
        help="Pip dependency to include in the endpoint bundle. Repeatable.",
    ),
    system_dependency: list[str] = typer.Option(
        [],
        "--system-dependency",
        help="System package to install in the endpoint environment. Repeatable.",
    ),
    endpoint_env: list[str] = typer.Option(
        [],
        "--env",
        help="Endpoint environment literal as NAME=value. Repeatable; avoid secrets.",
    ),
    endpoint_env_from: list[str] = typer.Option(
        [],
        "--env-from",
        help="Endpoint environment variable to copy from os.environ at deploy time. Repeatable.",
    ),
    project_root: Path = typer.Option(Path("."), "--project-root", help="Workload project root."),
    workspace: Path = typer.Option(default_workspace_dir(), "--workspace", "-w"),
) -> None:
    """Scaffold a Runpod Flash endpoint from the project binding."""
    path = _scaffold_endpoint(
        runner_import=runner_import,
        output=output,
        endpoint_name=endpoint_name,
        gpu=gpu,
        workers_min=workers_min,
        workers_max=workers_max,
        idle_timeout=idle_timeout,
        dependency=dependency,
        system_dependency=system_dependency,
        endpoint_env=endpoint_env,
        endpoint_env_from=endpoint_env_from,
        project_root=project_root,
        workspace=workspace,
    )
    console.print(f"Wrote Runpod Flash endpoint to [bold]{path}[/bold].")


if __name__ == "__main__":
    app()
