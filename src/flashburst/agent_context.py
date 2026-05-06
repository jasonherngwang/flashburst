"""Machine-readable project inspection helpers for agents."""

from __future__ import annotations

import ast
import inspect
import os
import tomllib
from pathlib import Path
from typing import Any

from flashburst.config import (
    get_r2_config,
    list_runpod_profiles,
    load_project_config,
    project_path,
)
from flashburst.workload import (
    final_run_records,
    latest_run_id,
    load_records,
    load_workload,
    read_results,
    run_dir_for,
)


IGNORED_DIRS = {
    ".flashburst",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
}


def _is_url(value: str) -> bool:
    return value.startswith(("http://", "https://"))


def _resolve_input_file(value: str, project_root: Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _walk_project_files(project_root: Path, pattern: str) -> list[Path]:
    paths: list[Path] = []
    for path in project_root.rglob(pattern):
        if any(
            part in IGNORED_DIRS or (part.startswith(".") and part != ".") for part in path.parts
        ):
            continue
        if path.is_file():
            paths.append(path)
    return sorted(paths)


def discover_workloads(project_root: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for path in _walk_project_files(project_root, "*.py"):
        if path.name == "endpoint.py" or path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            arg_names = [arg.arg for arg in node.args.args]
            if len(arg_names) < 3:
                continue
            if arg_names[:3] != ["input_path", "output_path", "params"]:
                continue
            score = 100
            if node.name == "run_job":
                score += 20
            if any(token in node.name for token in ("transcribe", "manifest", "run")):
                score += 10
            if score <= 0:
                continue
            relative = _relative(path, project_root)
            candidates.append(
                {
                    "spec": f"{relative}:{node.name}",
                    "path": relative,
                    "function": node.name,
                    "score": score,
                    "args": arg_names,
                }
            )
    return sorted(candidates, key=lambda item: (-int(item["score"]), str(item["spec"])))


def discover_manifests(project_root: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for path in _walk_project_files(project_root, "*.jsonl"):
        info = inspect_manifest_file(path, project_root=project_root)
        if not info["exists"] or info["records"] == 0:
            continue
        name = path.name
        score = int(info["records"])
        if name == "manifest.local.jsonl":
            score += 100
        elif name == "manifest.jsonl":
            score += 80
        elif name.startswith("manifest"):
            score += 60
        elif name.startswith("input"):
            score += 30
        info["score"] = score
        info["path"] = _relative(path, project_root)
        candidates.append(info)
    return sorted(candidates, key=lambda item: (-int(item["score"]), str(item["path"])))


def discover_project_dependencies(project_root: Path) -> list[str]:
    path = project_root / "pyproject.toml"
    if not path.exists():
        return []
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    raw = data.get("project", {}).get("dependencies", [])
    if not isinstance(raw, list):
        return []
    dependencies: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        normalized = item.split("[", 1)[0].split("=", 1)[0].split("<", 1)[0].split(">", 1)[0]
        normalized = normalized.strip().lower().replace("_", "-")
        if normalized in {"flashburst", "runpod-flash"}:
            continue
        dependencies.append(item)
    return dependencies


def inspect_workload_spec(workload_spec: str, *, project_root: Path) -> dict[str, Any]:
    try:
        function = load_workload(workload_spec, project_root=project_root)
        signature = str(inspect.signature(function))
        return {
            "spec": workload_spec,
            "valid": True,
            "callable": getattr(function, "__name__", workload_spec),
            "signature": signature,
        }
    except Exception as exc:
        return {
            "spec": workload_spec,
            "valid": False,
            "error": str(exc),
        }


def inspect_manifest_file(
    source: Path,
    *,
    project_root: Path,
    stage_fields: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    if not source.exists():
        return {
            "path": source.as_posix(),
            "exists": False,
            "records": 0,
            "keys": [],
            "string_fields": [],
            "candidate_file_fields": list(stage_fields),
            "stage_fields": list(stage_fields),
            "stage_field_status": {},
            "valid": False,
            "error": f"manifest does not exist: {source}",
        }
    records = load_records(source)
    keys: set[str] = set()
    string_fields: set[str] = set()
    candidate_file_fields: set[str] = set()
    stage_field_status: dict[str, dict[str, int]] = {
        field: {
            "present": 0,
            "missing": 0,
            "non_string": 0,
            "url": 0,
            "local_file": 0,
            "missing_file": 0,
        }
        for field in stage_fields
    }

    for record in records:
        keys.update(str(key) for key in record)
        for key, value in record.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            string_fields.add(key)
            if key.endswith("_path") or _resolve_input_file(value, project_root).exists():
                candidate_file_fields.add(key)
        for field, status in stage_field_status.items():
            if field not in record:
                status["missing"] += 1
                continue
            status["present"] += 1
            value = record[field]
            if not isinstance(value, str):
                status["non_string"] += 1
                continue
            if _is_url(value):
                status["url"] += 1
                continue
            if _resolve_input_file(value, project_root).is_file():
                status["local_file"] += 1
            else:
                status["missing_file"] += 1

    for field in stage_fields:
        candidate_file_fields.add(field)

    return {
        "path": source.as_posix(),
        "exists": source.exists(),
        "records": len(records),
        "keys": sorted(keys),
        "string_fields": sorted(string_fields),
        "candidate_file_fields": sorted(candidate_file_fields),
        "stage_fields": list(stage_fields),
        "stage_field_status": stage_field_status,
        "valid": all(
            status["present"] > 0
            and status["non_string"] == 0
            and status["missing"] == 0
            and status["missing_file"] == 0
            for status in stage_field_status.values()
        ),
    }


def latest_run_summary(workspace: Path) -> dict[str, Any] | None:
    selected = latest_run_id(workspace)
    if selected is None:
        return None
    records = read_results(run_dir_for(workspace, selected))
    final_records = final_run_records(records)
    summary: dict[str, int] = {}
    for record in final_records:
        summary[record.status] = summary.get(record.status, 0) + 1
    return {
        "id": selected,
        "records": len(final_records),
        "ledger_records": len(records),
        "summary": summary,
        "outputs": [
            record.output_path
            for record in final_records
            if record.status == "succeeded" and record.output_path
        ],
    }


def build_agent_context(*, workspace: Path, project_root: Path) -> dict[str, Any]:
    project = load_project_config(workspace)
    profiles = list_runpod_profiles(workspace)
    r2_config: dict[str, Any] | None
    try:
        r2_config = get_r2_config(workspace)
    except KeyError:
        r2_config = None
    r2_credentials_present = bool(
        (os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID"))
        and (os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))
    )

    workloads = discover_workloads(project_root)
    manifests = discover_manifests(project_root)
    bound_manifest: dict[str, Any] | None = None
    if isinstance(project.get("manifest"), str):
        manifest_path = Path(project["manifest"])
        if not manifest_path.is_absolute():
            manifest_path = project_root / manifest_path
        bound_manifest = inspect_manifest_file(
            manifest_path,
            project_root=project_root,
            stage_fields=list(project.get("stage_fields") or []),
        )

    workload = None
    if isinstance(project.get("workload"), str):
        workload = inspect_workload_spec(str(project["workload"]), project_root=project_root)

    suggestions: list[str] = []
    if not project:
        suggestions.append("bind a workload and manifest with `flashburst bind`")
    elif workload and not workload.get("valid"):
        suggestions.append("fix the bound workload import")
    elif bound_manifest and not bound_manifest.get("valid"):
        suggestions.append("fix missing stage fields or missing local files in the manifest")
    elif latest_run_summary(workspace) is None:
        suggestions.append("run a local smoke with `flashburst run`")
    if project.get("stage_fields") and r2_config is None:
        suggestions.append("configure R2 before hybrid cloud runs")
    if project.get("stage_fields") and r2_config is not None and not r2_credentials_present:
        suggestions.append("export R2 credential environment variables before hybrid cloud runs")
    if project.get("runpod_profile") and project.get("runpod_profile") not in profiles:
        suggestions.append("configure the bound Runpod profile")

    return {
        "workspace": workspace.as_posix(),
        "project_root": project_root.as_posix(),
        "project_config_path": project_path(workspace).as_posix(),
        "project": project,
        "workload": workload,
        "discovered_workloads": workloads,
        "bound_manifest": bound_manifest,
        "manifests": manifests,
        "project_dependencies": discover_project_dependencies(project_root),
        "runpod_profiles": sorted(profiles),
        "r2": {
            "configured": r2_config is not None,
            "bucket": r2_config.get("bucket") if r2_config else None,
            "endpoint_url": r2_config.get("endpoint_url") if r2_config else None,
            "credentials_present": r2_credentials_present,
        },
        "latest_run": latest_run_summary(workspace),
        "suggested_next_actions": suggestions,
    }
