"""Workspace configuration helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def default_workspace_dir() -> Path:
    return Path(".flashburst")


def config_path(workspace: Path | None = None) -> Path:
    return (workspace or default_workspace_dir()) / "config.json"


def project_path(workspace: Path | None = None) -> Path:
    return (workspace or default_workspace_dir()) / "project.json"


def load_config(workspace: Path | None = None) -> dict[str, Any]:
    path = config_path(workspace)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_config(config: dict[str, Any], workspace: Path | None = None) -> None:
    path = config_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")


def load_project_config(workspace: Path | None = None) -> dict[str, Any]:
    path = project_path(workspace)
    if not path.exists():
        return {}
    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("project config must be a JSON object")
    return config


def save_project_config(config: dict[str, Any], workspace: Path | None = None) -> None:
    path = project_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")


def bind_project(
    *,
    workspace: Path,
    workload: str,
    manifest: str,
    params: dict[str, Any] | None = None,
    stage_fields: list[str] | None = None,
    runpod_profile: str | None = None,
) -> dict[str, Any]:
    if not workload:
        raise ValueError("workload is required")
    if not manifest:
        raise ValueError("manifest is required")
    config: dict[str, Any] = {
        "workload": workload,
        "manifest": manifest,
        "params": params or {},
        "stage_fields": list(dict.fromkeys(stage_fields or [])),
    }
    if runpod_profile:
        config["runpod_profile"] = runpod_profile
    save_project_config(config, workspace)
    return config


def configure_r2_store(
    *,
    workspace: Path,
    bucket: str,
    account_id: str | None = None,
    endpoint_url: str | None = None,
    region: str = "auto",
) -> dict[str, Any]:
    """Save non-secret R2 artifact settings for remote input/output staging."""
    if not bucket:
        raise ValueError("R2 bucket is required")
    if endpoint_url is None:
        if not account_id:
            raise ValueError("either account_id or endpoint_url is required")
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
    config = load_config(workspace)
    config["artifact_store"] = {
        "type": "s3",
        "provider": "r2",
        "bucket": bucket,
        "account_id": account_id,
        "endpoint_url": endpoint_url,
        "region": region or "auto",
    }
    save_config(config, workspace)
    return config


def get_r2_config(workspace: Path) -> dict[str, Any]:
    config = load_config(workspace)
    store = config.get("artifact_store")
    if not isinstance(store, dict) or store.get("provider") != "r2":
        raise KeyError("R2 artifact store is not configured")
    return dict(store)


def configure_runpod_profile(
    *,
    workspace: Path,
    profile: str,
    endpoint_id: str,
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    if timeout_seconds <= 0:
        raise ValueError("run timeout must be positive")
    config = load_config(workspace)
    profiles = config.setdefault("runpod_profiles", {})
    if not isinstance(profiles, dict):
        raise ValueError("runpod_profiles config must be an object")
    profiles[profile] = {
        "endpoint_id": endpoint_id,
        "timeout_seconds": timeout_seconds,
    }
    save_config(config, workspace)
    return config


def get_runpod_profile(workspace: Path, profile: str) -> dict[str, Any]:
    config = load_config(workspace)
    profiles = config.get("runpod_profiles", {})
    if not isinstance(profiles, dict):
        raise ValueError("runpod_profiles config must be an object")
    selected = profiles.get(profile)
    if not isinstance(selected, dict):
        raise KeyError(f"Runpod profile not configured: {profile}")
    return selected


def list_runpod_profiles(workspace: Path) -> dict[str, dict[str, Any]]:
    config = load_config(workspace)
    profiles = config.get("runpod_profiles", {})
    if not isinstance(profiles, dict):
        raise ValueError("runpod_profiles config must be an object")
    return {
        str(name): dict(value)
        for name, value in profiles.items()
        if isinstance(name, str) and isinstance(value, dict)
    }
