"""Workspace configuration helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def default_workspace_dir() -> Path:
    return Path(".flashburst")


def default_db_path() -> Path:
    return default_workspace_dir() / "flashburst.db"


def config_path(workspace: Path | None = None) -> Path:
    return (workspace or default_workspace_dir()) / "config.json"


def load_config(workspace: Path | None = None) -> dict[str, Any]:
    path = config_path(workspace)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_config(config: dict[str, Any], workspace: Path | None = None) -> None:
    path = config_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")


def configure_s3_store(
    *,
    workspace: Path,
    provider: str,
    bucket: str,
    endpoint_url: str | None = None,
    region: str = "auto",
) -> dict[str, Any]:
    config = load_config(workspace)
    config["artifact_store"] = {
        "type": "s3",
        "provider": provider,
        "bucket": bucket,
        "endpoint_url": endpoint_url,
        "region": region,
    }
    save_config(config, workspace)
    return config


def get_artifact_store_config(workspace: Path) -> dict[str, Any]:
    config = load_config(workspace)
    store = config.get("artifact_store")
    if not isinstance(store, dict):
        raise ValueError("artifact store is not configured")
    return store
