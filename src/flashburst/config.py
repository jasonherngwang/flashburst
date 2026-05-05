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


def add_capability_import(
    *,
    workspace: Path,
    import_path: str,
    project_root: str | None = None,
) -> dict[str, Any]:
    config = load_config(workspace)
    capabilities = config.setdefault("capabilities", [])
    if not isinstance(capabilities, list):
        raise ValueError("capabilities config must be a list")
    entry = {
        "import_path": import_path,
        "project_root": project_root,
    }
    capabilities[:] = [
        item
        for item in capabilities
        if not isinstance(item, dict) or item.get("import_path") != import_path
    ]
    capabilities.append(entry)
    save_config(config, workspace)
    return config


def get_capability_imports(workspace: Path) -> list[dict[str, str | None]]:
    config = load_config(workspace)
    raw = config.get("capabilities", [])
    if not isinstance(raw, list):
        raise ValueError("capabilities config must be a list")
    imports: list[dict[str, str | None]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        import_path = item.get("import_path")
        if not isinstance(import_path, str) or not import_path:
            continue
        project_root = item.get("project_root")
        imports.append(
            {
                "import_path": import_path,
                "project_root": project_root if isinstance(project_root, str) else None,
            }
        )
    return imports
