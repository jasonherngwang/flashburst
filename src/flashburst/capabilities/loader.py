"""Dynamic capability loading for user projects."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from flashburst.capabilities.registry import Capability
from flashburst.config import get_capability_imports


@contextmanager
def _sys_path_entry(path: Path | None) -> Iterator[None]:
    if path is None:
        yield
        return
    resolved = str(path.resolve())
    inserted = False
    if resolved not in sys.path:
        sys.path.insert(0, resolved)
        inserted = True
    try:
        yield
    finally:
        if inserted:
            try:
                sys.path.remove(resolved)
            except ValueError:
                pass


def load_object(import_path: str, *, project_root: Path | None = None):
    if ":" not in import_path:
        raise ValueError("capability import path must be in module:attribute format")
    module_name, attribute = import_path.split(":", 1)
    if not module_name or not attribute:
        raise ValueError("capability import path must be in module:attribute format")
    with _sys_path_entry(project_root):
        module = importlib.import_module(module_name)
    target = module
    for part in attribute.split("."):
        target = getattr(target, part)
    return target


def load_capability(import_path: str, *, project_root: Path | None = None) -> Capability:
    loaded = load_object(import_path, project_root=project_root)
    if not isinstance(loaded, Capability):
        raise TypeError(f"{import_path} did not resolve to a Capability")
    return loaded


def configured_capabilities(workspace: Path) -> dict[str, Capability]:
    capabilities: dict[str, Capability] = {}
    for item in get_capability_imports(workspace):
        root = Path(item["project_root"]) if item["project_root"] else None
        capability = load_capability(item["import_path"], project_root=root)
        capabilities[capability.spec.name] = capability
    return capabilities
