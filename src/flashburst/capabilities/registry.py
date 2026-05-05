"""Minimal in-repo capability registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from flashburst.models import CapabilitySpec, JobResult

LocalRunner = Callable[[Path, Path, dict], JobResult]


@dataclass(frozen=True)
class Capability:
    spec: CapabilitySpec
    local_runner: LocalRunner | None = None


def built_in_capabilities() -> dict[str, Capability]:
    from flashburst.capabilities.fake_embeddings import capability as fake_embeddings

    return {
        fake_embeddings.spec.name: fake_embeddings,
    }


def default_capabilities() -> dict[str, Capability]:
    return built_in_capabilities()


def all_capabilities(*, workspace: Path | None = None) -> dict[str, Capability]:
    capabilities = built_in_capabilities()
    if workspace is not None:
        from flashburst.capabilities.loader import configured_capabilities

        capabilities.update(configured_capabilities(workspace))
    return capabilities


def get_capability(name: str, *, workspace: Path | None = None) -> Capability:
    capabilities = all_capabilities(workspace=workspace)
    try:
        return capabilities[name]
    except KeyError as exc:
        known = ", ".join(sorted(capabilities))
        raise KeyError(f"unknown capability '{name}'. known capabilities: {known}") from exc
