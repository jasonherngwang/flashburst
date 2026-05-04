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


def default_capabilities() -> dict[str, Capability]:
    from flashburst.capabilities.bge_embeddings import capability as bge_embeddings
    from flashburst.capabilities.fake_embeddings import capability as fake_embeddings

    return {
        bge_embeddings.spec.name: bge_embeddings,
        fake_embeddings.spec.name: fake_embeddings,
    }


def get_capability(name: str) -> Capability:
    capabilities = default_capabilities()
    try:
        return capabilities[name]
    except KeyError as exc:
        known = ", ".join(sorted(capabilities))
        raise KeyError(f"unknown capability '{name}'. known capabilities: {known}") from exc
