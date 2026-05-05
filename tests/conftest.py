from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
import importlib
import sys
from pathlib import Path

import pytest


@dataclass(frozen=True)
class ExternalWorkloadProject:
    root: Path
    capability_import: str
    capability_name: str
    prepare_jobs: Callable[..., Path]


@pytest.fixture
def external_workload_project(tmp_path: Path) -> Iterator[ExternalWorkloadProject]:
    package = tmp_path / "external_audio"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "core.py").write_text(
        """from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_job(input_path: Path, output_path: Path, params: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    manifest = json.loads(input_path.read_text(encoding="utf-8"))
    if "source_url" not in manifest and "audio_path" not in manifest:
        raise ValueError("manifest requires source_url or audio_path")
    source_kind = "url" if "source_url" in manifest else "local_file"
    input_size_bytes = 0
    if source_kind == "local_file":
        input_size_bytes = Path(str(manifest["audio_path"])).stat().st_size
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {
                "id": f"{manifest.get('id', 'audio')}-dry-run",
                "text": "dry run transcript omitted",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\\n",
        encoding="utf-8",
    )
    return {
        "status": "succeeded",
        "output_artifacts": [
            {
                "uri": f"local://{output_path.name}",
                "media_type": "application/x-ndjson",
                "storage": "local",
                "sha256": sha256_file(output_path),
                "size_bytes": output_path.stat().st_size,
            }
        ],
        "metrics": {
            "mode": "dry_run",
            "source_kind": source_kind,
            "input_size_bytes": input_size_bytes,
            "total_seconds": time.perf_counter() - started,
        },
    }
""",
        encoding="utf-8",
    )
    (package / "capabilities.py").write_text(
        """from __future__ import annotations

from pathlib import Path
from typing import Any

from flashburst.capabilities.registry import Capability
from flashburst.models import CapabilitySpec, JobResult

from external_audio.core import run_job

CAPABILITY_NAME = "audio.transcribe.test"


def local_runner(input_path: Path, output_path: Path, params: dict[str, Any]) -> JobResult:
    return JobResult.model_validate(run_job(input_path, output_path, params))


capability = Capability(
    spec=CapabilitySpec(
        name=CAPABILITY_NAME,
        job_type="audio.transcribe",
        supports_local=True,
        supports_runpod_flash=True,
    ),
    local_runner=local_runner,
)
""",
        encoding="utf-8",
    )
    (package / "prepare.py").write_text(
        """from __future__ import annotations

import json
from pathlib import Path

from flashburst.artifacts.local import LocalArtifactStore
from flashburst.ids import compute_idempotency_key
from flashburst.models import JobSpec, Privacy

from external_audio.capabilities import CAPABILITY_NAME


def _records(source: Path) -> list[dict]:
    items = []
    with source.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                items.append(json.loads(line))
    return items


def prepare_jobs(
    *,
    source: Path,
    workspace: Path,
    params: dict | None = None,
    limit: int | None = None,
) -> Path:
    selected = _records(source)
    if limit is not None:
        selected = selected[:limit]
    store = LocalArtifactStore(workspace / "artifacts")
    jobs_dir = workspace / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    jobs_path = jobs_dir / "external-audio.jsonl"
    job_params = params or {}
    with jobs_path.open("w", encoding="utf-8") as jobs_file:
        for index, record in enumerate(selected):
            relative = f"inputs/external-audio-{index:04d}.json"
            artifact_path = store.ensure_parent_for_uri(f"local://{relative}")
            artifact_path.write_text(
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            artifact = store.ref_for_path(relative, media_type="application/json")
            spec = JobSpec(
                job_type="audio.transcribe",
                required_capability=CAPABILITY_NAME,
                input_artifacts=[artifact],
                params=job_params,
                privacy=Privacy.CLOUD_OK if "source_url" in record else Privacy.LOCAL_ONLY,
                idempotency_key=compute_idempotency_key(
                    job_type="audio.transcribe",
                    required_capability=CAPABILITY_NAME,
                    input_artifacts=[artifact.model_dump(mode="json")],
                    params=job_params,
                ),
            )
            jobs_file.write(spec.model_dump_json() + "\\n")
    return jobs_path
""",
        encoding="utf-8",
    )

    sys.path.insert(0, str(tmp_path))
    try:
        prepare_module = importlib.import_module("external_audio.prepare")
        yield ExternalWorkloadProject(
            root=tmp_path,
            capability_import="external_audio.capabilities:capability",
            capability_name="audio.transcribe.test",
            prepare_jobs=prepare_module.prepare_jobs,
        )
    finally:
        try:
            sys.path.remove(str(tmp_path))
        except ValueError:
            pass
        for name in list(sys.modules):
            if name == "external_audio" or name.startswith("external_audio."):
                sys.modules.pop(name, None)
