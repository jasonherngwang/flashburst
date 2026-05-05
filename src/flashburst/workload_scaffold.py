"""Generate custom workload project scaffolding."""

from __future__ import annotations

import re
from pathlib import Path


REQUIRED_GITIGNORE_LINES = [
    ".flash/",
    ".flashburst/",
    ".venv/",
    "__pycache__/",
    "*.pyc",
    ".env",
    ".env.*",
    "!.env.example",
    "samples/",
    "data/",
    "manifests/",
    "*.mp3",
    "*.wav",
    "*.m4a",
    "*.mp4",
    "*.jsonl",
]


def normalize_package_name(value: str) -> str:
    package = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    if not package:
        raise ValueError("package name cannot be empty")
    if package[0].isdigit():
        package = f"workload_{package}"
    return package


def job_file_name_for(job_type: str) -> str:
    return f"{_slug(job_type)}.jsonl"


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "jobs"


def _write(path: Path, content: str, *, overwrite: bool) -> bool:
    if path.exists() and not overwrite:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def _merge_gitignore(path: Path) -> bool:
    existing = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    changed = False
    lines = list(existing)
    if lines and lines[-1] != "":
        lines.append("")
    for line in REQUIRED_GITIGNORE_LINES:
        if line not in existing:
            lines.append(line)
            changed = True
    if changed or not path.exists():
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return changed


def _render_init() -> str:
    return '"""Workload package generated for Flashburst."""\n'


def _render_core(runner_name: str) -> str:
    return f'''"""Flashburst-free workload logic.

Replace the placeholder body with code from your script or notebook. Keep this
module independent from Flashburst so it can run locally, in tests, and inside a
generated Runpod Flash endpoint.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def {runner_name}(
    input_path: Path,
    output_path: Path,
    params: dict[str, Any],
) -> dict[str, Any]:
    manifest = json.loads(input_path.read_text(encoding="utf-8"))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            {{
                "manifest": manifest,
                "params": params,
                "message": "replace this placeholder with real workload output",
            }},
            sort_keys=True,
        )
        + "\\n",
        encoding="utf-8",
    )
    return {{
        "status": "succeeded",
        "metrics": {{
            "mode": "scaffold_placeholder",
        }},
    }}
'''


def _split_runner_import(runner_import: str) -> tuple[str, str]:
    if ":" not in runner_import:
        raise ValueError("runner import must be in module:function format")
    module_name, function_name = runner_import.split(":", 1)
    if not module_name or not function_name:
        raise ValueError("runner import must be in module:function format")
    return module_name, function_name


def _render_capabilities(
    *,
    runner_import: str,
    capability: str,
    job_type: str,
    supports_runpod_flash: bool,
) -> str:
    module_name, function_name = _split_runner_import(runner_import)
    return f'''"""Flashburst capability adapter for this workload."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from flashburst.capabilities.registry import Capability
from flashburst.models import CapabilitySpec, JobResult

from {module_name} import {function_name} as run_job

CAPABILITY_NAME = "{capability}"
JOB_TYPE = "{job_type}"


def local_runner(input_path: Path, output_path: Path, params: dict[str, Any]) -> JobResult:
    return JobResult.model_validate(run_job(input_path, output_path, params))


capability = Capability(
    spec=CapabilitySpec(
        name=CAPABILITY_NAME,
        job_type=JOB_TYPE,
        supports_local=True,
        supports_runpod_flash={supports_runpod_flash!r},
    ),
    local_runner=local_runner,
)
'''


def _render_prepare(*, package: str, job_type: str) -> str:
    job_file_name = job_file_name_for(job_type)
    return f'''"""Prepare Flashburst JobSpecs for this workload."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

from flashburst.artifacts.local import LocalArtifactStore
from flashburst.ids import compute_idempotency_key
from flashburst.models import JobSpec, Privacy

from {package}.capabilities import CAPABILITY_NAME, JOB_TYPE

JOB_FILE_NAME = "{job_file_name}"


def _safe_id(value: str, index: int) -> str:
    parsed = urlparse(value)
    candidate = Path(parsed.path).stem or f"job-{{index:04d}}"
    safe = "".join(char if char.isalnum() or char in "_.-" else "-" for char in candidate)
    return safe.strip("-") or f"job-{{index:04d}}"


def _load_records(source: str | Path) -> list[dict]:
    source_text = str(source)
    source_path = Path(source_text)
    if source_path.exists():
        records = []
        with source_path.open("r", encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    parsed = {{"id": _safe_id(stripped, index), "source": stripped}}
                if isinstance(parsed, str):
                    parsed = {{"id": _safe_id(parsed, index), "source": parsed}}
                if not isinstance(parsed, dict):
                    raise ValueError(f"line {{index + 1}} must be JSON object or string")
                parsed.setdefault("id", _safe_id(json.dumps(parsed, sort_keys=True), index))
                records.append(parsed)
        return records
    return [{{"id": _safe_id(source_text, 0), "source": source_text}}]


def prepare_jobs(
    source: str | Path,
    *,
    workspace: Path = Path(".flashburst"),
    params: dict | None = None,
    cloud_ok: bool = False,
    limit: int | None = None,
) -> Path:
    records = _load_records(source)
    selected = records[:limit] if limit is not None else records

    store = LocalArtifactStore(workspace / "artifacts")
    jobs_dir = workspace / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    jobs_file = jobs_dir / JOB_FILE_NAME
    job_params = params or {{}}

    with jobs_file.open("w", encoding="utf-8") as handle:
        for index, record in enumerate(selected):
            relative = f"inputs/{{index:04d}}-{{record['id']}}.json"
            input_path = store.ensure_parent_for_uri(f"local://{{relative}}")
            input_path.write_text(
                json.dumps(record, sort_keys=True, separators=(",", ":")),
                encoding="utf-8",
            )
            artifact = store.ref_for_path(relative, media_type="application/json")
            spec = JobSpec(
                job_type=JOB_TYPE,
                required_capability=CAPABILITY_NAME,
                input_artifacts=[artifact],
                params=job_params,
                privacy=Privacy.CLOUD_OK if cloud_ok else Privacy.LOCAL_ONLY,
                idempotency_key=compute_idempotency_key(
                    job_type=JOB_TYPE,
                    required_capability=CAPABILITY_NAME,
                    input_artifacts=[artifact.model_dump(mode="json")],
                    params=job_params,
                ),
            )
            handle.write(spec.model_dump_json() + "\\n")
    return jobs_file
'''


def _render_prepare_cli(package: str) -> str:
    return f'''"""CLI wrapper for preparing Flashburst jobs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from {package}.prepare import prepare_jobs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", help="Input string or JSONL manifest path")
    parser.add_argument("--workspace", "-w", type=Path, default=Path(".flashburst"))
    parser.add_argument("--params-json", default="{{}}")
    parser.add_argument("--cloud-ok", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    job_path = prepare_jobs(
        args.source,
        workspace=args.workspace,
        params=json.loads(args.params_json),
        cloud_ok=args.cloud_ok,
        limit=args.limit,
    )
    print(job_path)


if __name__ == "__main__":
    main()
'''


def scaffold_workload_project(
    *,
    target: Path,
    package: str,
    capability: str,
    job_type: str,
    runner_import: str | None,
    runner_name: str,
    supports_runpod_flash: bool,
    overwrite: bool = False,
) -> list[Path]:
    package = normalize_package_name(package)
    target.mkdir(parents=True, exist_ok=True)
    package_dir = target / package
    generated: list[Path] = []

    if runner_import is None:
        runner_import = f"{package}.core:{runner_name}"
        if _write(package_dir / "core.py", _render_core(runner_name), overwrite=overwrite):
            generated.append(package_dir / "core.py")
    else:
        _split_runner_import(runner_import)

    if _write(package_dir / "__init__.py", _render_init(), overwrite=overwrite):
        generated.append(package_dir / "__init__.py")
    if _write(
        package_dir / "capabilities.py",
        _render_capabilities(
            runner_import=runner_import,
            capability=capability,
            job_type=job_type,
            supports_runpod_flash=supports_runpod_flash,
        ),
        overwrite=overwrite,
    ):
        generated.append(package_dir / "capabilities.py")
    if _write(
        package_dir / "prepare.py",
        _render_prepare(package=package, job_type=job_type),
        overwrite=overwrite,
    ):
        generated.append(package_dir / "prepare.py")
    if _write(target / "prepare_jobs.py", _render_prepare_cli(package), overwrite=overwrite):
        generated.append(target / "prepare_jobs.py")
    if _merge_gitignore(target / ".gitignore"):
        generated.append(target / ".gitignore")

    return generated
