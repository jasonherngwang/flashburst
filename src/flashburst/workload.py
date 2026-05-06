"""Workload loading, run directories, and local execution."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sys
import time
import threading
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback for packaging portability.
    fcntl = None  # type: ignore[assignment]

from flashburst.models import JobResult, RunRecord, WorkItem
from flashburst.time import utc_now

WorkloadCallable = Callable[[Path, Path, dict[str, Any]], dict[str, Any] | JobResult]
_append_lock = threading.Lock()


def utc_stamp() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")


def safe_id(value: str, index: int) -> str:
    raw = "".join(char if char.isalnum() or char in "_.-" else "-" for char in value)
    return raw.strip("-") or f"job-{index:04d}"


def stable_job_id(record: dict[str, Any], index: int) -> str:
    if record.get("id") is not None:
        return safe_id(str(record["id"]), index)
    payload = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
    return f"job-{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]}"


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


def _load_module_from_file(path: Path):
    resolved = path.resolve()
    module_name = f"_flashburst_workload_{hashlib.sha256(str(resolved).encode()).hexdigest()[:12]}"
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot import workload file: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_workload(spec: str, *, project_root: Path | None = None) -> WorkloadCallable:
    """Load ``module:function`` or ``path.py:function`` workload specs."""
    if ":" not in spec:
        spec = f"{spec}:run_job"
    module_part, function_name = spec.split(":", 1)
    if not module_part or not function_name:
        raise ValueError("workload must be in module:function or path.py:function format")

    path = Path(module_part)
    if path.suffix == ".py" or path.exists():
        module = _load_module_from_file(
            path if path.is_absolute() else (project_root or Path()) / path
        )
    else:
        with _sys_path_entry(project_root):
            module = importlib.import_module(module_part)
    function = module
    for part in function_name.split("."):
        function = getattr(function, part)
    if not callable(function):
        raise TypeError(f"workload target is not callable: {spec}")
    return function


def load_records(source: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = {"id": f"line-{index:04d}", "text": stripped}
            if isinstance(parsed, str):
                parsed = {"id": f"line-{index:04d}", "text": parsed}
            if not isinstance(parsed, dict):
                raise ValueError(f"{source}:{index + 1} must be a JSON object, string, or text")
            records.append(parsed)
    return records


def latest_run_id(workspace: Path) -> str | None:
    marker = workspace / "latest-run"
    if not marker.exists():
        return None
    value = marker.read_text(encoding="utf-8").strip()
    return value or None


def run_dir_for(workspace: Path, run_id: str) -> Path:
    return workspace / "runs" / run_id


def output_path_for(workspace: Path, run_id: str, job_id: str) -> Path:
    return run_dir_for(workspace, run_id) / "outputs" / job_id / "result.jsonl"


def relative_to_workspace(path: Path, workspace: Path) -> str:
    return path.relative_to(workspace).as_posix()


def mark_latest_run(workspace: Path, run_id: str) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "latest-run").write_text(run_id + "\n", encoding="utf-8")


def prepare_items(
    *,
    source: Path,
    workspace: Path,
    run_id: str,
    params: dict[str, Any],
    flash_ok: bool,
    limit: int | None = None,
) -> list[WorkItem]:
    records = load_records(source)
    selected = records[:limit] if limit is not None else records
    run_dir = run_dir_for(workspace, run_id)
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.jsonl"
    items: list[WorkItem] = []
    with manifest_path.open("w", encoding="utf-8") as manifest:
        for index, record in enumerate(selected):
            job_id = stable_job_id(record, index)
            relative = f"runs/{run_id}/inputs/{index:04d}-{job_id}.json"
            input_path = workspace / relative
            input_path.parent.mkdir(parents=True, exist_ok=True)
            input_path.write_text(
                json.dumps(record, sort_keys=True, separators=(",", ":"), default=str),
                encoding="utf-8",
            )
            item = WorkItem(
                id=job_id,
                input=record,
                params=params,
                flash_ok=flash_ok,
                input_path=relative,
            )
            items.append(item)
            manifest.write(item.model_dump_json() + "\n")
    mark_latest_run(workspace, run_id)
    return items


def read_results(run_dir: Path) -> list[RunRecord]:
    path = run_dir / "results.jsonl"
    if not path.exists():
        return []
    results: list[RunRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                results.append(RunRecord.model_validate_json(line))
    return results


def final_run_records(records: list[RunRecord]) -> list[RunRecord]:
    """Return only the latest ledger record for each job id."""
    latest: dict[str, RunRecord] = {}
    order: list[str] = []
    for record in records:
        if record.job_id not in latest:
            order.append(record.job_id)
        latest[record.job_id] = record
    return [latest[job_id] for job_id in order]


def completed_job_ids(run_dir: Path) -> set[str]:
    return {
        record.job_id
        for record in final_run_records(read_results(run_dir))
        if record.status == "succeeded"
    }


def append_result_once(run_dir: Path, record: RunRecord) -> bool:
    """Append a run record unless the job already has a succeeded record."""
    run_dir.mkdir(parents=True, exist_ok=True)
    lock_path = run_dir / "results.lock"
    with _append_lock, lock_path.open("a", encoding="utf-8") as lock_handle:
        if fcntl is not None:
            fcntl.flock(lock_handle, fcntl.LOCK_EX)
        try:
            if record.status == "succeeded":
                existing = read_results(run_dir)
                if any(
                    item.job_id == record.job_id and item.status == "succeeded" for item in existing
                ):
                    return False
            with (run_dir / "results.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(record.model_dump_json() + "\n")
            return True
        finally:
            if fcntl is not None:
                fcntl.flock(lock_handle, fcntl.LOCK_UN)


def normalize_job_result(
    value: dict[str, Any] | JobResult,
    *,
    output_path: Path,
) -> JobResult:
    result = value if isinstance(value, JobResult) else JobResult.model_validate(value)
    if result.status == "succeeded" and result.output_text is None and output_path.exists():
        return JobResult(
            status=result.status,
            output_text=output_path.read_text(encoding="utf-8"),
            output_media_type=result.output_media_type,
            metrics=result.metrics,
            error=result.error,
        )
    return result


def run_item_locally(
    *,
    workload: WorkloadCallable,
    item: WorkItem,
    workspace: Path,
    run_id: str,
) -> RunRecord:
    input_path = workspace / item.input_path
    output_path = output_path_for(workspace, run_id, item.id)
    started = utc_now()
    started_counter = time.perf_counter()
    try:
        raw_result = workload(input_path, output_path, dict(item.params))
        result = normalize_job_result(raw_result, output_path=output_path)
        if result.status == "succeeded" and result.output_text is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(result.output_text, encoding="utf-8")
        metrics = dict(result.metrics)
        metrics.setdefault("total_seconds", time.perf_counter() - started_counter)
        return RunRecord(
            job_id=item.id,
            status=result.status,
            placement="local",
            input=item.input,
            input_path=item.input_path,
            output_path=relative_to_workspace(output_path, workspace)
            if output_path.exists()
            else None,
            output_media_type=result.output_media_type if output_path.exists() else None,
            metrics=metrics,
            error=result.error,
            started_at=started,
            finished_at=utc_now(),
        )
    except Exception as exc:
        return RunRecord(
            job_id=item.id,
            status="failed",
            placement="local",
            input=item.input,
            input_path=item.input_path,
            error=str(exc),
            started_at=started,
            finished_at=utc_now(),
        )
