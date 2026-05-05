"""DBOS queues for local-first execution with optional Runpod Flash burst."""

from __future__ import annotations

import logging
import os
import re
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flashburst.adapters.r2_artifacts import (
    R2ArtifactStore,
    artifact_url_ttl_seconds,
    guess_media_type,
)
from flashburst.adapters.runpod_flash import (
    DEFAULT_RUNPOD_TIMEOUT_SECONDS,
    RunpodFlashAdapter,
    RunpodFlashJobError,
)
from flashburst.models import JobResult, RunRecord, WorkItem
from flashburst.workload import (
    append_result_once,
    completed_job_ids,
    load_workload,
    output_path_for,
    relative_to_workspace,
    run_dir_for,
    run_item_locally,
)
from flashburst.time import utc_now

try:
    from dbos import DBOS, Queue, SetEnqueueOptions
    from dbos import error as dbos_error
except ImportError as exc:  # pragma: no cover - exercised through CLI error path.
    raise ImportError(
        "DBOS is required. Install with `uv sync` or `pip install flashburst`."
    ) from exc


@dataclass(frozen=True)
class FlashConfig:
    endpoint_id: str
    timeout_seconds: int = DEFAULT_RUNPOD_TIMEOUT_SECONDS
    stage_fields: tuple[str, ...] = ()
    artifact_url_ttl_seconds: int | None = None


@dataclass
class _RouteState:
    local: threading.BoundedSemaphore | None
    flash: threading.BoundedSemaphore | None


_route_states: dict[str, _RouteState] = {}
_route_states_lock = threading.Lock()


def _workflow_status_to_record(status: Any) -> dict[str, Any]:
    return {
        "workflow_id": getattr(status, "workflow_id", None),
        "status": getattr(status, "status", None),
        "name": getattr(status, "name", None),
        "queue_name": getattr(status, "queue_name", None),
        "executor_id": getattr(status, "executor_id", None),
        "created_at": getattr(status, "created_at", None),
        "updated_at": getattr(status, "updated_at", None),
        "dequeued_at": getattr(status, "dequeued_at", None),
        "deduplication_id": getattr(status, "deduplication_id", None),
        "priority": getattr(status, "priority", None),
        "error": getattr(status, "error", None),
    }


def inspect_queue_state(
    *,
    workspace: Path,
    run_id: str | None = None,
    database_url: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Inspect DBOS workflow state for Flashburst queues."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    queue_names = None
    if run_id:
        queue_names = [
            _safe_queue_name("work", run_id),
            _safe_queue_name("local", run_id),
            _safe_queue_name("flash", run_id),
        ]

    dbos_url = _database_url(workspace, database_url)
    config: dict[str, Any] = {
        "name": "flashburst-inspect",
        "executor_id": f"flashburst-inspect-{os.getpid()}",
        "system_database_url": dbos_url,
        "console_log_level": "ERROR",
        "log_level": "ERROR",
    }

    DBOS(config=config)
    try:
        DBOS.launch()
        workflows = DBOS.list_workflows(
            queue_name=queue_names,
            sort_desc=True,
            load_input=False,
            load_output=False,
        )
    finally:
        DBOS.destroy()

    records = [
        _workflow_status_to_record(workflow)
        for workflow in workflows
        if getattr(workflow, "queue_name", None)
    ]
    summary_by_key: dict[tuple[str, str], int] = {}
    for record in records:
        queue_name = str(record.get("queue_name") or "")
        status = str(record.get("status") or "UNKNOWN")
        summary_by_key[(queue_name, status)] = summary_by_key.get((queue_name, status), 0) + 1

    summary = [
        {"queue_name": queue_name, "status": status, "count": count}
        for (queue_name, status), count in sorted(summary_by_key.items())
    ]
    return {
        "database_url": dbos_url,
        "run_id": run_id,
        "queue_names": queue_names,
        "summary": summary,
        "workflow_count": len(records),
        "workflow_limit": limit,
        "workflows": records[:limit],
    }


def _safe_queue_name(prefix: str, run_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", run_id).strip("-")
    return f"flashburst-{prefix}-{cleaned or 'run'}"[:80]


def _route_state_key(run_id: str) -> str:
    return f"{os.getpid()}:{run_id}"


def _route_state(run_id: str, *, local_slots: int, flash_slots: int) -> _RouteState:
    key = _route_state_key(run_id)
    with _route_states_lock:
        state = _route_states.get(key)
        if state is None:
            state = _RouteState(
                local=threading.BoundedSemaphore(local_slots) if local_slots else None,
                flash=threading.BoundedSemaphore(flash_slots) if flash_slots else None,
            )
            _route_states[key] = state
        return state


def _clear_route_state(run_id: str) -> None:
    with _route_states_lock:
        _route_states.pop(_route_state_key(run_id), None)


def _acquire_route(
    *,
    state: _RouteState,
    item: WorkItem,
    local_slots: int,
    flash_slots: int,
) -> tuple[str, threading.BoundedSemaphore]:
    if local_slots == 0 and (flash_slots == 0 or not item.flash_ok):
        raise ValueError("pending items are not flash-approved and no local slots are available")

    while True:
        if state.local is not None and state.local.acquire(blocking=False):
            return "local", state.local
        if item.flash_ok and state.flash is not None and state.flash.acquire(blocking=False):
            return "flash", state.flash
        time.sleep(0.1)


def _database_url(workspace: Path, explicit_url: str | None) -> str:
    if explicit_url:
        return explicit_url
    env_url = os.getenv("DBOS_SYSTEM_DATABASE_URL")
    if env_url:
        return env_url
    return f"sqlite:///{(workspace / 'dbos.sqlite').resolve()}"


def _is_url(value: str) -> bool:
    return value.startswith(("http://", "https://"))


def _resolve_stage_source(raw_path: str, project_root: Path) -> Path:
    source = Path(raw_path)
    resolved = source if source.is_absolute() else project_root / source
    if not resolved.exists():
        raise FileNotFoundError(f"staged input does not exist: {raw_path}")
    if not resolved.is_file():
        raise ValueError(f"staged input is not a file: {raw_path}")
    return resolved


def _prepare_flash_payload(
    *,
    item: WorkItem,
    workspace: Path,
    run_id: str,
    project_root: Path,
    timeout_seconds: int,
    stage_fields: tuple[str, ...],
    configured_ttl_seconds: int | None,
    artifact_store: R2ArtifactStore | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None, R2ArtifactStore | None]:
    payload: dict[str, Any] = {
        "schema_version": "1",
        "job_id": item.id,
        "input": item.input,
        "params": item.params,
    }
    if not stage_fields:
        return payload, [], None, None

    store = artifact_store or R2ArtifactStore.from_workspace(workspace)
    expires_seconds = artifact_url_ttl_seconds(
        timeout_seconds=timeout_seconds,
        configured_seconds=configured_ttl_seconds,
    )
    input_files: list[dict[str, Any]] = []
    input_artifacts: list[dict[str, Any]] = []

    for field in stage_fields:
        if field not in item.input:
            raise KeyError(f"staged input field is missing from job {item.id}: {field}")
        raw_value = item.input[field]
        if not isinstance(raw_value, str):
            raise TypeError(f"staged input field must be a string path or URL: {field}")
        if _is_url(raw_value):
            continue
        source = _resolve_stage_source(raw_value, project_root)
        media_type = guess_media_type(source)
        key = store.input_key(run_id=run_id, job_id=item.id, field=field, source=source)
        ref = store.upload_file(source, key=key, media_type=media_type).as_record()
        ref["field"] = field
        ref["source_path"] = raw_value
        input_artifacts.append(ref)
        input_files.append(
            {
                "field": field,
                "filename": source.name,
                "media_type": media_type,
                "get_url": store.presign_get(key=key, expires_seconds=expires_seconds),
                "object": {
                    "storage": "r2",
                    "bucket": store.bucket,
                    "key": key,
                },
            }
        )

    output_media_type = "application/x-ndjson"
    output_key = store.output_key(run_id=run_id, job_id=item.id)
    output_artifact = store.object_ref(key=output_key, media_type=output_media_type).as_record()
    payload["input_files"] = input_files
    payload["output_file"] = {
        "filename": "result.jsonl",
        "media_type": output_media_type,
        "put_url": store.presign_put(
            key=output_key,
            media_type=output_media_type,
            expires_seconds=expires_seconds,
        ),
        "object": {
            "storage": "r2",
            "bucket": store.bucket,
            "key": output_key,
        },
    }
    payload["artifact_url_ttl_seconds"] = expires_seconds
    return payload, input_artifacts, output_artifact, store


def _materialize_flash_output(
    *,
    result: JobResult,
    output_path: Path,
    output_artifact: dict[str, Any] | None,
    artifact_store: R2ArtifactStore | None,
) -> dict[str, Any] | None:
    if result.status != "succeeded":
        return output_artifact
    if output_artifact is not None and artifact_store is not None:
        artifact_store.download_file(key=str(output_artifact["key"]), destination=output_path)
        return artifact_store.object_ref(
            key=str(output_artifact["key"]),
            media_type=str(output_artifact["media_type"]),
            path=output_path,
        ).as_record()
    if result.output_text is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(result.output_text, encoding="utf-8")
    return output_artifact


@DBOS.step(name="flashburst.execute_local")
def _execute_local(
    workload_spec: str,
    item_json: str,
    workspace: str,
    run_id: str,
    project_root: str,
) -> str:
    item = WorkItem.model_validate_json(item_json)
    loaded = load_workload(workload_spec, project_root=Path(project_root))
    record = run_item_locally(
        workload=loaded,
        item=item,
        workspace=Path(workspace),
        run_id=run_id,
    )
    return record.model_dump_json()


@DBOS.step(name="flashburst.execute_flash")
def _execute_flash(
    item_json: str,
    workspace: str,
    run_id: str,
    endpoint_id: str,
    timeout_seconds: int,
    project_root: str,
    stage_fields_json: str,
    artifact_url_ttl_seconds_value: int | None,
) -> str:
    item = WorkItem.model_validate_json(item_json)
    workspace_path = Path(workspace)
    output_path = output_path_for(workspace_path, run_id, item.id)
    started = utc_now()
    remote_job_id: str | None = None
    input_artifacts: list[dict[str, Any]] = []
    output_artifact: dict[str, Any] | None = None
    try:
        configured_ttl = artifact_url_ttl_seconds_value if artifact_url_ttl_seconds_value else None
        stage_fields = tuple(json.loads(stage_fields_json or "[]"))
        payload, input_artifacts, output_artifact, artifact_store = _prepare_flash_payload(
            item=item,
            workspace=workspace_path,
            run_id=run_id,
            project_root=Path(project_root),
            timeout_seconds=timeout_seconds,
            stage_fields=stage_fields,
            configured_ttl_seconds=configured_ttl,
        )
        remote_job_id, result = RunpodFlashAdapter(endpoint_id=endpoint_id).run_payload_sync(
            payload,
            timeout_seconds=timeout_seconds,
        )
        output_artifact = _materialize_flash_output(
            result=result,
            output_path=output_path,
            output_artifact=output_artifact,
            artifact_store=artifact_store,
        )
        return RunRecord(
            job_id=item.id,
            status=result.status,
            placement="flash",
            input=item.input,
            input_path=item.input_path,
            input_artifacts=input_artifacts,
            output_path=relative_to_workspace(output_path, workspace_path)
            if output_path.exists()
            else None,
            output_media_type=result.output_media_type if output_path.exists() else None,
            output_artifact=output_artifact,
            metrics=result.metrics,
            error=result.error,
            remote_job_id=remote_job_id,
            started_at=started,
            finished_at=utc_now(),
        ).model_dump_json()
    except Exception as exc:
        if isinstance(exc, RunpodFlashJobError):
            remote_job_id = exc.remote_job_id
        return RunRecord(
            job_id=item.id,
            status="failed",
            placement="flash",
            input=item.input,
            input_path=item.input_path,
            input_artifacts=input_artifacts,
            output_artifact=output_artifact,
            error=str(exc),
            remote_job_id=remote_job_id,
            started_at=started,
            finished_at=utc_now(),
        ).model_dump_json()


@DBOS.step(name="flashburst.append_result")
def _append_record(workspace: str, run_id: str, record_json: str) -> str:
    record = RunRecord.model_validate_json(record_json)
    append_result_once(run_dir_for(Path(workspace), run_id), record)
    return record_json


@DBOS.workflow(name="flashburst.routed_job")
def _routed_job(
    workload_spec: str,
    item_json: str,
    workspace: str,
    run_id: str,
    local_slots: int,
    flash_slots: int,
    endpoint_id: str,
    timeout_seconds: int,
    project_root: str,
    stage_fields_json: str,
    artifact_url_ttl_seconds_value: int | None,
) -> str:
    item = WorkItem.model_validate_json(item_json)
    state = _route_state(run_id, local_slots=local_slots, flash_slots=flash_slots)
    placement, semaphore = _acquire_route(
        state=state,
        item=item,
        local_slots=local_slots,
        flash_slots=flash_slots,
    )
    try:
        if placement == "local":
            record_json = _execute_local(workload_spec, item_json, workspace, run_id, project_root)
        else:
            if not endpoint_id:
                raise ValueError("flash placement requires a Runpod Flash profile")
            record_json = _execute_flash(
                item_json,
                workspace,
                run_id,
                endpoint_id,
                timeout_seconds,
                project_root,
                stage_fields_json,
                artifact_url_ttl_seconds_value,
            )
        return _append_record(workspace, run_id, record_json)
    finally:
        semaphore.release()


def _enqueue(queue: Queue, func, *args: Any, deduplication_id: str):
    with SetEnqueueOptions(deduplication_id=deduplication_id):
        try:
            return queue.enqueue(func, *args)
        except dbos_error.DBOSQueueDeduplicatedError as exc:
            return DBOS.retrieve_workflow(exc.workflow_id)


def _wait_handle(handle) -> str:
    return str(handle.get_result(polling_interval_sec=0.1))


def drain_items(
    *,
    workload_spec: str,
    items: list[WorkItem],
    workspace: Path,
    run_id: str,
    local_slots: int,
    flash_slots: int,
    project_root: Path,
    flash_config: FlashConfig | None = None,
    database_url: str | None = None,
) -> tuple[int, int, int]:
    """Drain a run through DBOS queues, filling local slots before flash slots."""
    if local_slots < 0 or flash_slots < 0:
        raise ValueError("slot counts must be zero or positive")
    if local_slots == 0 and flash_slots == 0:
        raise ValueError("at least one local or flash slot is required")
    if flash_slots > 0 and flash_config is None:
        raise ValueError("flash slots require a Runpod Flash profile")

    run_dir = run_dir_for(workspace, run_id)
    done = completed_job_ids(run_dir)
    pending = [item for item in items if item.id not in done]
    skipped = len(items) - len(pending)
    if not pending:
        return 0, 0, skipped

    logging.getLogger("dbos").setLevel(logging.WARNING)
    worker_slots = max(local_slots + flash_slots, local_slots, flash_slots, 1)
    work_queue = Queue(
        _safe_queue_name("work", run_id),
        concurrency=worker_slots,
        worker_concurrency=worker_slots,
        polling_interval_sec=0.1,
    )

    config: dict[str, Any] = {
        "name": "flashburst",
        "executor_id": f"flashburst-{os.getpid()}",
        "system_database_url": _database_url(workspace, database_url),
        "console_log_level": "ERROR",
        "log_level": "ERROR",
    }
    records: list[RunRecord] = []
    queued: list[tuple[str, Any]] = []

    DBOS(config=config)
    try:
        DBOS.listen_queues([work_queue])
        DBOS.launch()

        endpoint_id = flash_config.endpoint_id if flash_config else ""
        timeout_seconds = (
            flash_config.timeout_seconds if flash_config else DEFAULT_RUNPOD_TIMEOUT_SECONDS
        )
        stage_fields_json = json.dumps(list(flash_config.stage_fields)) if flash_config else "[]"
        artifact_url_ttl = flash_config.artifact_url_ttl_seconds if flash_config else None
        for item in pending:
            handle = _enqueue(
                work_queue,
                _routed_job,
                workload_spec,
                item.model_dump_json(),
                str(workspace.resolve()),
                run_id,
                local_slots,
                flash_slots,
                endpoint_id,
                timeout_seconds,
                str(project_root.resolve()),
                stage_fields_json,
                artifact_url_ttl,
                deduplication_id=f"{run_id}:{item.id}:work",
            )
            queued.append(("work", handle))

        waiters = min(len(queued), worker_slots)
        with ThreadPoolExecutor(max_workers=waiters) as pool:
            futures = {pool.submit(_wait_handle, handle): placement for placement, handle in queued}
            for future in as_completed(futures):
                records.append(RunRecord.model_validate_json(future.result()))
    finally:
        DBOS.destroy()
        _clear_route_state(run_id)

    succeeded = sum(1 for record in records if record.status == "succeeded")
    failed = sum(1 for record in records if record.status == "failed")
    return succeeded, failed, skipped
