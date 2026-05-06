from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from flashburst.adapters.dbos_queue import FlashConfig, drain_items
from flashburst.adapters.runpod_flash import RunpodFlashJobError
from flashburst.cli import app
from flashburst.models import JobResult, RunRecord
from flashburst.workload import (
    completed_job_ids,
    load_records,
    load_workload,
    prepare_items,
    read_results,
    run_dir_for,
    run_item_locally,
)

runner = CliRunner()


def test_load_records_accepts_json_objects_strings_and_text(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n"world"\nplain text\n', encoding="utf-8")

    records = load_records(source)

    assert records == [
        {"id": "a", "text": "hello"},
        {"id": "line-0001", "text": "world"},
        {"id": "line-0002", "text": "plain text"},
    ]


def test_load_workload_from_file_and_run_locally(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.py"
    workload_file.write_text(
        """from __future__ import annotations

import json
from pathlib import Path


def run_job(input_path: Path, output_path: Path, params: dict) -> dict:
    record = json.loads(input_path.read_text())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({"id": record["id"], "value": params["suffix"]}) + "\\n")
    return {"status": "succeeded", "metrics": {"seen": record["id"]}}
""",
        encoding="utf-8",
    )
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"one"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    items = prepare_items(
        source=source,
        workspace=workspace,
        run_id="test-run",
        params={"suffix": "ok"},
        flash_ok=False,
    )
    workload = load_workload(f"{workload_file}:run_job")

    record = run_item_locally(
        workload=workload,
        item=items[0],
        workspace=workspace,
        run_id="test-run",
    )

    assert record.status == "succeeded"
    assert record.placement == "local"
    assert record.output_path == "runs/test-run/outputs/one/result.jsonl"
    output = workspace / "runs/test-run/outputs/one/result.jsonl"
    assert json.loads(output.read_text(encoding="utf-8")) == {"id": "one", "value": "ok"}


def test_cli_run_writes_result_ledger_and_resumes(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n{"id":"b","text":"bye"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"

    first = runner.invoke(
        app,
        [
            "run",
            "flashburst.workloads.fake_embeddings:run_job",
            str(source),
            "--run-id",
            "local",
            "--workspace",
            str(workspace),
        ],
    )
    second = runner.invoke(
        app,
        [
            "run",
            "flashburst.workloads.fake_embeddings:run_job",
            str(source),
            "--run-id",
            "local",
            "--workspace",
            str(workspace),
        ],
    )
    status = runner.invoke(app, ["status", "--results", "--workspace", str(workspace)])

    assert first.exit_code == 0
    assert "Run complete: 2 succeeded, 0 failed, 0 already complete." in first.output
    assert second.exit_code == 0
    assert "Run complete: 0 succeeded, 0 failed, 2 already complete." in second.output
    records = read_results(run_dir_for(workspace, "local"))
    assert [record.job_id for record in records] == ["a", "b"]
    assert completed_job_ids(run_dir_for(workspace, "local")) == {"a", "b"}
    assert status.exit_code == 0
    assert "succeeded: 2" in status.output
    assert "a succeeded local -> runs/local/outputs/a/result.jsonl" in status.output


def test_cli_run_uses_dbos_queue(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"

    result = runner.invoke(
        app,
        [
            "run",
            "flashburst.workloads.fake_embeddings:run_job",
            str(source),
            "--run-id",
            "dbos-local",
            "--workspace",
            str(workspace),
        ],
    )

    assert result.exit_code == 0
    assert "local_slots=1 flash_slots=0" in result.output
    assert "Run complete: 1 succeeded, 0 failed, 0 already complete." in result.output
    assert (workspace / "dbos.sqlite").exists()
    records = read_results(run_dir_for(workspace, "dbos-local"))
    assert len(records) == 1
    assert records[0].placement == "local"


def test_queue_status_reports_dbos_workflows(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    run = runner.invoke(
        app,
        [
            "run",
            "flashburst.workloads.fake_embeddings:run_job",
            str(source),
            "--run-id",
            "queue-local",
            "--workspace",
            str(workspace),
        ],
    )

    status = runner.invoke(
        app,
        ["queue", "--run-id", "queue-local", "--details", "--workspace", str(workspace)],
    )
    status_json = runner.invoke(
        app,
        ["queue", "--run-id", "queue-local", "--json", "--workspace", str(workspace)],
    )

    assert run.exit_code == 0
    assert status.exit_code == 0
    assert "run: queue-local" in status.output
    assert "flashburst-work-queue-local SUCCESS: 1" in status.output
    assert "flashburst.routed_job" in status.output
    assert status_json.exit_code == 0
    payload = json.loads(status_json.output)
    assert payload["run_id"] == "queue-local"
    assert payload["summary"] == [
        {
            "queue_name": "flashburst-work-queue-local",
            "status": "SUCCESS",
            "count": 1,
        }
    ]
    assert payload["workflows"][0]["name"] == "flashburst.routed_job"


def test_cli_run_local_uses_project_binding(tmp_path: Path) -> None:
    workload_file = tmp_path / "workload.py"
    workload_file.write_text(
        """from __future__ import annotations

import json
from pathlib import Path


def run_job(input_path: Path, output_path: Path, params: dict) -> dict:
    record = json.loads(input_path.read_text())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({"id": record["id"], "suffix": params["suffix"]}) + "\\n")
    return {"status": "succeeded", "metrics": {"bound": True}}
""",
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.jsonl"
    (tmp_path / "samples").mkdir()
    (tmp_path / "samples" / "sample.mp3").write_bytes(b"audio")
    manifest.write_text('{"id":"bound","audio_path":"samples/sample.mp3"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    bind = runner.invoke(
        app,
        [
            "bind",
            "--workload",
            "workload.py:run_job",
            "--manifest",
            "manifest.jsonl",
            "--params-json",
            '{"suffix":"ok"}',
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
        ],
    )

    result = runner.invoke(
        app,
        [
            "run",
            "--run-id",
            "bound-local",
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
        ],
    )

    assert bind.exit_code == 0
    assert result.exit_code == 0
    assert "Prepared 1 item(s) for run" in result.output
    output = workspace / "runs/bound-local/outputs/bound/result.jsonl"
    assert json.loads(output.read_text(encoding="utf-8")) == {"id": "bound", "suffix": "ok"}


def test_drain_items_runs_flash_queue_with_fake_adapter(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    items = prepare_items(
        source=source,
        workspace=workspace,
        run_id="flash-inline",
        params={},
        flash_ok=True,
    )
    payloads: list[dict] = []

    class FakeRunpodFlashAdapter:
        def __init__(self, *, endpoint_id: str):
            assert endpoint_id == "endpoint"

        def run_payload_sync(
            self,
            payload: dict,
            *,
            timeout_seconds: int,
        ) -> tuple[str, JobResult]:
            assert timeout_seconds == 1
            payloads.append(payload)
            return (
                "remote-1",
                JobResult(
                    status="succeeded",
                    output_text='{"remote":true}\n',
                    metrics={"remote": True},
                ),
            )

    monkeypatch.setattr(
        "flashburst.adapters.dbos_queue.RunpodFlashAdapter",
        FakeRunpodFlashAdapter,
    )

    succeeded, failed, skipped = drain_items(
        workload_spec="unused.py:run_job",
        items=items,
        workspace=workspace,
        run_id="flash-inline",
        local_slots=0,
        flash_slots=1,
        project_root=tmp_path,
        flash_config=FlashConfig(endpoint_id="endpoint", timeout_seconds=1),
    )

    assert (succeeded, failed, skipped) == (1, 0, 0)
    assert payloads == [
        {
            "schema_version": "1",
            "job_id": "a",
            "input": {"id": "a", "text": "hello"},
            "params": {},
        }
    ]
    output = workspace / "runs/flash-inline/outputs/a/result.jsonl"
    assert output.read_text(encoding="utf-8") == '{"remote":true}\n'
    records = read_results(run_dir_for(workspace, "flash-inline"))
    assert records[0].placement == "flash"
    assert records[0].remote_job_id == "remote-1"
    assert records[0].output_path == "runs/flash-inline/outputs/a/result.jsonl"


def test_hybrid_drain_routes_overflow_to_flash_without_partitioning(
    monkeypatch, tmp_path: Path
) -> None:
    workload_file = tmp_path / "workload.py"
    workload_file.write_text(
        """from __future__ import annotations

import json
import time
from pathlib import Path


def run_job(input_path: Path, output_path: Path, params: dict) -> dict:
    record = json.loads(input_path.read_text())
    time.sleep(0.3)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({"local": record["id"]}) + "\\n")
    return {"status": "succeeded", "metrics": {"local": True}}
""",
        encoding="utf-8",
    )
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a"}\n{"id":"b"}\n{"id":"c"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    items = prepare_items(
        source=source,
        workspace=workspace,
        run_id="hybrid-route",
        params={},
        flash_ok=True,
    )
    payloads: list[dict] = []

    class FakeRunpodFlashAdapter:
        def __init__(self, *, endpoint_id: str):
            assert endpoint_id == "endpoint"

        def run_payload_sync(
            self,
            payload: dict,
            *,
            timeout_seconds: int,
        ) -> tuple[str, JobResult]:
            payloads.append(payload)
            return (
                f"remote-{payload['job_id']}",
                JobResult(
                    status="succeeded",
                    output_text=json.dumps({"flash": payload["job_id"]}) + "\n",
                ),
            )

    monkeypatch.setattr(
        "flashburst.adapters.dbos_queue.RunpodFlashAdapter",
        FakeRunpodFlashAdapter,
    )

    succeeded, failed, skipped = drain_items(
        workload_spec=f"{workload_file}:run_job",
        items=items,
        workspace=workspace,
        run_id="hybrid-route",
        local_slots=1,
        flash_slots=1,
        project_root=tmp_path,
        flash_config=FlashConfig(endpoint_id="endpoint", timeout_seconds=1),
    )

    assert (succeeded, failed, skipped) == (3, 0, 0)
    records = read_results(run_dir_for(workspace, "hybrid-route"))
    placements = [record.placement for record in records]
    assert placements.count("local") >= 1
    assert placements.count("flash") >= 1
    assert len(payloads) >= 1


def test_status_json_outputs_run_records(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    run_dir = run_dir_for(workspace, "json")
    run_dir.mkdir(parents=True)
    (workspace / "latest-run").write_text("json\n", encoding="utf-8")
    (run_dir / "results.jsonl").write_text(
        RunRecord(job_id="a", status="failed", error="boom").model_dump_json() + "\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["status", "--json", "--workspace", str(workspace)])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["job_id"] == "a"
    assert payload[0]["error"] == "boom"


def test_status_and_context_count_only_final_job_state(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    run_dir = run_dir_for(workspace, "retry")
    output = run_dir / "outputs" / "a" / "result.jsonl"
    output.parent.mkdir(parents=True)
    output.write_text('{"ok":true}\n', encoding="utf-8")
    (workspace / "latest-run").write_text("retry\n", encoding="utf-8")
    (run_dir / "results.jsonl").write_text(
        "\n".join(
            [
                RunRecord(job_id="a", status="failed", error="first attempt").model_dump_json(),
                RunRecord(
                    job_id="a",
                    status="succeeded",
                    placement="local",
                    output_path="runs/retry/outputs/a/result.jsonl",
                ).model_dump_json(),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    status = runner.invoke(
        app,
        ["status", "--run-id", "retry", "--results", "--workspace", str(workspace)],
    )
    status_json = runner.invoke(
        app,
        ["status", "--run-id", "retry", "--json", "--workspace", str(workspace)],
    )
    context = runner.invoke(
        app,
        ["context", "--project-root", str(tmp_path), "--workspace", str(workspace)],
    )

    assert status.exit_code == 0
    assert "succeeded: 1" in status.output
    assert "failed:" not in status.output
    assert "a succeeded local -> runs/retry/outputs/a/result.jsonl" in status.output
    final_status_records = json.loads(status_json.output)
    assert status_json.exit_code == 0
    assert len(final_status_records) == 1
    assert final_status_records[0]["status"] == "succeeded"
    payload = json.loads(context.output)
    assert context.exit_code == 0
    assert payload["latest_run"]["summary"] == {"succeeded": 1}
    assert payload["latest_run"]["records"] == 1
    assert payload["latest_run"]["ledger_records"] == 2


def test_flash_failure_records_remote_job_id_from_adapter_error(
    monkeypatch, tmp_path: Path
) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    items = prepare_items(
        source=source,
        workspace=workspace,
        run_id="flash-timeout",
        params={},
        flash_ok=True,
    )

    class FakeRunpodFlashAdapter:
        def __init__(self, *, endpoint_id: str):
            assert endpoint_id == "endpoint"

        def run_payload_sync(
            self,
            payload: dict,
            *,
            timeout_seconds: int,
        ) -> tuple[str, JobResult]:
            raise RunpodFlashJobError("remote-timeout", "timed out")

    monkeypatch.setattr(
        "flashburst.adapters.dbos_queue.RunpodFlashAdapter",
        FakeRunpodFlashAdapter,
    )

    succeeded, failed, skipped = drain_items(
        workload_spec="unused.py:run_job",
        items=items,
        workspace=workspace,
        run_id="flash-timeout",
        local_slots=0,
        flash_slots=1,
        project_root=tmp_path,
        flash_config=FlashConfig(endpoint_id="endpoint", timeout_seconds=1),
    )

    assert (succeeded, failed, skipped) == (0, 1, 0)
    records = read_results(run_dir_for(workspace, "flash-timeout"))
    assert records[0].status == "failed"
    assert records[0].remote_job_id == "remote-timeout"
    assert records[0].error == "timed out"
