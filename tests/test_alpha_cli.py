import json
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from flashburst.cli import app
from flashburst.db import FlashburstDB
from flashburst.examples.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import AttemptStatus, JobSpec, PlacementKind
from flashburst.scheduler import create_plan_from_jobs_file


runner = CliRunner()


def test_doctor_passes_for_initialized_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    db_path = workspace / "flashburst.db"
    FlashburstDB(db_path).init_schema()

    result = runner.invoke(
        app,
        ["doctor", "--workspace", str(workspace), "--db", str(db_path)],
    )

    assert result.exit_code == 0
    assert "ok workspace directory" in result.output
    assert "ok database" in result.output
    assert "ok capability registry" in result.output


def test_check_alias_passes_for_initialized_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    db_path = workspace / "flashburst.db"
    FlashburstDB(db_path).init_schema()

    result = runner.invoke(
        app,
        ["check", "--workspace", str(workspace), "--db", str(db_path)],
    )

    assert result.exit_code == 0
    assert "ok workspace directory" in result.output


def test_friendly_configure_commands(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    db_path = workspace / "flashburst.db"

    r2 = runner.invoke(
        app,
        [
            "configure",
            "r2",
            "--bucket",
            "bucket",
            "--endpoint-url",
            "https://r2.example.test",
            "--workspace",
            str(workspace),
        ],
    )
    runpod = runner.invoke(
        app,
        [
            "configure",
            "runpod",
            "--endpoint-id",
            "rp_test",
            "--profile",
            "bge-small-burst",
            "--db",
            str(db_path),
        ],
    )

    assert r2.exit_code == 0
    assert runpod.exit_code == 0
    assert (workspace / "config.json").exists()
    assert FlashburstDB(db_path).get_cloud_profile("bge-small-burst") is not None


def test_friendly_prepare_preview_and_execute_mock(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    db_path = workspace / "flashburst.db"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    FlashburstDB(db_path).init_schema()

    prepare = runner.invoke(
        app,
        [
            "prepare",
            "embeddings",
            str(input_path),
            "--capability",
            "embedding.fake-deterministic",
            "--model-name",
            "",
            "--workspace",
            str(workspace),
        ],
    )
    preview = runner.invoke(
        app,
        [
            "preview",
            str(workspace / "jobs" / "embeddings.jsonl"),
            "--cloud",
            "--backend",
            "mock",
            "--budget",
            "1.00",
            "--workspace",
            str(workspace),
            "--db",
            str(db_path),
        ],
    )
    plan_ids = [path.stem for path in (workspace / "plans").glob("*.json")]
    execute = runner.invoke(
        app,
        ["execute", plan_ids[0], "--approve", "--workspace", str(workspace), "--db", str(db_path)],
    )
    status = runner.invoke(app, ["status", "--results", "--db", str(db_path)])

    assert prepare.exit_code == 0
    assert preview.exit_code == 0
    assert "Run with: flashburst execute" in preview.output
    assert execute.exit_code == 0
    assert "Plan run complete: 1 completed, 0 skipped." in execute.output
    assert status.exit_code == 0
    assert "succeeded: 1" in status.output


def test_inspect_plan_outputs_job_state(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.fake-deterministic",
        batch_size=1,
    )
    db_path = workspace / "flashburst.db"
    db = FlashburstDB(db_path)
    db.init_schema()
    plan = create_plan_from_jobs_file(
        db=db,
        workspace=workspace,
        jobs_file=jobs_path,
        allow_cloud=False,
        backend=None,
        budget_usd=Decimal("1.00"),
    )

    result = runner.invoke(
        app,
        ["inspect", "plan", plan.id, "--workspace", str(workspace), "--db", str(db_path), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["id"] == plan.id
    assert payload["budget_limit_usd"] == "1.00"
    assert payload["items"][0]["placement_kind"] == "local"
    assert payload["items"][0]["job_status"] == "queued"


def test_inspect_attempts_and_retry_expired_cli(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.fake-deterministic",
        batch_size=1,
    )
    db_path = workspace / "flashburst.db"
    db = FlashburstDB(db_path)
    db.init_schema()
    with jobs_path.open("r", encoding="utf-8") as handle:
        job_id = db.insert_job(JobSpec.model_validate_json(handle.readline()))
    attempt_id = db.create_attempt(
        job_id=job_id,
        placement_kind=PlacementKind.MOCK_CLOUD,
        status=AttemptStatus.SUBMITTED,
        cloud_profile_id="mock",
        remote_job_id="remote_123",
        reserved_cost_usd=Decimal("0.05"),
    )

    attempts = runner.invoke(
        app,
        ["inspect", "attempts", "--db", str(db_path), "--json"],
    )
    retry = runner.invoke(app, ["leases", "retry-expired", "--db", str(db_path)])

    assert attempts.exit_code == 0
    payload = json.loads(attempts.output)
    assert payload[0]["id"] == attempt_id
    assert payload[0]["remote_job_id"] == "remote_123"
    assert retry.exit_code == 0
    assert "Retried 0 expired leases." in retry.output
