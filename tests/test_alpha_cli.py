from pathlib import Path

from typer.testing import CliRunner

from flashburst.cli import app
from flashburst.db import FlashburstDB


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
            "fake-burst",
            "--capability",
            "embedding.fake-deterministic",
            "--db",
            str(db_path),
        ],
    )

    assert r2.exit_code == 0
    assert runpod.exit_code == 0
    assert (workspace / "config.json").exists()
    profile = FlashburstDB(db_path).get_cloud_profile("fake-burst")
    assert profile is not None
    assert profile.config["run_timeout_seconds"] == 600
    assert profile.config["artifact_grant_expires_seconds"] == 3600


def test_friendly_prepare_and_run_queue_local(tmp_path: Path) -> None:
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
            "--workspace",
            str(workspace),
        ],
    )
    run = runner.invoke(
        app,
        [
            "run-queue",
            str(workspace / "jobs" / "embeddings.jsonl"),
            "--local-slots",
            "1",
            "--workspace",
            str(workspace),
            "--db",
            str(db_path),
        ],
    )
    status = runner.invoke(app, ["status", "--results", "--db", str(db_path)])

    assert prepare.exit_code == 0
    assert run.exit_code == 0
    assert "Queue run complete: 1 completed, 0 skipped." in run.output
    assert status.exit_code == 0
    assert "succeeded: 1" in status.output


def test_run_queue_executes_external_workload_locally(
    tmp_path: Path,
    external_workload_project,
) -> None:
    workspace = tmp_path / ".flashburst"
    db_path = workspace / "flashburst.db"
    manifest = tmp_path / "episodes.jsonl"
    manifest.write_text(
        "\n".join(
            [
                '{"id":"sse-511","source_url":"https://example.test/sse-511.mp3"}',
                '{"id":"sse-512","source_url":"https://example.test/sse-512.mp3"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    FlashburstDB(db_path).init_schema()
    add_capability = runner.invoke(
        app,
        [
            "capability",
            "add",
            external_workload_project.capability_import,
            "--project-root",
            str(external_workload_project.root),
            "--workspace",
            str(workspace),
        ],
    )
    jobs_path = external_workload_project.prepare_jobs(
        source=manifest,
        workspace=workspace,
        params={"dry_run": True},
    )

    run = runner.invoke(
        app,
        [
            "run-queue",
            str(jobs_path),
            "--local-slots",
            "1",
            "--workspace",
            str(workspace),
            "--db",
            str(db_path),
        ],
    )

    assert add_capability.exit_code == 0
    assert run.exit_code == 0
    assert "Queue run complete: 2 completed, 0 skipped." in run.output
    jobs = FlashburstDB(db_path).list_jobs()
    assert [job["status"] for job in jobs] == ["succeeded", "succeeded"]


def test_retry_expired_leases_cli(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    db_path = workspace / "flashburst.db"
    db = FlashburstDB(db_path)
    db.init_schema()

    retry = runner.invoke(app, ["leases", "retry-expired", "--db", str(db_path)])

    assert retry.exit_code == 0
    assert "Retried 0 expired leases." in retry.output
