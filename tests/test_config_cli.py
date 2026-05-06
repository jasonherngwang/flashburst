from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from flashburst.agent_context import discover_workloads
from flashburst.cli import app
from flashburst.config import (
    get_r2_config,
    get_runpod_profile,
    list_runpod_profiles,
    load_project_config,
)

runner = CliRunner()


def test_check_passes_for_initialized_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    workspace.mkdir()

    result = runner.invoke(app, ["check", "--workspace", str(workspace)])

    assert result.exit_code == 0
    assert "ok workspace directory" in result.output


def test_configure_runpod_profile_is_saved_in_workspace_config(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"

    result = runner.invoke(
        app,
        [
            "configure",
            "runpod",
            "--profile",
            "transcribe",
            "--endpoint-id",
            "rp_test",
            "--timeout-seconds",
            "7200",
            "--workspace",
            str(workspace),
        ],
    )

    assert result.exit_code == 0
    profile = get_runpod_profile(workspace, "transcribe")
    assert profile["endpoint_id"] == "rp_test"
    assert profile["timeout_seconds"] == 7200
    assert list(list_runpod_profiles(workspace)) == ["transcribe"]


def test_configure_r2_store_is_saved_without_credentials(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"

    result = runner.invoke(
        app,
        [
            "configure",
            "r2",
            "--bucket",
            "podcast-artifacts",
            "--account-id",
            "account123",
            "--workspace",
            str(workspace),
        ],
    )

    assert result.exit_code == 0
    config = get_r2_config(workspace)
    assert config["bucket"] == "podcast-artifacts"
    assert config["endpoint_url"] == "https://account123.r2.cloudflarestorage.com"
    assert "access_key_id" not in config
    assert "secret_access_key" not in config


def test_flash_slots_require_explicit_approval_and_flash_ok(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","text":"hello"}\n', encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "run",
            "flashburst.workloads.fake_embeddings:run_job",
            str(source),
            "--flash-slots",
            "1",
            "--workspace",
            str(tmp_path / ".flashburst"),
        ],
    )

    assert result.exit_code == 1
    assert "Runpod Flash slots require explicit --approve-flash." in result.output


def test_stage_field_requires_r2_configuration(tmp_path: Path) -> None:
    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"a","audio_path":"sample.mp3"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    configure = runner.invoke(
        app,
        [
            "configure",
            "runpod",
            "--profile",
            "transcribe",
            "--endpoint-id",
            "rp_test",
            "--workspace",
            str(workspace),
        ],
    )
    assert configure.exit_code == 0

    result = runner.invoke(
        app,
        [
            "run",
            "flashburst.workloads.fake_embeddings:run_job",
            str(source),
            "--flash-slots",
            "1",
            "--local-slots",
            "0",
            "--flash-ok",
            "--approve-flash",
            "--profile",
            "transcribe",
            "--stage-field",
            "audio_path",
            "--workspace",
            str(workspace),
        ],
        env={"CI": "true", "GITHUB_ACTIONS": "true"},
    )

    assert result.exit_code == 1
    assert "--stage-field requires R2 configuration" in result.output


def test_bind_and_context_json_are_agent_readable(tmp_path: Path) -> None:
    workload = tmp_path / "workload.py"
    workload.write_text(
        """from pathlib import Path


def run_job(input_path: Path, output_path: Path, params: dict) -> dict:
    return {"status": "succeeded", "metrics": params}
""",
        encoding="utf-8",
    )
    (tmp_path / "samples").mkdir()
    (tmp_path / "samples" / "sample.mp3").write_bytes(b"audio")
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        '{"id":"a","audio_path":"samples/sample.mp3"}\n',
        encoding="utf-8",
    )
    workspace = tmp_path / ".flashburst"

    result = runner.invoke(
        app,
        [
            "bind",
            "--params-json",
            '{"sample_rate":16000}',
            "--profile",
            "transcribe",
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
        ],
    )

    assert result.exit_code == 0
    config = load_project_config(workspace)
    assert config["workload"] == "workload.py:run_job"
    assert config["manifest"] == "manifest.jsonl"
    assert config["stage_fields"] == ["audio_path"]

    context = runner.invoke(
        app,
        [
            "context",
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
        ],
    )

    assert context.exit_code == 0
    payload = json.loads(context.output)
    assert payload["workload"]["valid"] is True
    assert payload["bound_manifest"]["records"] == 1
    assert payload["bound_manifest"]["stage_field_status"]["audio_path"]["local_file"] == 1
    assert payload["project"]["params"] == {"sample_rate": 16000}


def test_bind_does_not_auto_select_run_like_helper_without_file_contract(tmp_path: Path) -> None:
    (tmp_path / "server.py").write_text(
        """def run_server(host, port, debug):
    return None
""",
        encoding="utf-8",
    )
    (tmp_path / "manifest.jsonl").write_text('{"id":"a"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"

    result = runner.invoke(
        app,
        [
            "bind",
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
        ],
    )

    assert discover_workloads(tmp_path) == []
    assert result.exit_code != 0
    assert "could not discover a workload" in result.output


def test_manifest_validate_reports_missing_stage_files(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        '{"id":"a","audio_path":"samples/missing.mp3"}\n',
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "manifest",
            "validate",
            str(manifest),
            "--stage-field",
            "audio_path",
            "--project-root",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 1
    assert "manifest validation failed" in result.output
