from pathlib import Path

from typer.testing import CliRunner

from flashburst.cli import app
from flashburst.workload_scaffold import scaffold_workload_project

runner = CliRunner()


def test_scaffold_workload_project_generates_adapter_prep_and_placeholder_core(
    tmp_path: Path,
) -> None:
    generated = scaffold_workload_project(
        target=tmp_path,
        package="podcast_jobs",
        capability="audio.transcribe.local",
        job_type="audio.transcribe",
        runner_import=None,
        runner_name="transcribe_manifest",
        supports_runpod_flash=True,
    )

    generated_names = {path.relative_to(tmp_path).as_posix() for path in generated}
    assert "podcast_jobs/core.py" in generated_names
    assert "podcast_jobs/capabilities.py" in generated_names
    assert "podcast_jobs/prepare.py" in generated_names
    assert "prepare_jobs.py" in generated_names
    assert ".gitignore" in generated_names
    gitignore = (tmp_path / ".gitignore").read_text(encoding="utf-8")
    assert "*.mp3" in gitignore
    assert "samples/" in gitignore

    capabilities = (tmp_path / "podcast_jobs" / "capabilities.py").read_text(encoding="utf-8")
    assert 'CAPABILITY_NAME = "audio.transcribe.local"' in capabilities
    assert "supports_runpod_flash=True" in capabilities
    assert "from podcast_jobs.core import transcribe_manifest as run_job" in capabilities
    for path in tmp_path.rglob("*.py"):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")


def test_scaffold_workload_project_can_reference_existing_runner(tmp_path: Path) -> None:
    generated = scaffold_workload_project(
        target=tmp_path,
        package="podcast_jobs",
        capability="audio.transcribe.local",
        job_type="audio.transcribe",
        runner_import="notebook_export:transcribe_manifest",
        runner_name="run_job",
        supports_runpod_flash=False,
    )

    generated_names = {path.relative_to(tmp_path).as_posix() for path in generated}
    assert "podcast_jobs/core.py" not in generated_names
    capabilities = (tmp_path / "podcast_jobs" / "capabilities.py").read_text(encoding="utf-8")
    assert "from notebook_export import transcribe_manifest as run_job" in capabilities
    assert "supports_runpod_flash=False" in capabilities


def test_workload_scaffold_cli(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "workload",
            "scaffold",
            str(tmp_path),
            "--package",
            "podcast_jobs",
            "--capability",
            "audio.transcribe.local",
            "--job-type",
            "audio.transcribe",
            "--runner-name",
            "transcribe_manifest",
        ],
    )

    assert result.exit_code == 0
    assert "Generated workload files" in result.output
    assert (tmp_path / "podcast_jobs" / "core.py").exists()
    assert (tmp_path / "prepare_jobs.py").exists()
    capabilities = (tmp_path / "podcast_jobs" / "capabilities.py").read_text(encoding="utf-8")
    assert "supports_runpod_flash=False" in capabilities
