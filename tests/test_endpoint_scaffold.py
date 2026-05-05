import ast
import asyncio
import json
import sys
import types
from pathlib import Path

from typer.testing import CliRunner

from flashburst.cli import app
from flashburst.endpoint_scaffold import (
    parse_endpoint_env,
    render_runpod_endpoint,
    scaffold_runpod_endpoint,
)


runner = CliRunner()


def test_scaffold_runpod_endpoint_writes_user_owned_wrapper(tmp_path: Path) -> None:
    output = tmp_path / "endpoints" / "runpod_flash.py"

    scaffold_runpod_endpoint(
        output=output,
        runner_import="my_jobs.core:run",
        endpoint_name="my-job",
        gpu="AMPERE_24",
        workers_min=0,
        workers_max=1,
        idle_timeout=30,
        dependencies=["httpx>=0.27"],
        system_dependencies=["ffmpeg"],
        env={"HF_HOME": "/tmp/huggingface"},
        env_from=["HF_TOKEN"],
    )

    source = output.read_text(encoding="utf-8")
    module = ast.parse(source)
    assert "from my_jobs.core import run as run_job" in source
    assert "from flashburst" not in source
    assert "GpuGroup.AMPERE_24" in source
    assert "input_path.write_text(" in source
    assert "def download_input_files" in source
    assert "def upload_output_file" in source
    assert 'input_payload = dict(payload.get("input") or {})' in source
    assert "downloaded_input_files = download_input_files" in source
    assert '"artifact_input_files": downloaded_input_files' in source
    assert '"artifact_output_uploaded": uploaded_output_file' in source
    assert '"output_text": output_text' in source
    assert '"output_media_type": "application/x-ndjson"' in source
    assert "ENDPOINT_WRAPPER_VERSION" in source
    assert "flashburst-artifacts-v1" in source
    assert "endpoint_flash_source_fingerprint" in source
    assert "endpoint_runpod_pod_id" in source
    assert "def params_with_wrapper_context" in source
    assert '"wrapper_context" = endpoint_metrics()' not in source
    assert 'merged["wrapper_context"] = endpoint_metrics()' in source
    assert "env={" in source
    assert "'HF_HOME': '/tmp/huggingface'" in source
    assert "'HF_TOKEN': os.environ['HF_TOKEN']" in source
    assert any(isinstance(node, ast.AsyncFunctionDef) for node in ast.walk(module))


def test_generated_endpoint_env_uses_literals_and_env_from(monkeypatch) -> None:
    monkeypatch.setenv("HF_TOKEN", "secret-value")
    source = render_runpod_endpoint(
        runner_import="my_jobs.core:run",
        endpoint_name="my-job",
        gpu="AMPERE_24",
        workers_min=0,
        workers_max=1,
        idle_timeout=30,
        dependencies=[],
        system_dependencies=[],
        env={"HF_HOME": "/tmp/huggingface"},
        env_from=["HF_TOKEN"],
    )
    assert "secret-value" not in source

    runpod_flash = types.ModuleType("runpod_flash")
    captured_endpoint_kwargs: dict[str, object] = {}

    class GpuGroup:
        AMPERE_24 = "AMPERE_24"

    def endpoint_decorator(**kwargs):
        captured_endpoint_kwargs.update(kwargs)

        def decorator(function):
            return function

        return decorator

    runpod_flash.Endpoint = endpoint_decorator
    runpod_flash.GpuGroup = GpuGroup
    monkeypatch.setitem(sys.modules, "runpod_flash", runpod_flash)

    package = types.ModuleType("my_jobs")
    workload_module = types.ModuleType("my_jobs.core")

    def run(input_path: Path, output_path: Path, params: dict) -> dict:
        return {"status": "succeeded", "metrics": {}}

    workload_module.run = run
    monkeypatch.setitem(sys.modules, "my_jobs", package)
    monkeypatch.setitem(sys.modules, "my_jobs.core", workload_module)

    generated = types.ModuleType("generated_endpoint")
    exec(compile(source, "generated_endpoint.py", "exec"), generated.__dict__)

    assert captured_endpoint_kwargs["env"] == {
        "HF_HOME": "/tmp/huggingface",
        "HF_TOKEN": "secret-value",
    }


def test_parse_endpoint_env_rejects_duplicate_sources() -> None:
    try:
        parse_endpoint_env(["HF_TOKEN=literal"], ["HF_TOKEN"])
    except ValueError as exc:
        assert "both --env and --env-from" in str(exc)
    else:
        raise AssertionError("expected duplicate endpoint env validation failure")


def test_generated_endpoint_downloads_inputs_and_uploads_output(monkeypatch) -> None:
    source = render_runpod_endpoint(
        runner_import="my_jobs.core:run",
        endpoint_name="my-job",
        gpu="AMPERE_24",
        workers_min=0,
        workers_max=1,
        idle_timeout=30,
        dependencies=[],
        system_dependencies=[],
    )
    runpod_flash = types.ModuleType("runpod_flash")

    class GpuGroup:
        AMPERE_24 = "AMPERE_24"

    def endpoint_decorator(**_kwargs):
        def decorator(function):
            return function

        return decorator

    runpod_flash.Endpoint = endpoint_decorator
    runpod_flash.GpuGroup = GpuGroup
    monkeypatch.setitem(sys.modules, "runpod_flash", runpod_flash)

    package = types.ModuleType("my_jobs")
    workload_module = types.ModuleType("my_jobs.core")
    seen: dict[str, str] = {}

    def run(input_path: Path, output_path: Path, params: dict) -> dict:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        seen["audio_path"] = payload["audio_path"]
        assert Path(payload["audio_path"]).read_bytes() == b"audio"
        assert "wrapper_context" in params
        output_path.write_text('{"ok":true}\n', encoding="utf-8")
        return {"status": "succeeded", "metrics": {"workload": True}}

    workload_module.run = run
    monkeypatch.setitem(sys.modules, "my_jobs", package)
    monkeypatch.setitem(sys.modules, "my_jobs.core", workload_module)

    generated = types.ModuleType("generated_endpoint")
    exec(compile(source, "generated_endpoint.py", "exec"), generated.__dict__)

    uploaded: list[bytes | None] = []

    class FakeResponse:
        def __init__(self, data: bytes):
            self.data = data

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self) -> bytes:
            return self.data

    def fake_urlopen(request, timeout: int):
        assert timeout == 300
        if request.get_method() == "GET":
            return FakeResponse(b"audio")
        if request.get_method() == "PUT":
            uploaded.append(request.data)
            return FakeResponse(b"")
        raise AssertionError(f"unexpected method: {request.get_method()}")

    monkeypatch.setattr(generated, "urlopen", fake_urlopen)

    result = asyncio.run(
        generated.run_flashburst_job(
            input={"id": "sample-1", "audio_path": "samples/sample.mp3"},
            params={},
            input_files=[
                {
                    "field": "audio_path",
                    "filename": "sample.mp3",
                    "get_url": "https://signed.example/get",
                }
            ],
            output_file={
                "put_url": "https://signed.example/put",
                "media_type": "application/x-ndjson",
            },
        )
    )

    assert Path(seen["audio_path"]).name == "sample.mp3"
    assert uploaded == [b'{"ok":true}\n']
    assert result["output_text"] is None
    assert result["metrics"]["artifact_input_files"] == 1
    assert result["metrics"]["artifact_output_uploaded"] is True
    assert result["metrics"]["workload"] is True


def test_generated_endpoint_keeps_same_basename_staged_inputs_separate(monkeypatch) -> None:
    source = render_runpod_endpoint(
        runner_import="my_jobs.core:run",
        endpoint_name="my-job",
        gpu="AMPERE_24",
        workers_min=0,
        workers_max=1,
        idle_timeout=30,
        dependencies=[],
        system_dependencies=[],
    )
    runpod_flash = types.ModuleType("runpod_flash")

    class GpuGroup:
        AMPERE_24 = "AMPERE_24"

    def endpoint_decorator(**_kwargs):
        def decorator(function):
            return function

        return decorator

    runpod_flash.Endpoint = endpoint_decorator
    runpod_flash.GpuGroup = GpuGroup
    monkeypatch.setitem(sys.modules, "runpod_flash", runpod_flash)

    package = types.ModuleType("my_jobs")
    workload_module = types.ModuleType("my_jobs.core")
    seen: dict[str, str] = {}

    def run(input_path: Path, output_path: Path, params: dict) -> dict:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        audio_path = Path(payload["audio_path"])
        reference_path = Path(payload["reference_path"])
        assert audio_path.name == "sample.bin"
        assert reference_path.name == "sample.bin"
        assert audio_path != reference_path
        assert audio_path.read_bytes() == b"audio"
        assert reference_path.read_bytes() == b"reference"
        seen["audio_path"] = str(audio_path)
        seen["reference_path"] = str(reference_path)
        output_path.write_text('{"ok":true}\n', encoding="utf-8")
        return {"status": "succeeded", "metrics": {}}

    workload_module.run = run
    monkeypatch.setitem(sys.modules, "my_jobs", package)
    monkeypatch.setitem(sys.modules, "my_jobs.core", workload_module)

    generated = types.ModuleType("generated_endpoint")
    exec(compile(source, "generated_endpoint.py", "exec"), generated.__dict__)

    class FakeResponse:
        def __init__(self, data: bytes):
            self.data = data

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self) -> bytes:
            return self.data

    def fake_urlopen(request, timeout: int):
        if request.get_method() == "GET":
            if "audio" in request.full_url:
                return FakeResponse(b"audio")
            if "reference" in request.full_url:
                return FakeResponse(b"reference")
        if request.get_method() == "PUT":
            return FakeResponse(b"")
        raise AssertionError(f"unexpected request: {request.get_method()} {request.full_url}")

    monkeypatch.setattr(generated, "urlopen", fake_urlopen)

    result = asyncio.run(
        generated.run_flashburst_job(
            input={
                "id": "sample-1",
                "audio_path": "left/sample.bin",
                "reference_path": "right/sample.bin",
            },
            params={},
            input_files=[
                {
                    "field": "audio_path",
                    "filename": "sample.bin",
                    "get_url": "https://signed.example/audio",
                },
                {
                    "field": "reference_path",
                    "filename": "sample.bin",
                    "get_url": "https://signed.example/reference",
                },
            ],
        )
    )

    assert result["status"] == "succeeded"
    assert "/audio_path/" in seen["audio_path"]
    assert "/reference_path/" in seen["reference_path"]


def test_cli_scaffold_uses_project_binding_and_pyproject_dependencies(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """[project]
name = "podcast"
dependencies = [
  "flashburst[r2,runpod]",
  "mutagen>=1.47",
  "imageio-ffmpeg>=0.6",
]
""",
        encoding="utf-8",
    )
    (tmp_path / "transcribe.py").write_text(
        """from pathlib import Path


def transcribe_manifest(input_path: Path, output_path: Path, params: dict) -> dict:
    return {"status": "succeeded", "metrics": {}}
""",
        encoding="utf-8",
    )
    (tmp_path / "manifest.local.jsonl").write_text('{"id":"a"}\n', encoding="utf-8")
    workspace = tmp_path / ".flashburst"
    bind = runner.invoke(
        app,
        [
            "bind",
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
        ],
    )

    result = runner.invoke(
        app,
        [
            "scaffold",
            "--project-root",
            str(tmp_path),
            "--workspace",
            str(workspace),
            "--env",
            "HF_HOME=/tmp/huggingface",
            "--env-from",
            "HF_TOKEN",
        ],
    )

    assert bind.exit_code == 0
    assert result.exit_code == 0
    source = (tmp_path / "endpoint.py").read_text(encoding="utf-8")
    assert "from transcribe import transcribe_manifest as run_job" in source
    assert "'mutagen>=1.47'" in source
    assert "'imageio-ffmpeg>=0.6'" in source
    assert "'flashburst[r2,runpod]'" not in source
    assert "from flashburst" not in source
    assert "'HF_HOME': '/tmp/huggingface'" in source
    assert "'HF_TOKEN': os.environ['HF_TOKEN']" in source
