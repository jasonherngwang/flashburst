import ast
from pathlib import Path

from flashburst.endpoint_scaffold import scaffold_runpod_endpoint


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
    )

    source = output.read_text(encoding="utf-8")
    module = ast.parse(source)
    assert "from my_jobs.core import run as run_job" in source
    assert "from flashburst" not in source
    assert "GpuGroup.AMPERE_24" in source
    assert 'client.stream("GET", url)' in source
    assert "ENDPOINT_WRAPPER_VERSION" in source
    assert "endpoint_flash_source_fingerprint" in source
    assert "endpoint_runpod_pod_id" in source
    assert "def params_with_wrapper_context" in source
    assert '"wrapper_context" = endpoint_metrics()' not in source
    assert 'merged["wrapper_context"] = endpoint_metrics()' in source
    assert '"upload_strategy": "buffered-bytes-put"' in source
    assert '"Content-Length": str(source.stat().st_size)' in source
    assert "source.read_bytes()" in source
    assert any(isinstance(node, ast.AsyncFunctionDef) for node in ast.walk(module))
