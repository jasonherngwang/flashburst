import ast
import re
from pathlib import Path


ENDPOINT_PATH = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "runpod_flash_embedding_endpoint"
    / "endpoint.py"
)


def _endpoint_dependencies() -> list[str]:
    module = ast.parse(ENDPOINT_PATH.read_text(encoding="utf-8"))
    for node in ast.walk(module):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        if node.name != "run_flashburst_embedding":
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            for keyword in decorator.keywords:
                if keyword.arg == "dependencies":
                    return ast.literal_eval(keyword.value)
    raise AssertionError("run_flashburst_embedding Endpoint dependencies not found")


def test_runpod_flash_endpoint_uses_direct_transformers_dependency() -> None:
    dependencies = _endpoint_dependencies()
    dependency_names = {
        re.split(r"[<>=!~]", dependency, maxsplit=1)[0] for dependency in dependencies
    }

    assert "transformers" in dependency_names
    assert "sentence-transformers" not in dependency_names
    assert "pydantic" not in dependency_names
    assert "torch" not in dependency_names


def test_runpod_flash_endpoint_does_not_import_sentence_transformers() -> None:
    module = ast.parse(ENDPOINT_PATH.read_text(encoding="utf-8"))
    imported_modules = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    assert "sentence_transformers" not in imported_modules
