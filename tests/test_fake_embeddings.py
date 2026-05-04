import json
from pathlib import Path

from flashburst.capabilities.fake_embeddings import embed_text, run


def test_fake_embedding_is_deterministic() -> None:
    assert embed_text("hello") == embed_text("hello")
    assert embed_text("hello") != embed_text("world")


def test_fake_embedding_runner_writes_jsonl(tmp_path: Path) -> None:
    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n{"id":"b","text":"world"}\n')
    result = run(input_path, output_path, {})
    rows = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert result.status == "succeeded"
    assert result.metrics["input_count"] == 2
    assert rows[0]["id"] == "a"
    assert len(rows[0]["embedding"]) == 8
