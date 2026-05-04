import json
from pathlib import Path

from flashburst.capabilities import bge_embeddings


class FakeModel:
    def __init__(self, model_name: str):
        self.model_name = model_name

    def encode(self, texts, **kwargs):
        return [[float(index), float(len(text))] for index, text in enumerate(texts)]


def test_bge_runner_writes_jsonl(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(bge_embeddings, "_load_model", lambda model_name: FakeModel(model_name))
    monkeypatch.setattr(bge_embeddings, "_device_name", lambda: "test-device")
    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n{"id":"b","text":"world"}\n')

    result = bge_embeddings.run(input_path, output_path, {"batch_size": 2})

    rows = [json.loads(line) for line in output_path.read_text().splitlines()]
    assert result.status == "succeeded"
    assert result.metrics["model_name"] == bge_embeddings.DEFAULT_MODEL_NAME
    assert result.metrics["device"] == "test-device"
    assert result.metrics["input_count"] == 2
    assert result.metrics["vector_dim"] == 2
    assert rows[0] == {"id": "a", "embedding": [0.0, 5.0]}
