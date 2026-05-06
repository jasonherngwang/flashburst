from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from flashburst.cli import app
from flashburst.workload import read_results, run_dir_for

runner = CliRunner()


def test_transcription_demo_runs_real_audio_decode(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"

    result = runner.invoke(
        app,
        [
            "run",
            "examples/transcription_demo/transcriber.py:transcribe_manifest",
            "examples/transcription_demo/manifest.jsonl",
            "--run-id",
            "example-transcription",
            "--params-json",
            '{"max_duration_seconds":1,"sample_rate":16000}',
            "--workspace",
            str(workspace),
        ],
    )

    assert result.exit_code == 0, result.output
    records = read_results(run_dir_for(workspace, "example-transcription"))
    assert len(records) == 1
    record = records[0]
    assert record.status == "succeeded"
    assert record.metrics["mode"] == "ffmpeg_decode"
    assert record.metrics["decoded_pcm_bytes"] > 0
    output_path = workspace / str(record.output_path)
    segment = json.loads(output_path.read_text(encoding="utf-8"))
    assert segment["text"] == "local audio decode smoke succeeded"
    assert segment["segment_schema_version"] == "transcript-segment-v1"
