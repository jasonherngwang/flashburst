from __future__ import annotations

import pytest
from pydantic import ValidationError

from flashburst.models import JobResult, RunRecord, WorkItem


def test_work_item_round_trip() -> None:
    item = WorkItem(
        id="job-1",
        input={"id": "job-1", "text": "hello"},
        params={"model": "tiny"},
        flash_ok=True,
        input_path="runs/run-1/inputs/0000-job-1.json",
    )

    restored = WorkItem.model_validate_json(item.model_dump_json())

    assert restored.id == "job-1"
    assert restored.input["text"] == "hello"
    assert restored.params == {"model": "tiny"}
    assert restored.flash_ok is True


def test_job_result_and_run_record_contracts_are_small() -> None:
    item = WorkItem(id="a", input={"id": "a"}, input_path="runs/r/inputs/a.json")
    result = JobResult(
        status="succeeded",
        output_text='{"ok": true}\n',
        output_media_type="application/x-ndjson",
        metrics={"x": 1},
    )
    record = RunRecord(
        job_id=item.id,
        status=result.status,
        placement="local",
        input=item.input,
        input_path=item.input_path,
        input_artifacts=[{"storage": "r2", "bucket": "b", "key": "in"}],
        output_path="runs/r/outputs/a/result.jsonl",
        output_media_type=result.output_media_type,
        output_artifact={"storage": "r2", "bucket": "b", "key": "out"},
        metrics=result.metrics,
    )

    assert item.params == {}
    assert record.output_path == "runs/r/outputs/a/result.jsonl"
    assert record.input_artifacts[0]["key"] == "in"
    assert record.output_artifact == {"storage": "r2", "bucket": "b", "key": "out"}
    assert record.metrics == {"x": 1}


def test_models_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        WorkItem(id="a", input={}, input_path="runs/r/inputs/a.json", cloud_ok=True)
