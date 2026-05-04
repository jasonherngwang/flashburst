import json
from pathlib import Path

from flashburst.db import FlashburstDB
from flashburst.examples.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import JobSpec, JobStatus
from flashburst.worker import run_once


def test_worker_once_completes_fake_embedding_job(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.fake-deterministic",
        batch_size=1,
    )
    db = FlashburstDB(workspace / "flashburst.db")
    db.init_schema()
    with jobs_path.open("r", encoding="utf-8") as handle:
        spec = JobSpec.model_validate_json(handle.readline())
    job_id = db.insert_job(spec)

    assert run_once(
        db=db,
        workspace=workspace,
        worker_id="local-test",
        capability_name="embedding.fake-deterministic",
    )

    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.SUCCEEDED.value
    result = json.loads(job["result_json"])
    assert result["output_artifacts"][0]["uri"].startswith("local://outputs/")
