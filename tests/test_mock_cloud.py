from decimal import Decimal
from pathlib import Path

from flashburst.adapters.mock_cloud import MockCloudAdapter
from flashburst.db import FlashburstDB
from flashburst.examples.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import JobStatus
from flashburst.scheduler import create_plan_from_jobs_file


def test_mock_cloud_completes_job(tmp_path: Path) -> None:
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
    plan = create_plan_from_jobs_file(
        db=db,
        workspace=workspace,
        jobs_file=jobs_path,
        allow_cloud=True,
        backend="mock",
        budget_usd=Decimal("1.00"),
    )
    adapter = MockCloudAdapter(db=db, workspace=workspace)
    assert adapter.run_item(plan.items[0])
    job = db.get_job(plan.items[0].job_id)
    assert job is not None
    assert job["status"] == JobStatus.SUCCEEDED.value
