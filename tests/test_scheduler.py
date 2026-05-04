from decimal import Decimal
from pathlib import Path

from flashburst.db import FlashburstDB
from flashburst.examples.prepare_embeddings import prepare_embedding_jobs
from flashburst.models import CloudProfile
from flashburst.scheduler import approve_plan, create_plan_from_jobs_file, load_plan


def test_create_and_approve_mock_cloud_plan(tmp_path: Path) -> None:
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
    assert len(plan.items) == 1
    assert plan.items[0].placement_kind == "mock_cloud"
    assert not load_plan(workspace, plan.id).approved
    assert approve_plan(workspace, plan.id).approved


def test_create_runpod_flash_plan_from_profile(tmp_path: Path) -> None:
    workspace = tmp_path / ".flashburst"
    input_path = tmp_path / "texts.jsonl"
    input_path.write_text('{"id":"a","text":"hello"}\n')
    jobs_path = prepare_embedding_jobs(
        input_path=input_path,
        workspace=workspace,
        capability="embedding.bge-small-en-v1.5",
        batch_size=1,
        params={"model_name": "sentence-transformers/all-MiniLM-L6-v2"},
    )
    db = FlashburstDB(workspace / "flashburst.db")
    db.init_schema()
    db.upsert_cloud_profile(
        CloudProfile(
            id="bge-small-burst",
            backend="runpod_flash",
            endpoint_id="rp_test",
            capability="embedding.bge-small-en-v1.5",
            estimated_cost_per_job_usd=Decimal("0.07"),
        )
    )

    plan = create_plan_from_jobs_file(
        db=db,
        workspace=workspace,
        jobs_file=jobs_path,
        allow_cloud=True,
        backend=None,
        profile_id="bge-small-burst",
        budget_usd=Decimal("1.00"),
    )

    assert len(plan.items) == 1
    assert plan.items[0].placement_kind == "runpod_flash"
    assert plan.items[0].cloud_profile_id == "bge-small-burst"
    assert plan.items[0].estimated_cost_usd == Decimal("0.07")
