from pathlib import Path

from flashburst.db import FlashburstDB
from flashburst.ids import compute_idempotency_key
from flashburst.models import ArtifactRef, JobSpec, JobStatus, Privacy


def make_spec() -> JobSpec:
    artifact = ArtifactRef(
        uri="local://inputs/batch.jsonl",
        media_type="application/x-ndjson",
        storage="local",
        sha256="abc",
    )
    idempotency_key = compute_idempotency_key(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[artifact.model_dump(mode="json")],
        params={"normalize": True},
    )
    return JobSpec(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[artifact],
        params={"normalize": True},
        privacy=Privacy.CLOUD_OK,
        idempotency_key=idempotency_key,
    )


def test_init_schema_creates_database(tmp_path: Path) -> None:
    db_path = tmp_path / "flashburst.db"
    db = FlashburstDB(db_path)
    db.init_schema()
    assert db_path.exists()


def test_duplicate_idempotency_insert_returns_existing_job(tmp_path: Path) -> None:
    db = FlashburstDB(tmp_path / "flashburst.db")
    db.init_schema()
    spec = make_spec()
    first = db.insert_job(spec)
    second = db.insert_job(spec)
    assert first == second
    assert len(db.list_jobs()) == 1


def test_claim_next_local_job_is_atomic_for_single_process(tmp_path: Path) -> None:
    db = FlashburstDB(tmp_path / "flashburst.db")
    db.init_schema()
    job_id = db.insert_job(make_spec())
    claim = db.claim_next_local_job(
        worker_id="local-test",
        capability="embedding.fake-deterministic",
    )
    assert claim is not None
    assert claim.job_id == job_id
    assert (
        db.claim_next_local_job(
            worker_id="local-test-2",
            capability="embedding.fake-deterministic",
        )
        is None
    )
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.RUNNING.value


def test_claim_next_cloud_job_only_claims_cloud_ok(tmp_path: Path) -> None:
    db = FlashburstDB(tmp_path / "flashburst.db")
    db.init_schema()
    job_id = db.insert_job(make_spec())
    claim = db.claim_next_cloud_job(
        worker_id="cloud-test",
        capability="embedding.fake-deterministic",
        cloud_profile_id="mock-profile",
        job_ids=[job_id],
    )

    assert claim is not None
    claimed_job_id, attempt_id = claim
    assert claimed_job_id == job_id
    attempts = db.list_attempts(job_id=job_id)
    assert attempts[0]["id"] == attempt_id
    assert attempts[0]["placement_kind"] == "runpod_flash"
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.RUNNING.value


def test_retry_expired_lease_requeues_job(tmp_path: Path) -> None:
    db = FlashburstDB(tmp_path / "flashburst.db")
    db.init_schema()
    job_id = db.insert_job(make_spec())
    claim = db.claim_next_local_job(
        worker_id="local-test",
        capability="embedding.fake-deterministic",
        lease_seconds=-1,
    )
    assert claim is not None
    assert db.retry_expired_leases() == 1
    job = db.get_job(job_id)
    assert job is not None
    assert job["status"] == JobStatus.QUEUED.value
