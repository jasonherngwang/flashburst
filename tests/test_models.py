from datetime import datetime, timezone

from flashburst.ids import compute_idempotency_key
from flashburst.models import ArtifactGrant, ArtifactRef, ExecutionEnvelope, JobSpec, Privacy


def test_models_serialize_to_json() -> None:
    artifact = ArtifactRef(
        uri="local://inputs/batch.jsonl",
        media_type="application/x-ndjson",
        storage="local",
        sha256="abc",
        size_bytes=10,
    )
    spec = JobSpec(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[artifact],
        params={"normalize": True},
        privacy=Privacy.CLOUD_OK,
        idempotency_key="idem_123",
    )
    assert "embedding.fake-deterministic" in spec.model_dump_json()


def test_execution_envelope_json_round_trip() -> None:
    grant = ArtifactGrant(
        artifact_uri="s3://bucket/input.jsonl",
        method="GET",
        url="https://example.com/input",
        expires_at=datetime.now(timezone.utc),
    )
    envelope = ExecutionEnvelope(
        job_id="job_1",
        attempt_id="att_1",
        capability="embedding.fake-deterministic",
        artifact_grants=[grant],
    )
    restored = ExecutionEnvelope.model_validate_json(envelope.model_dump_json())
    assert restored.job_id == "job_1"
    assert restored.artifact_grants[0].method == "GET"


def test_idempotency_key_is_deterministic_for_canonical_params() -> None:
    artifact = {
        "uri": "local://inputs/batch.jsonl",
        "sha256": "abc",
        "media_type": "application/x-ndjson",
        "storage": "local",
    }
    first = compute_idempotency_key(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[artifact],
        params={"b": 2, "a": 1},
    )
    second = compute_idempotency_key(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[artifact],
        params={"a": 1, "b": 2},
    )
    assert first == second


def test_idempotency_key_ignores_volatile_artifact_metadata() -> None:
    stable_artifact = {
        "uri": "local://inputs/batch.jsonl",
        "sha256": "abc",
        "media_type": "application/x-ndjson",
        "storage": "local",
        "created_at": "2026-05-04T00:00:00Z",
    }
    later_artifact = {
        **stable_artifact,
        "created_at": "2026-05-04T01:00:00Z",
    }

    first = compute_idempotency_key(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[stable_artifact],
        params={"a": 1},
    )
    second = compute_idempotency_key(
        job_type="embedding.embed_text_batch",
        required_capability="embedding.fake-deterministic",
        input_artifacts=[later_artifact],
        params={"a": 1},
    )

    assert first == second
