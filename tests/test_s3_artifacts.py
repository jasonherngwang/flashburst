from __future__ import annotations

import pytest

from flashburst.config import configure_s3_store
from flashburst.artifacts.s3 import S3ArtifactStore, parse_s3_uri

boto3 = pytest.importorskip("boto3")


def test_parse_s3_uri() -> None:
    parsed = parse_s3_uri("s3://bucket/path/to/file.jsonl")
    assert parsed.bucket == "bucket"
    assert parsed.key == "path/to/file.jsonl"


def test_parse_s3_uri_with_default_bucket() -> None:
    parsed = parse_s3_uri("s3://file.jsonl", default_bucket="default")
    assert parsed.bucket == "default"
    assert parsed.key == "file.jsonl"


def test_presigned_grants_use_expected_methods() -> None:
    store = S3ArtifactStore(
        bucket="bucket",
        endpoint_url="https://example-account.r2.cloudflarestorage.com",
        region="auto",
        access_key_id="test-access",
        secret_access_key="test-secret",
    )
    read = store.presign_get("s3://bucket/input.jsonl", expires_seconds=60)
    write = store.presign_put(
        "s3://bucket/output.jsonl",
        media_type="application/x-ndjson",
        expires_seconds=60,
    )
    assert read.method == "GET"
    assert write.method == "PUT"
    assert read.url.startswith("https://")
    assert write.content_type == "application/x-ndjson"


def test_configure_s3_store_does_not_persist_credentials(tmp_path) -> None:
    config = configure_s3_store(
        workspace=tmp_path / ".flashburst",
        provider="r2",
        bucket="bucket",
        endpoint_url="https://example-account.r2.cloudflarestorage.com",
    )
    store_config = config["artifact_store"]
    assert "access_key_id" not in store_config
    assert "secret_access_key" not in store_config


def test_s3_store_credentials_resolve_from_env(monkeypatch) -> None:
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-access")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    store = S3ArtifactStore.from_config(
        {
            "bucket": "bucket",
            "endpoint_url": "https://example-account.r2.cloudflarestorage.com",
            "region": "auto",
        }
    )
    assert store.access_key_id == "test-access"
    assert store.secret_access_key == "test-secret"
