"""R2-backed artifact staging for Runpod Flash jobs."""

from __future__ import annotations

import hashlib
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flashburst.config import get_r2_config


MAX_PRESIGNED_URL_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class R2Object:
    bucket: str
    key: str
    media_type: str
    size_bytes: int | None = None
    sha256: str | None = None

    def as_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "storage": "r2",
            "bucket": self.bucket,
            "key": self.key,
            "media_type": self.media_type,
        }
        if self.size_bytes is not None:
            record["size_bytes"] = self.size_bytes
        if self.sha256 is not None:
            record["sha256"] = self.sha256
        return record


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def guess_media_type(path: Path) -> str:
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def _clean_key_part(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "_.-" else "-" for char in value)
    return cleaned.strip("-") or "value"


def artifact_url_ttl_seconds(
    *,
    timeout_seconds: int,
    configured_seconds: int | None,
) -> int:
    if configured_seconds is not None:
        if configured_seconds <= 0:
            raise ValueError("artifact URL TTL must be positive")
        return min(configured_seconds, MAX_PRESIGNED_URL_SECONDS)
    return min(max(timeout_seconds + 1800, 6 * 60 * 60), MAX_PRESIGNED_URL_SECONDS)


class R2ArtifactStore:
    """Small S3-compatible wrapper for Cloudflare R2.

    The workspace config stores only bucket/endpoint details. Credentials are
    resolved from environment variables or the normal boto3 provider chain.
    """

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str,
        region: str = "auto",
        client: Any | None = None,
    ):
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.region = region
        self._client = client

    @classmethod
    def from_workspace(cls, workspace: Path) -> "R2ArtifactStore":
        return cls.from_config(get_r2_config(workspace))

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "R2ArtifactStore":
        endpoint_url = config.get("endpoint_url")
        bucket = config.get("bucket")
        if not bucket:
            raise ValueError("R2 bucket is not configured")
        if not endpoint_url:
            account_id = config.get("account_id")
            if not account_id:
                raise ValueError("R2 endpoint_url or account_id is not configured")
            endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        return cls(
            bucket=str(bucket),
            endpoint_url=str(endpoint_url),
            region=str(config.get("region") or "auto"),
        )

    def client(self):
        if self._client is not None:
            return self._client
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - exercised through CLI usage.
            raise ImportError("R2 artifact staging requires `flashburst[r2]`.") from exc

        access_key_id = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
        secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
        session_token = os.getenv("R2_SESSION_TOKEN") or os.getenv("AWS_SESSION_TOKEN")
        kwargs: dict[str, Any] = {
            "endpoint_url": self.endpoint_url,
            "region_name": self.region,
        }
        if access_key_id and secret_access_key:
            kwargs["aws_access_key_id"] = access_key_id
            kwargs["aws_secret_access_key"] = secret_access_key
        if session_token:
            kwargs["aws_session_token"] = session_token
        self._client = boto3.client("s3", **kwargs)
        return self._client

    def input_key(self, *, run_id: str, job_id: str, field: str, source: Path) -> str:
        return "/".join(
            [
                "flashburst",
                "runs",
                _clean_key_part(run_id),
                "inputs",
                _clean_key_part(job_id),
                _clean_key_part(field),
                _clean_key_part(source.name),
            ]
        )

    def output_key(self, *, run_id: str, job_id: str) -> str:
        return "/".join(
            [
                "flashburst",
                "runs",
                _clean_key_part(run_id),
                "outputs",
                _clean_key_part(job_id),
                "result.jsonl",
            ]
        )

    def object_ref(
        self,
        *,
        key: str,
        media_type: str,
        path: Path | None = None,
    ) -> R2Object:
        size_bytes = path.stat().st_size if path is not None and path.exists() else None
        digest = sha256_file(path) if path is not None and path.exists() else None
        return R2Object(
            bucket=self.bucket,
            key=key,
            media_type=media_type,
            size_bytes=size_bytes,
            sha256=digest,
        )

    def upload_file(self, source: Path, *, key: str, media_type: str) -> R2Object:
        self.client().upload_file(
            str(source),
            self.bucket,
            key,
            ExtraArgs={"ContentType": media_type},
        )
        return self.object_ref(key=key, media_type=media_type, path=source)

    def download_file(self, *, key: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client().download_file(self.bucket, key, str(destination))
        return destination

    def presign_get(self, *, key: str, expires_seconds: int) -> str:
        return str(
            self.client().generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expires_seconds,
            )
        )

    def presign_put(self, *, key: str, media_type: str, expires_seconds: int) -> str:
        return str(
            self.client().generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": key,
                    "ContentType": media_type,
                },
                ExpiresIn=expires_seconds,
            )
        )
