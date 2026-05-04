"""S3-compatible artifact store, used first with Cloudflare R2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import os
from pathlib import Path

from flashburst.artifacts.local import sha256_file
from flashburst.models import ArtifactGrant, ArtifactRef
from flashburst.time import utc_now


@dataclass(frozen=True)
class S3Uri:
    bucket: str
    key: str


def parse_s3_uri(uri: str, *, default_bucket: str | None = None) -> S3Uri:
    prefix = "s3://"
    if not uri.startswith(prefix):
        raise ValueError(f"unsupported s3 uri: {uri}")
    rest = uri[len(prefix) :]
    if "/" not in rest:
        if default_bucket is None:
            raise ValueError(f"s3 uri must include bucket and key: {uri}")
        return S3Uri(bucket=default_bucket, key=rest)
    bucket, key = rest.split("/", 1)
    if not bucket:
        if default_bucket is None:
            raise ValueError(f"s3 uri is missing bucket: {uri}")
        bucket = default_bucket
    if not key:
        raise ValueError(f"s3 uri is missing key: {uri}")
    return S3Uri(bucket=bucket, key=key)


class S3ArtifactStore:
    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str,
        region: str = "auto",
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
    ):
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.region = region
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token

    @classmethod
    def from_config(cls, config: dict) -> "S3ArtifactStore":
        endpoint_url = config.get("endpoint_url") or os.getenv("R2_ENDPOINT_URL")
        access_key_id = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
        secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        )
        session_token = os.getenv("R2_SESSION_TOKEN") or os.getenv("AWS_SESSION_TOKEN")
        required = {
            "bucket": config.get("bucket"),
            "endpoint_url": endpoint_url,
            "R2_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID": access_key_id,
            "R2_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY": secret_access_key,
        }
        missing = [key for key, value in required.items() if not value]
        if missing:
            raise ValueError(f"missing S3 artifact store setting: {', '.join(missing)}")
        return cls(
            bucket=str(config["bucket"]),
            endpoint_url=str(endpoint_url),
            region=str(config.get("region") or "auto"),
            access_key_id=str(access_key_id),
            secret_access_key=str(secret_access_key),
            session_token=session_token,
        )

    def client(self):
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token,
        )

    def upload_file(self, source: Path, uri: str, *, media_type: str) -> ArtifactRef:
        parsed = parse_s3_uri(uri, default_bucket=self.bucket)
        self.client().upload_file(
            str(source),
            parsed.bucket,
            parsed.key,
            ExtraArgs={"ContentType": media_type},
        )
        return ArtifactRef(
            uri=f"s3://{parsed.bucket}/{parsed.key}",
            media_type=media_type,
            storage="s3",
            sha256=sha256_file(source),
            size_bytes=source.stat().st_size,
            created_at=utc_now(),
        )

    def download_file(self, uri: str, destination: Path) -> Path:
        parsed = parse_s3_uri(uri, default_bucket=self.bucket)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client().download_file(parsed.bucket, parsed.key, str(destination))
        return destination

    def presign_get(self, uri: str, *, expires_seconds: int = 3600) -> ArtifactGrant:
        parsed = parse_s3_uri(uri, default_bucket=self.bucket)
        url = self.client().generate_presigned_url(
            "get_object",
            Params={"Bucket": parsed.bucket, "Key": parsed.key},
            ExpiresIn=expires_seconds,
        )
        return ArtifactGrant(
            artifact_uri=f"s3://{parsed.bucket}/{parsed.key}",
            method="GET",
            url=url,
            expires_at=utc_now() + timedelta(seconds=expires_seconds),
        )

    def presign_put(
        self,
        uri: str,
        *,
        media_type: str = "application/octet-stream",
        expires_seconds: int = 3600,
    ) -> ArtifactGrant:
        parsed = parse_s3_uri(uri, default_bucket=self.bucket)
        url = self.client().generate_presigned_url(
            "put_object",
            Params={
                "Bucket": parsed.bucket,
                "Key": parsed.key,
                "ContentType": media_type,
            },
            ExpiresIn=expires_seconds,
        )
        return ArtifactGrant(
            artifact_uri=f"s3://{parsed.bucket}/{parsed.key}",
            method="PUT",
            url=url,
            expires_at=utc_now() + timedelta(seconds=expires_seconds),
            content_type=media_type,
        )
