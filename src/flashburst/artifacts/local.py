"""Local filesystem artifact store."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from flashburst.models import ArtifactRef
from flashburst.time import utc_now


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class LocalArtifactStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for_uri(self, uri: str) -> Path:
        prefix = "local://"
        if not uri.startswith(prefix):
            raise ValueError(f"unsupported local artifact uri: {uri}")
        relative = uri[len(prefix) :].lstrip("/")
        if not relative:
            raise ValueError("local artifact uri must include a relative path")
        path = (self.root / relative).resolve()
        root = self.root.resolve()
        if root not in path.parents and path != root:
            raise ValueError(f"local artifact path escapes store root: {uri}")
        return path

    def ref_for_path(
        self,
        relative_path: str,
        *,
        media_type: str,
        producer_job_id: str | None = None,
    ) -> ArtifactRef:
        path = self.path_for_uri(f"local://{relative_path}")
        return ArtifactRef(
            uri=f"local://{relative_path}",
            media_type=media_type,
            storage="local",
            sha256=sha256_file(path),
            size_bytes=path.stat().st_size,
            producer_job_id=producer_job_id,
            created_at=utc_now(),
        )

    def put_file(self, source: Path, relative_path: str, *, media_type: str) -> ArtifactRef:
        destination = self.path_for_uri(f"local://{relative_path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        return self.ref_for_path(relative_path, media_type=media_type)

    def ensure_parent_for_uri(self, uri: str) -> Path:
        path = self.path_for_uri(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
