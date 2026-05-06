from __future__ import annotations

from pathlib import Path
from typing import Any

from flashburst.adapters.dbos_queue import _materialize_flash_output, _prepare_flash_payload
from flashburst.adapters.r2_artifacts import (
    MAX_PRESIGNED_URL_SECONDS,
    R2ArtifactStore,
    artifact_url_ttl_seconds,
    sha256_file,
)
from flashburst.models import JobResult, WorkItem


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[dict[str, Any]] = []
        self.presigned: list[dict[str, Any]] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, Any],
    ) -> None:
        self.uploads.append(
            {
                "filename": filename,
                "bucket": bucket,
                "key": key,
                "extra_args": ExtraArgs,
            }
        )

    def generate_presigned_url(
        self,
        method: str,
        *,
        Params: dict[str, Any],
        ExpiresIn: int,
    ) -> str:
        self.presigned.append({"method": method, "params": Params, "expires": ExpiresIn})
        return f"https://signed.example/{method}/{Params['Key']}?ttl={ExpiresIn}"

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        Path(filename).write_text(f"downloaded from {bucket}/{key}\n", encoding="utf-8")


def test_artifact_url_ttl_is_late_and_bounded() -> None:
    assert artifact_url_ttl_seconds(timeout_seconds=600, configured_seconds=None) == 21600
    assert artifact_url_ttl_seconds(timeout_seconds=7200, configured_seconds=900) == 900
    assert (
        artifact_url_ttl_seconds(timeout_seconds=600, configured_seconds=10**9)
        == MAX_PRESIGNED_URL_SECONDS
    )


def test_prepare_flash_payload_stages_input_and_presigns_output(tmp_path: Path) -> None:
    audio = tmp_path / "samples" / "sample.mp3"
    audio.parent.mkdir()
    audio.write_bytes(b"audio bytes")
    client = FakeS3Client()
    store = R2ArtifactStore(
        bucket="podcast-artifacts",
        endpoint_url="https://account.r2.cloudflarestorage.com",
        client=client,
    )
    item = WorkItem(
        id="sample-1",
        input={"id": "sample-1", "audio_path": "samples/sample.mp3"},
        params={"sample_rate": 16000},
        flash_ok=True,
        input_path="runs/run-1/inputs/0000-sample-1.json",
    )

    payload, input_artifacts, output_artifact, returned_store = _prepare_flash_payload(
        item=item,
        workspace=tmp_path / ".flashburst",
        run_id="run-1",
        project_root=tmp_path,
        timeout_seconds=7200,
        stage_fields=("audio_path",),
        configured_ttl_seconds=3600,
        artifact_store=store,
    )

    assert returned_store is store
    assert payload["input"] == {"id": "sample-1", "audio_path": "samples/sample.mp3"}
    assert payload["artifact_url_ttl_seconds"] == 3600
    assert payload["input_files"][0]["field"] == "audio_path"
    assert payload["input_files"][0]["get_url"].startswith("https://signed.example/get_object/")
    assert payload["output_file"]["put_url"].startswith("https://signed.example/put_object/")
    assert "get_url" not in input_artifacts[0]
    assert "put_url" not in output_artifact
    assert input_artifacts[0]["bucket"] == "podcast-artifacts"
    assert input_artifacts[0]["key"].endswith("/inputs/sample-1/audio_path/sample.mp3")
    assert input_artifacts[0]["sha256"] == sha256_file(audio)
    assert output_artifact["key"].endswith("/outputs/sample-1/result.jsonl")
    assert client.uploads[0]["extra_args"] == {"ContentType": "audio/mpeg"}
    assert [item["method"] for item in client.presigned] == ["get_object", "put_object"]


def test_prepare_flash_payload_does_not_store_presigned_urls_for_url_inputs(
    tmp_path: Path,
) -> None:
    client = FakeS3Client()
    store = R2ArtifactStore(
        bucket="podcast-artifacts",
        endpoint_url="https://account.r2.cloudflarestorage.com",
        client=client,
    )
    item = WorkItem(
        id="url-source",
        input={"id": "url-source", "audio_path": "https://example.com/audio.mp3"},
        flash_ok=True,
        input_path="runs/run-1/inputs/0000-url-source.json",
    )

    payload, input_artifacts, output_artifact, _store = _prepare_flash_payload(
        item=item,
        workspace=tmp_path / ".flashburst",
        run_id="run-1",
        project_root=tmp_path,
        timeout_seconds=600,
        stage_fields=("audio_path",),
        configured_ttl_seconds=None,
        artifact_store=store,
    )

    assert payload["input_files"] == []
    assert input_artifacts == []
    assert output_artifact["storage"] == "r2"
    assert "put_url" not in output_artifact
    assert client.uploads == []
    assert [item["method"] for item in client.presigned] == ["put_object"]


def test_materialize_flash_output_downloads_to_normal_run_output(tmp_path: Path) -> None:
    client = FakeS3Client()
    store = R2ArtifactStore(
        bucket="podcast-artifacts",
        endpoint_url="https://account.r2.cloudflarestorage.com",
        client=client,
    )
    output_path = tmp_path / ".flashburst" / "runs" / "run-1" / "outputs" / "a" / "result.jsonl"
    artifact = store.object_ref(
        key="flashburst/runs/run-1/outputs/a/result.jsonl",
        media_type="application/x-ndjson",
    ).as_record()

    updated = _materialize_flash_output(
        result=JobResult(status="succeeded", metrics={"remote": True}),
        output_path=output_path,
        output_artifact=artifact,
        artifact_store=store,
    )

    assert output_path.read_text(encoding="utf-8") == (
        "downloaded from podcast-artifacts/flashburst/runs/run-1/outputs/a/result.jsonl\n"
    )
    assert updated["size_bytes"] == output_path.stat().st_size
    assert updated["sha256"] == sha256_file(output_path)
