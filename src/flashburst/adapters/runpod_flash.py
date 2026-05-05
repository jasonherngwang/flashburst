"""Runpod Flash adapters."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from flashburst.artifacts.local import LocalArtifactStore
from flashburst.artifacts.s3 import S3ArtifactStore
from flashburst.db import FlashburstDB
from flashburst.models import (
    ArtifactRef,
    CloudProfile,
    ExecutionEnvelope,
    JobResult,
)

DEFAULT_RUNPOD_TIMEOUT_SECONDS = 600
DEFAULT_ARTIFACT_GRANT_EXPIRES_SECONDS = 3600
RUNPOD_TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT", "EXPIRED"}
RUNPOD_SUCCESS_STATUS = "COMPLETED"
RUNPOD_ERROR_STATUSES = RUNPOD_TERMINAL_STATUSES - {RUNPOD_SUCCESS_STATUS}


RemoteJobCallback = Callable[[str], None]
RemoteStatusCallback = Callable[[str, str, dict[str, Any]], None]
RunpodStatusFetcher = Callable[[str, str, str], dict[str, Any]]


def _profile_int(profile: CloudProfile, key: str, default: int) -> int:
    value = profile.config.get(key)
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"cloud profile {profile.id} has invalid {key}: {value!r}") from exc
    if parsed <= 0:
        raise ValueError(f"cloud profile {profile.id} {key} must be positive")
    return parsed


class RunpodFlashAdapter:
    def __init__(
        self,
        *,
        endpoint_id: str,
        endpoint_factory: Callable[[str], Any] | None = None,
        status_fetcher: RunpodStatusFetcher | None = None,
        poll_interval_seconds: float = 2.0,
    ):
        self.endpoint_id = endpoint_id
        self._endpoint_factory = endpoint_factory
        self._status_fetcher = status_fetcher
        self.poll_interval_seconds = poll_interval_seconds

    def _make_endpoint(self):
        if self._endpoint_factory is not None:
            return self._endpoint_factory(self.endpoint_id)
        from runpod_flash import Endpoint

        return Endpoint(id=self.endpoint_id)

    def _fetch_status(self, *, job_id: str, api_key: str) -> dict[str, Any]:
        if self._status_fetcher is not None:
            return self._status_fetcher(self.endpoint_id, job_id, api_key)
        url = f"https://api.runpod.ai/v2/{self.endpoint_id}/status/{job_id}"
        request = Request(url, headers={"Authorization": f"Bearer {api_key}"})
        try:
            with urlopen(request, timeout=30) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Runpod status poll failed: HTTP {exc.code} {detail}") from exc
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Runpod status poll returned non-object payload: {parsed!r}")
        return parsed

    def _result_from_payload(self, payload: dict[str, Any]) -> JobResult:
        status = str(payload.get("status") or "")
        if status in RUNPOD_ERROR_STATUSES:
            return JobResult(
                status="failed",
                error=str(payload.get("error") or payload),
                metrics={"runpod_status": status},
            )
        output = payload.get("output")
        if isinstance(output, dict) and "output" in output and isinstance(output["output"], dict):
            output = output["output"]
        if not isinstance(output, dict):
            return JobResult(
                status="failed",
                error=f"unexpected Runpod output: {output!r}",
                metrics={"runpod_status": status or None},
            )
        return JobResult.model_validate(output)

    async def _poll_status(
        self,
        *,
        job_id: str,
        timeout_seconds: float,
        on_status: RemoteStatusCallback | None,
    ) -> JobResult | None:
        api_key = os.getenv("RUNPOD_API_KEY")
        if not api_key:
            return None

        deadline = time.monotonic() + timeout_seconds
        last_status: str | None = None
        while True:
            payload = await asyncio.to_thread(
                self._fetch_status,
                job_id=job_id,
                api_key=api_key,
            )
            status = str(payload.get("status") or "")
            if status and status != last_status:
                if on_status is not None:
                    on_status(job_id, status, payload)
                last_status = status
            if status in RUNPOD_TERMINAL_STATUSES:
                return self._result_from_payload(payload)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Runpod job {job_id} did not finish within {timeout_seconds}s")
            await asyncio.sleep(self.poll_interval_seconds)

    async def run_envelope(
        self,
        envelope: ExecutionEnvelope,
        *,
        timeout_seconds: float = DEFAULT_RUNPOD_TIMEOUT_SECONDS,
        on_remote_job_id: RemoteJobCallback | None = None,
        on_status: RemoteStatusCallback | None = None,
    ) -> tuple[str, JobResult]:
        endpoint = self._make_endpoint()
        job = await endpoint.run(envelope.model_dump(mode="json"))
        remote_job_id = str(job.id)
        if on_remote_job_id is not None:
            on_remote_job_id(remote_job_id)

        polled_result = await self._poll_status(
            job_id=remote_job_id,
            timeout_seconds=timeout_seconds,
            on_status=on_status,
        )
        if polled_result is not None:
            return remote_job_id, polled_result

        await job.wait(timeout=timeout_seconds)
        if job.error:
            return remote_job_id, JobResult(status="failed", error=str(job.error))
        output = job.output
        if isinstance(output, dict) and "output" in output and isinstance(output["output"], dict):
            output = output["output"]
        if not isinstance(output, dict):
            return remote_job_id, JobResult(
                status="failed",
                error=f"unexpected Runpod output: {output!r}",
            )
        return remote_job_id, JobResult.model_validate(output)


class RunpodFlashPlanRunner:
    def __init__(
        self,
        *,
        db: FlashburstDB,
        workspace: Path,
        s3_store: S3ArtifactStore,
        adapter_factory: Callable[[CloudProfile], RunpodFlashAdapter] | None = None,
        status_callback: RemoteStatusCallback | None = None,
        timeout_seconds: float = DEFAULT_RUNPOD_TIMEOUT_SECONDS,
        grant_expires_seconds: int = DEFAULT_ARTIFACT_GRANT_EXPIRES_SECONDS,
    ):
        self.db = db
        self.workspace = workspace
        self.s3_store = s3_store
        self._adapter_factory = adapter_factory
        self.status_callback = status_callback
        self.timeout_seconds = timeout_seconds
        self.grant_expires_seconds = grant_expires_seconds

    def _adapter_for(self, profile: CloudProfile) -> RunpodFlashAdapter:
        if self._adapter_factory is not None:
            return self._adapter_factory(profile)
        if profile.endpoint_id is None:
            raise ValueError(f"cloud profile {profile.id} has no endpoint_id")
        return RunpodFlashAdapter(endpoint_id=profile.endpoint_id)

    def _stage_input(self, *, job_id: str, input_ref: ArtifactRef) -> ArtifactRef:
        if input_ref.storage == "s3":
            return input_ref
        local_store = LocalArtifactStore(self.workspace / "artifacts")
        source = local_store.path_for_uri(input_ref.uri)
        destination = f"s3://{self.s3_store.bucket}/flashburst/inputs/{job_id}/{source.name}"
        staged = self.s3_store.upload_file(
            source,
            destination,
            media_type=input_ref.media_type,
        )
        self.db.record_artifact(staged)
        return staged

    def _output_ref(self, *, job_id: str, attempt_id: str) -> ArtifactRef:
        return ArtifactRef(
            uri=f"s3://{self.s3_store.bucket}/flashburst/outputs/{job_id}/{attempt_id}/result.jsonl",
            media_type="application/x-ndjson",
            storage="s3",
            producer_job_id=job_id,
        )

    async def run_claimed_job(
        self,
        *,
        job_id: str,
        attempt_id: str,
        profile: CloudProfile,
    ) -> bool:
        try:
            inputs = self.db.get_job_input_artifacts(job_id)
            if len(inputs) != 1:
                raise ValueError("Runpod Flash runner currently expects exactly one input artifact")
            staged_input = self._stage_input(job_id=job_id, input_ref=inputs[0])
            output_ref = self._output_ref(job_id=job_id, attempt_id=attempt_id)
            grant_expires_seconds = _profile_int(
                profile,
                "artifact_grant_expires_seconds",
                self.grant_expires_seconds,
            )
            timeout_seconds = _profile_int(
                profile,
                "run_timeout_seconds",
                int(self.timeout_seconds),
            )
            read_grant = self.s3_store.presign_get(
                staged_input.uri,
                expires_seconds=grant_expires_seconds,
            )
            write_grant = self.s3_store.presign_put(
                output_ref.uri,
                media_type=output_ref.media_type,
                expires_seconds=grant_expires_seconds,
            )
            envelope = ExecutionEnvelope(
                job_id=job_id,
                attempt_id=attempt_id,
                capability=profile.capability,
                params=self.db.get_job_params(job_id),
                input_artifacts=[staged_input],
                output_artifacts=[output_ref],
                artifact_grants=[read_grant, write_grant],
            )
            remote_job_id: str | None = None

            def record_remote_job_id(value: str) -> None:
                nonlocal remote_job_id
                remote_job_id = value
                self.db.update_attempt_remote_job(
                    attempt_id=attempt_id,
                    remote_job_id=value,
                )

            remote_job_id, result = await self._adapter_for(profile).run_envelope(
                envelope,
                timeout_seconds=timeout_seconds,
                on_remote_job_id=record_remote_job_id,
                on_status=self.status_callback,
            )
            if remote_job_id is not None:
                self.db.update_attempt_remote_job(
                    attempt_id=attempt_id,
                    remote_job_id=remote_job_id,
                )
            self.db.record_backend_run(
                attempt_id=attempt_id,
                backend="runpod_flash",
                remote_job_id=remote_job_id,
                request=envelope.model_dump(mode="json"),
                response=result.model_dump(mode="json"),
            )
            if result.status == "succeeded":
                self.db.complete_attempt(
                    job_id=job_id,
                    attempt_id=attempt_id,
                    result=result,
                )
                return True
            self.db.fail_attempt(
                job_id=job_id,
                attempt_id=attempt_id,
                error=result.error or "Runpod Flash job failed",
            )
            return False
        except Exception as exc:
            self.db.fail_attempt(job_id=job_id, attempt_id=attempt_id, error=str(exc))
            raise
