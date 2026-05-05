"""Small Runpod Flash client wrapper used by DBOS flash workflows."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from flashburst.models import JobResult

DEFAULT_RUNPOD_TIMEOUT_SECONDS = 600


class RunpodFlashJobError(RuntimeError):
    """Error raised after Runpod accepted a job and returned an id."""

    def __init__(self, remote_job_id: str, message: str):
        super().__init__(message)
        self.remote_job_id = remote_job_id


class RunpodFlashAdapter:
    def __init__(
        self,
        *,
        endpoint_id: str,
        endpoint_factory: Callable[[str], Any] | None = None,
    ):
        self.endpoint_id = endpoint_id
        self._endpoint_factory = endpoint_factory

    def _make_endpoint(self):
        if self._endpoint_factory is not None:
            return self._endpoint_factory(self.endpoint_id)
        from runpod_flash import Endpoint

        return Endpoint(id=self.endpoint_id)

    async def run_payload(
        self,
        payload: dict[str, Any],
        *,
        timeout_seconds: float = DEFAULT_RUNPOD_TIMEOUT_SECONDS,
    ) -> tuple[str, JobResult]:
        endpoint = self._make_endpoint()
        job = await endpoint.run(payload)
        remote_job_id = str(job.id)
        try:
            await job.wait(timeout=timeout_seconds)
            if job.error:
                return remote_job_id, JobResult(status="failed", error=str(job.error))
            output = job.output
            if (
                isinstance(output, dict)
                and "output" in output
                and isinstance(output["output"], dict)
            ):
                output = output["output"]
            if not isinstance(output, dict):
                return remote_job_id, JobResult(
                    status="failed",
                    error=f"unexpected Runpod output: {output!r}",
                )
            return remote_job_id, JobResult.model_validate(output)
        except Exception as exc:
            raise RunpodFlashJobError(remote_job_id, str(exc)) from exc

    def run_payload_sync(
        self,
        payload: dict[str, Any],
        *,
        timeout_seconds: float = DEFAULT_RUNPOD_TIMEOUT_SECONDS,
    ) -> tuple[str, JobResult]:
        return asyncio.run(self.run_payload(payload, timeout_seconds=timeout_seconds))
