"""SQLite persistence for the local Flashburst control plane."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from flashburst.ids import new_id
from flashburst.models import (
    AttemptStatus,
    ArtifactRef,
    CloudProfile,
    JobResult,
    JobSpec,
    JobStatus,
    PlacementKind,
)
from flashburst.time import utc_now, utc_now_iso


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  job_type TEXT NOT NULL,
  required_capability TEXT NOT NULL,
  privacy TEXT NOT NULL,
  status TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  params_json TEXT NOT NULL,
  input_artifacts_json TEXT NOT NULL,
  result_json TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS attempts (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  placement_kind TEXT NOT NULL,
  worker_id TEXT,
  cloud_profile_id TEXT,
  status TEXT NOT NULL,
  remote_job_id TEXT,
  reserved_cost_usd TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(job_id) REFERENCES jobs(id)
);

CREATE TABLE IF NOT EXISTS leases (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(job_id) REFERENCES jobs(id),
  FOREIGN KEY(attempt_id) REFERENCES attempts(id)
);

CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  uri TEXT NOT NULL UNIQUE,
  storage TEXT NOT NULL,
  media_type TEXT NOT NULL,
  sha256 TEXT,
  size_bytes INTEGER,
  producer_job_id TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cloud_profiles (
  id TEXT PRIMARY KEY,
  backend TEXT NOT NULL,
  endpoint_id TEXT,
  capability TEXT NOT NULL,
  estimated_cost_per_job_usd TEXT NOT NULL,
  max_concurrent_jobs INTEGER NOT NULL,
  config_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS budget_ledgers (
  id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  limit_usd TEXT NOT NULL,
  reserved_usd TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backend_runs (
  id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  backend TEXT NOT NULL,
  remote_job_id TEXT,
  request_json TEXT,
  response_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(attempt_id) REFERENCES attempts(id)
);
"""


@dataclass(frozen=True)
class ClaimedJob:
    job_id: str
    attempt_id: str
    lease_id: str


class FlashburstDB:
    def __init__(self, path: Path | str):
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def insert_job(self, spec: JobSpec) -> str:
        now = utc_now_iso()
        job_id = new_id("job")
        params_json = json.dumps(spec.params, sort_keys=True, separators=(",", ":"), default=str)
        input_artifacts_json = json.dumps(
            [a.model_dump(mode="json") for a in spec.input_artifacts],
            sort_keys=True,
            separators=(",", ":"),
        )
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM jobs WHERE idempotency_key = ?",
                (spec.idempotency_key,),
            ).fetchone()
            if existing:
                return str(existing["id"])
            conn.execute(
                """
                INSERT INTO jobs (
                  id, job_type, required_capability, privacy, status,
                  idempotency_key, params_json, input_artifacts_json,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    spec.job_type,
                    spec.required_capability,
                    spec.privacy.value if hasattr(spec.privacy, "value") else spec.privacy,
                    JobStatus.QUEUED.value,
                    spec.idempotency_key,
                    params_json,
                    input_artifacts_json,
                    now,
                    now,
                ),
            )
        return job_id

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None

    def get_job_input_artifacts(self, job_id: str) -> list[ArtifactRef]:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(f"job not found: {job_id}")
        raw = json.loads(job["input_artifacts_json"])
        return [ArtifactRef.model_validate(item) for item in raw]

    def get_job_params(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(f"job not found: {job_id}")
        return dict(json.loads(job["params_json"]))

    def list_jobs(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM jobs ORDER BY created_at, id").fetchall()
        return [dict(row) for row in rows]

    def list_artifacts(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM artifacts ORDER BY created_at, id").fetchall()
        return [dict(row) for row in rows]

    def list_attempts(self, job_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM attempts"
        params: tuple[str, ...] = ()
        if job_id is not None:
            query += " WHERE job_id = ?"
            params = (job_id,)
        query += " ORDER BY created_at, id"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def list_cloud_profiles(self) -> list[CloudProfile]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM cloud_profiles ORDER BY id").fetchall()
        return [
            CloudProfile(
                id=row["id"],
                backend=row["backend"],
                endpoint_id=row["endpoint_id"],
                capability=row["capability"],
                estimated_cost_per_job_usd=Decimal(str(row["estimated_cost_per_job_usd"])),
                max_concurrent_jobs=int(row["max_concurrent_jobs"]),
                config=json.loads(row["config_json"]),
            )
            for row in rows
        ]

    def claim_next_local_job(
        self,
        *,
        worker_id: str,
        capability: str,
        lease_seconds: int = 60,
    ) -> ClaimedJob | None:
        now_dt = utc_now()
        now = now_dt.isoformat()
        expires_at = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    """
                    SELECT id FROM jobs
                    WHERE status = ?
                      AND required_capability = ?
                    ORDER BY created_at, id
                    LIMIT 1
                    """,
                    (JobStatus.QUEUED.value, capability),
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None

                job_id = str(row["id"])
                attempt_id = new_id("att")
                lease_id = new_id("lease")
                conn.execute(
                    """
                    INSERT INTO attempts (
                      id, job_id, placement_kind, worker_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_id,
                        job_id,
                        PlacementKind.LOCAL.value,
                        worker_id,
                        AttemptStatus.LEASED.value,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO leases (
                      id, job_id, attempt_id, worker_id, expires_at, heartbeat_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (lease_id, job_id, attempt_id, worker_id, expires_at, now, now),
                )
                conn.execute(
                    "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                    (JobStatus.RUNNING.value, now, job_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return ClaimedJob(job_id=job_id, attempt_id=attempt_id, lease_id=lease_id)

    def create_attempt(
        self,
        *,
        job_id: str,
        placement_kind: PlacementKind,
        status: AttemptStatus,
        worker_id: str | None = None,
        cloud_profile_id: str | None = None,
        remote_job_id: str | None = None,
        reserved_cost_usd: Decimal | None = None,
    ) -> str:
        now = utc_now_iso()
        attempt_id = new_id("att")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO attempts (
                      id, job_id, placement_kind, worker_id, cloud_profile_id,
                      status, remote_job_id, reserved_cost_usd, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        attempt_id,
                        job_id,
                        placement_kind.value,
                        worker_id,
                        cloud_profile_id,
                        status.value,
                        remote_job_id,
                        str(reserved_cost_usd) if reserved_cost_usd is not None else None,
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
                    (JobStatus.RUNNING.value, now, job_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return attempt_id

    def reserve_budget(self, *, plan_id: str, limit_usd: Decimal, amount_usd: Decimal) -> bool:
        now = utc_now_iso()
        ledger_id = f"budget_{plan_id}"
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM budget_ledgers WHERE id = ?",
                    (ledger_id,),
                ).fetchone()
                if row is None:
                    reserved = Decimal("0")
                    conn.execute(
                        """
                        INSERT INTO budget_ledgers (
                          id, plan_id, limit_usd, reserved_usd, status, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (ledger_id, plan_id, str(limit_usd), "0", "open", now, now),
                    )
                else:
                    reserved = Decimal(str(row["reserved_usd"]))
                    if row["status"] != "open":
                        conn.execute("ROLLBACK")
                        return False

                if reserved + amount_usd > limit_usd:
                    status = "exhausted"
                    conn.execute(
                        """
                        UPDATE budget_ledgers
                        SET status = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (status, now, ledger_id),
                    )
                    conn.execute("COMMIT")
                    return False

                conn.execute(
                    """
                    UPDATE budget_ledgers
                    SET reserved_usd = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (str(reserved + amount_usd), now, ledger_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return True

    def get_budget_ledger(self, plan_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM budget_ledgers WHERE plan_id = ?",
                (plan_id,),
            ).fetchone()
        return dict(row) if row else None

    def upsert_cloud_profile(self, profile: CloudProfile) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO cloud_profiles (
                  id, backend, endpoint_id, capability, estimated_cost_per_job_usd,
                  max_concurrent_jobs, config_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  backend = excluded.backend,
                  endpoint_id = excluded.endpoint_id,
                  capability = excluded.capability,
                  estimated_cost_per_job_usd = excluded.estimated_cost_per_job_usd,
                  max_concurrent_jobs = excluded.max_concurrent_jobs,
                  config_json = excluded.config_json,
                  updated_at = excluded.updated_at
                """,
                (
                    profile.id,
                    profile.backend,
                    profile.endpoint_id,
                    profile.capability,
                    str(profile.estimated_cost_per_job_usd),
                    profile.max_concurrent_jobs,
                    json.dumps(profile.config, sort_keys=True, separators=(",", ":")),
                    now,
                    now,
                ),
            )

    def get_cloud_profile(self, profile_id: str) -> CloudProfile | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM cloud_profiles WHERE id = ?",
                (profile_id,),
            ).fetchone()
        if row is None:
            return None
        return CloudProfile(
            id=row["id"],
            backend=row["backend"],
            endpoint_id=row["endpoint_id"],
            capability=row["capability"],
            estimated_cost_per_job_usd=Decimal(str(row["estimated_cost_per_job_usd"])),
            max_concurrent_jobs=int(row["max_concurrent_jobs"]),
            config=json.loads(row["config_json"]),
        )

    def update_attempt_remote_job(self, *, attempt_id: str, remote_job_id: str) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE attempts
                SET remote_job_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (remote_job_id, now, attempt_id),
            )

    def record_backend_run(
        self,
        *,
        attempt_id: str,
        backend: str,
        remote_job_id: str | None,
        request: dict[str, Any] | None,
        response: dict[str, Any] | None,
    ) -> str:
        now = utc_now_iso()
        run_id = new_id("backend")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO backend_runs (
                  id, attempt_id, backend, remote_job_id,
                  request_json, response_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    attempt_id,
                    backend,
                    remote_job_id,
                    json.dumps(request, sort_keys=True, separators=(",", ":"), default=str)
                    if request is not None
                    else None,
                    json.dumps(response, sort_keys=True, separators=(",", ":"), default=str)
                    if response is not None
                    else None,
                    now,
                    now,
                ),
            )
        return run_id

    def retry_expired_leases(self) -> int:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    """
                    SELECT job_id, attempt_id
                    FROM leases
                    WHERE expires_at <= ?
                    ORDER BY expires_at, id
                    """,
                    (now,),
                ).fetchall()
                for row in rows:
                    conn.execute(
                        """
                        UPDATE attempts
                        SET status = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (AttemptStatus.EXPIRED.value, now, row["attempt_id"]),
                    )
                    conn.execute(
                        """
                        UPDATE jobs
                        SET status = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (JobStatus.QUEUED.value, now, row["job_id"]),
                    )
                    conn.execute("DELETE FROM leases WHERE attempt_id = ?", (row["attempt_id"],))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(rows)

    def record_artifact(self, artifact: ArtifactRef) -> str:
        artifact_id = new_id("art")
        created_at = artifact.created_at.isoformat() if artifact.created_at else utc_now_iso()
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM artifacts WHERE uri = ?",
                (artifact.uri,),
            ).fetchone()
            if existing:
                return str(existing["id"])
            conn.execute(
                """
                INSERT INTO artifacts (
                  id, uri, storage, media_type, sha256, size_bytes, producer_job_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    artifact.uri,
                    artifact.storage,
                    artifact.media_type,
                    artifact.sha256,
                    artifact.size_bytes,
                    artifact.producer_job_id,
                    created_at,
                ),
            )
        return artifact_id

    def complete_attempt(self, *, job_id: str, attempt_id: str, result: JobResult) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for artifact in result.output_artifacts:
                    artifact_id = new_id("art")
                    created_at = artifact.created_at.isoformat() if artifact.created_at else now
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO artifacts (
                          id, uri, storage, media_type, sha256, size_bytes,
                          producer_job_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            artifact_id,
                            artifact.uri,
                            artifact.storage,
                            artifact.media_type,
                            artifact.sha256,
                            artifact.size_bytes,
                            artifact.producer_job_id,
                            created_at,
                        ),
                    )
                conn.execute(
                    """
                    UPDATE attempts
                    SET status = ?, updated_at = ?, error = NULL
                    WHERE id = ?
                    """,
                    (AttemptStatus.SUCCEEDED.value, now, attempt_id),
                )
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, result_json = ?, updated_at = ?, error = NULL
                    WHERE id = ?
                    """,
                    (
                        JobStatus.SUCCEEDED.value,
                        result.model_dump_json(),
                        now,
                        job_id,
                    ),
                )
                conn.execute("DELETE FROM leases WHERE attempt_id = ?", (attempt_id,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def fail_attempt(self, *, job_id: str, attempt_id: str, error: str) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    UPDATE attempts
                    SET status = ?, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (AttemptStatus.FAILED.value, now, error, attempt_id),
                )
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, updated_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (JobStatus.FAILED.value, now, error, job_id),
                )
                conn.execute("DELETE FROM leases WHERE attempt_id = ?", (attempt_id,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
