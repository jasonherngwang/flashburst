# Testing

The routine validation path does not require a GPU, Runpod credentials, or paid
cloud work. It also does not require R2 credentials; artifact staging is covered
with fake S3-compatible clients in unit tests.

## Routine Check

```bash
make quality-check
```

This runs Ruff, format checking, pytest, and package build.

## Local Smoke

```bash
make smoke-local
```

The smoke target writes one JSONL input under `demo/`, drains it through the
DBOS-backed local queue with the built-in deterministic workload, and checks the
latest run with `flashburst status --results`.

Expected result:

```text
one job succeeds
.flashburst/dbos.sqlite is written
.flashburst/runs/local-smoke/results.jsonl is written
.flashburst/runs/local-smoke/outputs/local-smoke/result.jsonl is written
```

## Transcription Example

```bash
make example-transcription
```

This runs the checked-in transcription demo through the real DBOS-backed CLI
path. The workload decodes `examples/transcription_demo/samples/sample.mp3`,
records audio metadata and hashes, validates a transcript segment, and writes
JSONL output.

Expected result:

```text
one job succeeds
.flashburst/runs/example-transcription-<timestamp>/results.jsonl is written
.flashburst/runs/example-transcription-<timestamp>/outputs/sample-1/result.jsonl is written
```

## Custom Workload Smoke

For a user-owned workload:

```bash
uv run flashburst bind \
  --workload my_workload.py:run_job \
  --manifest input.jsonl \
  --params-json '{}'
uv run flashburst context
uv run flashburst run --run-id local-test
uv run flashburst status --run-id local-test --results
```

To validate a non-default DBOS system database:

```bash
uv run flashburst run my_workload.py:run_job input.jsonl \
  --run-id local-test-postgres \
  --local-slots 1 \
  --dbos-database-url postgresql://user:password@localhost:5432/flashburst
```

## Optional Runpod Flash Smoke

Runpod Flash validation may create paid Runpod usage. Run it only when you
intentionally want to validate a deployed endpoint.

Prerequisites:

```text
the flashburst controller installed with the runpod extra
RUNPOD_API_KEY or completed flash login
an already deployed Runpod Flash endpoint id
```

Configure from the workload repo:

```bash
uv run flashburst configure runpod \
  --profile <profile-name> \
  --endpoint-id <runpod-endpoint-id>

uv run flashburst check --flash --profile <profile-name>
```

Then run one bounded flash-approved input:

```bash
uv run flashburst run --hybrid \
  --limit 1 \
  --profile <profile-name> \
  --approve-flash

uv run flashburst status --results
```

Expected result:

```text
one Runpod Flash record is appended to results.jsonl
the record includes placement="flash" and a remote_job_id
```

For local-file workloads that should run unchanged on cloud workers, configure
R2 and stage the file field:

```bash
uv run flashburst configure r2 \
  --bucket <r2-bucket> \
  --account-id <cloudflare-account-id>

export R2_ACCESS_KEY_ID=<r2-access-key-id>
export R2_SECRET_ACCESS_KEY=<r2-secret-access-key>

uv run flashburst run --hybrid \
  --limit 1 \
  --profile <profile-name> \
  --approve-flash

uv run flashburst status --results
```

Expected R2-staged result:

```text
the workload still receives a local path in the audio_path field on the endpoint
the output is downloaded to .flashburst/runs/<run-id>/outputs/<job-id>/result.jsonl
results.jsonl includes durable input_artifacts/output_artifact refs without presigned URLs
```
