# Testing

This file covers the external validation path for Flashburst. The default checks
do not require credentials, a GPU, R2, Runpod, or paid cloud work.

## Routine Check

Run the normal local quality gate:

```bash
make quality-check
```

This runs Ruff, format checking, pytest, and package build. A clean result should
look like:

```text
All checks passed
tests passing
sdist and wheel built successfully
```

## Local Smoke

This verifies the CLI, local SQLite workspace, deterministic built-in workload,
artifact writing, and result inspection without cloud credentials.

```bash
mkdir -p demo
printf '%s\n' '{"id":"local-a","text":"hello from local flashburst"}' > demo/texts.jsonl

uv run flashburst init
uv run flashburst prepare embeddings demo/texts.jsonl \
  --capability embedding.fake-deterministic \
  --batch-size 1

uv run flashburst run-queue .flashburst/jobs/embeddings.jsonl \
  --local-slots 1

uv run flashburst status --results
```

Expected result:

```text
one job succeeds
an output artifact is written under .flashburst/artifacts/outputs/
status --results shows the completed result
```

## Custom Workload Smoke

For a user-owned workload, the validation sequence is the same shape:

```text
flashburst init
flashburst capability add <module>:capability --project-root .
python prepare_jobs.py <manifest>
flashburst run-queue .flashburst/jobs/<job-file>.jsonl --local-slots 1
flashburst status --results
```

The companion `transcription-test` repo is an example of this shape.
It keeps transcription logic in the workload project and uses Flashburst only
for job state, capability loading, local execution, artifact tracking, and
optional cloud placement.

## Optional Cloud Smoke

Cloud validation is optional and may create paid Runpod/R2 usage. Run it only
when you intentionally want to validate a deployed endpoint.

Prerequisites:

```text
R2_BUCKET
R2_ENDPOINT_URL
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
RUNPOD_API_KEY or completed flash login
an already deployed Runpod Flash endpoint id
```

Configure Flashburst from the workload project:

```bash
uv run flashburst configure r2 \
  --bucket "$R2_BUCKET" \
  --endpoint-url "$R2_ENDPOINT_URL"

uv run flashburst configure runpod \
  --profile <profile-name> \
  --capability <capability-name> \
  --endpoint-id <runpod-endpoint-id> \
  --max-concurrent-jobs 1

uv run flashburst check --cloud
```

Then prepare a cloud-eligible job with the workload's prep script and run a
bounded queue:

```bash
uv run python prepare_jobs.py cloud-manifest.jsonl --cloud-ok --limit 1

uv run flashburst run-queue .flashburst/jobs/<job-file>.jsonl \
  --local-slots 1 \
  --cloud-slots 1 \
  --profile <profile-name> \
  --approve-cloud

uv run flashburst status --pull --results
```

Expected result:

```text
local and/or Runpod attempts are recorded
remote artifacts are staged through R2/S3 grants
status --pull --results shows completed output artifacts
```
