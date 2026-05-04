# Flashburst

Flashburst is a local-first GPU job runner with explicit Runpod Flash burst
execution. It tracks jobs, attempts, artifacts, results, cloud profiles, and
budget reservations in a local SQLite workspace.

Validated path:

```text
embedding job -> local worker or Runpod Flash -> local/R2 artifacts -> result record
```

## Install

```bash
uv sync --extra dev --extra s3 --extra runpod
uv run flashburst --help
make quality-check
```

For local embedding/GPU smoke tests:

```bash
uv sync --extra dev --extra s3 --extra runpod --extra embeddings
```

## Configuration

Flashburst stores non-secret state under `.flashburst/`.

Secrets are read from the shell environment:

```bash
cp .env.example .env.local
$EDITOR .env.local
chmod 600 .env.local
set -a
source .env.local
set +a
```

Required for R2:

```bash
export R2_BUCKET=your-r2-bucket
export R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
```

Required for Runpod execution:

```bash
export RUNPOD_API_KEY=...
```

## CLI

The normal workflow uses seven top-level commands:

```bash
uv run flashburst init
uv run flashburst check --cloud
uv run flashburst configure r2 --bucket "$R2_BUCKET" --endpoint-url "$R2_ENDPOINT_URL"
uv run flashburst configure runpod --endpoint-id <runpod-endpoint-id>

mkdir -p demo
printf '%s\n' '{"id":"cloud-a","text":"hello from flashburst on runpod"}' > demo/cloud-texts.jsonl
uv run flashburst prepare embeddings demo/cloud-texts.jsonl

uv run flashburst preview .flashburst/jobs/embeddings.jsonl \
  --cloud \
  --profile bge-small-burst \
  --budget 1.00
uv run flashburst execute <plan-id> --approve
uv run flashburst status --pull --results
```

Advanced diagnostic commands remain available but are hidden from top-level help:
`artifacts`, `cloud`, `inspect`, `leases`, `worker`, `examples`, `submit`,
`plan`, `approve`, `run`, and `doctor`.

## Local Smoke

```bash
mkdir -p demo
printf '%s\n' '{"id":"local-a","text":"hello from local flashburst"}' > demo/texts.jsonl

uv run flashburst init
uv run flashburst prepare embeddings demo/texts.jsonl \
  --capability embedding.bge-small-en-v1.5 \
  --model-name sentence-transformers/all-MiniLM-L6-v2 \
  --batch-size 1
uv run flashburst submit .flashburst/jobs/embeddings.jsonl
uv run flashburst worker run \
  --id local-smoke \
  --capability embedding.bge-small-en-v1.5 \
  --once
uv run flashburst status --results
```

## R2 Smoke

```bash
uv run flashburst configure r2 --bucket "$R2_BUCKET" --endpoint-url "$R2_ENDPOINT_URL"

printf 'hello r2\n' > demo/r2-smoke.txt
uv run flashburst artifacts put \
  demo/r2-smoke.txt \
  "s3://$R2_BUCKET/flashburst/smoke/test.txt" \
  --media-type text/plain
uv run flashburst artifacts grant-read \
  "s3://$R2_BUCKET/flashburst/smoke/test.txt" \
  --expires-seconds 300
```

The returned presigned URL should download `hello r2`.

## Runpod Flash Smoke

Deploy the example endpoint:

```bash
cd examples/runpod_flash_embedding_endpoint
uv run flash deploy
```

Copy the endpoint id from the deploy URL:

```text
https://api.runpod.ai/v2/<endpoint-id>/runsync
```

Configure and run from the repo root:

```bash
uv run flashburst configure runpod \
  --endpoint-id <endpoint-id> \
  --estimated-cost-per-job-usd 0.05
uv run flashburst check --cloud

mkdir -p demo
printf '%s\n' '{"id":"cloud-a","text":"hello from flashburst on runpod"}' > demo/cloud-texts.jsonl
uv run flashburst prepare embeddings demo/cloud-texts.jsonl

uv run flashburst preview .flashburst/jobs/embeddings.jsonl \
  --cloud \
  --profile bge-small-burst \
  --budget 1.00
uv run flashburst execute <plan-id> --approve
uv run flashburst status --pull --results
```

Expected result:

```text
remote job id recorded
output artifact stored in R2
output artifact pulled under .flashburst/artifacts/pulled/
metrics include model, device, input count, vector dimension, and timing
```
