# Testing

## Routine Checks

No credentials, GPU, R2 bucket, or Runpod endpoint required:

```bash
make dev
make quality-check
```

Equivalent direct commands:

```bash
uv sync --extra dev --extra s3 --extra runpod
uv run ruff check
uv run pytest
```

## Local GPU Smoke

Requires the embeddings extra and a working local GPU environment:

```bash
make dev-full
make smoke-local
```

Expected result:

```text
one embedding job succeeds
status --results reports the GPU device
output is written under .flashburst/artifacts/outputs/
```

## Cloud Smoke

Requires Cloudflare R2 credentials, Runpod authentication, a deployed Runpod
Flash endpoint, and explicit approval before paid work.

```bash
uv run flashburst configure r2 --bucket "$R2_BUCKET" --endpoint-url "$R2_ENDPOINT_URL"
uv run flashburst configure runpod --endpoint-id <runpod-endpoint-id>
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
output artifact pulled locally
result metrics include model, device, input count, vector dimension, and timing
```
