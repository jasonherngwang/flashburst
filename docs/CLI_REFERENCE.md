# Flashburst CLI Reference

This is backup material for humans operating without an agent and for agents
that need exact syntax during a narrow diagnosis. The primary README uses
agent requests and outcomes instead of command sequences.

Active short primitives: `context`, `bind`, `run`, `status`, `queue`,
`scaffold`, `configure`, `workload inspect`, and `manifest inspect/validate`.

## Install In A Workload Repo

```bash
uv add --editable ../flashburst
```

Add optional extras only when the repo will run remote jobs or R2-staged local
files.

```bash
uv add --editable "../flashburst[runpod,r2]"
```

## Bind And Run Locally

```bash
uv run flashburst context --text
uv run flashburst bind \
  --workload my_workload.py:run_job \
  --manifest input.jsonl \
  --params-json '{"max_duration_seconds":1}'
uv run flashburst run --run-id local-smoke
uv run flashburst status --run-id local-smoke --results
```

If discovery is enough:

```bash
uv run flashburst bind --params-json '{"max_duration_seconds":1}'
uv run flashburst run --run-id local-smoke
```

Inspect primitives:

```bash
uv run flashburst workload inspect my_workload.py:run_job --json
uv run flashburst manifest inspect input.jsonl --json
uv run flashburst manifest validate input.jsonl
```

## Repo Transcription Demo

From the Flashburst checkout:

```bash
make dev
make example-transcription
```

## Runpod Flash And R2

Generate a user-owned endpoint wrapper from the bound workload:

```bash
uv run flashburst scaffold --gpu AMPERE_24
```

When the deployed endpoint needs environment variables, declare them on the
generated Flash resource instead of relying on `.env` being carried into the
worker:

```bash
uv run flashburst scaffold \
  --gpu AMPERE_24 \
  --env HF_HOME=/tmp/huggingface \
  --env-from HF_TOKEN
```

Use `--env` only for non-secret literal values. Use `--env-from` for secrets or
machine-specific values; the generated endpoint reads those values from
`os.environ` at Flash build/deploy time rather than writing secret values into
source.

Build/deploy and save the endpoint id only after cloud work is approved:

```bash
uv run flash build
uv run flash deploy
```

```bash
uv run flashburst configure runpod \
  --profile flash-burst \
  --endpoint-id <runpod-endpoint-id>
```

For local-file manifests, configure R2 once from a workload-local `.env.local`.
Use Flashburst's `.env.example` as the template. Fill `R2_BUCKET`,
`R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and
`RUNPOD_API_KEY`. Credentials stay in environment variables and are not written
to `.flashburst/config.json`.

```bash
set -a
source .env.local
set +a

uv run flashburst configure r2 \
  --bucket "$R2_BUCKET" \
  --endpoint-url "$R2_ENDPOINT_URL"
```

Run the same bound manifest across local and cloud workers only after explicit
approval:

```bash
uv run flashburst run --hybrid \
  --profile flash-burst \
  --approve-flash \
  --run-id hybrid-smoke
```
