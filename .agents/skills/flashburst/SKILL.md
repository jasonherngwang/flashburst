---
name: flashburst
description: Adapt and operate Flashburst workload repositories. Use when asked to make a Python workload Flashburst-ready from scratch or from existing scripts/packages/CLIs, bind or run manifests, inspect results, scaffold Runpod Flash endpoints, check R2/Runpod readiness, run local/cloud/hybrid work, or inspect DBOS queue state without making the user memorize commands.
---

# Flashburst

## Rules

- This is an agent-native app. Translate the user's requested outcome into
  Flashburst actions, run them, verify results, and report what happened. Do
  not make the user memorize or execute command sequences.
- Operate from the workload repo. Treat the Flashburst checkout as the editable
  dependency and docs source.
- Workload repos own business logic, dependencies, JSONL manifests, the thin
  `run_job(input_path, output_path, params)` adapter, and optional `endpoint.py`.
- Keep the original workload easy to edit: domain logic should remain in normal
  project modules with normal tests; Flashburst glue should be thin and obvious.
- Flashburst owns binding, DBOS local execution, resume behavior, local/cloud
  placement, R2 staging, Runpod submission, remote output download, and
  `.flashburst/runs/<run-id>/results.jsonl`.
- Do not add Flashburst, DBOS, Runpod, boto3, cloud SDKs, or presigned URL
  handling to workload business code.
- Use short commands: `context`, `bind`, `run`, `status`, `queue`, `scaffold`,
  `configure`. Use `workload inspect` and `manifest inspect/validate` only for
  diagnosis.
- Do not resurrect deleted legacy patterns: capability registry, `JobSpec`,
  `run-queue`, workload scaffold, prepare commands, SQLite job DB, leases, or
  generic artifact modules.

## New Or Existing Workload

First inspect project shape: `pyproject.toml`, `requirements.txt`, source
layout, scripts, CLIs, notebooks/exports, tests, sample inputs, and existing
manifests. If the repo is only loose Python files plus a uv venv, make it a
normal uv project unless the user explicitly wants a temporary install. Run
commands yourself and summarize outcomes; do not give setup commands as the
main answer.

```bash
uv init --bare
uv add --editable ../flashburst
```

Use extras only when preparing cloud/hybrid work:

```bash
uv add --editable "../flashburst[runpod,r2]"
```

Preserve existing CLI behavior. Prefer the least invasive adapter:

- If an existing module already has clean callable logic, add `run_job` beside
  it or bind that callable directly when it already matches the contract.
- If the workload is a CLI/script, extract reusable core logic and keep the CLI
  as a wrapper.
- If extraction is risky, create `flashburst_adapter.py` that imports and calls
  the existing code.
- If the project uses `src/`, keep imports package-native and bind with
  `package.module:run_job`; otherwise `path/to/file.py:run_job` is fine.

Editability guardrails:

- Keep `run_job` as orchestration only: parse the input record, call ordinary
  workload functions, write output, return metrics.
- Do not move all logic into `run_job`, `endpoint.py`, generated files, or
  Flashburst-specific directories.
- Preserve existing commands and tests. Add focused tests for any extracted core
  function when the original code had no coverage.
- Keep sample manifests small, human-readable, and checked in only when they are
  useful examples. Keep large data, outputs, `.flashburst/`, `.flash/`, and
  secrets out of git.

Adapter contract:

```python
def run_job(input_path: Path, output_path: Path, params: dict) -> dict:
    ...
```

The adapter reads one JSON object from `input_path`, writes one result file at
`output_path` (usually JSONL), and returns `{"status": "succeeded", "metrics": {...}}`
or `{"status": "failed", "error": "..."}`. Keep all domain dependencies in the
workload repo's `pyproject.toml`.

Create a tiny real manifest if none exists. Put it somewhere natural for the
repo, such as `manifests/local-smoke.jsonl`, `examples/manifest.jsonl`, or
`input.jsonl`:

```jsonl
{"id":"smoke-1","text":"hello"}
```

Use local file fields such as `audio_path` only when the file exists relative
to the workload repo; Flashburst can later stage those same fields through R2.

## Local Workflow

1. Inspect state:
   `uv run flashburst context --text`.
2. Diagnose only when needed:
   `uv run flashburst workload inspect <workload-spec> --json` and
   `uv run flashburst manifest validate <manifest>`.
3. Bind workload, manifest, params, and stage fields:
   `uv run flashburst bind --workload <workload-spec> --manifest <manifest>`.
   If discovery is unambiguous, `uv run flashburst bind` is acceptable.
4. Run a bounded local smoke:
   `uv run flashburst run --run-id local-smoke --local-slots 1`.
5. Inspect final counts and outputs:
   `uv run flashburst status --run-id local-smoke --results`.

Generated state lives under `.flashburst/runs/<run-id>/`. Status summaries must
count only the latest ledger record per job id.

The normal user-facing flow is: user asks for an outcome, agent adapts/binds/runs
as needed, agent reports changed files plus run ids, counts, and output paths.

## Testing Ladder

- Run the workload repo's normal tests if present, usually `uv run pytest`.
- Run `uv run flashburst check` for workspace and DBOS readiness.
- Run one fresh local smoke with a new run id, then inspect `status --results`.
- For larger local validation, use a bounded real manifest before a full batch.
- For full local validation, run the bound manifest with an explicit run id and
  no limit only after the smoke and bounded checks pass.
- Do not run credentialed R2, deploy, endpoint calls, hybrid runs, paid GPU
  work, or full batches without explicit approval.

## Cloud Preparation

- Generate remote glue from the bound workload:
  `uv run flashburst scaffold`.
- Compile/import check `endpoint.py`; keep it glue-only.
- Pass non-secret endpoint env as `--env NAME=value`; pass secrets or
  machine-specific values as `--env-from NAME`. Do not assume `.env.local`
  values reach deployed workers automatically.
- Keep resource choices not represented by scaffold flags, such as CUDA
  selectors, datacenter, image, and network volume details, in workload-owned
  `endpoint.py`.
- Prefer endpoint dependencies with wheels. Do not package CUDA/cuDNN runtime
  stacks by default; use endpoint image/resource settings.
- For local-file manifests, bind stage fields and configure R2 with non-secret
  values:
  `uv run flashburst configure r2 --bucket "$R2_BUCKET" --endpoint-url "$R2_ENDPOINT_URL"`.
- Read secrets from environment without printing values. Required cloud env:
  `R2_BUCKET`, `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`,
  `R2_SECRET_ACCESS_KEY`, and `RUNPOD_API_KEY`.
- Do not ask for `RUNPOD_ENDPOINT_ID` before deployment. After approved deploy,
  discover the id from `flash deploy` or `uv run flash env get <env> --app <app>`
  and save it with
  `uv run flashburst configure runpod --profile flash-burst --endpoint-id <id>`.

## Cloud And Hybrid Runs

- Re-check readiness before submission:
  `uv run flashburst check --flash --profile flash-burst`.
- After deploy, run a remote-only canary first:
  `uv run flashburst run --local-slots 0 --flash-slots 1 --flash-ok --approve-flash`.
- If the endpoint reports `endpoint_flash_source_fingerprint`, require it to
  match `.flash/flash_manifest.json`; retry only within a bounded rollout
  window.
- For real hybrid placement, use at least two manifest records with one local
  slot and one Flash slot:
  `uv run flashburst run --hybrid --approve-flash --run-id hybrid-smoke`.
- Hybrid should be local-first burst routing: all items enter one DBOS work
  queue, local slots stay occupied first, and Flash slots handle overflow.
- For larger hybrid validation, run a bounded real subset first. Run the full
  bound manifest only after canary and bounded hybrid pass and the user approves
  the larger paid run.
- For load tests, start with a small real batch that exercises both local and
  Flash slots. Record manifest size, slot counts, endpoint `workersMax`, queue
  observations, final ledger counts, failed records, and output paths.
- Interpret endpoint concurrency with care: `workersMax=1` validates queueing
  and handoff, not horizontal scale.
- For public URL inputs, keep the URL in the manifest and make the workload
  download it inside the job; do not predownload large files just to stage them.
- During or after longer runs, inspect DBOS state:
  `uv run flashburst queue --run-id <run-id> --details`.

## Cleanup And Reporting

- Clear only named generated state when explicitly requested. Never delete
  manifests, samples, workload code, user outputs, or secrets unless named.
- Report changed files, run id, final counts, output paths, remote job ids when
  relevant, and any paid or credentialed steps skipped.
