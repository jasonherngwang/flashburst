---
name: flashburst
description: Adapt and operate Flashburst workload repositories. Use when a user asks to install Flashburst from a local checkout, make a Python workload Flashburst-ready, add a thin run_job(input_path, output_path, params) adapter, bind or run manifests, inspect results, clear generated state, scaffold Runpod Flash endpoints, check R2/Runpod readiness, run local/cloud/hybrid work, or observe DBOS queue state without requiring command memorization.
---

# Flashburst

## Mission

Operate from the workload repo. Inspect the existing workload, make the
smallest durable adaptation, run the right Flashburst commands, verify results,
and report changed files plus run output paths.

Use the Flashburst checkout only as an editable dependency and documentation
source. Do not run the workload inside the Flashburst development environment.

## Responsibilities

Workload repo owns business logic, domain dependencies, JSONL manifests, the
thin `run_job(input_path, output_path, params)` adapter, and optional
`endpoint.py`.

Flashburst owns binding, DBOS-backed local execution, resume behavior,
local/cloud placement, R2 staging, Runpod Flash submission, remote output
download, and `.flashburst/runs/<run-id>/results.jsonl`.

Do not add Flashburst, DBOS, Runpod, boto3, cloud SDKs, or presigned URL logic
to workload business code.

## Bootstrap

- Copy this skill directory into `.agents/skills/flashburst` when repo-local
  skills are supported.
- Install Flashburst into the workload repo environment from the local checkout,
  usually `uv add --editable ../flashburst`.
- Add Runpod/R2 extras only when preparing or running cloud/hybrid work.
- Add `AGENTS.md` only when persistent repo-local guidance is useful; keep it
  short and point back to this skill.

## Adapt A Workload

1. Inspect dependency files, entrypoints, tests, existing manifests, and current
   `.flashburst` state.
2. Preserve existing CLI behavior.
3. Prefer extracting reusable core logic from CLI/notebook-style code before
   adding Flashburst. Use a smallest wrapper only when extraction is riskier
   than the requested smoke.
4. Add a thin adapter:

```python
def run_job(input_path: Path, output_path: Path, params: dict) -> dict:
    ...
```

5. Keep `endpoint.py` as generated remote glue only; no business logic there.
6. Create or identify a small JSONL manifest that does real work.
7. Bind workload, manifest, params, and any local-file stage fields.
8. Run a local smoke and inspect final status plus outputs.

## Cloud Preparation

- Generate or update `endpoint.py` from the bound workload and compile/import
  check it.
- Prefer endpoint dependencies that have prebuilt wheels. Avoid runtime source
  installs unless that is the smallest reliable compatibility path.
- Do not package GPU runtime stacks such as CUDA/cuDNN wheels into the Runpod
  Flash endpoint by default. Use the endpoint's image/CUDA selectors and let
  the Flash worker image provide the GPU runtime. Local developer machines may
  need separate local runtime packages; keep that local-only.
- Ask the user to fill `.env.local` from the Flashburst checkout's
  `.env.example` when present. Required cloud keys are `R2_BUCKET`,
  `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and
  `RUNPOD_API_KEY`.
- Source `.env.local` without printing secrets. It is okay to report variable
  names that are present or missing.
- Configure R2 with non-secret values:
  `uv run flashburst configure r2 --bucket "$R2_BUCKET" --endpoint-url "$R2_ENDPOINT_URL"`.
  Do not require `R2_ACCOUNT_ID` when `R2_ENDPOINT_URL` is set.
- Do not ask for `RUNPOD_ENDPOINT_ID` before deployment. `flash deploy`
  generates it; discover it from deploy output or
  `uv run flash env get <env> --app <app>`, then save it with
  `uv run flashburst configure runpod --profile flash-burst --endpoint-id <id>`.
- Do not ask for `RUNPOD_PROFILE`. It is only Flashburst's local alias; use
  `flash-burst` unless the user explicitly wants multiple profiles.
- Stop before uploads, deploys, endpoint calls, or paid GPU work unless the
  user has approved cloud execution in the conversation.

## Cloud And Hybrid Runs

- Re-check Flashburst, R2, and Runpod readiness before submission.
- If the user has expressed intent to perform a cloud test, handle setup with
  minimal prompting: source `.env.local`, configure R2, generate and verify
  `endpoint.py`, deploy when needed, discover the endpoint id, and save the
  `flash-burst` profile.
- After `flash deploy`, use a deterministic rollout canary. `flash env get`
  confirms the active build id, and endpoint health shows worker/queue counts,
  but neither proves a warm queue worker is serving new code. Run a remote-only
  one-job canary and require its `endpoint_flash_source_fingerprint` to match
  `.flash/flash_manifest.json` when the endpoint reports that metric. If stale,
  retry within a bounded rollout window.
- Run remote-only first with `--local-slots 0 --flash-slots 1`.
- For true hybrid placement, use at least two manifest records with one local
  slot and one Flash slot.
- Bind local-file fields as stage fields so local and cloud runs use the same
  manifest.
- Report final status counts, output paths, remote job ids, and any rollout or
  staging failures.

## Load Testing

- Start with a bounded real workload unless the user explicitly asks for a full
  batch.
- Use enough records to exercise both local and Flash queues.
- Note endpoint `workersMax` before interpreting cloud concurrency:
  `workersMax=1` tests queueing, not horizontal scale.
- During the run, inspect DBOS queue state from the workload workspace.
  Prefer `DBOS.list_queued_workflows()` / `DBOS.list_workflows()` when
  practical; direct SQLite inspection of `.flashburst/dbos.sqlite` can
  summarize `workflow_status` by `queue_name` and `status`.
- Endpoint health is separate from DBOS state; it can show Runpod
  `jobs.inQueue`, `jobs.inProgress`, and worker counts.
- Report queue observations, final ledger counts, remote job ids, and endpoint
  health after the run.

## Status And State

- `uv run flashburst context` gives agent-readable binding and next actions.
- `uv run flashburst status --run-id <id> --results` gives final ledger counts
  and output paths.
- Generated run state belongs under `.flashburst/runs/<run-id>/`.
- Clear only named generated state when explicitly requested. Never delete
  source manifests, samples, workload code, user outputs, or secrets unless
  explicitly named.
- Status summaries should count only the latest ledger record per job id.

## Approval Gates

Ask before modifying core workload logic beyond a thin adapter, deleting user
data, uploading local files, writing real cloud config, deploying or invoking a
Runpod endpoint, running paid GPU work, or expanding from a smoke to a full
batch.

Local smoke testing with local files and no paid cloud resources is allowed.

## Completion Checklist

- Binding/context matches the expected workload.
- Local smoke succeeded unless the request was inspection-only.
- Cloud/hybrid runs, when requested and approved, passed a remote-only canary
  before hybrid or load testing.
- Outputs and `results.jsonl` exist under `.flashburst/runs/<run-id>/`.
- Endpoint wrapper is present only when requested and remains glue code.
- Final response lists changed files, output paths, status counts, remote job
  ids when relevant, and any paid or credentialed steps intentionally skipped.
