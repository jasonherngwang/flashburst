# Flashburst

Flashburst is a GPU workload runner that explores several ideas:
- Combining local GPU work with optional "burst" into cloud GPUs using Runpod Flash, simulating a "hybrid" GPU fleet
- Durable workflow execution, using DBOS
- An "agent-native" approach where Flashburst provides a job orchestration mechanism, and you tell your agent to adapt your existing job code to fit into the orchestrator

I'm using this to batch process podcast transcriptions using `faster-whisper`, combining my local 3090 with `AMPERE_24` group GPUs on Runpod.

## Agent-Native Flow

In an existing Python job repo that is typically manually run using CLI commands, ask your agent to:

- "Make this repo Flashburst-ready. Preserve the existing CLI."
- "Generate the Runpod Flash endpoint wrapper, but don't deploy it yet."
- "After I approve paid cloud work, run a hybrid smoke test and report results."
- "Begin batch transcriptions."

The result is that the agent wraps your job function into a Flash endpoint, and generates the scaffolding needed to track workflow executions.

## Flow

```text
user intent
   |
   v
agent sets up Flashburst bindings and scaffolding
   |
   v
prepare a batch of job inputs under .flashburst/runs/<run-id>/
   |
   v
continuously dequeue and run jobs from a DBOS queue
   |
   v
job router
   |
   +--> PRIORITY 1: run job on local GPU
   |        |
   |        v
   |     run_job(input_path, output_path, params)
   |
   |
   +- -------> PRIORITY 2: if local GPU busy, run on cloud GPU
   |              |
   |              v
   |           store inputs/outputs on Cloudflare R2; use presigned URLs
   |              |
   |              v
   |           hit Runpod Flash endpoint
   |              |
   |              v
   |           download output
   |
   v
save outputs to results.jsonl
```

## Separation of Concerns

The workload repo owns the original business logic:

- `run_job(input_path, output_path, params)`
- Flash endpoint
- JSONL manifests
- Python dependencies

Flashburst is only the orchestration layer:

- Agent-native discovery and binding
- DBOS queueing and routing across local and Runpod Flash GPUs
- Cloudflare R2 storage for cloud inputs and outputs
- Runpod Flash job submission and result download

## State

Flashburst writes durable local state that both agents and humans can inspect:

```text
.flashburst/
  config.json          # non-secret Runpod/R2 settings
  project.json         # bound workload, manifest, params, stage fields
  latest-run
  dbos.sqlite          # default DBOS system database
  runs/<run-id>/
    manifest.jsonl
    inputs/*.json
    outputs/<job-id>/result.jsonl
    results.jsonl
```

## Example

https://github.com/jasonherngwang/podcast-transcriber 
is an example repo with a CLI-invoked Python job. Its README contains an example of how you would tell your agent to adapt `podcast-transcriber` to set up Flashburst.

## Docs

For exact command syntax, use [docs/CLI_REFERENCE.md](docs/CLI_REFERENCE.md).
For validation paths, use [docs/TESTING.md](docs/TESTING.md).
