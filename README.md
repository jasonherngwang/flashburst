# Flashburst

Flashburst is an agent-native GPU workload runner that prioritizes **local 
GPU use first, with optional cloud GPU burst using Runpod Flash**. It's for 
Python jobs that need durable job state, artifact tracking, and resumability.

The user should describe an outcome to an agent, not memorize orchestration
commands. Flashburst gives the agent a small command surface for inspecting a
workload repo, binding the right files, running local smoke tests, generating
Runpod Flash wrappers, and reporting outputs.

My current use case is batch processing podcast transcriptions. I'm GPU-poor
with only one 3090, so when there are hundreds of episodes to transcribe, I
could use a little help. Runpod Flash is a convenient abstraction for 
serverless worker deployment and autoscaling. For transcription I'm using 
`faster-whisper` on an `AMPERE_24` GPU group.

## Agent-Native Flow

In a workload repo, ask an agent for outcomes like:

- "Make this repo Flashburst-ready. Preserve the existing CLI."
- "Bind the obvious workload and manifest, then run a local smoke."
- "Generate the Runpod Flash endpoint wrapper, but do not deploy it."
- "After I approve paid cloud work, run one hybrid smoke and report outputs."

The agent uses Flashburst primitives such as `context`, `bind`, `run`,
`status`, `queue`, and `scaffold`. `context` emits machine-readable project
state: likely workload functions, JSONL manifests, stageable file fields,
Runpod profiles, R2 readiness, and the latest result ledger.

## Architecture

```text
user intent
   |
   v
agent
   |
   v
Flashburst context/bind/run/scaffold
   |
   v
.flashburst/project.json + manifest
   |
   v
prepare inputs under .flashburst/runs/<run-id>/
   |
   v
DBOS work queue
   |
   +--> completed job id? skip already-succeeded work
   |
   v
flashburst.routed_job
   |
   +--> PRIORITY 1: acquire a local GPU slot
   |        |
   |        v
   |     run_job(input_path, output_path, params)
   |
   +--> PRIORITY 2: if flash_ok, cloud is approved,
   |        and local slots are busy, acquire a Flash slot
   |        |
   |        v
   |     R2 staged files + presigned URLs
   |        |
   |        v
   |     Runpod Flash endpoint
   |        |
   |        v
   |     downloaded output in the same local run tree
   |
   v
append latest record to results.jsonl
```

The workload repo owns business logic:

- `run_job(input_path, output_path, params)`
- Optional Flash `endpoint.py`
- JSONL manifests
- Python dependencies

Flashburst owns orchestration:

- Agent-readable discovery and binding
- DBOS queueing and routing across local and Runpod Flash slots
- R2 staging for cloud inputs and outputs
- Runpod Flash submission and remote result download
- `.flashburst/runs/<run-id>/results.jsonl`

DBOS deduplication ids make reruns resumable: succeeded jobs are skipped. Local
and cloud records append to one ledger, and successful outputs land under
`.flashburst/runs/<run-id>/outputs/<job-id>/result.jsonl`.

Cloud work requires explicit user approval. For local-file manifests,
Flashburst stages inputs in R2, passes presigned input/output URLs to Runpod
Flash, then downloads remote results into the local run tree. URLs are minted
inside the DBOS flash step right before submission, so local queue backlog does
not burn URL lifetime.

## Workload Contract

A Flashburst workload is just a Python callable with a file boundary:

```python
from pathlib import Path
from typing import Any


def run_job(
    input_path: Path,
    output_path: Path,
    params: dict[str, Any],
) -> dict[str, Any]:
    record = input_path.read_text(encoding="utf-8")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(record, encoding="utf-8")
    return {"status": "succeeded", "metrics": {}}
```

The workload should not import Flashburst, DBOS, Runpod, or R2 helpers. Keep
domain code ordinary and editable; let Flashburst handle run state and cloud
handoff.

`podcast-transcriber`(https://github.com/jasonherngwang/podcast-transcriber) 
is the intended consumer shape: a normal domain repo keeps its own CLI and 
model code, while its README tells an agent to configure Flashburst state, 
run local validation, scaffold or verify `endpoint.py`, and only run paid 
cloud canaries after explicit approval.

## Shared State

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

## Docs

For exact command syntax, use [docs/CLI_REFERENCE.md](docs/CLI_REFERENCE.md).
For validation paths, use [docs/TESTING.md](docs/TESTING.md). Manual CLI usage
is supported, but it is backup material; the primary interface is an agent
carrying the workflow through inspection, execution, validation, and reporting.
