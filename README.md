# Flashburst

Flashburst is a lightweight local control plane for **distributing workloads
across local and cloud GPUs.**

It runs user-owned Python jobs, defaults to local execution, and can "burst" to
a Runpod Flash endpoint to get things done faster. It's meant for projects that
have outgrown a Jupyter notebook and need durable job state, artifact tracking,
and resumability.

My current use case is batch processing podcast transcriptions. I am GPU-poor
with only one 3090, so when there are hundreds of episodes to transcribe, I
could use a little help. Runpod Flash offers a pretty easy-to-use interface that
abstracts worker deployment and autoscaling. For transcription I'm using
`faster-whisper` on an `AMPERE_24` GPU group, with a few cloud workers.

## Features

Flashburst is simple and local-first:

- We use SQLite as a durable local queue, storing jobs, attempts, leases,
  artifacts, and results.
- The local GPU gets first claim. I rescued my poor GPU from a crypto miner's
  garage and want to put it to good use. Cloud slots only claim `cloud_ok` work
  when local capacity is already busy.
- Workloads often involve loading large model weights. Flash workers can keep
  model state in memory across jobs so we don't have to reload it, as long as
  the idle timeout has not expired. When the job is done, the workers can scale
  down to zero.
- Cloudflare R2 (object storage) serves as the handoff layer between our local
  controller and cloud workers. We use presigned URLs so cloud workers get only
  temporary artifact access.
- Model logic stays in our workload project. Flashburst only owns queueing,
  placement, artifacts, and state.

```text
             .flashburst queue
                    |
                    v
          flashburst run-queue
                    |
        +-----------+------------+
        |                        |
 local slot available?      local slots busy
        |                        |
        v                        v
  run on local GPU       cloud_ok + approved?
                                 |
                                 v
              Cloudflare R2 presigned URL handoff
                                 |
                                 v
                         Runpod Flash worker
```

`run-queue` is the foreground controller that imports jobs, leases work to local
and cloud slots, records attempts, and exits when the selected queue is drained.
If interrupted, rerun the same command to continue from saved state.

## Commands

Flashburst is in alpha; install from a local checkout:

```bash
uv add --editable ../flashburst
```

Your workload exposes one file-based runner:

```python
from pathlib import Path
from typing import Any


def run_job(input_path: Path, output_path: Path, params: dict[str, Any]) -> dict[str, Any]:
    # read one input file, write one output file
    ...
    return {"status": "succeeded", "metrics": {...}}
```

Scaffold the thin Flashburst glue around that runner:

```bash
uv run flashburst workload scaffold \
  --package my_workload \
  --capability audio.transcribe.local \
  --job-type audio.transcribe \
  --runner-import my_workload.core:run_job
```

Add `--runpod` if this workload should also be eligible for cloud placement.

Run locally first:

```bash
uv run flashburst init
uv run flashburst capability add my_workload.capabilities:capability --project-root .
uv run python prepare_jobs.py manifest.jsonl
uv run flashburst run-queue .flashburst/jobs/audio.transcribe.jsonl
uv run flashburst status --results
```

## Cloud Burst

Configure a Runpod endpoint and a Cloudflare R2 bucket:

```bash
uv run flashburst configure r2 \
  --bucket "$R2_BUCKET" \
  --endpoint-url "$R2_ENDPOINT_URL"

uv run flashburst configure runpod \
  --capability audio.transcribe.local \
  --endpoint-id <runpod-endpoint-id>
```

Prepare cloud-eligible jobs with your workload script, then allow one local slot
and one cloud slot:

```bash
uv run python prepare_jobs.py manifest.jsonl --cloud-ok

uv run flashburst run-queue .flashburst/jobs/audio.transcribe.jsonl \
  --cloud-slots 1 \
  --profile runpod-burst \
  --approve-cloud

uv run flashburst status --pull --results
```
