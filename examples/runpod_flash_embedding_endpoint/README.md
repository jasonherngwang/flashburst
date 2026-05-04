# Flashburst Runpod Flash Embedding Endpoint

This example is deployed manually for the MVP. It is intentionally separate
from `flashburst cloud deploy` automation.

```bash
cd examples/runpod_flash_embedding_endpoint
uv run flash deploy
```

Then configure the endpoint in Flashburst:

```bash
uv run flashburst configure runpod --endpoint-id <runpod-endpoint-id>
```

The endpoint accepts a Flashburst `ExecutionEnvelope` JSON object. It reads the
input artifact through a presigned GET grant, writes the output artifact through
a presigned PUT grant, and returns a `JobResult`-shaped JSON object.

The default resource name is `flashburst-embed`. If `flash deploy` appears to
leave workers serving stale code, temporarily change the resource name in
`endpoint.py` to force a fresh serverless endpoint, deploy, update the Flashburst
profile, and retry. After validation, change the name back to
`flashburst-embed` so the example stays stable.
If Runpod deploy output shows:

```text
flashburst-embed  https://api.runpod.ai/v2/<endpoint-id>/runsync
```

use only `<endpoint-id>` in the Flashburst cloud profile.
