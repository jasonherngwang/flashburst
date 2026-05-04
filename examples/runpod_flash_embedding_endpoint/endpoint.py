"""Runpod Flash endpoint for Flashburst embedding jobs.

Keep top-level work cheap because Flash scans/imports endpoint modules.
"""

from runpod_flash import Endpoint, GpuGroup


@Endpoint(
    name="flashburst-embed",
    gpu=GpuGroup.AMPERE_24,
    workers=(0, 1),
    idle_timeout=30,
    dependencies=[
        "httpx>=0.27",
        "transformers>=4.42",
    ],
)
async def run_flashburst_embedding(**envelope: dict) -> dict:
    import hashlib
    import json
    import os
    import tempfile
    import time
    from pathlib import Path

    import httpx
    import torch
    from transformers import AutoModel, AutoTokenizer

    def find_grant(method: str) -> dict:
        for grant in envelope.get("artifact_grants", []):
            if grant.get("method") == method:
                return grant
        raise ValueError(f"missing {method} artifact grant")

    def sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def mean_pool(last_hidden_state, attention_mask):
        mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
        summed = (last_hidden_state * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1e-9)
        return summed / counts

    def pooling_strategy(model_name: str, requested: str | None) -> str:
        if requested in {"cls", "mean"}:
            return requested
        if model_name.lower().startswith("baai/bge-"):
            return "cls"
        return "mean"

    read_grant = find_grant("GET")
    write_grant = find_grant("PUT")
    params = envelope.get("params") or {}
    model_name = params.get("model_name", "BAAI/bge-small-en-v1.5")
    normalize_embeddings = bool(params.get("normalize_embeddings", True))
    max_length = int(params.get("max_length", 512))
    batch_size = max(1, int(params.get("batch_size", 32)))
    requested_pooling = params.get("pooling")
    pooling = pooling_strategy(
        model_name, requested_pooling if isinstance(requested_pooling, str) else None
    )

    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        input_path = tmp_path / "input.jsonl"
        output_path = tmp_path / "output.jsonl"

        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.get(read_grant["url"])
            response.raise_for_status()
            input_path.write_bytes(response.content)

        model_started = time.perf_counter()
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModel.from_pretrained(model_name)
        model_load_seconds = time.perf_counter() - model_started

        texts = []
        ids = []
        with input_path.open("r", encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                if not line.strip():
                    continue
                item = json.loads(line)
                ids.append(item.get("id", f"text-{index}"))
                texts.append(str(item["text"]))

        device = "cuda" if torch.cuda.is_available() else "cpu"
        embed_started = time.perf_counter()
        vectors = []
        if texts:
            model.to(device)
            model.eval()
            with torch.no_grad():
                for offset in range(0, len(texts), batch_size):
                    batch = texts[offset : offset + batch_size]
                    encoded = tokenizer(
                        batch,
                        padding=True,
                        truncation=True,
                        max_length=max_length,
                        return_tensors="pt",
                    )
                    encoded = {key: value.to(device) for key, value in encoded.items()}
                    output = model(**encoded)
                    if pooling == "cls":
                        embeddings = output.last_hidden_state[:, 0]
                    else:
                        embeddings = mean_pool(output.last_hidden_state, encoded["attention_mask"])
                    if normalize_embeddings:
                        embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
                    vectors.extend(embeddings.cpu().tolist())
        embedding_seconds = time.perf_counter() - embed_started

        with output_path.open("w", encoding="utf-8") as handle:
            for item_id, vector in zip(ids, vectors, strict=True):
                handle.write(
                    json.dumps(
                        {"id": item_id, "embedding": vector},
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )

        headers = {}
        if write_grant.get("content_type"):
            headers["Content-Type"] = write_grant["content_type"]
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.put(
                write_grant["url"],
                content=output_path.read_bytes(),
                headers=headers,
            )
            response.raise_for_status()

        output_uri = write_grant["artifact_uri"]
        media_type = write_grant.get("content_type") or "application/x-ndjson"
        device_name = "unknown"
        try:
            if torch.cuda.is_available():
                device_name = torch.cuda.get_device_name(0)
            else:
                device_name = "cpu"
        except Exception:
            device_name = os.getenv("CUDA_VISIBLE_DEVICES", "unknown")

        return {
            "status": "succeeded",
            "output_artifacts": [
                {
                    "uri": output_uri,
                    "media_type": media_type,
                    "storage": "s3",
                    "sha256": sha256_file(output_path),
                    "size_bytes": output_path.stat().st_size,
                    "producer_job_id": envelope.get("job_id"),
                }
            ],
            "metrics": {
                "model_name": model_name,
                "device": device_name,
                "input_count": len(texts),
                "vector_dim": len(vectors[0]) if len(vectors) else 0,
                "model_load_seconds": model_load_seconds,
                "embedding_seconds": embedding_seconds,
                "total_seconds": time.perf_counter() - started,
            },
        }
