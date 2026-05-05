from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from mutagen import File as MutagenFile

RUNNER_VERSION = "flashburst-transcription-demo-v1"
SEGMENT_SCHEMA_VERSION = "transcript-segment-v1"
SEGMENT_REQUIRED_KEYS = {
    "segment_schema_version",
    "episode_id",
    "podcast_title",
    "title",
    "source",
    "id",
    "start_seconds",
    "end_seconds",
    "text",
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _audio_duration_seconds(path: Path) -> float | None:
    audio = MutagenFile(path)
    info = getattr(audio, "info", None)
    length = getattr(info, "length", None)
    return float(length) if length is not None else None


def _ffmpeg_binary() -> str:
    binary = shutil.which("ffmpeg")
    if binary:
        return binary
    import imageio_ffmpeg

    return imageio_ffmpeg.get_ffmpeg_exe()


def _decode_with_ffmpeg(
    audio_path: Path,
    *,
    sample_rate: int,
    max_duration_seconds: float | None,
) -> tuple[bytes, float, str]:
    command = [
        _ffmpeg_binary(),
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(audio_path),
    ]
    if max_duration_seconds is not None:
        command.extend(["-t", str(max_duration_seconds)])
    command.extend(["-ac", "1", "-ar", str(sample_rate), "-f", "s16le", "-"])
    completed = subprocess.run(command, check=True, capture_output=True)
    pcm = completed.stdout
    return pcm, len(pcm) / (sample_rate * 2), command[0]


def _source_path(manifest: dict[str, Any], project_root: Path) -> Path:
    value = manifest.get("audio_path") or manifest.get("source")
    if not value:
        raise ValueError("manifest record must include audio_path or source")
    path = Path(str(value))
    return path if path.is_absolute() else project_root / path


def _segment(
    manifest: dict[str, Any], source_path: Path, text: str, end_seconds: float
) -> dict[str, Any]:
    return {
        "segment_schema_version": SEGMENT_SCHEMA_VERSION,
        "episode_id": manifest.get("id"),
        "podcast_title": manifest.get("podcast_title"),
        "title": manifest.get("title"),
        "source": str(manifest.get("audio_path") or manifest.get("source") or source_path),
        "id": f"{manifest.get('id', source_path.stem)}-0",
        "start_seconds": 0,
        "end_seconds": end_seconds,
        "text": text,
    }


def _validate_segment(segment: dict[str, Any]) -> None:
    missing = sorted(SEGMENT_REQUIRED_KEYS - set(segment))
    if missing:
        raise ValueError(f"transcript segment is missing required keys: {', '.join(missing)}")
    if segment["segment_schema_version"] != SEGMENT_SCHEMA_VERSION:
        raise ValueError(f"unsupported segment schema: {segment['segment_schema_version']!r}")


def transcribe_manifest(
    input_path: Path, output_path: Path, params: dict[str, Any]
) -> dict[str, Any]:
    """Decode a real MP3 fixture and emit transcript-shaped JSONL.

    The demo intentionally stops short of Whisper model inference so it remains
    cheap and deterministic, but it still performs audio metadata parsing,
    ffmpeg decode, source hashing, and segment validation.
    """
    started = time.perf_counter()
    manifest = json.loads(input_path.read_text(encoding="utf-8"))
    project_root = Path(str(params.get("project_root") or ".")).resolve()
    sample_rate = int(params.get("sample_rate") or 16_000)
    max_duration = params.get("max_duration_seconds", 1)
    max_duration_seconds = float(max_duration) if max_duration is not None else None

    with tempfile.TemporaryDirectory():
        source_path = _source_path(manifest, project_root)
        source_duration_seconds = _audio_duration_seconds(source_path)
        pcm, decoded_duration_seconds, ffmpeg_binary = _decode_with_ffmpeg(
            source_path,
            sample_rate=sample_rate,
            max_duration_seconds=max_duration_seconds,
        )
        text = str(
            manifest.get("expected_text")
            or params.get("fallback_text")
            or f"decoded {decoded_duration_seconds:.2f}s from {source_path.name}"
        )
        segment = _segment(
            manifest,
            source_path,
            text,
            end_seconds=round(decoded_duration_seconds, 3),
        )
        _validate_segment(segment)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(segment, sort_keys=True) + "\n", encoding="utf-8")
        return {
            "status": "succeeded",
            "metrics": {
                "runner_version": RUNNER_VERSION,
                "mode": "ffmpeg_decode",
                "audio_size_bytes": source_path.stat().st_size,
                "audio_sha256": _sha256_file(source_path),
                "source_duration_seconds": source_duration_seconds,
                "decoded_duration_seconds": decoded_duration_seconds,
                "decoded_pcm_bytes": len(pcm),
                "sample_rate": sample_rate,
                "ffmpeg_binary": ffmpeg_binary,
                "segment_count": 1,
                "total_seconds": time.perf_counter() - started,
            },
        }
