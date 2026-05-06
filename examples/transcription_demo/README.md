# Transcription Demo

This is a local transcription-shaped workload. It decodes a checked-in MP3
fixture with ffmpeg, records audio metadata, hashes the source, validates a
transcript segment, and writes JSONL output. It intentionally stops short of
Whisper inference so it stays deterministic and cheap.

Ask an agent:

- "Run the checked-in transcription demo from a clean local state and report the output path."

For a workload repo version of this demo, keep the same `audio_path` manifest
for local and cloud runs. Binding auto-detects `audio_path` when the records
point at local files. Flashburst stages that local file through R2 for remote
jobs and writes the remote result back under the same
`.flashburst/runs/<run-id>/outputs/` tree.

Manual syntax is available in [../../docs/CLI_REFERENCE.md](../../docs/CLI_REFERENCE.md).
