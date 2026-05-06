.PHONY: help dev dev-full test lint format format-check quality-check build smoke-local example-transcription clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "%-18s %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Install normal development dependencies
	uv sync --extra dev --extra examples --extra runpod --extra r2

dev-full: ## Install all local development and optional runtime dependencies
	uv sync --extra dev --extra examples --extra runpod --extra r2

test: ## Run the unit test suite
	uv run --extra dev --extra examples pytest

lint: ## Run Ruff lint checks
	uv run --extra dev ruff check

format: ## Format Python code with Ruff
	uv run --extra dev ruff format

format-check: ## Check Python formatting
	uv run --extra dev ruff format --check

quality-check: lint format-check test build ## Run the lightweight local quality gate

build: ## Build the Python package
	uv build

smoke-local: ## Run the deterministic local smoke test
	mkdir -p demo
	printf '%s\n' '{"id":"local-smoke","text":"hello from local flashburst"}' > demo/texts.jsonl
	uv run flashburst init
	uv run flashburst run flashburst.workloads.fake_embeddings:run_job demo/texts.jsonl \
		--run-id local-smoke \
		--local-slots 1
	uv run flashburst status --results

example-transcription: ## Run the checked-in MP3 decode transcription demo
	run_id=example-transcription-$$(date -u +%Y%m%d-%H%M%S); \
	uv run --extra examples flashburst run examples/transcription_demo/transcriber.py:transcribe_manifest \
		examples/transcription_demo/manifest.jsonl \
		--run-id "$$run_id" \
		--params-json '{"max_duration_seconds":1,"sample_rate":16000}' \
		--local-slots 1 && \
	uv run --extra examples flashburst status --run-id "$$run_id" --results

clean: ## Remove local caches and build artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
