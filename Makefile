.PHONY: help dev dev-full test lint format format-check quality-check build smoke-local clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "%-18s %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Install normal development dependencies
	uv sync --extra dev --extra s3 --extra runpod

dev-full: ## Install all local development and optional runtime dependencies
	uv sync --extra dev --extra s3 --extra runpod

test: ## Run the unit test suite
	uv run pytest

lint: ## Run Ruff lint checks
	uv run ruff check

format: ## Format Python code with Ruff
	uv run ruff format

format-check: ## Check Python formatting
	uv run ruff format --check

quality-check: lint format-check test build ## Run the lightweight local quality gate

build: ## Build the Python package
	uv build

smoke-local: ## Run the deterministic local smoke test
	mkdir -p demo
	printf '%s\n' '{"id":"local-smoke","text":"hello from local flashburst"}' > demo/texts.jsonl
	uv run flashburst init
	uv run flashburst prepare embeddings demo/texts.jsonl \
		--capability embedding.fake-deterministic
	uv run flashburst run-queue .flashburst/jobs/embeddings.jsonl --local-slots 1
	uv run flashburst status --results

clean: ## Remove local caches and build artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
