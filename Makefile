.PHONY: help dev dev-full test lint format format-check quality-check build smoke-local clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "%-18s %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*## / {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Install normal development dependencies
	uv sync --extra dev --extra s3 --extra runpod

dev-full: ## Install all local MVP dependencies, including embeddings/GPU support
	uv sync --extra dev --extra s3 --extra runpod --extra embeddings

test: ## Run the unit test suite
	uv run pytest

lint: ## Run Ruff lint checks
	uv run ruff check

format: ## Format Python code with Ruff
	uv run ruff format

format-check: ## Check Python formatting
	uv run ruff format --check

quality-check: lint test ## Run the lightweight local quality gate

build: ## Build the Python package
	uv build

smoke-local: ## Run the local GPU embedding smoke test
	uv run flashburst init
	uv run flashburst examples embeddings prepare demo/texts.jsonl \
		--capability embedding.bge-small-en-v1.5 \
		--model-name sentence-transformers/all-MiniLM-L6-v2
	uv run flashburst submit .flashburst/jobs/embeddings.jsonl
	uv run flashburst worker run --id local-smoke --capability embedding.bge-small-en-v1.5 --once
	uv run flashburst inspect results

clean: ## Remove local caches and build artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
