# Agent Notes

- Agent-native flow first: translate user requests into outcomes. Inspect
  Flashburst context, bind when needed, run local smokes, scaffold cloud
  wrappers, and report outputs without making the user memorize command syntax.
- Prefer the short top-level Flashburst primitives (`context`, `bind`, `run`,
  `scaffold`) over older long forms. Use `workload inspect` and
  `manifest inspect/validate` only for diagnosis.
- Update `docs_internal/DEV_LOG.md` with material changes, validation,
  decisions, and blockers.
- Use `make quality-check` for routine validation; use `make dev-full && make smoke-local` for local GPU smoke on the RTX 3090.
- Do not run credentialed or paid Runpod/R2 cloud steps without explicit approval.
- Use Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`, `build:`.
