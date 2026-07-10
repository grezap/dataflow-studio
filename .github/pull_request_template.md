<!--
  Every DataFlow Studio PR must demonstrate progress against the four portfolio skill
  dimensions (MASTER-PLAN §2). Missing evidence blocks merge.
-->

## Summary

<!-- What does this PR change and why? Link the relevant MASTER-PLAN enhancement (E1/E4/E14/E16/...) or acceptance-gate box (§6). -->

## Four skill dimensions

- [ ] **.NET engineering + architecture** — boundaries enforced by NetArchTest; ADR added/updated if a decision was made.
- [ ] **Advanced SQL + analytics** — new SQL techniques documented in `docs/sql-showcase.md` (if applicable).
- [ ] **Python** — modern toolchain (uv + Ruff + mypy --strict) if this PR touches Python (if applicable).
- [ ] **DevOps literacy** — operator-facing changes go through `nexus-cli`; runbook updated with a panic button (if applicable).

## Checks

- [ ] `dotnet build` clean (warnings are errors).
- [ ] `dotnet format --verify-no-changes` clean.
- [ ] `dotnet test` green — including the E1 up → down → up migration gate.
- [ ] Conventional Commit messages.
- [ ] CHANGELOG updated.
