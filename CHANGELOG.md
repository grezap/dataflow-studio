# Changelog

All notable changes to DataFlow Studio are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Solution scaffold** — modular-monolith solution (`.slnx`): `Api` composition root, `SharedKernel`,
  four module assemblies (`Commerce`, `Ingestion`, `Warehouse`, `Telemetry`), and the
  `Migrations.Oltp` tool. House conventions mirrored from `nexus-cli` (central package management,
  `net10.0`, warnings-as-errors, AOT analyzers).
- **OltpDb migrations** — FluentMigrator migrations for all 11 tables from `schemas/dataflow-studio`
  (system-versioned temporal `Customers`/`Products`, `audit.ChangeLog`, persisted computed
  `LineTotalUsd`, `ROWVERSION`, standard audit columns), each with a reversible `Down()`.
- **E1 acceptance gate** — `up → down → up` migration test on a fresh SQL Server (Testcontainers in
  CI, LocalDB locally).
- **Architecture tests** — NetArchTest rules enforcing module isolation and "no EF Core on AOT paths"
  (E4).
- **Repo hygiene** — README (14-section), CONTRIBUTING, ADR-0001/0002, `docs/sql-showcase.md`,
  OpenAPI + AsyncAPI stubs, devcontainer, GitHub Actions CI, Dependabot, PR template, Conventional
  Commits tooling.

[Unreleased]: https://github.com/grezap/dataflow-studio/commits/main
