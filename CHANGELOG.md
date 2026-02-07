# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Documentation improvements: architecture page, examples page, full API reference in Sphinx.
- CONTRIBUTING.md with development setup and PR expectations.
- CHANGELOG.md (this file).
- LICENSE (MIT).
- Architecture Decision Records (ADR) in `docs/adr/`.
- `docs/_static` for Sphinx static assets.

### Changed

- Sphinx toctree now includes architecture, examples, and working API modules (core, repositories, services, utils).
- `docs/conf.py`: added project root to `sys.path` so autodoc can import `src`.

## [2.1.0] - 2026-02-06

### Added

- Clean architecture (core, repositories, services, factories).
- Integration of data engineering (dbt + Airflow), data quality (Soda), and data governance (Collibra).
- Quality-gated metadata sync: Build → Validate → Govern per layer.
- Pipeline DAGs: initialization, main pipeline, strict RAW, strict MART.
- Soda checks and configurations per layer (RAW, STAGING, MART, QUALITY).
- Collibra metadata synchronization and Soda–Collibra quality metrics sync.
- Centralized configuration (Pydantic), logging, exceptions, retry logic, health checks.
- Justfile for setup, Airflow, tests, lint, format, docs.
- Component READMEs (Airflow, dbt, Soda, Collibra, scripts).
- Unit and integration tests; health check and stack test scripts.

### Notes

- Version and date inferred from README and component READMEs; adjust if your release history differs.

[Unreleased]: https://github.com/your-org/unified-data-governance/compare/v2.1.0...HEAD
[2.1.0]: https://github.com/your-org/unified-data-governance/releases/tag/v2.1.0
