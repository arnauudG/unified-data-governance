# Contributing to the Unified Data Governance Platform

Thank you for your interest in contributing. This document explains how to set up your environment, run checks, and submit changes.

## Development Setup

### Prerequisites

- **Python 3.11+**
- **uv** (recommended): <https://github.com/astral-sh/uv>  
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- **Docker & Docker Compose** (for Airflow and full stack)
- **Just** (optional, for running project commands): <https://github.com/casey/just>

### One-time setup

1. **Clone the repository** (if you haven’t already).

2. **Create virtual environment and install dependencies:**
   ```bash
   just setup
   ```
   Or manually:
   ```bash
   just venv
   just deps
   ```

3. **Configure environment:**  
   Copy `.env.example` to `.env` in the project root and fill in your Snowflake, Soda Cloud, and Collibra credentials. See the main [README](README.md#environment-setup) for required variables.

4. **(Optional) Install pre-commit hooks:**
   ```bash
   just pre-commit-install
   ```

## Running Checks Before Submitting

Run these locally so CI stays green:

| Check            | Command              | Description                    |
|------------------|----------------------|--------------------------------|
| Unit tests       | `just test-unit`     | Run unit tests only            |
| Integration tests| `just test-integration` | Run integration tests       |
| All tests        | `just test`          | Run full test suite            |
| Tests + coverage | `just test-coverage` | Tests with coverage report     |
| Type checking    | `just type-check`    | mypy                           |
| Linting          | `just lint`          | Ruff                           |
| Formatting       | `just format`        | Black (fix)                     |
| Format check     | (in ci-local)        | Black --check                  |
| Security         | `just security-check`| Safety                         |
| Docs build       | `just docs-build`    | Build Sphinx docs              |
| **All CI checks**| `just ci-local`      | Simulate full CI pipeline      |

**Recommendation:** Before opening a PR, run:

```bash
just ci-local
```

This runs type-check, lint, format check, security check, and tests with coverage.

## Code Style and Quality

- **Formatting:** Black. Run `just format` to fix.
- **Linting:** Ruff. Run `just lint` to see issues.
- **Types:** Type hints are expected; mypy is run in CI.
- **Docstrings:** Use Google or NumPy style so Sphinx (Napoleon) can render them.

## Pull Request Expectations

1. **Tests:** New or changed behavior should be covered by unit or integration tests. Existing tests should remain passing.
2. **No regressions:** `just test` (and ideally `just ci-local`) should pass.
3. **Documentation:** Update the README, component READMEs, or `docs/` as needed for user-facing or architectural changes.
4. **Scope:** Prefer focused PRs (one feature or fix per PR) with a clear description.

## Project Structure Quick Reference

- **`src/`** – Application code (core, repositories, services, factories).
- **`tests/`** – Unit and integration tests.
- **`airflow/`** – DAGs and Docker setup for Airflow.
- **`dbt/`** – dbt project (models, sources).
- **`soda/`** – Soda checks and configuration.
- **`collibra/`** – Collibra metadata sync and config.
- **`scripts/`** – Setup and utility scripts.
- **`docs/`** – Sphinx documentation (architecture, examples, API).

For full architecture and commands, see the main [README](README.md) and [Architecture](docs/architecture.rst).

## Questions or Issues

- Open an issue for bugs, feature requests, or documentation improvements.
- For architecture or design decisions, see [Architecture Decision Records](docs/adr/README.md) if available.
- **Maintainers:** Internal docs (codebase review, documentation audit) are in [docs/internal/](docs/internal/README.md).

Thank you for contributing.
