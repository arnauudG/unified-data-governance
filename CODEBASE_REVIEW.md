# Codebase Review — Unified Data Governance Platform

**Review date**: February 2026  
**Scope**: Full project (src/, tests/, airflow/, dbt/, soda/, collibra/, scripts/, docs/)

---

## Executive Summary

The project is a **data engineering, governance, and quality integration platform** with a clear **clean-architecture** layout: Core → Repositories → Services → Factories, plus Airflow DAGs, dbt models, and Soda/Collibra integrations. Code quality is good overall: typed config (Pydantic), structured exceptions, retries, and logging. A number of small fixes were applied during this review (test mocks, justfile, pyproject, docs, orphan file). Remaining items are optional improvements and one naming inconsistency.

---

## 1. Architecture & Design

### Strengths

- **Layered design**: `src/core` (config, logging, exceptions, retry, health), `src/repositories` (Soda, Collibra), `src/services`, `src/factories` — clear separation and dependency direction.
- **Configuration**: Single `Config` (Pydantic) with `get_config()` and `reset_config()` for tests; env-based loading with `.env` support; path resolution in `PathsConfig`.
- **Exceptions**: Custom hierarchy (`DataGovernanceError` → `ConfigurationError`, `APIError` → `RetryableError`/`NonRetryableError`, `ValidationError`, etc.) supports consistent handling and retries.
- **Repositories**: `BaseRepository` with `connect`/`disconnect` and context-manager support; `SodaRepository` and `CollibraRepository` encapsulate HTTP/API details.
- **Retries**: `retry_with_backoff` with configurable attempts/delay and classification of retryable vs non-retryable errors.

### Minor observations

- **dbt project name**: `dbt_project.yml` and `profiles.yml` use profile/project name `data_governance_platform` while the default DB is `DATA PLATFORM XYZ`. Consider aligning names (e.g. `data_platform_xyz`) for consistency; current setup works.
- **TimeoutError**: `src.core.exceptions` defines `TimeoutError`; Python 3 has a built-in `TimeoutError`. No conflict in current usage, but worth being aware of for future imports.

---

## 2. Configuration & Consistency

### Database and naming

- **Default database**: `DATA PLATFORM XYZ` is used consistently in:
  - `src/core/config.py`, `constants.py`
  - `dbt/profiles.yml`, `models/raw/sources.yml` (with quoting for spaces)
  - `soda/helpers.py`, `update_data_source_names.py`, Soda configs
  - `scripts/setup/setup_snowflake.py`, `reset_snowflake.py`
  - `soda-collibra-integration-configuration` (integration.py, utils.py)
- **Quoting**: dbt `quoting.database: true` (project + source) and Snowflake scripts quoting DB names with spaces — correct.

### Fixes applied during review

- **Test mocks** (`tests/conftest.py`): Mock used `config.collibra.host` and `config.paths.soda_config_path`. Updated to `config.collibra.base_url` and removed non-existent `soda_config_path` so mocks match real `Config`/`PathsConfig`.
- **Justfile**: Removed reference to removed task `superset_upload_data` in the fallback `airflow-task-logs` example.
- **pyproject.toml**: Removed `superset` from Ruff exclude (Superset was removed from the project).
- **docs/index.rst**: Toctree referenced missing `architecture`, `examples`, `contributing`; reduced to `api/index` so Sphinx builds without missing-file warnings.
- **Orphan file**: Deleted `scripts/requirements_dump.txt` (leftover from removed Soda-dump/Superset flow).

---

## 3. Security & Credentials

- **No hardcoded secrets**: Credentials come from env (e.g. `SNOWFLAKE_*`, `SODA_CLOUD_*`, `COLLIBRA_*`) and a single root `.env` used by Docker and scripts.
- **.env in .gitignore**: Correct.
- **Validation**: Pydantic validates config; required fields enforced in `_validate_config`. Good base for avoiding bad config in production.

**Recommendation**: Document that `.env` must not be committed and that production should use a secrets manager or platform-specific env (e.g. Airflow Variables/Connections).

---

## 4. Testing

- **Layout**: `tests/unit/` (core, repositories, services), `tests/integration/` (pipeline, services); `conftest.py` with shared fixtures and config reset.
- **Fixtures**: `mock_config`, `mock_soda_response`, `mock_collibra_response`, etc. — useful for unit tests.
- **Config reset**: `reset_config()` in conftest avoids cross-test config leakage.

**Recommendation**: Add a few tests that use a real `Config` (e.g. from env) with safe defaults or env overrides, to guard against config refactors breaking attribute names (e.g. `base_url` vs `host`).

---

## 5. Documentation

- **README.md**: Single entrypoint with architecture, quick start, commands, troubleshooting, and links to component READMEs. “How to run & verify” section is clear.
- **Component READMEs**: `dbt/`, `soda/`, `airflow/`, `collibra/`, `scripts/` each have a README with structure, config, and usage.
- **Sphinx**: `docs/conf.py` and `api/index.rst` present; toctree fixed to avoid missing pages.
- **In-code**: Docstrings and type hints are used in `src/` and key scripts.

**Recommendation**: Add minimal `architecture.rst` and `examples.rst` (or remove from toctree if you prefer all high-level docs in README).

---

## 6. Dependency & Tooling

- **pyproject.toml**: Single place for project metadata, dependencies, and dev tool config (mypy, Ruff, Black, Safety). Python ≥3.11.
- **Justfile**: Clear targets for setup, Airflow, DAG triggers, tests, lint, etc.; `airflow-up` depends on `setup`.
- **Versions**: dbt-core 1.10.11, dbt-snowflake 1.10.2, Pydantic 2.x, protobuf pinned to avoid conflicts. Soda installed from private PyPI; documented.

**Note**: `scripts/requirements_dump.txt` was removed as orphaned; any future dump/export feature should declare its deps in `pyproject.toml` or a dedicated requirements file referenced in docs.

---

## 7. Airflow & DAGs

- **DAGs**: `soda_initialization`, `soda_pipeline_run`, `soda_pipeline_run_strict_raw`, `soda_pipeline_run_strict_mart` with clear task boundaries and dependencies.
- **Quality gating**: Build → Validate → Govern; Collibra sync only after quality checks; lenient vs strict pipelines documented.
- **Data source names**: Taken from `soda.helpers.get_data_source_name()` (underscores, from DB name); BashOperator commands quote the data source name to handle spaces.
- **Path**: Project root and `src` mounted in containers; `sys.path` and dotenv set so DAGs and helpers run correctly.

No issues found; previous fixes (data source quoting, config/source quoting for DB name with spaces) are in place.

---

## 8. dbt

- **Profiles**: Use `env_var('SNOWFLAKE_DATABASE', 'DATA PLATFORM XYZ')`; `quote_identifiers: true`.
- **Project**: `quoting.database: true` and source-level `quoting.database: true` for DB names with spaces.
- **Sources**: `models/raw/sources.yml` defines raw layer with correct default DB and quoting.
- **Models**: Staging/mart layers and custom schema macro; no issues found.

---

## 9. Soda & Collibra

- **Soda**: Configs per layer; `update_data_source_names.py` keeps YAML in sync with `SNOWFLAKE_DATABASE`; helpers normalize DB name to data source name (e.g. spaces → underscores).
- **Collibra**: `metadata_sync.py` and `airflow_helper.py` use `MetadataService` and factories; config in `collibra/config.yml` (e.g. database_id, schema_connection_ids).
- **Soda–Collibra integration**: Subpackage under `soda/soda-collibra-integration-configuration/` with its own config; defaults updated to `DATA PLATFORM XYZ` where applicable.

---

## 10. Recommendations Summary

| Priority | Item |
|----------|------|
| Done     | Align test mocks with `Config`/`PathsConfig` (`base_url`, no `soda_config_path`). |
| Done     | Remove Superset/orphan references (justfile, pyproject, requirements_dump.txt). |
| Done     | Fix Sphinx toctree so it only references existing docs. |
| Low      | Align dbt project/profile name with “DATA PLATFORM XYZ” if you want naming consistency. |
| Low      | Add optional `architecture.rst` / `examples.rst` or document that main docs live in README. |
| Low      | Consider renaming `src.core.exceptions.TimeoutError` to avoid shadowing built-in (e.g. `GovernanceTimeoutError`) if you ever import both. |
| Info     | Document that production credentials should use a secrets manager or Airflow Variables/Connections. |

---

## 11. How to Re-run Checks

- **Lint**: `just lint` (Ruff)
- **Format**: `just format` (Black)
- **Type check**: `just type-check` (mypy)
- **Tests**: `just test` / `just test-unit` / `just test-integration`
- **Stack health**: `just test-stack`
- **Docs build**: `just docs-build` (Sphinx)

---

**Conclusion**: The codebase is in good shape: clear architecture, consistent config and naming, and no critical issues. The changes made during this review improve test accuracy, remove obsolete references, and prevent Sphinx and future readers from hitting missing files or wrong attribute names.
