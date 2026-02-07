# Data Governance Platform Project Justfile
# Using uv for fast Python package management

# Variables
python := "python3.11"
venv := ".venv"
uv := "uv"

# Default recipe
default:
    @just --list

# Setup environment
all: venv deps

# Check if uv is installed
check-uv:
    @if ! command -v {{uv}} >/dev/null 2>&1; then \
        echo "❌ Error: uv not found. Please install uv:"; \
        echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"; \
        echo "   Or: pip install uv"; \
        exit 1; \
    fi
    @echo "✅ uv found: $({{uv}} --version)"

# Create virtual environment with uv
venv: check-uv
    @if [ ! -d "{{venv}}" ]; then \
        {{uv}} venv --python {{python}}; \
        echo "[OK] Virtual environment created with uv using {{python}}"; \
    else \
        echo "[OK] Virtual environment exists"; \
    fi

# Install dependencies with uv
deps: venv
    @echo "📦 Installing dependencies with uv..."
    @. {{venv}}/bin/activate && \
    echo "📌 Installing critical dependencies first..." && \
    {{uv}} pip install "protobuf>=6.30.0,<6.31.0" "pydantic>=2.5.2,<3.0.0" "pyarrow>=15.0.0,<22.0.0" && \
    echo "📦 Installing project dependencies from pyproject.toml..." && \
    {{uv}} pip install "python-dotenv>=1.0.1" "pyyaml>=6.0" "requests>=2.31.0" "pandas>=2.2.2,<2.4" "dbt-core==1.10.11" "dbt-snowflake==1.10.2" "snowflake-connector-python>=3.17.0" "google-api-core>=2.23.0" "googleapis-common-protos>=1.66.0" "proto-plus>=1.26.0" && \
    echo "🧹 Removing conflicting soda-postgres if present..." && \
    {{uv}} pip uninstall -y soda-postgres 2>/dev/null || true && \
    echo "📦 Installing soda-snowflake from Soda Cloud PyPI..." && \
    {{uv}} pip install -i https://pypi.cloud.soda.io "soda-snowflake==1.12.24" || (echo "⚠️  Warning: Could not install soda-snowflake" && echo "   This may be due to:" && echo "   - Network connectivity issues" && echo "   - Private PyPI access required (check Soda Cloud credentials)" && echo "   - DNS resolution problems" && echo "   You can install it later with: uv pip install -i https://pypi.cloud.soda.io soda-snowflake==1.12.24") && \
    echo "🔧 Ensuring critical dependencies remain at correct versions..." && \
    {{uv}} pip install --upgrade "protobuf>=6.30.0,<6.31.0" "pydantic>=2.5.2,<3.0.0" "pyarrow>=15.0.0,<22.0.0" && \
    {{uv}} pip install --upgrade "google-api-core>=2.23.0" "googleapis-common-protos>=1.66.0" "proto-plus>=1.26.0" || true && \
    echo "[OK] Dependencies installed"
    @echo "ℹ️  Note: This project uses Snowflake, not PostgreSQL for Soda checks."
    @echo "   Removed soda-postgres to avoid version conflicts."
    @echo "⚠️  Some dependency warnings may appear for transitive dependencies"
    @echo "   (mlflow, anyscale, fastapi) but these are not"
    @echo "   directly used and should not affect functionality."
    @echo "ℹ️  Note: If you see a warning about proto_plus RECORD file,"
    @echo "   this is harmless and can be safely ignored."

# Complete environment setup
# Note: This is idempotent - safe to run multiple times
#       However, 'just airflow-up' automatically runs setup, so you typically
#       don't need to run 'just setup' separately unless you want to setup
#       without starting Airflow.
setup: venv deps
    @echo "🔧 Setting up environment..."
    @if [ ! -f .env ]; then \
        echo "⚠️  .env file not found!"; \
        echo "   Please create .env file with your credentials"; \
        echo "   Copy from template: cp .env.example .env"; \
        echo ""; \
        echo "   Required variables:"; \
        echo "   - Snowflake: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"; \
        echo "   - Soda Cloud: SODA_CLOUD_API_KEY_ID, SODA_CLOUD_API_KEY_SECRET"; \
        echo "   - Collibra: COLLIBRA_BASE_URL, COLLIBRA_USERNAME, COLLIBRA_PASSWORD"; \
        exit 1; \
    else \
        echo "✅ .env file found"; \
    fi
    @echo "🔄 Updating Soda data source names to match database configuration..."
    @echo "ℹ️  (Note: Any proto_plus RECORD warnings are harmless)"
    @if [ -f ".venv/bin/activate" ]; then \
        . .venv/bin/activate && {{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"; \
    else \
        echo "⚠️  Warning: Virtual environment not found. Run 'just deps' first."; \
    fi
    @echo "[OK] Environment setup completed"
    @echo "[INFO] Next steps:"
    @echo "  1. Ensure .env file has all required credentials"
    @echo "  2. Run: just airflow-up  (this will automatically run setup if needed)"
    @echo "  3. Run: just airflow-trigger-init (first time setup)"
    @echo "  4. Access Airflow UI: http://localhost:8081"
    @echo ""
    @echo "💡 Tip: Just run 'just airflow-up' - it handles setup automatically!"
    @echo "💡 Using uv: Run commands with 'uv run' (no activation needed)"
    @echo "   Example: uv run python3 soda/update_data_source_names.py"

# Start Airflow services with Docker
# Note: If you run 'just setup' first, this will skip re-running setup automatically
#       You can also just run 'just airflow-up' alone - it will run setup automatically
airflow-up: setup
    @echo "🚀 Starting Airflow services..."
    @echo "🌐 Ensuring shared Docker network exists..."
    @docker network create data-governance-network 2>/dev/null || echo "Network already exists"
    @echo "📥 Loading environment variables and starting Docker containers..."
    @cd airflow/docker && docker-compose --env-file ../../.env up -d
    @echo "⏳ Waiting for services to be ready..."
    @sleep 30
    @echo "▶️  Unpausing all Soda DAGs..."
    @docker exec soda-airflow-webserver airflow dags unpause soda_initialization || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_raw || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_mart || true
    @echo "[OK] Airflow services started with Docker"
    @echo "[INFO] Web UI: http://localhost:8081 (admin/admin)"
    @echo "[INFO] Available DAGs:"
    @just airflow-list

# Start all services (Airflow)
# Start all services (automatically runs setup first)
all-up: setup
    @echo "🚀 Starting all services..."
    @echo "🌐 Ensuring shared Docker network exists..."
    @docker network create data-governance-network 2>/dev/null || echo "Network already exists"
    @echo "📥 Loading environment variables and starting Docker containers..."
    @cd airflow/docker && docker-compose --env-file ../../.env up -d
    @echo "⏳ Waiting for services to be ready..."
    @sleep 30
    @echo "▶️  Unpausing all Soda DAGs..."
    @docker exec soda-airflow-webserver airflow dags unpause soda_initialization || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_raw || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_mart || true
    @echo "[OK] All services started with Docker"
    @echo "[INFO] Airflow UI: http://localhost:8081 (admin/admin)"

# Stop all services (Airflow)
all-down:
    @echo "🛑 Stopping all services..."
    @echo "🔄 Stopping Airflow services..."
    @cd airflow/docker && docker-compose down
    @echo "[OK] All services stopped"

# Stop Airflow services
airflow-down:
    @cd airflow/docker && docker-compose down
    @echo "[OK] Airflow services stopped"


# Check Airflow services status
airflow-status:
    @echo "🔍 Checking Airflow services..."
    @cd airflow/docker && docker-compose ps

# View Airflow logs
airflow-logs:
    @cd airflow/docker && docker-compose logs -f

# View logs for a specific task
airflow-task-logs task dag:
    @if [ -z "{{task}}" ] || [ -z "{{dag}}" ]; then \
        echo "Usage: just airflow-task-logs <task_id> <dag_id>"; \
        echo "Example: just airflow-task-logs soda_scan_raw soda_pipeline_run"; \
        echo ""; \
        echo "Finding latest task logs..."; \
        docker exec soda-airflow-scheduler find /opt/airflow/logs -name "*.log" -type f | head -10; \
    else \
        echo "📋 Finding latest logs for task {{task}} in DAG {{dag}}..."; \
        LATEST_LOG=$$(docker exec soda-airflow-scheduler find /opt/airflow/logs -path "*dag_id={{dag}}*" -path "*task_id={{task}}*" -name "*.log" -type f -exec ls -t {} + 2>/dev/null | head -1); \
        if [ -n "$$LATEST_LOG" ]; then \
            echo "📄 Viewing: $$LATEST_LOG"; \
            echo "---"; \
            docker exec soda-airflow-scheduler tail -f "$$LATEST_LOG" 2>/dev/null || docker exec soda-airflow-scheduler cat "$$LATEST_LOG"; \
        else \
            echo "❌ No logs found for task {{task}} in DAG {{dag}}"; \
            echo "Available logs:"; \
            docker exec soda-airflow-scheduler find /opt/airflow/logs -name "*.log" -type f | head -10; \
        fi; \
    fi

# Unpause all Soda DAGs
airflow-unpause-all:
    @echo "▶️  Unpausing all Soda DAGs..."
    @docker exec soda-airflow-webserver airflow dags unpause soda_initialization
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_raw || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_mart || true
    @echo "[OK] All Soda DAGs unpaused"

# Pause all Soda DAGs
airflow-pause-all:
    @echo "⏸️  Pausing all Soda DAGs..."
    @docker exec soda-airflow-webserver airflow dags pause soda_initialization
    @docker exec soda-airflow-webserver airflow dags pause soda_pipeline_run
    @docker exec soda-airflow-webserver airflow dags pause soda_pipeline_run_strict_raw || true
    @docker exec soda-airflow-webserver airflow dags pause soda_pipeline_run_strict_mart || true
    @echo "[OK] All Soda DAGs paused"

# Rebuild Airflow containers
airflow-rebuild:
    @cd airflow/docker && docker-compose down
    @cd airflow/docker && docker-compose build --no-cache
    @cd airflow/docker && docker-compose up -d
    @echo "[OK] Airflow containers rebuilt and started"

# Trigger initialization DAG (fresh setup only)
airflow-trigger-init:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🚀 Triggering initialization DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_initialization
    @echo "[OK] Initialization DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8081"

# Trigger layered pipeline DAG (layer-by-layer processing)
airflow-trigger-pipeline:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🔄 Triggering layered pipeline DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run
    @echo "[OK] Layered pipeline DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8081"

# Trigger pipeline with strict RAW layer guardrails
airflow-trigger-pipeline-strict-raw:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🔄 Triggering strict RAW pipeline DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run_strict_raw
    @echo "[OK] Strict RAW pipeline DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8081"
    @echo "[INFO] This pipeline will FAIL if RAW layer critical checks fail"

# Trigger pipeline with strict MART layer guardrails
airflow-trigger-pipeline-strict-mart:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🔄 Triggering strict MART pipeline DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run_strict_mart
    @echo "[OK] Strict MART pipeline DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8081"
    @echo "[INFO] This pipeline will FAIL if MART layer critical checks fail"

# List available DAGs
airflow-list:
    @echo "📋 Listing available DAGs..."
    @docker exec soda-airflow-webserver airflow dags list | grep soda

# Update Soda data source names in config files
soda-update-datasources:
    @echo "🔄 Updating Soda data source names..."
    @{{uv}} run python3 soda/update_data_source_names.py
    @echo "[OK] Data source names updated"


# Test Collibra metadata sync module
test-collibra:
    @echo "🧪 Testing Collibra metadata sync module..."
    @python3 collibra/test_metadata_sync.py

# Clean up artifacts and temporary files
clean:
    @echo "🧹 Cleaning up artifacts..."
    @rm -rf dbt/target dbt/logs snowflake_connection_test.log
    @find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    @rm -rf airflow/airflow-logs 2>/dev/null || true
    @echo "[OK] Artifacts cleaned"

# Clean up old Airflow logs
clean-logs:
    @echo "🧹 Cleaning up old logs..."
    @rm -rf airflow/airflow-logs 2>/dev/null || true
    @echo "[OK] Old logs cleaned"

# Deep clean: artifacts, logs, and cache
clean-all: clean clean-logs
    @echo "🧹 Deep cleaning project..."
    @find . -name "*.pyc" -delete 2>/dev/null || true
    @find . -name ".DS_Store" -delete 2>/dev/null || true

# Dump Airflow database only
dump-airflow:
    @echo "🔄 Dumping Airflow database..."
    @./scripts/dump_databases.sh --airflow-only
    @echo "[OK] Airflow database dumped"

# Run health checks
health-check:
    @echo "🏥 Running platform health checks..."
    @{{uv}} run python3 scripts/health_check.py

# Run all tests
test:
    @echo "🧪 Running all tests..."
    @{{uv}} run pytest tests/ -v

# Run unit tests only
test-unit:
    @echo "🧪 Running unit tests..."
    @{{uv}} run pytest tests/unit/ -v

# Run integration tests only
test-integration:
    @echo "🧪 Running integration tests..."
    @{{uv}} run pytest tests/integration/ -v

# Run tests with coverage report
test-coverage:
    @echo "🧪 Running tests with coverage..."
    @{{uv}} run pytest tests/ --cov=src --cov-report=html --cov-report=term
    @echo "📊 Coverage report: htmlcov/index.html"

# Run type checking
type-check:
    @echo "🔍 Running type checking..."
    @{{uv}} run mypy src --config-file pyproject.toml

# Run linting with Ruff
lint:
    @echo "🔍 Running Ruff linter..."
    @{{uv}} run ruff check src scripts tests

# Format code with Black
format:
    @echo "🎨 Formatting code with Black..."
    @{{uv}} run black src scripts tests

# Check security vulnerabilities
security-check:
    @echo "🔒 Checking for security vulnerabilities..."
    @{{uv}} run safety check

# Run all quality checks
quality-check: type-check lint test-coverage
    @echo "✅ All quality checks passed!"

# Generate Sphinx documentation
docs-build:
    @echo "📚 Building documentation..."
    @cd docs && {{uv}} run sphinx-build -b html . _build/html
    @echo "📚 Documentation built: docs/_build/html/index.html"

# Serve documentation locally
docs-serve:
    @echo "📚 Serving documentation..."
    @cd docs/_build/html && python3 -m http.server 8000
    @echo "📚 Documentation available at: http://localhost:8000"

# Install pre-commit hooks
pre-commit-install:
    @echo "🔧 Installing pre-commit hooks..."
    @{{uv}} run pip install pre-commit
    @{{uv}} run pre-commit install
    @echo "✅ Pre-commit hooks installed"

# Run pre-commit hooks on all files
pre-commit-run:
    @echo "🔍 Running pre-commit hooks..."
    @{{uv}} run pre-commit run --all-files

# Update pre-commit hooks
pre-commit-update:
    @echo "🔄 Updating pre-commit hooks..."
    @{{uv}} run pre-commit autoupdate

# CI/CD simulation (run all checks locally)
ci-local:
    @echo "🚀 Running CI checks locally..."
    @echo "1. Type checking..."
    @just type-check
    @echo "2. Linting..."
    @just lint
    @echo "3. Formatting check..."
    @{{uv}} run black --check src scripts tests || echo "⚠️  Formatting issues found. Run 'just format' to fix."
    @echo "4. Security check..."
    @just security-check
    @echo "5. Running tests..."
    @just test-coverage
    @echo "✅ All CI checks completed!"

# Test Snowflake connection
test-snowflake:
    @echo "🔍 Testing Snowflake connection..."
    @{{uv}} run python3 scripts/setup/setup_snowflake.py --test-only

# Setup Snowflake infrastructure
setup-snowflake:
    @echo "🏗️  Setting up Snowflake infrastructure..."
    @{{uv}} run python3 scripts/setup/setup_snowflake.py

# Setup Snowflake with reset
setup-snowflake-reset:
    @echo "🏗️  Resetting and setting up Snowflake infrastructure..."
    @{{uv}} run python3 scripts/setup/setup_snowflake.py --reset

# Test entire platform stack
test-stack:
    @echo "🧪 Testing entire platform stack..."
    @{{uv}} run python3 scripts/test_stack.py --component all

# Test specific component
test-stack-component COMPONENT:
    @echo "🧪 Testing component: {{COMPONENT}}..."
    @{{uv}} run python3 scripts/test_stack.py --component {{COMPONENT}}
