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
    echo "📦 Installing project dependencies from requirements.txt..." && \
    grep -v "^soda-snowflake\|^#.*soda" requirements.txt > /tmp/requirements_no_soda.txt && \
    {{uv}} pip install -r /tmp/requirements_no_soda.txt && \
    rm -f /tmp/requirements_no_soda.txt && \
    echo "🧹 Removing conflicting soda-postgres if present..." && \
    {{uv}} pip uninstall -y soda-postgres 2>/dev/null || true && \
    echo "📦 Installing soda-snowflake from Soda Cloud PyPI..." && \
    {{uv}} pip install -i https://pypi.cloud.soda.io "soda-snowflake==1.12.24" && \
    echo "🔧 Ensuring critical dependencies remain at correct versions..." && \
    {{uv}} pip install --upgrade "protobuf>=6.30.0,<6.31.0" "pydantic>=2.5.2,<3.0.0" "pyarrow>=15.0.0,<22.0.0" && \
    {{uv}} pip install --upgrade "google-api-core>=2.23.0" "googleapis-common-protos>=1.66.0" "proto-plus>=1.26.0" || true && \
    echo "[OK] Dependencies installed"
    @echo "ℹ️  Note: This project uses Snowflake, not PostgreSQL for Soda checks."
    @echo "   Removed soda-postgres to avoid version conflicts."
    @echo "⚠️  Some dependency warnings may appear for transitive dependencies"
    @echo "   (mlflow, anyscale, great-expectations, fastapi) but these are not"
    @echo "   directly used and should not affect functionality."

# Complete environment setup
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
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "[OK] Environment setup completed"
    @echo "[INFO] Next steps:"
    @echo "  1. Ensure .env file has all required credentials"
    @echo "  2. Run: just airflow-up"
    @echo "  3. Run: just airflow-trigger-init (first time setup)"
    @echo "  4. Access Airflow UI: http://localhost:8080"
    @echo ""
    @echo "💡 Using uv: Run commands with 'uv run' (no activation needed)"
    @echo "   Example: uv run python3 soda/update_data_source_names.py"

# Start Airflow services with Docker
airflow-up:
    @echo "🚀 Starting Airflow services..."
    @echo "🌐 Ensuring shared Docker network exists..."
    @docker network create data-governance-network 2>/dev/null || echo "Network already exists"
    @echo "🔄 Updating Soda data source names to match database configuration..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names (this is OK if running in Docker)"
    @echo "📥 Loading environment variables and starting Docker containers..."
    @cd airflow/docker && docker-compose up -d
    @echo "⏳ Waiting for services to be ready..."
    @sleep 30
    @echo "▶️  Unpausing all Soda DAGs..."
    @docker exec soda-airflow-webserver airflow dags unpause soda_initialization || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_raw || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_mart || true
    @echo "[OK] Airflow services started with Docker"
    @echo "[INFO] Web UI: http://localhost:8080 (admin/admin)"
    @echo "[INFO] Available DAGs:"
    @just airflow-list

# Start Superset visualization service
superset-up:
    @echo "📊 Starting Superset services..."
    @echo "🌐 Ensuring shared Docker network exists..."
    @docker network create data-governance-network 2>/dev/null || echo "Network already exists"
    @echo "📥 Loading environment variables and starting Docker containers..."
    @cd superset && docker-compose up -d
    @echo "⏳ Waiting for Superset to be ready..."
    @sleep 45
    @echo "[OK] Superset started with Docker"
    @echo "[INFO] Superset UI: http://localhost:8089 (admin/admin)"

# Start all services (Airflow + Superset)
all-up:
    @echo "🚀 Starting all services..."
    @echo "🌐 Ensuring shared Docker network exists..."
    @docker network create data-governance-network 2>/dev/null || echo "Network already exists"
    @echo "🔄 Updating Soda data source names to match database configuration..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names (this is OK if running in Docker)"
    @echo "📥 Loading environment variables and starting Docker containers..."
    @cd airflow/docker && docker-compose up -d && cd ../../superset && docker-compose up -d
    @echo "⏳ Waiting for services to be ready..."
    @sleep 45
    @echo "▶️  Unpausing all Soda DAGs..."
    @docker exec soda-airflow-webserver airflow dags unpause soda_initialization || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_raw || true
    @docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_mart || true
    @echo "[OK] All services started with Docker"
    @echo "[INFO] Airflow UI: http://localhost:8080 (admin/admin)"
    @echo "[INFO] Superset UI: http://localhost:8089 (admin/admin)"

# Stop all services (Airflow + Superset)
all-down:
    @echo "🛑 Stopping all services..."
    @echo "🔄 Stopping Airflow services..."
    @cd airflow/docker && docker-compose down
    @echo "🔄 Stopping Superset services..."
    @cd superset && docker-compose down
    @echo "[OK] All services stopped"

# Stop Airflow services
airflow-down:
    @cd airflow/docker && docker-compose down
    @echo "[OK] Airflow services stopped"

# Stop Superset services
superset-down:
    @cd superset && docker-compose down
    @echo "[OK] Superset services stopped"

# Check Superset services status
superset-status:
    @echo "🔍 Checking Superset services..."
    @cd superset && docker-compose ps

# View Superset logs
superset-logs:
    @cd superset && docker-compose logs -f superset

# Reset Superset database and restart
superset-reset:
    @echo "🔄 Resetting Superset..."
    @cd superset && docker-compose down
    @cd superset && docker volume rm superset_superset-postgres-data superset_superset-data 2>/dev/null || true
    @cd superset && docker-compose up -d
    @echo "⏳ Waiting for Superset to be ready..."
    @sleep 45
    @echo "[OK] Superset reset and restarted"

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
        echo "Example: just airflow-task-logs superset_upload_data soda_pipeline_run"; \
        echo ""; \
        echo "Finding latest task logs..."; \
        docker exec soda-airflow-scheduler find /opt/airflow/logs -name "*.log" -type f -path "*superset_upload_data*" -exec ls -lt {} + 2>/dev/null | head -5 || \
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
    @echo "[INFO] Check progress at: http://localhost:8080"

# Trigger layered pipeline DAG (layer-by-layer processing)
airflow-trigger-pipeline:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🔄 Triggering layered pipeline DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run
    @echo "[OK] Layered pipeline DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8080"

# Trigger pipeline with strict RAW layer guardrails
airflow-trigger-pipeline-strict-raw:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🔄 Triggering strict RAW pipeline DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run_strict_raw
    @echo "[OK] Strict RAW pipeline DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8080"
    @echo "[INFO] This pipeline will FAIL if RAW layer critical checks fail"

# Trigger pipeline with strict MART layer guardrails
airflow-trigger-pipeline-strict-mart:
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "🔄 Triggering strict MART pipeline DAG..."
    @docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run_strict_mart
    @echo "[OK] Strict MART pipeline DAG triggered"
    @echo "[INFO] Check progress at: http://localhost:8080"
    @echo "[INFO] This pipeline will FAIL if MART layer critical checks fail"

# Extract Soda Cloud data to CSV files
soda-dump:
    @echo "📊 Extracting Soda Cloud data..."
    @./scripts/run_soda_dump.sh
    @echo "[OK] Soda Cloud data extracted to CSV files"

# List available DAGs
airflow-list:
    @echo "📋 Listing available DAGs..."
    @docker exec soda-airflow-webserver airflow dags list | grep soda

# Update Soda data source names in config files
soda-update-datasources:
    @echo "🔄 Updating Soda data source names..."
    @{{uv}} run python3 soda/update_data_source_names.py
    @echo "[OK] Data source names updated"

# Organize Soda dump data in user-friendly structure
organize-soda-data:
    @echo "📁 Organizing Soda dump data..."
    @python3 scripts/organize_soda_data.py
    @echo "✅ Data organized successfully!"

# Complete Soda workflow: dump + organize + upload to Superset
superset-upload-data:
    @echo "📤 Complete Soda data workflow..."
    @echo "🔄 Ensuring Soda data source names are up to date..."
    @{{uv}} run python3 soda/update_data_source_names.py || echo "⚠️  Warning: Could not update data source names"
    @echo "1. Extracting data from Soda Cloud..."
    @just soda-dump
    @echo "2. Organizing data..."
    @just organize-soda-data
    @echo "3. Uploading to Superset..."
    @cp scripts/upload_soda_data_docker.py superset/data/
    @cd superset && docker-compose exec superset python /app/soda_data/upload_soda_data_docker.py
    @echo "✅ Complete Soda data workflow finished!"

# Clean restart Superset (removes all data)
superset-clean-restart:
    @echo "🧹 Performing clean Superset restart..."
    @just superset-down
    @cd superset && docker-compose down -v
    @echo "🗑️  Removed all Superset data and volumes"
    @just superset-up
    @echo "✅ Superset clean restart completed!"

# Reset only Superset data (keep containers)
superset-reset-data:
    @echo "🔄 Resetting Superset data..."
    @cd superset && docker-compose exec superset-db psql -U superset -d superset -c "DROP SCHEMA IF EXISTS soda CASCADE;"
    @echo "✅ Superset data reset completed!"

# Reset only the soda schema (fixes table structure issues)
superset-reset-schema:
    @echo "🔄 Resetting soda schema..."
    @cd superset && docker-compose exec superset-db psql -U superset -d superset -c "DROP SCHEMA IF EXISTS soda CASCADE;"
    @echo "✅ Soda schema reset complete"

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

# Dump all databases (Superset, Airflow, Soda data)
dump-databases:
    @echo "🗄️  Dumping all databases..."
    @./scripts/dump_databases.sh --all
    @echo "[OK] All databases dumped"

# Dump Superset database only
dump-superset:
    @echo "📊 Dumping Superset database..."
    @./scripts/dump_databases.sh --superset-only
    @echo "[OK] Superset database dumped"

# Dump Airflow database only
dump-airflow:
    @echo "🔄 Dumping Airflow database..."
    @./scripts/dump_databases.sh --airflow-only
    @echo "[OK] Airflow database dumped"

# Dump Soda data only
dump-soda:
    @echo "📈 Dumping Soda data..."
    @./scripts/dump_databases.sh --soda-only
    @echo "[OK] Soda data dumped"
