# Data Governance Platform Project Makefile
PY?=python3.11
VENV=.venv

.PHONY: help all venv deps pipeline fresh smooth airflow-up airflow-down airflow-status airflow-trigger clean clean-logs clean-all

help: ## Show this help message
	@echo "Data Governance Platform Project - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

all: venv deps ## Setup environment

venv: ## Create virtual environment
	@if ! command -v $(PY) >/dev/null 2>&1; then \
		echo "❌ Error: $(PY) not found. Please install Python 3.11 or set PY variable."; \
		exit 1; \
	fi
	@if [ ! -d "$(VENV)" ]; then \
		$(PY) -m venv $(VENV); \
		echo "[OK] Virtual environment created with $(PY)"; \
	else \
		echo "[OK] Virtual environment exists"; \
	fi

deps: venv ## Install dependencies
	@echo "📦 Installing dependencies..."
	@. $(VENV)/bin/activate && pip install -q --upgrade pip && \
	echo "🧹 Removing conflicting soda-postgres if present..." && \
	pip uninstall -q -y soda-postgres 2>/dev/null || true && \
	echo "📌 Installing critical dependencies first to prevent downgrades..." && \
	pip install -q "protobuf>=6.30.0,<6.31.0" "pydantic>=2.5.2,<3.0.0" "pyarrow>=15.0.0,<22.0.0" && \
	echo "📦 Upgrading conflicting packages to support protobuf 6.x..." && \
	pip install -q --upgrade "google-api-core>=2.23.0" "googleapis-common-protos>=1.66.0" "proto-plus>=1.26.0" || true && \
	grep -v "^soda-snowflake\|^#.*soda" scripts/setup/requirements.txt > /tmp/requirements_no_soda.txt && \
	pip install -q -r /tmp/requirements_no_soda.txt && \
	echo "📦 Installing soda-snowflake from Soda Cloud PyPI..." && \
	pip install -q --upgrade-strategy only-if-needed -i https://pypi.cloud.soda.io "soda-snowflake==1.12.24" && \
	echo "🔧 Ensuring critical dependencies remain at correct versions..." && \
	pip install -q --upgrade "protobuf>=6.30.0,<6.31.0" "pydantic>=2.5.2,<3.0.0" "pyarrow>=15.0.0,<22.0.0" && \
	pip install -q --upgrade "google-api-core>=2.23.0" "googleapis-common-protos>=1.66.0" "proto-plus>=1.26.0" || true && \
	rm -f /tmp/requirements_no_soda.txt && \
	echo "[OK] Dependencies installed"
	@echo "ℹ️  Note: This project uses Snowflake, not PostgreSQL for Soda checks."
	@echo "   Removed soda-postgres to avoid version conflicts."
	@echo "⚠️  Some dependency warnings may appear for transitive dependencies"
	@echo "   (mlflow, anyscale, great-expectations, fastapi) but these are not"
	@echo "   directly used and should not affect functionality."

pipeline: venv ## Run standard pipeline (via Airflow)
	@echo "Use Airflow DAGs for pipeline execution:"
	@echo "  make airflow-trigger-init    # First-time setup"
	@echo "  make airflow-trigger-pipeline # Regular runs"

airflow-up: ## Start Airflow services with Docker
	@echo "🚀 Starting Airflow services..."
	@echo "🌐 Ensuring shared Docker network exists..."
	@docker network create data-governance-network 2>/dev/null || echo "Network already exists"
	@echo "🔄 Updating Soda data source names to match database configuration..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names (this is OK if running in Docker)"
	@echo "📥 Loading environment variables and starting Docker containers..."
	@bash -c "source load_env.sh && cd airflow/docker && docker-compose up -d"
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
	@make airflow-list

superset-up: ## Start Superset visualization service (separate setup)
	@echo "📊 Starting Superset services..."
	@echo "🌐 Ensuring shared Docker network exists..."
	@docker network create data-governance-network 2>/dev/null || echo "Network already exists"
	@echo "📥 Loading environment variables and starting Docker containers..."
	@bash -c "source load_env.sh && cd superset && docker-compose up -d"
	@echo "⏳ Waiting for Superset to be ready..."
	@sleep 45
	@echo "[OK] Superset started with Docker"
	@echo "[INFO] Superset UI: http://localhost:8089 (admin/admin)"

all-up: ## Start all services (Airflow + Superset)
	@echo "🚀 Starting all services..."
	@echo "🌐 Ensuring shared Docker network exists..."
	@docker network create data-governance-network 2>/dev/null || echo "Network already exists"
	@echo "🔄 Updating Soda data source names to match database configuration..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names (this is OK if running in Docker)"
	@echo "📥 Loading environment variables and starting Docker containers..."
	@bash -c "source load_env.sh && cd airflow/docker && docker-compose up -d && cd ../../superset && docker-compose up -d"
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

all-down: ## Stop all services (Airflow + Superset)
	@echo "🛑 Stopping all services..."
	@echo "🔄 Stopping Airflow services..."
	@cd airflow/docker && docker-compose down
	@echo "🔄 Stopping Superset services..."
	@cd superset && docker-compose down
	@echo "[OK] All services stopped"

airflow-down: ## Stop Airflow services
	@cd airflow/docker && docker-compose down
	@echo "[OK] Airflow services stopped"

superset-down: ## Stop Superset services
	@cd superset && docker-compose down
	@echo "[OK] Superset services stopped"

superset-status: ## Check Superset services status
	@echo "🔍 Checking Superset services..."
	@cd superset && docker-compose ps

superset-logs: ## View Superset logs
	@cd superset && docker-compose logs -f superset

superset-reset: ## Reset Superset database and restart
	@echo "🔄 Resetting Superset..."
	@cd superset && docker-compose down
	@cd superset && docker volume rm superset_superset-postgres-data superset_superset-data 2>/dev/null || true
	@cd superset && docker-compose up -d
	@echo "⏳ Waiting for Superset to be ready..."
	@sleep 45
	@echo "[OK] Superset reset and restarted"

dump-databases: ## Dump all databases (Superset, Airflow, Soda data)
	@echo "🗄️  Dumping all databases..."
	@./scripts/dump_databases.sh --all
	@echo "[OK] All databases dumped"

dump-superset: ## Dump Superset database only
	@echo "📊 Dumping Superset database..."
	@./scripts/dump_databases.sh --superset-only
	@echo "[OK] Superset database dumped"

dump-airflow: ## Dump Airflow database only
	@echo "🔄 Dumping Airflow database..."
	@./scripts/dump_databases.sh --airflow-only
	@echo "[OK] Airflow database dumped"

dump-soda: ## Dump Soda data only
	@echo "📈 Dumping Soda data..."
	@./scripts/dump_databases.sh --soda-only
	@echo "[OK] Soda data dumped"

airflow-status: ## Check Airflow services status
	@echo "🔍 Checking Airflow services..."
	@cd airflow/docker && docker-compose ps

airflow-logs: ## View Airflow logs
	@cd airflow/docker && docker-compose logs -f

airflow-task-logs: ## View logs for a specific task (usage: make airflow-task-logs TASK=superset_upload_data DAG=soda_pipeline_run)
	@if [ -z "$(TASK)" ] || [ -z "$(DAG)" ]; then \
		echo "Usage: make airflow-task-logs TASK=<task_id> DAG=<dag_id>"; \
		echo "Example: make airflow-task-logs TASK=superset_upload_data DAG=soda_pipeline_run"; \
		echo ""; \
		echo "Finding latest task logs..."; \
		docker exec soda-airflow-scheduler find /opt/airflow/logs -name "*.log" -type f -path "*superset_upload_data*" -exec ls -lt {} + 2>/dev/null | head -5 || \
		docker exec soda-airflow-scheduler find /opt/airflow/logs -name "*.log" -type f | head -10; \
	else \
		echo "📋 Finding latest logs for task $(TASK) in DAG $(DAG)..."; \
		LATEST_LOG=$$(docker exec soda-airflow-scheduler find /opt/airflow/logs -path "*dag_id=$(DAG)*" -path "*task_id=$(TASK)*" -name "*.log" -type f -exec ls -t {} + 2>/dev/null | head -1); \
		if [ -n "$$LATEST_LOG" ]; then \
			echo "📄 Viewing: $$LATEST_LOG"; \
			echo "---"; \
			docker exec soda-airflow-scheduler tail -f "$$LATEST_LOG" 2>/dev/null || docker exec soda-airflow-scheduler cat "$$LATEST_LOG"; \
		else \
			echo "❌ No logs found for task $(TASK) in DAG $(DAG)"; \
			echo "Available logs:"; \
			docker exec soda-airflow-scheduler find /opt/airflow/logs -name "*.log" -type f | head -10; \
		fi; \
	fi

airflow-unpause-all: ## Unpause all Soda DAGs
	@echo "▶️  Unpausing all Soda DAGs..."
	@docker exec soda-airflow-webserver airflow dags unpause soda_initialization
	@docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run
	@docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_raw || true
	@docker exec soda-airflow-webserver airflow dags unpause soda_pipeline_run_strict_mart || true
	@echo "[OK] All Soda DAGs unpaused"

airflow-pause-all: ## Pause all Soda DAGs
	@echo "⏸️  Pausing all Soda DAGs..."
	@docker exec soda-airflow-webserver airflow dags pause soda_initialization
	@docker exec soda-airflow-webserver airflow dags pause soda_pipeline_run
	@docker exec soda-airflow-webserver airflow dags pause soda_pipeline_run_strict_raw || true
	@docker exec soda-airflow-webserver airflow dags pause soda_pipeline_run_strict_mart || true
	@echo "[OK] All Soda DAGs paused"

airflow-rebuild: ## Rebuild Airflow containers
	@cd airflow/docker && docker-compose down
	@cd airflow/docker && docker-compose build --no-cache
	@cd airflow/docker && docker-compose up -d
	@echo "[OK] Airflow containers rebuilt and started"

airflow-trigger-init: ## Trigger initialization DAG (fresh setup only)
	@echo "🔄 Ensuring Soda data source names are up to date..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names"
	@echo "🚀 Triggering initialization DAG..."
	@docker exec soda-airflow-webserver airflow dags trigger soda_initialization
	@echo "[OK] Initialization DAG triggered"
	@echo "[INFO] Check progress at: http://localhost:8080"

airflow-trigger-pipeline: ## Trigger layered pipeline DAG (layer-by-layer processing)
	@echo "🔄 Ensuring Soda data source names are up to date..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names"
	@echo "🔄 Triggering layered pipeline DAG..."
	@docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run
	@echo "[OK] Layered pipeline DAG triggered"
	@echo "[INFO] Check progress at: http://localhost:8080"

airflow-trigger-pipeline-strict-raw: ## Trigger pipeline with strict RAW layer guardrails (pipeline fails if RAW checks fail)
	@echo "🔄 Ensuring Soda data source names are up to date..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names"
	@echo "🔄 Triggering strict RAW pipeline DAG..."
	@docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run_strict_raw
	@echo "[OK] Strict RAW pipeline DAG triggered"
	@echo "[INFO] Check progress at: http://localhost:8080"
	@echo "[INFO] This pipeline will FAIL if RAW layer critical checks fail"

airflow-trigger-pipeline-strict-mart: ## Trigger pipeline with strict MART layer guardrails (pipeline fails if MART checks fail)
	@echo "🔄 Ensuring Soda data source names are up to date..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names"
	@echo "🔄 Triggering strict MART pipeline DAG..."
	@docker exec soda-airflow-webserver airflow dags trigger soda_pipeline_run_strict_mart
	@echo "[OK] Strict MART pipeline DAG triggered"
	@echo "[INFO] Check progress at: http://localhost:8080"
	@echo "[INFO] This pipeline will FAIL if MART layer critical checks fail"

soda-dump: ## Extract Soda Cloud data to CSV files
	@echo "📊 Extracting Soda Cloud data..."
	@./scripts/run_soda_dump.sh
	@echo "[OK] Soda Cloud data extracted to CSV files"

airflow-list: ## List available DAGs
	@echo "📋 Listing available DAGs..."
	@docker exec soda-airflow-webserver airflow dags list | grep soda

docs: ## Open documentation
	@echo "📚 Available Documentation:"
	@echo "  📖 README.md - Complete project documentation"
	@echo "  🔧 Makefile - Development commands and automation"
	@echo "  📋 Airflow UI - http://localhost:8080 (admin/admin)"
	@echo "  📊 Superset UI - http://localhost:8089 (admin/admin)"
	@echo ""
	@echo "💡 Quick commands:"
	@echo "  make help - Show all available commands"
	@echo "  make all-up - Start all services (Airflow + Superset)"
	@echo "  make airflow-trigger-init - Fresh initialization (first time)"
	@echo "  make airflow-trigger-pipeline - Layered pipeline runs"

setup: venv deps ## Complete environment setup
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
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names"
	@echo "[OK] Environment setup completed"
	@echo "[INFO] Next steps:"
	@echo "  1. Ensure .env file has all required credentials"
	@echo "  2. Run: make airflow-up"
	@echo "  3. Run: make airflow-trigger-init (first time setup)"
	@echo "  4. Access Airflow UI: http://localhost:8080"

clean: ## Clean up artifacts and temporary files
	@echo "🧹 Cleaning up artifacts..."
	@rm -rf dbt/target dbt/logs snowflake_connection_test.log
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@rm -rf airflow/airflow-logs 2>/dev/null || true
	@echo "[OK] Artifacts cleaned"

clean-logs: ## Clean up old Airflow logs
	@echo "🧹 Cleaning up old logs..."
	@rm -rf airflow/airflow-logs 2>/dev/null || true
	@echo "[OK] Old logs cleaned"

clean-all: clean clean-logs ## Deep clean: artifacts, logs, and cache
	@echo "🧹 Deep cleaning project..."
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@find . -name ".DS_Store" -delete 2>/dev/null || true

soda-update-datasources: ## Update Soda data source names in config files based on SNOWFLAKE_DATABASE
	@echo "🔄 Updating Soda data source names..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py"
	@echo "[OK] Data source names updated"
	@echo "[OK] Deep clean completed"

# =============================================================================
# SODA DATA MANAGEMENT
# =============================================================================

organize-soda-data: ## Organize Soda dump data in user-friendly structure
	@echo "📁 Organizing Soda dump data..."
	@python3 scripts/organize_soda_data.py
	@echo "✅ Data organized successfully!"

superset-upload-data: ## Complete Soda workflow: dump + organize + upload to Superset
	@echo "📤 Complete Soda data workflow..."
	@echo "🔄 Ensuring Soda data source names are up to date..."
	@bash -c "source load_env.sh && python3 soda/update_data_source_names.py" || echo "⚠️  Warning: Could not update data source names"
	@echo "1. Extracting data from Soda Cloud..."
	@make soda-dump
	@echo "2. Organizing data..."
	@make organize-soda-data
	@echo "3. Uploading to Superset..."
	@cp scripts/upload_soda_data_docker.py superset/data/
	@cd superset && docker-compose exec superset python /app/soda_data/upload_soda_data_docker.py
	@echo "✅ Complete Soda data workflow finished!"

superset-clean-restart: ## Clean restart Superset (removes all data)
	@echo "🧹 Performing clean Superset restart..."
	@make superset-down
	@cd superset && docker-compose down -v
	@echo "🗑️  Removed all Superset data and volumes"
	@make superset-up
	@echo "✅ Superset clean restart completed!"

superset-reset-data: ## Reset only Superset data (keep containers)
	@echo "🔄 Resetting Superset data..."
	@cd superset && docker-compose exec superset-db psql -U superset -d superset -c "DROP SCHEMA IF EXISTS soda CASCADE;"
	@echo "✅ Superset data reset completed!"

superset-reset-schema: ## Reset only the soda schema (fixes table structure issues)
	@echo "🔄 Resetting soda schema..."
	@cd superset && docker-compose exec superset-db psql -U superset -d superset -c "DROP SCHEMA IF EXISTS soda CASCADE;"
	@echo "✅ Soda schema reset complete"

test-collibra: ## Test Collibra metadata sync module (verify configuration and credentials)
	@echo "🧪 Testing Collibra metadata sync module..."
	@python3 collibra/test_metadata_sync.py

