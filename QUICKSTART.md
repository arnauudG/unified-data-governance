# Quick Start Guide

Get up and running with the unified-data-governance platform in minutes!

## Prerequisites

1. **Docker & Docker Compose** - For running Airflow and Superset
2. **Python 3.11+** - For local scripts
3. **uv** - Fast Python package manager
4. **Snowflake account** - With appropriate permissions
5. **Soda Cloud account** - For quality monitoring
6. **Collibra account** - For governance integration (optional for testing)

## Installation

### 1. Install uv (if not already installed)

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with pip
pip install uv
```

### 2. Install just (if not already installed)

```bash
# macOS
brew install just

# Linux
cargo install just

# Or download from: https://github.com/casey/just/releases
```

## Setup

### 1. Clone and Navigate

```bash
cd unified-data-governance
```

### 2. Create Environment File

```bash
# Copy the example file
cp .env.example .env

# Edit .env with your credentials
# Required variables:
# - SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
# - SNOWFLAKE_DATABASE (defaults to DATA_GOVERNANCE_PLATFORM)
# - SODA_CLOUD_API_KEY_ID, SODA_CLOUD_API_KEY_SECRET
# - COLLIBRA_BASE_URL, COLLIBRA_USERNAME, COLLIBRA_PASSWORD (optional)
```

### 3. Set Up Python Environment

```bash
# Create virtual environment and install dependencies
just setup

# This will:
# - Create .venv with uv
# - Install all dependencies
# - Update Soda data source names
```

## Running the Platform

### Option 1: Start Everything (Recommended for First Time)

```bash
# Start all services (Airflow + Superset)
just all-up

# Wait ~45 seconds for services to be ready
# Then access:
# - Airflow UI: http://localhost:8080 (admin/admin)
# - Superset UI: http://localhost:8089 (admin/admin)
```

### Option 2: Start Services Individually

```bash
# Start Airflow only
just airflow-up

# Start Superset only (in another terminal)
just superset-up
```

### 3. Initialize Data (First Time Only)

```bash
# This creates tables and loads sample data
just airflow-trigger-init

# Check progress at: http://localhost:8080
# Wait for the DAG to complete (green checkmarks)
```

### 4. Run the Pipeline

```bash
# Run complete pipeline (engineering + quality + governance)
just airflow-trigger-pipeline

# Or with strict guardrails:
just airflow-trigger-pipeline-strict-raw   # Fails if RAW checks fail
just airflow-trigger-pipeline-strict-mart # Fails if MART checks fail
```

### 5. View Results

**Airflow UI** (http://localhost:8080):
- View DAG runs and task logs
- Monitor pipeline execution
- Check quality check results

**Superset UI** (http://localhost:8089):
- Visualize quality metrics
- Explore data quality dashboards
- View check results over time

**Soda Cloud**:
- Detailed quality check results
- Failed row samples
- Data profiling information

## Common Commands

### Service Management

```bash
just all-up              # Start all services
just all-down            # Stop all services
just airflow-status      # Check Airflow status
just superset-status     # Check Superset status
just airflow-logs        # View Airflow logs
just superset-logs       # View Superset logs
```

### Pipeline Execution

```bash
just airflow-trigger-init              # Initialize data (first time)
just airflow-trigger-pipeline         # Run complete pipeline
just airflow-trigger-pipeline-strict-raw   # Strict RAW guardrails
just airflow-trigger-pipeline-strict-mart  # Strict MART guardrails
```

### Data Quality

```bash
just soda-dump              # Extract Soda Cloud data
just organize-soda-data      # Organize data structure
just superset-upload-data    # Upload to Superset
just soda-update-datasources # Update data source names
```

### Development

```bash
just clean              # Clean artifacts
just clean-logs        # Clean logs
just clean-all         # Deep clean
just test-collibra     # Test Collibra integration
```

## Testing Individual Components

### Test Soda Checks Locally

```bash
# Update data source names first
just soda-update-datasources

# Run Soda scan for a specific layer
cd soda
soda scan -d data_platform_xyz_raw -c configuration/configuration_raw.yml checks/raw/customers.yml
```

### Test Collibra Integration

```bash
# Test metadata sync
just test-collibra

# Or manually
uv run python3 collibra/test_metadata_sync.py
```

### Test Great Expectations (GX)

```bash
cd gx

# Validate setup
uv run python scripts/test_gx_setup.py

# Run a checkpoint (once GX is properly configured)
uv run python scripts/run_gx_check.py --layer mart --table fact_orders
```

## Troubleshooting

### Services Won't Start

```bash
# Check Docker is running
docker ps

# Check ports are available
lsof -i :8080  # Airflow
lsof -i :8089  # Superset

# View logs
just airflow-logs
just superset-logs
```

### Environment Variables Not Loading

```bash
# Verify .env file exists
ls -la .env

# Check variables are set
cat .env | grep SNOWFLAKE

# Update data source names
just soda-update-datasources
```

### Pipeline Fails

```bash
# Check Airflow task logs
just airflow-logs

# View specific task logs
just airflow-task-logs <task_id> <dag_id>

# Example:
just airflow-task-logs soda_scan_raw soda_pipeline_run
```

### Reset Everything

```bash
# Stop all services
just all-down

# Clean up
just clean-all

# Restart
just all-up
just airflow-trigger-init
```

## Next Steps

1. **Explore Airflow DAGs**: Check out the pipeline structure at http://localhost:8080
2. **View Quality Checks**: See quality results in Soda Cloud
3. **Visualize Data**: Explore dashboards in Superset
4. **Check Governance**: Verify metadata sync in Collibra (if configured)
5. **Customize Checks**: Edit check files in `soda/checks/` to match your needs

## Architecture Overview

```
RAW Layer (Snowflake)
    ↓
Soda Quality Checks (RAW)
    ↓
Collibra Metadata Sync (RAW) ← Quality Gate
    ↓
dbt Transformations (STAGING)
    ↓
Soda Quality Checks (STAGING)
    ↓
Collibra Metadata Sync (STAGING) ← Quality Gate
    ↓
dbt Models (MARTS)
    ↓
Soda Quality Checks (MARTS)
    ↓
Collibra Metadata Sync (MARTS) ← Quality Gate
    ↓
Visualization (Superset)
```

## Need Help?

- Check the main [README.md](README.md) for detailed documentation
- Review component-specific READMEs:
  - [Airflow README](airflow/README.md)
  - [Soda README](soda/README.md)
  - [Superset README](superset/README.md)
  - [Collibra README](collibra/README.md)
- View logs: `just airflow-logs` or `just superset-logs`
