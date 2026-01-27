# Apache Airflow - Data Pipeline Orchestration

This directory contains the Apache Airflow configuration and DAGs for orchestrating the integrated data engineering, governance, and quality pipeline.

## Directory Structure

```
airflow/
├── docker/                     # Docker configuration
│   ├── docker-compose.yml      # Multi-container setup
│   ├── Dockerfile              # Custom Airflow image
│   ├── requirements.txt        # Python dependencies
│   └── validate_env.sh         # Environment validation
├── dags/                       # Airflow DAGs
│   ├── soda_initialization.py  # Data initialization DAG
│   └── soda_pipeline_run.py    # Main pipeline DAG
├── plugins/                    # Airflow plugins
└── README.md                   # This file
```

## DAGs Overview

### 1. Soda Initialization DAG (`soda_initialization.py`)

**Purpose**: One-time setup and initialization of the data pipeline

**Tasks**:
- **`reset_snowflake`**: Clean up existing Snowflake database
- **`setup_snowflake`**: Create database, schemas, tables, and sample data

**When to Use**:
- First-time setup
- Fresh start with clean data
- Testing and demonstration
- Not for regular pipeline runs

### 2. Soda Pipeline Run DAG (`soda_pipeline_run.py`)

**Purpose**: Regular data quality monitoring and processing with quality-gated metadata synchronization

**Guardrail Configuration**: RAW layer lenient, MART layer strict

**Orchestration Philosophy: Quality Gates Metadata Sync**

Each layer follows the sequence: **Build → Validate → Govern**
- **dbt build** → "this model exists"
- **Soda checks** → "this model is acceptable"
- **Collibra sync** → "this model is governable and discoverable"

Quality checks **gate** metadata synchronization. Metadata sync only happens after quality validation, ensuring Collibra reflects commitments, not aspirations.

**Layered Approach**:
1. **RAW Layer**: Quality checks → Metadata sync (gated by quality)
2. **STAGING Layer**: Build → Quality checks → Metadata sync (gated)
3. **MART Layer**: Build → Quality checks → Metadata sync (gated, strictest standards)
4. **QUALITY Layer**: Final validation and monitoring

**Tasks**:
- **`soda_scan_raw`**: RAW layer quality checks (gates metadata sync)
- **`collibra_sync_raw`**: Collibra metadata sync for RAW schema (only after quality passes)
- **`dbt_run_staging`**: Execute staging models (build phase)
- **`soda_scan_staging`**: STAGING layer quality checks (validation phase, gates sync)
- **`collibra_sync_staging`**: Collibra metadata sync for STAGING schema (governance phase)
- **`dbt_run_mart`**: Execute mart models (build phase)
- **`soda_scan_mart`**: MART layer quality checks (validation phase, gates sync)
- **`collibra_sync_mart`**: Collibra metadata sync for MART schema (governance phase, badge of trust)
- **`soda_scan_quality`**: Quality monitoring
- **`dbt_test`**: Execute dbt tests
- **`collibra_sync_quality`**: Collibra metadata sync for QUALITY schema (only after quality passes)
- **`cleanup_artifacts`**: Clean up temporary files

### 3. Soda Pipeline Run - Strict RAW DAG (`soda_pipeline_run_strict_raw.py`)

**Purpose**: Pipeline with strict quality guardrails for RAW layer

**Guardrail Configuration**: RAW layer strict (pipeline fails if critical checks fail), MART layer lenient

**When to Use**:
- Early data quality validation
- Strict source data requirements
- Production environments where source data quality is critical

**Key Differences from Default Pipeline**:
- RAW layer: No `|| true` - pipeline fails if checks fail
- RAW layer: Quality gate validates before Collibra sync
- MART layer: Lenient mode - continues even if checks fail

### 4. Soda Pipeline Run - Strict MART DAG (`soda_pipeline_run_strict_mart.py`)

**Purpose**: Pipeline with strict quality guardrails for MART layer

**Guardrail Configuration**: RAW layer lenient, MART layer strict (pipeline fails if critical checks fail)

**When to Use**:
- Production-ready data validation
- Business-critical analytics
- Gold layer standards enforcement

**Key Differences from Default Pipeline**:
- RAW layer: Lenient mode - continues even if checks fail
- MART layer: No `|| true` - pipeline fails if checks fail
- MART layer: Quality gate validates before Collibra sync

**Integration Points**:
- Quality results automatically synchronized to Soda Cloud
- Quality metrics automatically pushed to Collibra (if configured)
- Metadata automatically synchronized to Collibra **only after quality validation**
- Governance assets updated with validated quality information and metadata
- Collibra becomes a historical record of accepted states

## Usage

### Start Airflow
```bash
just airflow-up
```

### Access Airflow UI
- URL: http://localhost:8080
- Username: admin
- Password: admin

### Trigger DAGs
```bash
# Initialize data (one-time)
just airflow-trigger-init

# Run main pipeline (RAW lenient, MART strict)
just airflow-trigger-pipeline

# Run pipeline with strict RAW guardrails
just airflow-trigger-pipeline-strict-raw

# Run pipeline with strict MART guardrails
just airflow-trigger-pipeline-strict-mart
```

### Check Status
```bash
just airflow-status
```

### View Logs
```bash
just airflow-logs
```

## Configuration

### Environment Variables

**Important**: Airflow uses the **root project `.env` file** (not a separate one in `airflow/docker/`).

The Docker setup:
- Loads environment variables from the root `.env` file via `env_file: ../../.env`
- Mounts the root `.env` file into containers at `/opt/airflow/.env` for `source .env` commands
- You only need to maintain **one `.env` file** in the project root

**After updating `.env` file**: You must restart Airflow containers for changes to take effect:
```bash
just airflow-down
just airflow-up
```

Airflow automatically loads environment variables from the root `.env` file:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DATA_GOVERNANCE_PLATFORM  # Database name (default: DATA_GOVERNANCE_PLATFORM if not set)
SNOWFLAKE_SCHEMA=RAW

# Soda Cloud Configuration
SODA_CLOUD_HOST=https://cloud.soda.io
SODA_CLOUD_API_KEY_ID=your_api_key_id
SODA_CLOUD_API_KEY_SECRET=your_api_key_secret

# Collibra Configuration (for governance integration)
COLLIBRA_BASE_URL=https://your-instance.collibra.com
COLLIBRA_USERNAME=your_username
COLLIBRA_PASSWORD=your_password
```

### Docker Configuration
- **Multi-container setup**: Airflow webserver, scheduler, worker, and PostgreSQL
- **Custom image**: Includes dbt, Soda, and project dependencies
- **Volume mounts**: Persistent logs and configuration
- **Environment validation**: Automatic environment variable checking

## Pipeline Flow

### Initialization Flow
```
init_start → reset_snowflake → setup_snowflake → init_end
```

### Main Pipeline Flow - Quality-Gated Metadata Sync

```
pipeline_start
    ↓
RAW Layer:
    raw_layer_start → soda_scan_raw → collibra_sync_raw → raw_layer_end
    (Quality gates metadata sync)
    ↓
STAGING Layer:
    staging_layer_start → dbt_run_staging → soda_scan_staging → collibra_sync_staging → staging_layer_end
    (Build → Validate → Govern)
    ↓
MART Layer:
    mart_layer_start → dbt_run_mart → soda_scan_mart → collibra_sync_mart → mart_layer_end
    (Build → Validate → Govern, strictest standards)
    ↓
QUALITY Layer:
    quality_layer_start → [soda_scan_quality, dbt_test] → collibra_sync_quality → quality_layer_end
    (Quality monitoring → Metadata sync)
    ↓
cleanup_artifacts → pipeline_end → superset_upload_data
    (Visualization: Upload quality data to Superset)
```

**Orchestration Philosophy**:
- **Quality gates metadata sync**: Metadata sync only happens after quality validation
- **Sequential phase transitions**: Build → Validate → Govern (no parallelism across semantic boundaries)
- **Parallelism within phases**: Multiple dbt models or checks can run in parallel within the same phase
- **Collibra reflects commitments**: Only validated data enters governance catalog

**Integration Flow**:
- Quality results → Soda Cloud (automatic)
- Quality metrics → Collibra (automatic, if configured)
- Metadata sync → Collibra (automatic, **only after quality validation**)
- Governance assets updated with validated quality information and metadata
- Quality data → Superset (automatic, **after pipeline completion**)
  - Uploads latest quality metrics for visualization
  - Requires Superset to be running (health check performed)
  - Extracts data from Soda Cloud API and organizes it

## Data Quality Layers

### RAW Layer
- **Purpose**: Initial data quality assessment
- **Thresholds**: Relaxed for source data
- **Checks**: Schema validation, completeness, basic quality

### STAGING Layer
- **Purpose**: Validation after transformation
- **Thresholds**: Stricter than RAW
- **Checks**: Data cleaning validation, business rules

### MART Layer
- **Purpose**: Business-ready data validation
- **Thresholds**: Strictest requirements
- **Checks**: Business logic, referential integrity

### QUALITY Layer
- **Purpose**: Overall quality monitoring
- **Thresholds**: Monitoring and alerting
- **Checks**: Cross-layer validation, trend analysis

### Superset Upload Task

The `superset_upload_data` task is automatically executed at the end of the pipeline:

- **Purpose**: Upload quality data to Superset for visualization
- **Health Checks**: Verifies Superset container and database are available before proceeding
- **Workflow**:
  1. Updates Soda data source names to match database configuration
  2. Extracts latest data from Soda Cloud API
  3. Organizes data (keeps only latest files)
  4. Uploads to Superset PostgreSQL database
- **Requirements**: Superset must be running (`just superset-up`)
- **Failure Handling**: Task fails with clear error message if Superset is not available
- **Manual Alternative**: Can be run manually with `just superset-upload-data`

## Monitoring & Observability

### Airflow UI Features
- **DAG Execution**: Visual pipeline execution monitoring
- **Task Logs**: Detailed task-level logging and debugging
- **Performance Metrics**: Execution time and resource usage
- **Error Tracking**: Failed task identification and retry logic

### Log Locations
- **Airflow logs**: `airflow/docker/airflow-logs/`
- **DAG logs**: Available in Airflow UI
- **Task logs**: Individual task execution logs
- **Soda logs**: Integrated with Airflow task logs

### Monitoring Commands
```bash
# Check service status
just airflow-status

# View recent logs
just airflow-logs

# Check specific DAG
# Access Airflow UI → DAGs → Select DAG → View logs
```

## Troubleshooting

### Common Issues

#### DAG Not Appearing
- **Cause**: DAG parsing errors or missing dependencies
- **Solution**: Check Airflow logs for parsing errors

#### Task Failures
- **Cause**: Environment variables, connection issues, or logic errors
- **Solution**: Check task logs in Airflow UI

#### Connection Issues
- **Cause**: Incorrect Snowflake credentials or network issues
- **Solution**: Verify environment variables and network connectivity

#### dbt Failures
- **Cause**: Schema issues, model errors, or dependency problems
- **Solution**: Check dbt logs and model configurations

#### Superset Upload Failures
- **Cause**: Superset container not running or database not accessible
- **Solution**: 
  1. Start Superset: `just superset-up`
  2. Wait for Superset to be ready (about 45 seconds)
  3. Verify status: `just superset-status`
  4. Check container logs: `just superset-logs`
  5. Alternatively, run manual upload: `just superset-upload-data`

#### Collibra Integration Issues
- **Cause**: Incorrect Collibra credentials or asset type IDs
- **Solution**: Verify Collibra configuration and asset type IDs in configuration file

#### Collibra Metadata Sync Issues
- **Cause**: Schema asset IDs not resolving to connection IDs, or sync trigger failures
- **Solution**: 
  - Verify schema asset IDs in `collibra/config.yml`
  - Check that schemas have been synchronized at least once in Collibra
  - Review Collibra job status in Collibra UI (syncs complete in background)
  - Check Airflow task logs for sync trigger confirmation
  - Verify Collibra credentials are correct

### Debug Commands
```bash
# Check Airflow status
just airflow-status

# View logs
just airflow-logs

# Restart services
just airflow-down && just airflow-up

# Check environment
just airflow-validate-env
```

## Best Practices

### DAG Development
1. **Idempotency**: Ensure tasks can be safely re-run
2. **Error Handling**: Include proper retry logic and error handling
3. **Documentation**: Document DAGs and tasks clearly
4. **Testing**: Test DAGs in development before production

### Task Design
1. **Atomicity**: Each task should perform one specific function
2. **Dependencies**: Define clear task dependencies
3. **Resource Management**: Use appropriate resource allocation
4. **Monitoring**: Include proper logging and monitoring

### Environment Management
1. **Configuration**: Use environment variables for configuration
2. **Secrets**: Store sensitive data securely
3. **Validation**: Validate environment before execution
4. **Documentation**: Document all configuration requirements

## Integration Points

### dbt Integration
- **Staging Models**: Executed in STAGING layer
- **Mart Models**: Executed in MART layer
- **Tests**: Executed in QUALITY layer
- **Schema Management**: Uses custom schema configuration

### Soda Integration
- **Quality Checks**: Executed at each layer
- **Configuration**: Layer-specific Soda configurations
- **Cloud Integration**: Results sent to Soda Cloud
- **Monitoring**: Integrated with Airflow monitoring

### Collibra Integration
- **Governance Sync**: Quality results automatically synchronized to Collibra
- **Metadata Sync**: Schema and table metadata automatically synchronized after each layer
- **Asset Mapping**: Quality metrics linked to data assets
- **Configuration**: 
  - Quality sync configured in `soda/soda-collibra-integration-configuration/configuration-collibra.yml`
  - Metadata sync configured in `collibra/config.yml`
- **Selective Sync**: Only datasets marked for sync are synchronized
- **Automatic Resolution**: Schema asset IDs automatically resolved to schema connection IDs
- **Background Completion**: Metadata syncs triggered and complete in Collibra background

### Snowflake Integration
- **Connection**: Uses environment-based connection
- **Schema Management**: Clean schema separation
- **Performance**: Optimized warehouse usage
- **Security**: Secure credential management

## Success Metrics

- **Reliable Orchestration**: Consistent pipeline execution
- **Layer Separation**: Clear data quality layer progression
- **Error Handling**: Robust error handling and retry logic
- **Monitoring**: Comprehensive logging and observability
- **Integration**: Seamless dbt, Soda, and Collibra integration
- **Documentation**: Clear DAG and task documentation
- **Performance**: Optimized execution and resource usage
- **Maintainability**: Clean, modular DAG design
- **Governance Integration**: Quality metrics automatically available in governance catalog
- **Metadata Sync**: Automatic metadata synchronization after each pipeline layer (RAW, STAGING, MART, QUALITY)
- **Background Sync**: Syncs triggered and complete in Collibra background

---

**Last Updated**: January 2025  
**Version**: 2.0.0  
**Airflow Version**: 2.8+
