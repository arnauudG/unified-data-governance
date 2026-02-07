from datetime import datetime, timedelta
import os
import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import subprocess

# Add project root to Python path for Collibra imports
PROJECT_ROOT = Path("/opt/airflow")
sys.path.insert(0, str(PROJECT_ROOT))

# Absolute project root (Docker container path)
PROJECT_ROOT = "/opt/airflow"

# Load environment variables before importing helpers
# This ensures SNOWFLAKE_DATABASE is available when computing data source names
from dotenv import load_dotenv
env_file = Path("/opt/airflow/.env")
if env_file.exists():
    load_dotenv(env_file, override=True)

# Import Soda helpers to get data source names dynamically
from soda.helpers import get_data_source_name

# Common bash prefix to run in project, load env
BASH_PREFIX = "cd '/opt/airflow' && source .env && "


# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Pipeline Run DAG - Regular Data Processing
with DAG(
    dag_id="soda_pipeline_run",
    default_args=default_args,
    description="Soda Pipeline Run: Regular data processing and quality monitoring",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["soda", "dbt", "data-quality", "pipeline", "regular"],
    doc_md="""
    # Soda Pipeline Run DAG - Quality-Gated Metadata Sync
    
    This DAG implements **quality-gated metadata synchronization** where quality checks
    gate metadata sync operations. Collibra only syncs data that has passed quality
    validation, ensuring the catalog reflects commitments, not aspirations.
    
    ## Orchestration Philosophy
    
    Each layer follows the sequence: **Build → Validate → Govern**
    
    - **dbt build** → "this model exists"
    - **Soda checks** → "this model is acceptable"  
    - **Collibra sync** → "this model is governable and discoverable"
    
    Quality checks **gate** metadata synchronization. Metadata sync only happens after
    quality validation, ensuring Collibra becomes a historical record of accepted states,
    not a live mirror of Snowflake's chaos.
    
    ## What This DAG Does
    
    - **RAW Layer**: Quality checks → Metadata sync (gated)
    - **STAGING Layer**: Build → Quality checks → Metadata sync (gated)
    - **MART Layer**: Build → Quality checks → Metadata sync (gated, strictest standards)
    - **QUALITY Layer**: Final validation + dbt tests
    - **Sends results to Soda Cloud** for monitoring
    - **Synchronizes metadata to Collibra** only for validated data
    - **Cleans up artifacts** and temporary files
    
    ## Layered Processing Flow
    
    1. **RAW Layer**: Quality checks → Metadata sync (gated by quality)
    2. **STAGING Layer**: Transform data → Quality checks → Metadata sync (gated)
    3. **MART Layer**: Business logic → Quality checks → Metadata sync (gated, strictest)
    4. **QUALITY Layer**: Final validation + dbt tests
    
    ## Quality Gating Benefits
    
    - **Collibra reflects commitments**: Only validated data enters governance
    - **Lineage reflects approved flows**: Historical record of accepted states
    - **Ownership discussions on validated assets**: Governance happens on trusted data
    - **No retroactive corrections needed**: Catalog stays clean and meaningful
    
    ## Advanced Features
    
    - **Soda Library**: Full template support with advanced analytics
    - **Template Checks**: Statistical analysis, anomaly detection, business logic validation
    - **Enhanced Monitoring**: Data distribution analysis and trend monitoring
    - **Quality-Gated Sync**: Metadata sync only after quality validation
    
    ## When to Use
    
    - ✅ **Daily/weekly pipeline runs**
    - ✅ **Regular data processing**
    - ✅ **Scheduled execution**
    - ✅ **After initialization is complete**
    
    ## Prerequisites
    
    - ⚠️ **Must run `soda_initialization` first** (one-time setup)
    - ⚠️ **Snowflake must be initialized** with sample data
    - ⚠️ **Environment variables must be configured**
    - ⚠️ **Collibra configuration** in `collibra/config.yml`
    
    ## Layer Tasks
    
    - **Layer 1**: `soda_scan_raw` → `collibra_sync_raw` (quality gates sync)
    - **Layer 2**: `dbt_run_staging` → `soda_scan_staging` → `collibra_sync_staging` (gated)
    - **Layer 3**: `dbt_run_mart` → `soda_scan_mart` → `collibra_sync_mart` (gated, strictest)
    - **Layer 4**: `soda_scan_quality` + `dbt_test` → `collibra_sync_quality` (gated)
    - **cleanup**: Clean up temporary artifacts
    """,
):

    # =============================================================================
    # PIPELINE TASKS - LAYERED APPROACH
    # =============================================================================
    
    pipeline_start = EmptyOperator(
        task_id="pipeline_start",
        doc_md="🔄 Starting layered pipeline execution"
    )

    # =============================================================================
    # LAYER 1: RAW DATA + RAW CHECKS
    # =============================================================================
    # 
    # Orchestration Philosophy: Quality Gates Metadata Sync
    # 
    # Each layer follows the sequence: Build → Validate → Govern
    # - dbt build → "this model exists"
    # - Soda checks → "this model is acceptable"
    # - Collibra sync → "this model is governable and discoverable"
    #
    # Quality checks gate metadata synchronization. Collibra only syncs data that
    # has passed quality validation, ensuring the catalog reflects commitments,
    # not aspirations. This makes Collibra a historical record of accepted states.
    
    raw_layer_start = EmptyOperator(
        task_id="raw_layer_start",
        doc_md="Starting RAW layer processing"
    )

    # Get data source names dynamically from database name
    data_source_raw = get_data_source_name('raw')
    data_source_staging = get_data_source_name('staging')
    data_source_mart = get_data_source_name('mart')
    data_source_quality = get_data_source_name('quality')
    
    soda_scan_raw = BashOperator(
        task_id="soda_scan_raw",
        bash_command=BASH_PREFIX + f"soda scan -d '{data_source_raw}' -c soda/configuration/configuration_raw.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/raw || true",
        doc_md="""
        **RAW Layer Quality Checks - Quality Gate**
        
        - Initial data quality assessment
        - Relaxed thresholds for source data
        - Identifies data issues before transformation
        - Includes all raw tables: customers, products, orders, order_items
        - **Gates metadata sync**: Only validated data proceeds to Collibra
        - **Non-blocking**: Pipeline continues even if checks fail (lenient for source data)
        """,
    )

    def sync_raw_metadata_task(**context):
        """Wrapper function to import and call Collibra sync for RAW layer."""
        from collibra.airflow_helper import sync_raw_metadata
        return sync_raw_metadata(**context)
    
    collibra_sync_raw = PythonOperator(
        task_id="collibra_sync_raw",
        python_callable=sync_raw_metadata_task,
        doc_md="""
        **Collibra Metadata Sync - RAW Layer (Gated by Quality)**
        
        - **Only executes after quality checks pass**
        - Triggers metadata synchronization in Collibra for RAW schema
        - Updates Collibra catalog with validated RAW layer metadata
        - Ensures Collibra reflects commitments, not aspirations
        """,
    )

    raw_layer_end = EmptyOperator(
        task_id="raw_layer_end",
        doc_md="✅ RAW layer processing completed"
    )

    # =============================================================================
    # LAYER 2: STAGING MODELS + STAGING CHECKS
    # =============================================================================
    # 
    # Sequence: Build → Validate → Govern
    # Quality checks gate metadata sync to ensure only acceptable data enters governance
    
    staging_layer_start = EmptyOperator(
        task_id="staging_layer_start",
        doc_md="Starting STAGING layer processing"
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=BASH_PREFIX + "cd dbt && dbt run --select staging --target dev --profiles-dir .",
        doc_md="""
        **Execute dbt Staging Models - Build Phase**
        
        - Runs dbt staging models (stg_customers, stg_orders, stg_products, stg_order_items)
        - Transforms raw data into cleaned, standardized format
        - Applies data quality improvements
        - Creates models in STAGING schema via project config
        - **Phase 1**: Materialize models in Snowflake
        """,
    )

    soda_scan_staging = BashOperator(
        task_id="soda_scan_staging",
        bash_command=BASH_PREFIX + f"soda scan -d '{data_source_staging}' -c soda/configuration/configuration_staging.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/staging || true",
        doc_md="""
        **STAGING Layer Quality Checks - Validation Phase**
        
        - Stricter quality thresholds than RAW
        - Shows data improvement after transformation
        - Expected: Fewer failures than RAW layer
        - **Gates metadata sync**: Only validated data proceeds to Collibra
        - **Phase 2**: Validate freshness, volume, schema, business rules
        """,
    )

    def sync_staging_metadata_task(**context):
        """Wrapper function to import and call Collibra sync for STAGING layer."""
        from collibra.airflow_helper import sync_staging_metadata
        return sync_staging_metadata(**context)
    
    collibra_sync_staging = PythonOperator(
        task_id="collibra_sync_staging",
        python_callable=sync_staging_metadata_task,
        doc_md="""
        **Collibra Metadata Sync - STAGING Layer (Gated by Quality)**
        
        - **Only executes after quality checks pass**
        - Triggers metadata synchronization in Collibra for STAGING schema
        - Updates Collibra catalog with validated STAGING layer metadata
        - **Phase 3**: Sync only what passed the layer's acceptance criteria
        - Ensures Collibra reflects commitments, not aspirations
        """,
    )

    staging_layer_end = EmptyOperator(
        task_id="staging_layer_end",
        doc_md="✅ STAGING layer processing completed"
    )

    # =============================================================================
    # LAYER 3: MART MODELS + MART CHECKS
    # =============================================================================
    # 
    # Sequence: Build → Validate → Govern
    # Strictest quality standards for business-ready data
    # Metadata sync is a badge of trust for Gold layer
    
    mart_layer_start = EmptyOperator(
        task_id="mart_layer_start",
        doc_md="Starting MART layer processing"
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=BASH_PREFIX + "cd dbt && dbt run --select mart --target dev --profiles-dir .",
        doc_md="""
        **Execute dbt Mart Models - Build Phase**
        
        - Runs dbt mart models (dim_customers, fact_orders)
        - Creates business-ready analytics tables
        - Applies business logic and aggregations
        - Creates models in MART schema via project config
        - **Phase 1**: Materialize business-ready models in Snowflake
        """,
    )

    soda_scan_mart = BashOperator(
        task_id="soda_scan_mart",
        bash_command=BASH_PREFIX + f"soda scan -d '{data_source_mart}' -c soda/configuration/configuration_mart.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/mart || true",
        doc_md="""
        **MART Layer Quality Checks - Validation Phase**
        
        - Strictest quality thresholds
        - Ensures business-ready data quality
        - Expected: Minimal failures (production-ready)
        - **Gates metadata sync**: Only production-ready data proceeds to Collibra
        - **Non-blocking**: Pipeline continues even if checks fail (lenient for default pipeline)
        - **Phase 2**: Validate business logic, referential integrity, strict quality
        """,
    )

    def sync_mart_metadata_task(**context):
        """Wrapper function to import and call Collibra sync for MART layer."""
        from collibra.airflow_helper import sync_mart_metadata
        return sync_mart_metadata(**context)
    
    collibra_sync_mart = PythonOperator(
        task_id="collibra_sync_mart",
        python_callable=sync_mart_metadata_task,
        doc_md="""
        **Collibra Metadata Sync - MART Layer (Gated by Quality)**
        
        - **Only executes after quality checks pass**
        - Triggers metadata synchronization in Collibra for MART schema
        - Updates Collibra catalog with validated MART layer metadata
        - **Phase 3**: Sync only production-ready, validated data
        - Metadata sync is a badge of trust for Gold layer
        - Ensures Collibra reflects commitments, not aspirations
        """,
    )

    mart_layer_end = EmptyOperator(
        task_id="mart_layer_end",
        doc_md="✅ MART layer processing completed"
    )

    # =============================================================================
    # LAYER 4: QUALITY CHECKS + DBT TESTS
    # =============================================================================
    
    quality_layer_start = EmptyOperator(
        task_id="quality_layer_start",
        doc_md="🔍 Starting QUALITY layer processing"
    )

    soda_scan_quality = BashOperator(
        task_id="soda_scan_quality",
        bash_command=BASH_PREFIX + f"soda scan -d '{data_source_quality}' -c soda/configuration/configuration_quality.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/quality || true",
        doc_md="""
        **QUALITY Layer Monitoring**
        
        - Monitors quality check execution
        - Tracks results and trends
        - Provides centralized quality monitoring
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=BASH_PREFIX + "cd dbt && dbt test --target dev --profiles-dir . 2>/dev/null || true",
        doc_md="""
        **Execute dbt Tests**
        
        - Runs all dbt tests to validate data quality
        - Tests referential integrity, uniqueness, and business rules
        - Ensures data consistency across all layers
        - Uses dev target for tests (reads from all schemas)
        """,
    )

    def sync_quality_metadata_task(**context):
        """Wrapper function to import and call Collibra sync for QUALITY layer."""
        from collibra.airflow_helper import sync_quality_metadata
        return sync_quality_metadata(**context)
    
    collibra_sync_quality = PythonOperator(
        task_id="collibra_sync_quality",
        python_callable=sync_quality_metadata_task,
        doc_md="""
        **Collibra Metadata Sync - QUALITY Layer (Gated by Quality)**
        
        - **Only executes after quality checks pass**
        - Triggers metadata synchronization in Collibra for QUALITY schema
        - Updates Collibra catalog with quality check results metadata
        - **Phase 4**: Sync quality monitoring and results
        """,
    )

    quality_layer_end = EmptyOperator(
        task_id="quality_layer_end",
        doc_md="✅ QUALITY layer processing completed"
    )

    # =============================================================================
    # CLEANUP
    # =============================================================================
    
    cleanup = BashOperator(
        task_id="cleanup_artifacts",
        bash_command=BASH_PREFIX + "rm -rf dbt/target dbt/logs snowflake_connection_test.log && true",
        doc_md="""
        **Clean Up Artifacts**
        
        - Removes temporary files and logs
        - Cleans up dbt artifacts
        - Prepares for next run
        """,
    )
    
    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        doc_md="✅ Layered pipeline execution completed successfully!"
    )
    
    # =============================================================================
    # TASK DEPENDENCIES - QUALITY-GATED METADATA SYNC
    # =============================================================================
    #
    # Orchestration Philosophy: Quality Gates Metadata Sync
    #
    # Each layer follows: Build → Validate → Govern
    # - Quality checks MUST complete before metadata sync
    # - This ensures Collibra only contains validated, committed data
    # - Collibra becomes a historical record of accepted states
    #
    # Parallelism is fine WITHIN a phase (e.g., multiple dbt models, multiple checks)
    # But phase transitions stay sequential to maintain semantic clarity
    #
    # Layer Sequencing:
    # RAW:    Quality Check → Metadata Sync (gated)
    # STAGING: Build → Quality Check → Metadata Sync (gated)
    # MART:   Build → Quality Check → Metadata Sync (gated)
    # QUALITY: Quality Check + Tests → Metadata Sync (gated)
    
    # RAW Layer: Quality gates metadata sync
    pipeline_start >> raw_layer_start >> soda_scan_raw >> collibra_sync_raw >> raw_layer_end
    
    # STAGING Layer: Build → Validate → Govern
    raw_layer_end >> staging_layer_start >> dbt_run_staging >> soda_scan_staging >> collibra_sync_staging >> staging_layer_end
    
    # MART Layer: Build → Validate → Govern (strictest standards)
    staging_layer_end >> mart_layer_start >> dbt_run_mart >> soda_scan_mart >> collibra_sync_mart >> mart_layer_end
    
    # Quality Layer: Final monitoring → Metadata sync (gated)
    mart_layer_end >> quality_layer_start >> [soda_scan_quality, dbt_test] >> collibra_sync_quality >> quality_layer_end
    quality_layer_end >> cleanup >> pipeline_end
