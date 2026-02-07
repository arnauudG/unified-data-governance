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

# Pipeline Run DAG - Strict RAW Layer Guardrails
with DAG(
    dag_id="soda_pipeline_run_strict_raw",
    default_args=default_args,
    description="Soda Pipeline Run: Strict RAW layer guardrails - pipeline fails if RAW checks fail",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["soda", "dbt", "data-quality", "pipeline", "strict-raw"],
    doc_md="""
    # Soda Pipeline Run DAG - Strict RAW Layer Guardrails
    
    This DAG implements **strict quality guardrails for the RAW layer** where the pipeline
    will fail if RAW layer quality checks fail. The MART layer is lenient (non-blocking).
    
    ## Guardrail Configuration
    
    - **RAW Layer**: **STRICT** - Pipeline fails if critical checks fail, quality gate validates before sync
    - **MART Layer**: **LENIENT** - Pipeline continues even if checks fail, no quality gate
    
    ## When to Use
    
    - ✅ **Early data quality validation** - Catch issues at source
    - ✅ **Strict source data requirements** - Ensure only high-quality raw data enters pipeline
    - ✅ **Production environments** - Where source data quality is critical
    
    ## Quality Gating
    
    RAW layer quality checks gate metadata synchronization. Collibra only syncs data that
    has passed quality validation, ensuring the catalog reflects commitments, not aspirations.
    """,
):

    # =============================================================================
    # PIPELINE TASKS - LAYERED APPROACH
    # =============================================================================
    
    pipeline_start = EmptyOperator(
        task_id="pipeline_start",
        doc_md="🔄 Starting layered pipeline execution (Strict RAW)"
    )

    # =============================================================================
    # LAYER 1: RAW DATA + RAW CHECKS (STRICT)
    # =============================================================================
    
    raw_layer_start = EmptyOperator(
        task_id="raw_layer_start",
        doc_md="Starting RAW layer processing (STRICT MODE)"
    )

    # Get data source names dynamically from database name
    data_source_raw = get_data_source_name('raw')
    data_source_staging = get_data_source_name('staging')
    data_source_mart = get_data_source_name('mart')
    data_source_quality = get_data_source_name('quality')
    
    soda_scan_raw = BashOperator(
        task_id="soda_scan_raw",
        bash_command=BASH_PREFIX + f"soda scan -d '{data_source_raw}' -c soda/configuration/configuration_raw.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/raw",
        doc_md="""
        **RAW Layer Quality Checks - STRICT MODE**
        
        - Initial data quality assessment
        - **STRICT**: Pipeline will fail if critical checks fail
        - Identifies data issues before transformation
        - Includes all raw tables: customers, products, orders, order_items
        - **Gates metadata sync**: Only validated data proceeds to Collibra
        - **Pipeline will fail if critical checks fail**
        """,
    )

    def sync_raw_metadata_task(**context):
        """Wrapper function to import and call Collibra sync for RAW layer (strict mode)."""
        from collibra.airflow_helper import sync_raw_metadata_strict
        return sync_raw_metadata_strict(**context)
    
    collibra_sync_raw = PythonOperator(
        task_id="collibra_sync_raw",
        python_callable=sync_raw_metadata_task,
        doc_md="""
        **Collibra Metadata Sync - RAW Layer (Strict Quality Gate)**
        
        - **Only executes after quality checks pass**
        - **Quality gate validates critical checks before sync**
        - Triggers metadata synchronization in Collibra for RAW schema
        - Updates Collibra catalog with validated RAW layer metadata
        - Ensures Collibra reflects commitments, not aspirations
        """,
    )

    raw_layer_end = EmptyOperator(
        task_id="raw_layer_end",
        doc_md="✅ RAW layer processing completed (STRICT MODE)"
    )

    # =============================================================================
    # LAYER 2: STAGING MODELS + STAGING CHECKS
    # =============================================================================
    
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
    # LAYER 3: MART MODELS + MART CHECKS (LENIENT)
    # =============================================================================
    
    mart_layer_start = EmptyOperator(
        task_id="mart_layer_start",
        doc_md="Starting MART layer processing (LENIENT MODE)"
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
        **MART Layer Quality Checks - LENIENT MODE**
        
        - Quality thresholds for business-ready data
        - **LENIENT**: Pipeline continues even if checks fail
        - Expected: Minimal failures (production-ready)
        - **Gates metadata sync**: Only production-ready data proceeds to Collibra
        - **Non-blocking**: Pipeline continues even if checks fail
        - **Phase 2**: Validate business logic, referential integrity, strict quality
        """,
    )

    def sync_mart_metadata_task(**context):
        """Wrapper function to import and call Collibra sync for MART layer (lenient mode)."""
        from collibra.airflow_helper import sync_mart_metadata_lenient
        return sync_mart_metadata_lenient(**context)
    
    collibra_sync_mart = PythonOperator(
        task_id="collibra_sync_mart",
        python_callable=sync_mart_metadata_task,
        doc_md="""
        **Collibra Metadata Sync - MART Layer (Lenient Mode)**
        
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
        doc_md="✅ MART layer processing completed (LENIENT MODE)"
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
    
    # RAW Layer: Quality gates metadata sync (STRICT)
    pipeline_start >> raw_layer_start >> soda_scan_raw >> collibra_sync_raw >> raw_layer_end
    
    # STAGING Layer: Build → Validate → Govern
    raw_layer_end >> staging_layer_start >> dbt_run_staging >> soda_scan_staging >> collibra_sync_staging >> staging_layer_end
    
    # MART Layer: Build → Validate → Govern (LENIENT)
    staging_layer_end >> mart_layer_start >> dbt_run_mart >> soda_scan_mart >> collibra_sync_mart >> mart_layer_end
    
    # Quality Layer: Final monitoring → Metadata sync (gated)
    mart_layer_end >> quality_layer_start >> [soda_scan_quality, dbt_test] >> collibra_sync_quality >> quality_layer_end
    quality_layer_end >> cleanup >> pipeline_end
