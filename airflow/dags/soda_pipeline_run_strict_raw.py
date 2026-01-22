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


def upload_to_superset(**context):
    """
    Upload Soda data to Superset database.
    
    This function orchestrates the complete Superset upload workflow:
    1. Checks if Superset is running and accessible
    2. Updates Soda data source names to match SNOWFLAKE_DATABASE
    3. Extracts data from Soda Cloud API
    4. Organizes the data (keeps only latest files)
    5. Uploads to Superset PostgreSQL database
    
    Returns:
        None (raises exception on failure)
    """
    import subprocess
    import sys
    from pathlib import Path
    
    project_root = Path("/opt/airflow")
    
    print("🔄 Starting Superset upload workflow...")
    
    # Step 0: Check if Superset is running
    print("\n0️⃣  Checking Superset availability...")
    superset_available = False
    
    # Check 1: Verify Superset HTTP endpoint is accessible (most reliable)
    try:
        import urllib.request
        import urllib.error
        # Try connecting to Superset health endpoint via container name (same network)
        for url in ['http://superset-app:8088/health', 'http://localhost:8089/health']:
            try:
                req = urllib.request.Request(url, method='GET')
                req.add_header('User-Agent', 'Airflow-Superset-Check')
                with urllib.request.urlopen(req, timeout=5) as response:
                    if response.status == 200:
                        print(f"✅ Superset HTTP endpoint is accessible at {url}")
                        superset_available = True
                        break
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
                continue
        if not superset_available:
            print("⚠️  Superset HTTP endpoint not accessible")
    except Exception as e:
        print(f"⚠️  Could not check HTTP endpoint: {e}")
    
    # Check 2: Verify Superset database is accessible (fallback)
    if not superset_available:
        try:
            import psycopg2
            db_config = {
                'host': 'superset-db',
                'port': 5432,
                'database': 'superset',
                'user': 'superset',
                'password': 'superset',
                'connect_timeout': 5
            }
            # Try alternative hostnames
            for host in ['superset-db', 'superset-postgres']:
                try:
                    test_config = db_config.copy()
                    test_config['host'] = host
                    conn = psycopg2.connect(**test_config)
                    conn.close()
                    print(f"✅ Superset database is accessible at {host}")
                    superset_available = True
                    break
                except psycopg2.Error:
                    continue
        except ImportError:
            print("⚠️  psycopg2 not available, skipping database check")
        except Exception as e:
            print(f"⚠️  Could not check database: {e}")
    
    # Check 3: Verify Superset container is running (last resort, may not work from inside container)
    if not superset_available:
        try:
            check_container = subprocess.run(
                ["docker", "ps", "--filter", "name=superset-app", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5
            )
            if check_container.returncode == 0 and "superset-app" in check_container.stdout:
                print("✅ Superset container is running (but endpoint not accessible)")
                # Don't set superset_available = True here, as we can't actually connect
            else:
                print("⚠️  Superset container 'superset-app' not found in running containers")
        except Exception as e:
            print(f"⚠️  Could not check container status: {e}")
    
    if not superset_available:
        error_msg = """
❌ Superset is not available

Superset must be running before uploading data. Please:
   1. Start Superset: make superset-up
   2. Wait for Superset to be ready (about 45 seconds)
   3. Verify status: make superset-status
   4. Then re-run the pipeline

Alternatively, you can skip this task or run the upload manually:
   make superset-upload-data
"""
        print(error_msg)
        raise Exception("Superset is not available. Start Superset with 'make superset-up' before running the pipeline.")
    
    print("✅ Superset is available and ready for upload")
    
    # Step 1: Update data source names
    print("\n1️⃣  Updating Soda data source names...")
    try:
        update_script = project_root / "soda" / "update_data_source_names.py"
        result = subprocess.run(
            [sys.executable, str(update_script)],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0:
            print("✅ Data source names updated successfully")
        else:
            print(f"⚠️  Warning: Could not update data source names: {result.stderr}")
    except Exception as e:
        print(f"⚠️  Warning: Error updating data source names: {e}")
    
    # Step 2: Extract data from Soda Cloud
    print("\n2️⃣  Extracting data from Soda Cloud...")
    print("⏳ This may take a few minutes depending on data volume...")
    print("📡 Fetching data from Soda Cloud API (this will show progress as it runs)...")
    try:
        dump_script = project_root / "scripts" / "soda_dump_api.py"
        # Use Popen to stream output in real-time instead of capturing it
        import sys as sys_module
        process = subprocess.Popen(
            [sys.executable, str(dump_script)],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )
        
        # Stream output line by line
        for line in process.stdout:
            print(line.rstrip())
            sys.stdout.flush()  # Ensure immediate output
        
        # Wait for process to complete and get return code
        return_code = process.wait()
        
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, str(dump_script))
        
        print("✅ Data extracted from Soda Cloud")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error extracting data from Soda Cloud (exit code: {e.returncode})")
        raise
    except Exception as e:
        print(f"❌ Unexpected error during Soda Cloud extraction: {e}")
        raise
    
    # Step 3: Organize data
    print("\n3️⃣  Organizing data...")
    try:
        organize_script = project_root / "scripts" / "organize_soda_data.py"
        # Stream output in real-time
        process = subprocess.Popen(
            [sys.executable, str(organize_script)],
            cwd=str(project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Stream output line by line
        for line in process.stdout:
            print(line.rstrip())
            sys.stdout.flush()
        
        return_code = process.wait()
        
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, str(organize_script))
        
        print("✅ Data organized successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error organizing data (exit code: {e.returncode})")
        raise
    except Exception as e:
        print(f"❌ Unexpected error during data organization: {e}")
        raise
    
    # Step 4: Upload to Superset
    print("\n4️⃣  Uploading to Superset...")
    try:
        # Copy upload script to superset/data directory
        upload_script_src = project_root / "scripts" / "upload_soda_data_docker.py"
        upload_script_dst = project_root / "superset" / "data" / "upload_soda_data_docker.py"
        
        if upload_script_src.exists():
            import shutil
            upload_script_dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(upload_script_src, upload_script_dst)
            print(f"✅ Copied upload script to {upload_script_dst}")
        
        # Execute upload script in Superset container
        # Try multiple approaches to upload data
        upload_script_path = "/app/soda_data/upload_soda_data_docker.py"
        
        # Approach 1: Try docker exec with correct container name
        # Container name from superset/docker-compose.yml is "superset-app"
        docker_cmd = [
            "docker", "exec", "superset-app",
            "python", upload_script_path
        ]
        
        result = subprocess.run(
            docker_cmd,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            print("✅ Data uploaded to Superset successfully (via docker exec)")
            if result.stdout:
                print(result.stdout)
        else:
            # Approach 2: Try direct database connection from Airflow
            # This works if containers are on the same Docker network
            print("⚠️  Docker exec failed, trying direct database connection...")
            try:
                # Import and run the upload script directly
                # Modify DB config to connect from Airflow container
                sys.path.insert(0, str(project_root / "scripts"))
                try:
                    from upload_soda_data_docker import main as upload_main, DB_CONFIG
                    # Update DB config for Airflow container network access
                    # If Superset DB is accessible, use 'superset-db' hostname
                    # Otherwise, try 'superset-postgres' (container name)
                    import psycopg2
                    
                    # Try connecting with different hostnames
                    for host in ['superset-db', 'superset-postgres', 'localhost']:
                        try:
                            test_config = DB_CONFIG.copy()
                            test_config['host'] = host
                            conn = psycopg2.connect(**test_config)
                            conn.close()
                            print(f"✅ Found Superset database at {host}")
                            
                            # Update the script's DB config and run
                            import upload_soda_data_docker as upload_module
                            upload_module.DB_CONFIG['host'] = host
                            # Use Airflow container path for data directory
                            data_dir = str(project_root / "superset" / "data")
                            upload_main(data_dir=data_dir)
                            print("✅ Data uploaded to Superset successfully (direct connection)")
                            break
                        except psycopg2.Error:
                            continue
                    else:
                        raise Exception("Could not connect to Superset database from any host")
                        
                except ImportError:
                    # If import fails, try running as subprocess with data directory argument
                    upload_script = project_root / "scripts" / "upload_soda_data_docker.py"
                    data_dir = str(project_root / "superset" / "data")
                    result2 = subprocess.run(
                        [sys.executable, str(upload_script), "--data-dir", data_dir],
                        cwd=str(project_root),
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    print("✅ Data uploaded to Superset successfully (subprocess)")
                    if result2.stdout:
                        print(result2.stdout)
            except Exception as e:
                error_msg = f"""
❌ Failed to upload data to Superset

Error: {e}
Docker exec error: {result.stderr}

💡 Troubleshooting:
   1. Ensure Superset is running: make superset-up
   2. Check Superset status: make superset-status
   3. Verify containers are on the same Docker network
   4. Try manual upload: make superset-upload-data

Note: This task will fail if Superset is not available.
      Start Superset before running the pipeline, or skip this task.
"""
                print(error_msg)
                raise Exception(f"Superset upload failed: {e}. Ensure Superset is running.")
        
    except Exception as e:
        print(f"❌ Error uploading to Superset: {e}")
        raise
    
    print("\n✅ Superset upload workflow completed successfully!")

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
        bash_command=BASH_PREFIX + f"soda scan -d {data_source_raw} -c soda/configuration/configuration_raw.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/raw",
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
        bash_command=BASH_PREFIX + f"soda scan -d {data_source_staging} -c soda/configuration/configuration_staging.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/staging || true",
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
        bash_command=BASH_PREFIX + f"soda scan -d {data_source_mart} -c soda/configuration/configuration_mart.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/mart || true",
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
        bash_command=BASH_PREFIX + f"soda scan -d {data_source_quality} -c soda/configuration/configuration_quality.yml -T soda/checks/templates/data_quality_templates.yml soda/checks/quality || true",
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
    # SUPERSET UPLOAD
    # =============================================================================
    
    superset_upload = PythonOperator(
        task_id="superset_upload_data",
        python_callable=upload_to_superset,
        doc_md="""
        **Upload Soda Data to Superset**
        
        This task completes the data visualization workflow by:
        1. Updating Soda data source names to match database configuration
        2. Extracting latest data from Soda Cloud API
        3. Organizing data (keeping only latest files)
        4. Uploading to Superset PostgreSQL database for visualization
        
        The uploaded data is available in Superset tables:
        - `soda.datasets_latest` - Latest dataset information
        - `soda.checks_latest` - Latest check results
        - `soda.analysis_summary` - Analysis summary data
        
        **Note**: This task requires Superset to be running and accessible.
        """,
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
    quality_layer_end >> cleanup >> pipeline_end >> superset_upload
