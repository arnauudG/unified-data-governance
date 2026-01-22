#!/usr/bin/env python3
"""
Airflow Helper Functions for Collibra Metadata Synchronization

This module provides Python functions that can be used as Airflow PythonOperators
to trigger Collibra metadata synchronization.
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Configure logging first
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables explicitly for Airflow
# In Airflow container, .env is mounted at /opt/airflow/.env
# Also check if variables are already set by Docker Compose env_file
env_path = Path('/opt/airflow/.env')
if env_path.exists():
    load_dotenv(env_path, override=True)
    logger.info(f"Loaded environment variables from {env_path}")
elif os.getenv('COLLIBRA_BASE_URL'):
    # Variables already set by Docker Compose env_file
    logger.info("Using environment variables from Docker Compose")
else:
    # Fallback to default behavior
    load_dotenv(override=True)
    logger.info("Using default dotenv behavior for environment variables")

# Debug: Log which Collibra variables are available (without showing values)
collibra_vars = {
    'COLLIBRA_BASE_URL': 'SET' if os.getenv('COLLIBRA_BASE_URL') else 'NOT SET',
    'COLLIBRA_USERNAME': 'SET' if os.getenv('COLLIBRA_USERNAME') else 'NOT SET',
    'COLLIBRA_PASSWORD': 'SET' if os.getenv('COLLIBRA_PASSWORD') else 'NOT SET',
}
logger.info(f"Collibra environment variables status: {collibra_vars}")

from collibra.metadata_sync import CollibraMetadataSync
from collibra.soda_quality_check import validate_quality_before_sync


def load_config():
    """Load Collibra configuration from config.yml."""
    config_path = PROJECT_ROOT / "collibra" / "config.yml"
    
    if not config_path.exists():
        raise FileNotFoundError(
            f"Collibra config file not found at {config_path}. "
            "Please create collibra/config.yml with your database and schema IDs."
        )
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def get_database_connection_id(config: dict, sync_client: CollibraMetadataSync) -> Optional[str]:
    """Get database connection ID from config or resolve it."""
    # Check if explicitly provided in config
    if 'database_connection_id' in config and config['database_connection_id']:
        return config['database_connection_id']
    
    # Otherwise, resolve from database asset ID
    database_id = config.get('database_id')
    if not database_id:
        return None
    
    try:
        return sync_client.get_database_connection_id(database_id)
    except Exception as e:
        logger.warning(f"Could not resolve database connection ID: {e}")
        return None


def sync_raw_metadata(**context):
    """Airflow task function to sync RAW layer metadata (lenient mode)."""
    logger.info("Starting Collibra metadata sync for RAW layer (lenient mode)")
    
    # Note: RAW layer is lenient - no quality gate validation
    # Quality checks run but don't block sync (|| true in scan task)
    
    return _sync_raw_metadata_internal(**context)


def sync_raw_metadata_strict(**context):
    """Airflow task function to sync RAW layer metadata (strict mode with quality gate)."""
    logger.info("Starting Collibra metadata sync for RAW layer (strict mode)")
    
    # Validate quality before syncing (strict mode)
    if not validate_quality_before_sync('raw'):
        error_msg = "Quality gate failed: Critical checks failed in RAW layer. Skipping metadata sync."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    return _sync_raw_metadata_internal(**context)


def _sync_raw_metadata_internal(**context):
    """Internal function to sync RAW layer metadata."""
    # Verify environment variables are loaded
    collibra_vars_check = {
        'COLLIBRA_BASE_URL': 'SET' if os.getenv('COLLIBRA_BASE_URL') else 'NOT SET',
        'COLLIBRA_USERNAME': 'SET' if os.getenv('COLLIBRA_USERNAME') else 'NOT SET',
        'COLLIBRA_PASSWORD': 'SET' if os.getenv('COLLIBRA_PASSWORD') else 'NOT SET',
    }
    logger.info(f"Environment variables check: {collibra_vars_check}")
    
    try:
        config = load_config()
        database_id = config['database_id']
        # Config contains schema asset IDs, not connection IDs
        schema_asset_ids = config.get('raw', {}).get('schema_connection_ids', [])
        
        if not schema_asset_ids:
            logger.warning("No schema asset IDs configured for RAW layer. Skipping sync.")
            return
        
        sync_client = CollibraMetadataSync()
        
        # Resolve schema asset IDs to connection IDs
        database_connection_id = get_database_connection_id(config, sync_client)
        schema_connection_ids = sync_client.resolve_schema_connection_ids(
            database_id=database_id,
            schema_asset_ids=schema_asset_ids,
            database_connection_id=database_connection_id
        )
        
        result = sync_client.trigger_metadata_sync(
            database_id=database_id,
            schema_connection_ids=schema_connection_ids  # Use resolved connection IDs
        )
        
        logger.info(f"RAW layer metadata sync triggered: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to sync RAW layer metadata: {e}")
        raise


def sync_staging_metadata(**context):
    """Airflow task function to sync STAGING layer metadata."""
    logger.info("Starting Collibra metadata sync for STAGING layer")
    
    try:
        config = load_config()
        database_id = config['database_id']
        # Config contains schema asset IDs, not connection IDs
        schema_asset_ids = config.get('staging', {}).get('schema_connection_ids', [])
        
        if not schema_asset_ids:
            logger.warning("No schema asset IDs configured for STAGING layer. Skipping sync.")
            return
        
        sync_client = CollibraMetadataSync()
        
        # Resolve schema asset IDs to connection IDs
        database_connection_id = get_database_connection_id(config, sync_client)
        schema_connection_ids = sync_client.resolve_schema_connection_ids(
            database_id=database_id,
            schema_asset_ids=schema_asset_ids,
            database_connection_id=database_connection_id
        )
        
        result = sync_client.trigger_metadata_sync(
            database_id=database_id,
            schema_connection_ids=schema_connection_ids  # Use resolved connection IDs
        )
        
        logger.info(f"STAGING layer metadata sync triggered: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to sync STAGING layer metadata: {e}")
        raise


def sync_mart_metadata(**context):
    """Airflow task function to sync MART layer metadata (strict mode with quality gate)."""
    logger.info("Starting Collibra metadata sync for MART layer (strict mode)")
    
    # Validate quality before syncing (strict mode)
    if not validate_quality_before_sync('mart'):
        error_msg = "Quality gate failed: Critical checks failed in MART layer. Skipping metadata sync."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    return _sync_mart_metadata_internal(**context)


def sync_mart_metadata_lenient(**context):
    """Airflow task function to sync MART layer metadata (lenient mode)."""
    logger.info("Starting Collibra metadata sync for MART layer (lenient mode)")
    
    # Note: MART layer is lenient - no quality gate validation
    # Quality checks run but don't block sync (|| true in scan task)
    
    return _sync_mart_metadata_internal(**context)


def _sync_mart_metadata_internal(**context):
    """Internal function to sync MART layer metadata."""
    try:
        config = load_config()
        database_id = config['database_id']
        # Config contains schema asset IDs, not connection IDs
        schema_asset_ids = config.get('mart', {}).get('schema_connection_ids', [])
        
        if not schema_asset_ids:
            logger.warning("No schema asset IDs configured for MART layer. Skipping sync.")
            return
        
        sync_client = CollibraMetadataSync()
        
        # Resolve schema asset IDs to connection IDs
        database_connection_id = get_database_connection_id(config, sync_client)
        schema_connection_ids = sync_client.resolve_schema_connection_ids(
            database_id=database_id,
            schema_asset_ids=schema_asset_ids,
            database_connection_id=database_connection_id
        )
        
        result = sync_client.trigger_metadata_sync(
            database_id=database_id,
            schema_connection_ids=schema_connection_ids  # Use resolved connection IDs
        )
        
        logger.info(f"MART layer metadata sync triggered: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to sync MART layer metadata: {e}")
        raise


def sync_quality_metadata(**context):
    """Airflow task function to sync QUALITY layer metadata."""
    logger.info("Starting Collibra metadata sync for QUALITY layer")
    
    try:
        config = load_config()
        database_id = config['database_id']
        # Config contains schema asset IDs, not connection IDs
        schema_asset_ids = config.get('quality', {}).get('schema_connection_ids', [])
        
        if not schema_asset_ids:
            logger.warning("No schema asset IDs configured for QUALITY layer. Skipping sync.")
            return
        
        sync_client = CollibraMetadataSync()
        
        # Resolve schema asset IDs to connection IDs
        database_connection_id = get_database_connection_id(config, sync_client)
        schema_connection_ids = sync_client.resolve_schema_connection_ids(
            database_id=database_id,
            schema_asset_ids=schema_asset_ids,
            database_connection_id=database_connection_id
        )
        
        result = sync_client.trigger_metadata_sync(
            database_id=database_id,
            schema_connection_ids=schema_connection_ids  # Use resolved connection IDs
        )
        
        logger.info(f"QUALITY layer metadata sync triggered: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Failed to sync QUALITY layer metadata: {e}")
        raise

