#!/usr/bin/env python3
"""
Airflow Helper Functions for Collibra Metadata Synchronization

This module provides Python functions that can be used as Airflow PythonOperators
to trigger Collibra metadata synchronization.

Refactored to use Service Layer pattern and centralized configuration.
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.logging import get_logger
from src.factories import ServiceFactory
from collibra.soda_quality_check import validate_quality_before_sync

logger = get_logger(__name__)


# Removed load_config() and get_database_connection_id() - now handled by MetadataService


def sync_raw_metadata(**context):
    """Airflow task function to sync RAW layer metadata (lenient mode)."""
    logger.info("Starting Collibra metadata sync for RAW layer (lenient mode)")
    
    # Note: RAW layer is lenient - no quality gate validation
    # Quality checks run but don't block sync (|| true in scan task)
    
    factory = ServiceFactory()
    metadata_service = factory.get_metadata_service()
    return metadata_service.sync_layer_metadata("raw")


def sync_raw_metadata_strict(**context):
    """Airflow task function to sync RAW layer metadata (strict mode with quality gate)."""
    logger.info("Starting Collibra metadata sync for RAW layer (strict mode)")
    
    # Validate quality before syncing (strict mode)
    if not validate_quality_before_sync('raw'):
        error_msg = "Quality gate failed: Critical checks failed in RAW layer. Skipping metadata sync."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    factory = ServiceFactory()
    metadata_service = factory.get_metadata_service()
    return metadata_service.sync_layer_metadata("raw")


def sync_staging_metadata(**context):
    """Airflow task function to sync STAGING layer metadata."""
    logger.info("Starting Collibra metadata sync for STAGING layer")
    
    factory = ServiceFactory()
    metadata_service = factory.get_metadata_service()
    return metadata_service.sync_layer_metadata("staging")


def sync_mart_metadata(**context):
    """Airflow task function to sync MART layer metadata (strict mode with quality gate)."""
    logger.info("Starting Collibra metadata sync for MART layer (strict mode)")
    
    # Validate quality before syncing (strict mode)
    if not validate_quality_before_sync('mart'):
        error_msg = "Quality gate failed: Critical checks failed in MART layer. Skipping metadata sync."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    factory = ServiceFactory()
    metadata_service = factory.get_metadata_service()
    return metadata_service.sync_layer_metadata("mart")


def sync_mart_metadata_lenient(**context):
    """Airflow task function to sync MART layer metadata (lenient mode)."""
    logger.info("Starting Collibra metadata sync for MART layer (lenient mode)")
    
    # Note: MART layer is lenient - no quality gate validation
    # Quality checks run but don't block sync (|| true in scan task)
    
    factory = ServiceFactory()
    metadata_service = factory.get_metadata_service()
    return metadata_service.sync_layer_metadata("mart")


def sync_quality_metadata(**context):
    """Airflow task function to sync QUALITY layer metadata."""
    logger.info("Starting Collibra metadata sync for QUALITY layer")
    
    factory = ServiceFactory()
    metadata_service = factory.get_metadata_service()
    return metadata_service.sync_layer_metadata("quality")

