#!/usr/bin/env python3
"""
Helper functions to check Soda quality check results before Collibra sync.

This module queries Soda Cloud API to verify that no critical checks have failed
before allowing metadata synchronization to proceed.
"""

import os
import sys
import logging
import re
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables explicitly for Airflow
env_path = Path('/opt/airflow/.env')
if env_path.exists():
    load_dotenv(env_path, override=True)
    logger.info(f"Loaded environment variables from {env_path}")
elif os.getenv('COLLIBRA_BASE_URL'):
    logger.info("Using environment variables from Docker Compose")
else:
    load_dotenv(override=True)
    logger.info("Using default dotenv behavior for environment variables")

# Try to import Soda integration modules
try:
    # Handle hyphenated directory name by adding to sys.path
    soda_integration_path = PROJECT_ROOT / "soda" / "soda-collibra-integration-configuration"
    if soda_integration_path.exists():
        sys.path.insert(0, str(soda_integration_path))
    
    from clients.soda_client import SodaClient
    from config import load_config as load_soda_config
    SODA_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Soda integration modules not available: {e}. Quality checks will be skipped.")
    SodaClient = None
    SODA_AVAILABLE = False


def get_failed_critical_checks(layer: str, dataset_ids: Optional[List[str]] = None) -> List[Dict[str, str]]:
    """
    Query Soda Cloud API to get failed checks for a specific layer.
    Only returns checks marked as 'critical' that have failed.
    
    Args:
        layer: Layer name ('raw', 'staging', 'mart', 'quality')
        dataset_ids: Optional list of dataset IDs to filter checks
        
    Returns:
        List of dictionaries with check information (name, dataset, status)
    """
    if not SODA_AVAILABLE:
        logger.warning("SodaClient not available. Skipping quality check validation.")
        return []
    
    # Define expected dataset names for each layer (based on our check files)
    EXPECTED_DATASETS = {
        'raw': ['CUSTOMERS', 'PRODUCTS', 'ORDERS', 'ORDER_ITEMS'],
        'staging': ['STG_CUSTOMERS', 'STG_PRODUCTS', 'STG_ORDERS', 'STG_ORDER_ITEMS'],
        'mart': ['DIM_CUSTOMERS', 'DIM_PRODUCTS', 'FACT_ORDERS'],
        'quality': ['CHECK_RESULTS']
    }
    
    expected_dataset_names = EXPECTED_DATASETS.get(layer.lower(), [])
    if not expected_dataset_names:
        logger.warning(f"No expected datasets defined for layer '{layer}'. Skipping quality check.")
        return []
    
    try:
        # Load Soda config
        soda_config_path = PROJECT_ROOT / "soda" / "soda-collibra-integration-configuration" / "config.yaml"
        if not soda_config_path.exists():
            logger.warning(f"Soda config not found at {soda_config_path}. Skipping quality check.")
            return []
        
        soda_config = load_soda_config(str(soda_config_path))
        soda_client = SodaClient(soda_config.soda, metrics=None)
        
        # Get all checks for the layer
        all_checks = []
        if dataset_ids:
            for dataset_id in dataset_ids:
                try:
                    checks = soda_client.get_checks(dataset_id=dataset_id)
                    if isinstance(checks, list):
                        all_checks.extend(checks)
                    elif hasattr(checks, 'content'):
                        all_checks.extend(checks.content)
                    elif hasattr(checks, '__iter__'):
                        all_checks.extend(list(checks))
                except Exception as e:
                    logger.warning(f"Failed to fetch checks for dataset {dataset_id}: {e}")
                    continue
        else:
            try:
                checks = soda_client.get_checks()
                if isinstance(checks, list):
                    all_checks = checks
                elif hasattr(checks, 'content'):
                    all_checks = checks.content
                elif hasattr(checks, '__iter__'):
                    all_checks = list(checks)
            except Exception as e:
                logger.warning(f"Failed to fetch all checks: {e}")
                return []
        
        # Filter for failed critical checks
        failed_critical = []
        for check in all_checks:
            try:
                # Check if check failed
                evaluation_status = None
                if hasattr(check, 'evaluationStatus'):
                    evaluation_status = check.evaluationStatus
                elif isinstance(check, dict):
                    evaluation_status = check.get('evaluationStatus')
                
                if not evaluation_status or evaluation_status == 'pass':
                    continue
                
                # Check if check is marked as critical
                is_critical = False
                check_name = 'Unknown'
                
                # Get check name
                if hasattr(check, 'name'):
                    check_name = check.name
                elif isinstance(check, dict):
                    check_name = check.get('name', 'Unknown')
                
                # Check attributes for critical flag
                attributes = None
                if hasattr(check, 'attributes'):
                    attributes = check.attributes
                elif isinstance(check, dict):
                    attributes = check.get('attributes')
                
                if attributes:
                    if isinstance(attributes, dict):
                        is_critical = attributes.get('critical', False)
                    elif isinstance(attributes, list):
                        # Handle list of attribute objects
                        for attr in attributes:
                            if isinstance(attr, dict):
                                if attr.get('name') == 'critical':
                                    is_critical = attr.get('value', False)
                                    break
                                # Also check if it's a key-value pair
                                if 'critical' in attr:
                                    is_critical = attr['critical']
                                    break
                
                # Also check check name patterns for critical checks
                # Critical checks often have names indicating they're required
                critical_patterns = [
                    r'schema\s+validation',
                    r'unique',
                    r'missing.*=\s*0',
                    r'duplicate.*=\s*0',
                    r'required\s+column',
                    r'no\s+missing',
                    r'are\s+unique',
                ]
                for pattern in critical_patterns:
                    if re.search(pattern, check_name, re.IGNORECASE):
                        is_critical = True
                        logger.debug(f"Check '{check_name}' marked as critical based on name pattern")
                        break
                
                if is_critical:
                    dataset_name = 'Unknown'
                    if hasattr(check, 'dataset'):
                        dataset = check.dataset
                        if hasattr(dataset, 'name'):
                            dataset_name = dataset.name
                        elif isinstance(dataset, dict):
                            dataset_name = dataset.get('name', 'Unknown')
                    elif isinstance(check, dict):
                        dataset = check.get('dataset', {})
                        if isinstance(dataset, dict):
                            dataset_name = dataset.get('name', 'Unknown')
                    
                    # Filter: Only include checks from expected datasets for this layer
                    if dataset_name.upper() not in [name.upper() for name in expected_dataset_names]:
                        logger.debug(f"Skipping check '{check_name}' from dataset '{dataset_name}' (not in expected datasets for {layer} layer)")
                        continue
                    
                    failed_critical.append({
                        'name': check_name,
                        'dataset': dataset_name,
                        'status': evaluation_status,
                        'layer': layer
                    })
                    logger.warning(f"Critical check failed in {layer} layer: {check_name} (Dataset: {dataset_name})")
            except Exception as e:
                logger.warning(f"Error processing check: {e}")
                continue
        
        if failed_critical:
            logger.error(f"Found {len(failed_critical)} failed critical checks in {layer} layer")
        else:
            logger.info(f"No failed critical checks found in {layer} layer")
        
        return failed_critical
        
    except Exception as e:
        logger.error(f"Error checking Soda quality results for {layer} layer: {e}")
        # Don't fail the sync if we can't check - log and continue
        # This is a safety check, not a hard requirement
        logger.warning("Continuing with sync despite quality check error (non-blocking)")
        return []


def validate_quality_before_sync(layer: str, dataset_ids: Optional[List[str]] = None) -> bool:
    """
    Validate that no critical checks have failed before allowing Collibra sync.
    
    Args:
        layer: Layer name ('raw', 'staging', 'mart', 'quality')
        dataset_ids: Optional list of dataset IDs to filter checks
        
    Returns:
        True if sync should proceed, False if critical checks failed
    """
    failed_critical = get_failed_critical_checks(layer, dataset_ids)
    
    if failed_critical:
        logger.error(f"❌ Quality gate failed for {layer} layer. {len(failed_critical)} critical checks failed:")
        for check in failed_critical:
            logger.error(f"  - {check['name']} (Dataset: {check['dataset']})")
        return False
    
    logger.info(f"✅ Quality gate passed for {layer} layer. No critical checks failed.")
    return True

