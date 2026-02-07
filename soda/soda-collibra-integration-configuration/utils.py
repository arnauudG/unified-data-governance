"""
Utility functions for Soda-Collibra Integration
"""

import json
import time
import logging
from functools import lru_cache, wraps
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timezone
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from constants import IntegrationConstants

logger = logging.getLogger(__name__)

# Caching utilities
@lru_cache(maxsize=IntegrationConstants.CACHE_MAX_SIZE)
def get_domain_mappings(domain_mapping_json: str) -> Dict[str, str]:
    """
    Parse and cache domain mappings from JSON string.
    
    Args:
        domain_mapping_json: JSON string containing domain mappings
        
    Returns:
        Dictionary of domain mappings
    """
    # Handle empty or whitespace-only strings gracefully
    if not domain_mapping_json or not domain_mapping_json.strip():
        logger.debug("Domain mapping is empty, returning empty dict")
        return {}
    
    try:
        return json.loads(domain_mapping_json)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse domain mappings: {e}. Returning empty dict.")
        logger.debug(f"Domain mapping JSON string: {domain_mapping_json[:200]}")
        return {}

@lru_cache(maxsize=IntegrationConstants.CACHE_MAX_SIZE)
def get_custom_attributes_mapping(custom_attributes_json: str) -> Dict[str, str]:
    """
    Parse and cache custom attributes mapping from JSON string.
    
    Args:
        custom_attributes_json: JSON string containing custom attributes mapping
        
    Returns:
        Dictionary of custom attributes mapping
    """
    try:
        return json.loads(custom_attributes_json)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse custom attributes mapping: {e}")
        return {}

# Error handling and retry utilities
@retry(
    stop=stop_after_attempt(IntegrationConstants.MAX_RETRIES),
    wait=wait_exponential(
        multiplier=1,
        min=IntegrationConstants.RETRY_DELAY_MIN,
        max=IntegrationConstants.RETRY_DELAY_MAX
    )
)
def safe_api_call(func: Callable, *args, **kwargs) -> Any:
    """
    Execute API call with retry logic.
    
    Args:
        func: Function to call
        *args: Function arguments
        **kwargs: Function keyword arguments
        
    Returns:
        Function result
        
    Raises:
        Exception: If all retries are exhausted
    """
    try:
        return func(*args, **kwargs)
    except requests.exceptions.RequestException as e:
        logger.warning(f"API call failed, retrying: {e}")
        raise
    except Exception as e:
        # Provide better context for Pydantic validation errors
        if "ValidationError" in str(type(e)):
            logger.error(f"Data validation error in API call: {e}")
            logger.error("This might indicate a change in the API response format. Please check the model definitions.")
        else:
            logger.error(f"Unexpected error in API call: {e}")
        raise

def handle_api_errors(func: Callable) -> Callable:
    """
    Decorator to handle common API errors gracefully.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("Failed to connect to API")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    return wrapper

# Timing utilities
def timing_decorator(func: Callable) -> Callable:
    """
    Decorator to measure function execution time.
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            logger.debug(f"{func.__name__} executed in {duration:.2f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"{func.__name__} failed after {duration:.2f} seconds: {e}")
            raise
    return wrapper

# Data processing utilities
def generate_asset_name(
    check_name: str, 
    dataset_name: str, 
    seen_names: set,
    database: str = None,
    schema: str = None,
    column: str = None
) -> str:
    """
    Generate a unique asset name for a check following the convention:
    [DATABASE]-[SCHEMA]-[TABLE][-COLUMN] NAME
    
    Args:
        check_name: Name of the check
        dataset_name: Name of the dataset/table
        seen_names: Set of already used names
        database: Database name (optional, will use SNOWFLAKE_DATABASE env var if not provided)
        schema: Schema name (optional)
        column: Column name for column-level checks (optional)
        
    Returns:
        Unique asset name in format: [DATABASE]-[SCHEMA]-[TABLE][-COLUMN] NAME
    """
    import os
    
    # Get database name
    if not database:
        database = os.getenv('SNOWFLAKE_DATABASE', 'DATA PLATFORM XYZ')
    
    # Normalize database name (uppercase)
    database = database.upper().replace(' ', '_').replace('-', '_')
    
    # Get schema name (uppercase)
    if schema:
        schema = schema.upper().replace(' ', '_').replace('-', '_')
    else:
        # Default to "UNKNOWN" if schema not provided
        schema = "UNKNOWN"
    
    # Table name (uppercase)
    table = dataset_name.upper().replace(' ', '_').replace('-', '_')
    
    # Build the name following: [DATABASE]-[SCHEMA]-[TABLE][-COLUMN] NAME
    parts = [database, schema, table]
    
    # Add column if it's a column-level check
    if column:
        column_clean = column.upper().replace(' ', '_').replace('-', '_')
        parts.append(column_clean)
    
    # Join parts with dashes, add space, then check name
    base_name = f"{'-'.join(parts)} {check_name}"
    asset_name = base_name
    
    # Handle duplicates
    counter = 1
    while asset_name in seen_names:
        asset_name = f"{base_name} ({counter})"
        counter += 1
    
    seen_names.add(asset_name)
    return asset_name

def generate_dataset_full_name(dataset, config) -> str:
    """
    Generate full dataset name for Collibra lookup.
    
    Args:
        dataset: Soda dataset object
        config: Application configuration
        
    Returns:
        Full dataset name
    """
    prefix = dataset.datasource.prefix or ""
    delimiter = config.collibra.general.naming_delimiter
    name = dataset.name
    
    full_name = f"{prefix}{delimiter}{name}".replace(
        IntegrationConstants.NAMING_DELIMITER_REPLACEMENT, 
        delimiter
    )
    return full_name

def generate_column_full_name(table_name: str, column_name: str, config) -> str:
    """
    Generate full column name for Collibra lookup.
    
    Args:
        table_name: Name of the table
        column_name: Name of the column
        config: Application configuration
        
    Returns:
        Full column name
    """
    delimiter = config.collibra.general.naming_delimiter
    return f"{table_name}{delimiter}{column_name}{IntegrationConstants.COLUMN_SUFFIX}"

def convert_to_utc_midnight_timestamp(dt_string: str) -> str:
    """
    Convert datetime string to UTC midnight timestamp.
    
    Args:
        dt_string: Datetime string in ISO format
        
    Returns:
        UTC midnight timestamp as string
    """
    dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
    midnight_utc = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=timezone.utc)
    return str(int(midnight_utc.timestamp() * 1000))

def get_current_utc_midnight_timestamp() -> str:
    """
    Get current UTC midnight timestamp.
    
    Returns:
        Current UTC midnight timestamp as string
    """
    current_time = datetime.utcnow()
    midnight_utc = datetime(
        current_time.year, 
        current_time.month, 
        current_time.day, 
        0, 0, 0, 
        tzinfo=timezone.utc
    )
    return str(int(midnight_utc.timestamp() * 1000))

def format_cloud_url(url: str) -> str:
    """
    Format cloud URL as HTML link.
    
    Args:
        url: Cloud URL
        
    Returns:
        HTML formatted URL
    """
    return IntegrationConstants.HTML_CLOUD_URL_TEMPLATE.format(url)

def format_check_definition(definition: str) -> str:
    """
    Format check definition as HTML code block.
    
    Args:
        definition: Check definition
        
    Returns:
        HTML formatted definition
    """
    return IntegrationConstants.HTML_DEFINITION_TEMPLATE.format(definition)

def validate_config(config) -> None:
    """
    Validate configuration object.
    
    Args:
        config: Configuration object
        
    Raises:
        ValueError: If configuration is invalid
    """
    required_fields = [
        ('collibra.base_url', config.collibra.base_url),
        ('soda.base_url', config.soda.base_url),
        ('collibra.asset_types.table_asset_type', config.collibra.asset_types.table_asset_type),
        ('collibra.asset_types.soda_check_asset_type', config.collibra.asset_types.soda_check_asset_type),
    ]
    
    missing_fields = []
    for field_name, field_value in required_fields:
        if not field_value:
            missing_fields.append(field_name)
    
    if missing_fields:
        raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")

def batch_items(items: List[Any], batch_size: int = IntegrationConstants.BATCH_SIZE) -> List[List[Any]]:
    """
    Split items into batches.
    
    Args:
        items: List of items to batch
        batch_size: Size of each batch
        
    Returns:
        List of batches
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size] 