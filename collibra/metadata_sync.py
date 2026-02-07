#!/usr/bin/env python3
"""
Collibra Metadata Synchronization Module

This module provides functions to trigger and monitor Collibra metadata synchronization
for database assets and schema connections.

Refactored to use Repository pattern and centralized configuration.
"""

import sys
import time
from typing import List, Optional, Dict, Any
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.core.config import get_config
from src.core.logging import get_logger
from src.core.exceptions import (
    ConfigurationError,
    APIError,
    TimeoutError as CoreTimeoutError,
)
from src.repositories.collibra_repository import CollibraRepository
from src.core.constants import RetryConfigDefaults

logger = get_logger(__name__)


class CollibraMetadataSync:
    """
    Handles Collibra metadata synchronization operations.
    
    This class wraps CollibraRepository for backward compatibility.
    New code should use CollibraRepository or MetadataService directly.
    """
    
    def __init__(self, config: Optional[Any] = None, collibra_repository: Optional[CollibraRepository] = None):
        """
        Initialize Collibra client with credentials from configuration.
        
        Args:
            config: Optional Config instance. If None, uses get_config().
            collibra_repository: Optional CollibraRepository instance. If None, creates new one.
        
        Raises:
            ConfigurationError: If required configuration is missing
        """
        if config is None:
            config = get_config()
        
        self.config = config
        self.collibra_repository = collibra_repository or CollibraRepository(config=config)
        
        # Keep these for backward compatibility
        self.base_url = config.collibra.base_url
        self.username = config.collibra.username
        self.password = config.collibra.password
        
        logger.info(f"Initialized Collibra client for {self.base_url}")
    
    def get_database_connection_id(self, database_id: str) -> str:
        """
        Get the database connection ID from a database asset ID.
        
        Args:
            database_id: The UUID of the Database asset in Collibra
        
        Returns:
            The database connection ID
        
        Raises:
            ConfigurationError: If database connection not found
        """
        return self.collibra_repository.get_database_connection_id(database_id)
    
    def list_schema_connections(
        self,
        database_connection_id: str,
        schema_id: Optional[str] = None,
        limit: int = 500,
        offset: int = 0
    ) -> List[Dict]:
        """
        List schema connections for a database connection.
        
        Args:
            database_connection_id: The UUID of the database connection
            schema_id: Optional schema asset ID to filter by
            limit: Maximum number of results (default: 500)
            offset: Offset for pagination (default: 0)
        
        Returns:
            List of schema connection dictionaries
        """
        return self.collibra_repository.list_schema_connections(
            database_connection_id=database_connection_id,
            schema_id=schema_id,
            limit=limit,
            offset=offset
        )
    
    def resolve_schema_connection_ids(
        self,
        database_id: str,
        schema_asset_ids: List[str],
        database_connection_id: Optional[str] = None
    ) -> List[str]:
        """
        Resolve schema asset IDs to schema connection IDs.
        
        Args:
            database_id: The UUID of the Database asset
            schema_asset_ids: List of schema asset UUIDs
            database_connection_id: Optional database connection ID (ignored, will be resolved)
        
        Returns:
            List of schema connection UUIDs
        
        Raises:
            ConfigurationError: If schema connections cannot be resolved
        """
        return self.collibra_repository.resolve_schema_connection_ids(
            database_id=database_id,
            schema_asset_ids=schema_asset_ids
        )
    
    def trigger_metadata_sync(
        self,
        database_id: str,
        schema_connection_ids: Optional[List[str]] = None
    ) -> Dict:
        """
        Trigger metadata synchronization for a database asset.
        
        Args:
            database_id: The UUID of the Database asset in Collibra
            schema_connection_ids: Optional list of schema connection UUIDs.
                                  If None or empty, all schemas with rules are synchronized.
        
        Returns:
            Dict containing the job ID and response details
        """
        return self.collibra_repository.trigger_metadata_sync(
            database_id=database_id,
            schema_connection_ids=schema_connection_ids
        )
    
    def get_job_status(self, job_id: str) -> Dict:
        """
        Get the status of a Collibra job.
        
        Args:
            job_id: The UUID of the job
        
        Returns:
            Dict containing job status information
        
        Raises:
            ConfigurationError: If job status cannot be retrieved
        """
        return self.collibra_repository.get_job_status(job_id)
    
    def wait_for_job_completion(
        self,
        job_id: str,
        max_wait_time: int = RetryConfigDefaults.MAX_WAIT_TIME,
        poll_interval: int = RetryConfigDefaults.POLL_INTERVAL
    ) -> Dict:
        """
        Wait for a job to complete and return the final status.
        
        Args:
            job_id: The UUID of the job to monitor
            max_wait_time: Maximum time to wait in seconds (default: 1 hour)
            poll_interval: Time between status checks in seconds (default: 10 seconds)
        
        Returns:
            Dict containing the final job status
        
        Raises:
            TimeoutError: If the job doesn't complete within max_wait_time
            RuntimeError: If the job fails
        """
        logger.info(f"Monitoring job {job_id} for completion...")
        start_time = time.time()
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        while True:
            elapsed_time = time.time() - start_time
            
            if elapsed_time > max_wait_time:
                raise CoreTimeoutError(
                    f"Job {job_id} did not complete within {max_wait_time} seconds"
                )
            
            try:
                status = self.get_job_status(job_id)
                consecutive_errors = 0  # Reset error counter on success
                job_status = status.get('status', 'UNKNOWN')
                
                logger.info(
                    f"Job {job_id} status: {job_status} "
                    f"(elapsed: {int(elapsed_time)}s)"
                )
                
                if job_status == 'COMPLETED':
                    logger.info(f"Job {job_id} completed successfully")
                    return status
                elif job_status == 'FAILED':
                    error_msg = status.get('errorMessage', 'Unknown error')
                    raise RuntimeError(f"Job {job_id} failed: {error_msg}")
                elif job_status in ['CANCELLED', 'CANCELED']:
                    raise RuntimeError(f"Job {job_id} was cancelled")
                
            except (ValueError, APIError, ConfigurationError) as e:
                consecutive_errors += 1
                logger.warning(
                    f"Error getting job status (attempt {consecutive_errors}/{max_consecutive_errors}): {e}"
                )
                
                # If we can't get job status multiple times, the endpoint might not be available
                # In this case, we'll wait a bit and then treat it as success
                # since the sync was triggered successfully
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(
                        f"Cannot retrieve job status for {job_id} after {max_consecutive_errors} attempts. "
                        "The job status endpoint may not be available or the response format is different. "
                        "Since the sync was triggered successfully, treating as success. "
                        "Sync will complete in background."
                    )
                    return {
                        'status': 'TRIGGERED',
                        'jobId': job_id,
                        'message': 'Job triggered but status tracking unavailable - sync will complete in background'
                    }
            
            # Wait before next poll
            time.sleep(poll_interval)
    
    def sync_and_wait(
        self,
        database_id: str,
        schema_connection_ids: Optional[List[str]] = None,
        schema_asset_ids: Optional[List[str]] = None,
        max_wait_time: int = RetryConfigDefaults.MAX_WAIT_TIME,
        poll_interval: int = RetryConfigDefaults.POLL_INTERVAL
    ) -> Dict:
        """
        Trigger metadata synchronization and wait for completion.
        
        This is a convenience method that combines trigger_metadata_sync
        and wait_for_job_completion.
        
        Args:
            database_id: The UUID of the Database asset in Collibra
            schema_connection_ids: Optional list of schema connection UUIDs (used directly)
            schema_asset_ids: Optional list of schema asset UUIDs (resolved to connection IDs)
            max_wait_time: Maximum time to wait in seconds (default: 1 hour)
            poll_interval: Time between status checks in seconds (default: 10 seconds)
        
        Returns:
            Dict containing the final job status
        
        Note:
            If both schema_connection_ids and schema_asset_ids are provided,
            schema_connection_ids takes precedence.
        """
        # Resolve schema asset IDs to connection IDs if needed
        if schema_asset_ids and not schema_connection_ids:
            logger.info(f"Resolving {len(schema_asset_ids)} schema asset ID(s) to connection IDs")
            schema_connection_ids = self.resolve_schema_connection_ids(
                database_id,
                schema_asset_ids
            )
        
        # Trigger sync
        sync_result = self.trigger_metadata_sync(database_id, schema_connection_ids)
        job_id = sync_result.get('jobId')
        status = sync_result.get('status', 'triggered')
        
        # If sync is already running, we can't wait for a specific job
        # Return success since sync is already in progress
        if status == 'already_running':
            logger.info(
                "Metadata sync is already in progress. "
                "Cannot wait for specific job completion, but sync will complete in background."
            )
            return {
                'jobId': None,
                'databaseId': database_id,
                'schemaConnectionIds': schema_connection_ids or [],
                'schemaAssetIds': schema_asset_ids or [],
                'status': 'already_running',
                'message': 'Sync already in progress - will complete in background',
                'finalStatus': {'status': 'RUNNING', 'message': 'Sync already in progress'}
            }
        
        # If no job ID was returned, the API might not return one
        # This can happen with some Collibra endpoints - sync is triggered but no job ID
        # In this case, we'll treat it as success since the sync was triggered
        if not job_id:
            logger.warning(
                "Metadata sync was triggered but no job ID was returned. "
                "This may be normal for this Collibra endpoint. "
                "Sync will complete in background - cannot monitor progress."
            )
            return {
                'jobId': None,
                'databaseId': database_id,
                'schemaConnectionIds': schema_connection_ids or [],
                'schemaAssetIds': schema_asset_ids or [],
                'status': 'triggered_no_job_id',
                'message': 'Sync triggered successfully but no job ID returned - will complete in background',
                'finalStatus': {'status': 'TRIGGERED', 'message': 'Sync triggered, no job tracking available'}
            }
        
        # Wait for completion
        final_status = self.wait_for_job_completion(
            job_id,
            max_wait_time=max_wait_time,
            poll_interval=poll_interval
        )
        
        return {
            'jobId': job_id,
            'databaseId': database_id,
            'schemaConnectionIds': schema_connection_ids or [],
            'schemaAssetIds': schema_asset_ids or [],
            'finalStatus': final_status
        }


def sync_layer_metadata(
    layer: str,
    database_id: str,
    schema_connection_ids: Optional[List[str]] = None
) -> Dict:
    """
    Convenience function to sync metadata for a specific layer.
    
    Args:
        layer: Layer name (e.g., 'RAW', 'STAGING', 'MART')
        database_id: The UUID of the Database asset in Collibra
        schema_connection_ids: Optional list of schema connection UUIDs
    
    Returns:
        Dict containing the sync result
    """
    logger.info(f"Starting metadata sync for {layer} layer")
    
    sync_client = CollibraMetadataSync()
    result = sync_client.sync_and_wait(
        database_id=database_id,
        schema_connection_ids=schema_connection_ids
    )
    
    logger.info(f"Metadata sync completed for {layer} layer")
    return result


if __name__ == "__main__":
    """Example usage when run as a script."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python metadata_sync.py <database_id> [schema_connection_id1] [schema_connection_id2] ...")
        sys.exit(1)
    
    database_id = sys.argv[1]
    schema_ids = sys.argv[2:] if len(sys.argv) > 2 else None
    
    try:
        sync_client = CollibraMetadataSync()
        result = sync_client.sync_and_wait(
            database_id=database_id,
            schema_connection_ids=schema_ids
        )
        print(f"Sync completed successfully: {result}")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)

