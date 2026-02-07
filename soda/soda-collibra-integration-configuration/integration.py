"""
Main Soda-Collibra Integration Class
"""

import logging
import re
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import requests

from config import load_config
from clients.collibra_client import CollibraClient
from clients.soda_client import SodaClient
from models.soda import FullDataset, SodaCheck, UpdateDatasetRequest, DatasetOwnerUpdate
from models.collibra import (
    AssetCreateRequest, AssetUpdateRequest, 
    AttributeCreateRequest, AttributeUpdateRequest,
    ResponsibilitySearchResponse, UserSearchResponse as CollibraUserSearchResponse
)
from constants import IntegrationConstants
from utils import (
    get_domain_mappings, get_custom_attributes_mapping, safe_api_call,
    handle_api_errors, timing_decorator, generate_asset_name,
    generate_dataset_full_name, generate_column_full_name,
    convert_to_utc_midnight_timestamp, get_current_utc_midnight_timestamp,
    format_cloud_url, format_check_definition, validate_config, batch_items
)
from metrics import MetricsCollector, DatasetMetrics

logger = logging.getLogger(__name__)

class SodaCollibraIntegration:
    """Main class for Soda-Collibra integration"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the integration.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.config = load_config(config_path)
        validate_config(self.config)
        
        self.metrics = MetricsCollector()
        self.collibra = CollibraClient(self.config.collibra, self.metrics.get_overall_metrics())
        self.soda = SodaClient(self.config.soda, self.metrics.get_overall_metrics())
        
        # Cache for frequently accessed data
        self._domain_mappings_cache = None
        self._custom_attributes_cache = None
        self._asset_cache = {}
        
        # Track datasets without table assets to avoid duplicate error messages
        self._datasets_without_table_assets = set()
        
        logger.debug("SodaCollibraIntegration initialized successfully")
    
    @timing_decorator
    def run(self) -> Dict[str, int]:
        """
        Run the complete integration process.
        
        Returns:
            Dictionary containing processing statistics
        """
        try:
            print(IntegrationConstants.MSG_INTEGRATION_STARTED)
            
            # Test connections
            self._test_connections()
            
            # Get and filter datasets
            datasets = self._get_datasets()
            datasets_to_sync = self._filter_datasets(datasets)
            
            print(IntegrationConstants.MSG_DATASETS_FOUND.format(len(datasets)))
            
            # Process datasets
            self._process_datasets(datasets_to_sync)
            
            # Finish processing and aggregate metrics
            self.metrics.finish_processing()
            
            # Print summary (after metrics are aggregated)
            self._print_summary()
            
            return self.metrics.get_overall_metrics().get_summary_dict()
            
        except Exception as e:
            logger.error(f"Integration failed: {e}")
            self.metrics.get_overall_metrics().add_error(f"Integration failed: {e}")
            raise
    
    @handle_api_errors
    def _test_connections(self) -> None:
        """Test connections to both Soda and Collibra"""
        logger.debug("Testing Soda connection...")
        safe_api_call(self.soda.test_connection)
        logger.debug("Soda connection test successful")
        
        # Could add Collibra connection test here if available
        logger.debug("Connection tests completed")
    
    @handle_api_errors
    @timing_decorator
    def _get_datasets(self) -> List[FullDataset]:
        """
        Fetch all datasets from Soda.
        
        Returns:
            List of datasets
        """
        logger.debug("Fetching datasets from Soda...")
        datasets = safe_api_call(self.soda.get_datasets)
        logger.debug(f"Retrieved {len(datasets)} datasets from Soda")
        return datasets
    
    def _filter_datasets(self, datasets: List[FullDataset]) -> List[FullDataset]:
        """
        Filter datasets based on configuration.
        
        Args:
            datasets: List of all datasets
            
        Returns:
            List of filtered datasets
        """
        logger.debug(f"Dataset filtering enabled: {self.config.soda.general.filter_datasets_to_sync_to_collibra}")
        
        if not self.config.soda.general.filter_datasets_to_sync_to_collibra:
            logger.debug("No dataset filtering - processing all datasets")
            return datasets
        
        logger.debug("Filtering datasets based on sync attribute")
        sync_attribute = self.config.soda.attributes.soda_collibra_sync_dataset_attribute
        logger.debug(f"Sync attribute name: {sync_attribute}")
        
        filtered_datasets = []
        for dataset in datasets:
            logger.debug(f"Checking dataset '{dataset.name}' for sync attribute")
            if dataset.attributes.get(sync_attribute):
                logger.debug(f"Dataset '{dataset.name}' added to sync list")
                filtered_datasets.append(dataset)
            else:
                logger.debug(f"Dataset '{dataset.name}' skipped - sync attribute not found or False")
                print(f"  ⏭️ Skipping dataset: {dataset.name} (sync attribute not found or False)")
                self.metrics.get_overall_metrics().datasets_skipped += 1
        
        logger.debug(f"Final dataset count for processing: {len(filtered_datasets)}")
        return filtered_datasets
    
    def _process_datasets(self, datasets: List[FullDataset]) -> None:
        """
        Process all datasets.
        
        Args:
            datasets: List of datasets to process
        """
        logger.debug(f"Starting to process {len(datasets)} datasets")
        
        # Process datasets sequentially for now (could be parallelized)
        for i, dataset in enumerate(datasets, 1):
            try:
                self._process_single_dataset(dataset, i, len(datasets))
                self.metrics.get_overall_metrics().datasets_processed += 1
            except Exception as e:
                logger.error(f"Failed to process dataset {dataset.name}: {e}")
                self.metrics.get_overall_metrics().add_error(f"Failed to process dataset {dataset.name}: {e}")
                self.metrics.get_overall_metrics().datasets_failed += 1
    
    def _process_single_dataset(self, dataset: FullDataset, index: int, total: int) -> None:
        """
        Process a single dataset.
        
        Args:
            dataset: Dataset to process
            index: Current dataset index
            total: Total number of datasets
        """
        dataset_metrics = self.metrics.start_dataset_processing(dataset.name)
        
        print(f"Processing dataset {index}/{total}: {dataset.name}")
        
        logger.debug(f"=== Processing dataset {index}/{total}: {dataset.name} ===")
        logger.debug(IntegrationConstants.MSG_PROCESSING_DATASET.format(index, total, dataset.name))
        
        logger.debug(f"Dataset details:")
        logger.debug(f"  - ID: {dataset.id}")
        logger.debug(f"  - Name: {dataset.name}")
        logger.debug(f"  - Datasource: {dataset.datasource.name} ({dataset.datasource.type})")
        logger.debug(f"  - Attributes: {dataset.attributes}")
        
        # Verify dataset exists in Collibra if required
        if not self._verify_dataset_in_collibra(dataset):
            self.metrics.get_overall_metrics().datasets_skipped += 1
            return
        
        # Get domain mapping
        domain_id = self._get_domain_id(dataset)
        
        # Get checks for dataset
        print(f"  📋 Getting checks...")
        checks = self._get_dataset_checks(dataset)
        dataset_metrics.checks_found = len(checks)
        logger.debug(IntegrationConstants.MSG_CHECKS_FOUND.format(len(checks)))
        
        if not checks:
            logger.debug("No checks found for dataset, skipping")
            print(f"  ℹ️ No checks found for dataset")
            return
        
        # Process checks
        checks_count = sum(1 for check in checks if check.checkType is not None)
        monitors_count = sum(1 for check in checks if check.metricType is not None)
        logger.debug(f"Check/monitor breakdown: {checks_count} check(s), {monitors_count} monitor(s)")
        if monitors_count > 0:
            print(f"  🔄 Processing {len(checks)} items ({checks_count} checks, {monitors_count} monitors)...")
            logger.debug(f"Processing {len(checks)} items ({checks_count} checks, {monitors_count} monitors) for dataset: {dataset.name}")
        else:
            print(f"  🔄 Processing {len(checks)} checks...")
            logger.debug(f"Processing {len(checks)} checks (no monitors) for dataset: {dataset.name}")
        self._process_dataset_checks(dataset, checks, domain_id, dataset_metrics)
        
        # Log summary for this dataset
        logger.debug(f"Dataset {dataset.name} processing summary:")
        logger.debug(f"  - Checks found: {len(checks)}")
        logger.debug(f"  - Checks processed: {dataset_metrics.checks_found}")
        logger.debug(f"  - Assets created: {dataset_metrics.checks_created}")
        logger.debug(f"  - Assets updated: {dataset_metrics.checks_updated}")
        
        # Synchronize ownership from Collibra to Soda
        print(f"  👥 Syncing ownership...")
        logger.debug(f"About to synchronize ownership for dataset: {dataset.name}")
        try:
            self._synchronize_dataset_ownership(dataset, dataset_metrics)
            logger.debug(f"Ownership sync completed for dataset: {dataset.name}. Owners synced: {dataset_metrics.owners_synced}")
        except Exception as e:
            logger.error(f"Failed to synchronize ownership for dataset {dataset.name}: {e}")
            dataset_metrics.add_error(f"Ownership sync failed: {e}")
        
        self.metrics.finish_dataset_processing(dataset.name)
    
    def _verify_dataset_in_collibra(self, dataset: FullDataset) -> bool:
        """
        Verify dataset exists in Collibra if verification is enabled.
        
        Args:
            dataset: Dataset to verify
            
        Returns:
            True if verification passes or is disabled, False otherwise
        """
        if not self.config.soda.general.soda_no_collibra_dataset_skip_checks:
            logger.debug("Collibra dataset verification disabled")
            return True
        
        logger.debug("Collibra dataset verification enabled")
        dataset_full_name = generate_dataset_full_name(dataset, self.config)
        logger.debug(f"Searching for Collibra asset with name: {dataset_full_name}")
        
        try:
            collibra_assets = safe_api_call(
                self.collibra.find_asset,
                name=dataset_full_name,
                type_id=self.config.collibra.asset_types.table_asset_type
            )
            logger.debug(f"Found {len(collibra_assets.results)} matching Collibra assets")
            
            if not collibra_assets.results:
                logger.debug(f"No Collibra assets found for dataset: {dataset.name}")
                print(f"  ⚠️ Skipping dataset: {dataset.name} (not found in Collibra)")
                return False
            elif len(collibra_assets.results) > 1:
                logger.debug(f"Multiple Collibra assets found for dataset: {dataset.name}")
                print(f"  ⚠️ Skipping dataset: {dataset.name} (multiple assets found in Collibra)")
                return False
            else:
                logger.debug(f"Single Collibra asset found for dataset: {dataset.name}")
                return True
                
        except Exception as e:
            logger.error(f"Error verifying dataset in Collibra: {e}")
            self.metrics.get_overall_metrics().add_error(f"Error verifying dataset {dataset.name} in Collibra: {e}")
            return False
    
    def _get_domain_id(self, dataset: FullDataset) -> str:
        """
        Get domain ID for dataset.
        
        Args:
            dataset: Dataset to get domain for
            
        Returns:
            Domain ID
        """
        logger.debug("Determining domain mapping for dataset")
        
        # Get cached domain mappings
        if self._domain_mappings_cache is None:
            self._domain_mappings_cache = get_domain_mappings(
                self.config.collibra.domains.soda_collibra_domain_mapping
            )
        
        domain_attribute_value = dataset.attributes.get(
            self.config.soda.attributes.soda_collibra_domain_dataset_attribute_name
        )
        logger.debug(f"Domain attribute value: {domain_attribute_value}")
        logger.debug(f"Available domain mappings: {self._domain_mappings_cache}")
        
        if domain_attribute_value and domain_attribute_value in self._domain_mappings_cache:
            domain_id = self._domain_mappings_cache[domain_attribute_value]
            logger.debug(f"Using mapped domain ID: {domain_id}")
        else:
            domain_id = self.config.collibra.domains.soda_collibra_default_domain
            logger.debug(f"Using default domain ID: {domain_id}")
        
        return domain_id
    
    @handle_api_errors
    def _get_dataset_checks(self, dataset: FullDataset) -> List[SodaCheck]:
        """
        Get checks for a dataset.
        Filters out monitors if sync_monitors is disabled in config.
        
        Args:
            dataset: Dataset to get checks for
            
        Returns:
            List of checks (and optionally monitors based on config)
        """
        logger.debug(f"Fetching checks for dataset ID: {dataset.id}")
        all_items = safe_api_call(self.soda.get_checks, dataset_id=dataset.id)
        logger.debug(f"Retrieved {len(all_items)} items (checks + monitors) for dataset: {dataset.name}")
        
        # Count checks vs monitors for logging
        checks_count = sum(1 for item in all_items if item.checkType is not None)
        monitors_count = sum(1 for item in all_items if item.metricType is not None)
        logger.debug(f"Breakdown: {checks_count} check(s) with checkType, {monitors_count} monitor(s) with metricType")
        
        # Filter monitors if sync_monitors is disabled
        # Checks have checkType, monitors have metricType
        if not self.config.soda.general.sync_monitors:
            checks = [item for item in all_items if item.checkType is not None]
            monitors_filtered = len(all_items) - len(checks)
            if monitors_filtered > 0:
                logger.debug(f"Filtered out {monitors_filtered} monitor(s) (sync_monitors is disabled)")
                print(f"  ⏭️ Filtered out {monitors_filtered} monitor(s) (sync_monitors is disabled)")
        else:
            checks = all_items
            if monitors_count > 0:
                logger.debug(f"Including {monitors_count} monitor(s) (sync_monitors is enabled)")
        
        logger.debug(f"Processing {len(checks)} item(s) (checks + monitors) for dataset: {dataset.name}")
        return checks
    
    def _process_dataset_checks(
        self, 
        dataset: FullDataset, 
        checks: List[SodaCheck], 
        domain_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process all checks for a dataset.
        
        Args:
            dataset: Dataset being processed
            checks: List of checks to process
            domain_id: Domain ID for the checks
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"Starting to process {len(checks)} check(s) for dataset: {dataset.name}")
        logger.debug("Preparing assets for checks")
        
        # Prepare assets for batch operations
        print(f"    🏗️ Preparing assets...")
        assets_to_create, assets_to_update = self._prepare_check_assets(
            checks, dataset, domain_id
        )
        
        # Execute batch operations
        print(f"    📤 Creating/updating assets...")
        self._execute_asset_operations(assets_to_create, assets_to_update, dataset_metrics)
        
        # Process attributes and relations for each check
        print(f"    📝 Processing metadata & relations...")
        self._process_check_attributes_and_relations(dataset, checks, dataset_metrics)
        
        # Sync deletions: remove checks from Collibra that no longer exist in Soda
        self._sync_check_deletions(dataset, checks, domain_id, dataset_metrics)
        
        logger.debug(f"Completed processing {len(checks)} check(s) for dataset: {dataset.name}")
    
    def _prepare_check_assets(
        self, 
        checks: List[SodaCheck], 
        dataset: FullDataset, 
        domain_id: str
    ) -> Tuple[List[AssetCreateRequest], List[AssetUpdateRequest]]:
        """
        Prepare assets for creation and update.
        
        Args:
            checks: List of checks
            dataset: Dataset being processed
            domain_id: Domain ID for assets
            
        Returns:
            Tuple of (assets_to_create, assets_to_update)
        """
        assets_to_create = []
        assets_to_update = []
        seen_names = set()
        
        # Extract database and schema from dataset
        database, schema = self._extract_database_and_schema(dataset)
        
        for check in checks:
            logger.debug(f"Processing check: {check.name}")
            logger.debug(f"  - Check ID: {check.id}")
            logger.debug(f"  - Evaluation Status: {check.evaluationStatus}")
            logger.debug(f"  - Column: {check.column}")
            logger.debug(f"  - Attributes: {check.attributes}")
            
            # Generate unique asset name with format: [DATABASE]-[SCHEMA]-[TABLE][-COLUMN] NAME
            asset_name = generate_asset_name(
                check_name=check.name,
                dataset_name=dataset.name,
                seen_names=seen_names,
                database=database,
                schema=schema,
                column=check.column
            )
            logger.debug(f"  - Generated asset name: {asset_name}")
            
            # Check if asset exists in the target domain first
            existing_assets = self._find_check_asset(asset_name, domain_id)
            
            # If not found in target domain, search across all domains
            # This handles cases where monitors/checks were previously created in wrong domain
            if not existing_assets or not existing_assets.results:
                logger.debug(f"  - Asset not found in target domain, searching across all domains...")
                existing_assets = self._find_check_asset(asset_name, None)
            
            if existing_assets and existing_assets.results:
                existing_asset = existing_assets.results[0]
                logger.debug(f"  - Asset exists in Collibra (ID: {existing_asset.id}, Domain: {existing_asset.domain.id if hasattr(existing_asset, 'domain') and existing_asset.domain else 'unknown'})")
                
                # Check if asset is in the correct domain
                asset_domain_id = existing_asset.domain.id if hasattr(existing_asset, 'domain') and existing_asset.domain else None
                if asset_domain_id != domain_id:
                    logger.debug(f"  - Asset is in different domain ({asset_domain_id}), will move to target domain ({domain_id})")
                
                update_request = AssetUpdateRequest(
                    id=existing_asset.id,
                    name=asset_name,
                    displayName=check.name,
                    typeId=self.config.collibra.asset_types.soda_check_asset_type,
                    domainId=domain_id  # This will move the asset to the correct domain
                )
                assets_to_update.append(update_request)
                logger.debug(f"  - Added to update list")
            else:
                logger.debug(f"  - Asset does not exist in Collibra - will create new in domain {domain_id}")
                asset = AssetCreateRequest(
                    name=asset_name,
                    displayName=check.name,
                    domainId=domain_id,
                    typeId=self.config.collibra.asset_types.soda_check_asset_type
                )
                assets_to_create.append(asset)
                logger.debug(f"  - Added to create list")
        
        logger.debug(f"Asset preparation complete for {len(checks)} check(s):")
        logger.debug(f"  - Assets to create: {len(assets_to_create)}")
        logger.debug(f"  - Assets to update: {len(assets_to_update)}")
        logger.debug(f"  - Total checks processed: {len(checks)}")
        
        return assets_to_create, assets_to_update
    
    def _extract_database_and_schema(self, dataset: FullDataset) -> tuple:
        """
        Extract database and schema from dataset information.
        
        Args:
            dataset: Dataset object
            
        Returns:
            Tuple of (database, schema)
        """
        import os
        
        # Get database from environment variable
        database = os.getenv('SNOWFLAKE_DATABASE', 'DATA PLATFORM XYZ')
        
        # Try to extract schema from qualifiedName (format: database.schema.table or similar)
        schema = None
        if dataset.qualifiedName:
            # Parse qualifiedName - could be in format: database.schema.table
            parts = dataset.qualifiedName.split('.')
            if len(parts) >= 2:
                # Assume second part is schema
                schema = parts[1].upper()
        
        # If schema not found in qualifiedName, try to extract from datasource name
        if not schema and dataset.datasource.name:
            # Datasource name format: {database_lowercase}_{layer}
            # e.g., "data_platform_xyz_raw" -> schema is "RAW"
            datasource_parts = dataset.datasource.name.split('_')
            if len(datasource_parts) > 1:
                # Last part is usually the layer (raw, staging, mart, quality)
                layer = datasource_parts[-1].upper()
                schema = layer
        
        # Fallback: use "UNKNOWN" if we can't determine schema
        if not schema:
            schema = "UNKNOWN"
            logger.warning(f"Could not determine schema for dataset {dataset.name}, using 'UNKNOWN'")
        
        logger.debug(f"Extracted database: {database}, schema: {schema} from dataset {dataset.name}")
        return database, schema
    
    @handle_api_errors
    def _find_check_asset(self, asset_name: str, domain_id: str = None):
        """
        Find check asset by name with caching.
        
        Args:
            asset_name: Name of asset to find
            domain_id: Optional domain ID to search within
            
        Returns:
            Asset search results
        """
        # Create cache key that includes domain_id
        cache_key = f"{asset_name}:{domain_id}" if domain_id else asset_name
        
        # Check cache first
        if cache_key in self._asset_cache:
            logger.debug(f"Found asset in cache: {cache_key}")
            return self._asset_cache[cache_key]
        
        logger.debug(f"  - Checking if asset exists in Collibra (domain: {domain_id})")
        
        # Search with or without domain constraint
        if domain_id:
            result = safe_api_call(
                self.collibra.find_asset,
                name=asset_name,
                type_id=self.config.collibra.asset_types.soda_check_asset_type,
                domain_id=domain_id,
                name_match_mode="EXACT"
            )
        else:
            result = safe_api_call(
                self.collibra.find_asset,
                name=asset_name,
                type_id=self.config.collibra.asset_types.soda_check_asset_type,
                name_match_mode="EXACT"
            )
        
        # Cache the result
        self._asset_cache[cache_key] = result
        return result
    
    @handle_api_errors
    def _sync_check_deletions(
        self,
        dataset: FullDataset,
        checks: List[SodaCheck],
        domain_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Sync deletions: find checks in Collibra that don't exist in Soda and delete them.
        
        Uses the naming convention {checkname}___{datasetName} to find all checks
        for a dataset and compares them with the checks returned by Soda.
        
        Args:
            dataset: Dataset being processed
            checks: List of checks from Soda
            domain_id: Domain ID for the checks
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"Starting deletion sync for dataset: {dataset.name}")
        
        # Search for all check assets in Collibra for this dataset
        # The name pattern is: {checkname}___{datasetName}
        search_pattern = f"___{dataset.name.lower()}"
        
        logger.debug(f"Searching for check assets ending with: {search_pattern}")
        
        result = safe_api_call(
            self.collibra.find_asset,
            name=search_pattern,
            type_id=self.config.collibra.asset_types.soda_check_asset_type,
            domain_id=domain_id,
            name_match_mode="END"  # Match names ending with the pattern
        )
        
        if not result or not result.results:
            logger.debug(f"No check assets found in Collibra for dataset: {dataset.name}")
            return
        
        # Generate expected asset names for all Soda checks
        database, schema = self._extract_database_and_schema(dataset)
        seen_names = set()
        soda_asset_names = set()
        for check in checks:
            asset_name = generate_asset_name(
                check_name=check.name,
                dataset_name=dataset.name,
                seen_names=seen_names,
                database=database,
                schema=schema,
                column=check.column
            )
            soda_asset_names.add(asset_name)
        
        # Compare Collibra asset names with Soda check names
        assets_to_delete = []
        for asset in result.results:
            if asset.name not in soda_asset_names:
                assets_to_delete.append(asset.id)
                logger.debug(f"  - Marked for deletion: {asset.name} (ID: {asset.id})")
        
        if not assets_to_delete:
            logger.debug(f"No checks to delete for dataset: {dataset.name}")
            return
        
        # Delete assets in bulk
        logger.debug(f"Deleting {len(assets_to_delete)} check asset(s) for dataset: {dataset.name}")
        print(f"    🗑️  Deleting {len(assets_to_delete)} obsolete check(s)...")
        
        try:
            # delete_bulk_assets now handles 404 errors gracefully (returns None)
            # safe_api_call will still retry on other errors (network issues, 500s, etc.)
            safe_api_call(
                self.collibra.delete_bulk_assets,
                asset_ids=assets_to_delete
            )
            dataset_metrics.checks_deleted = len(assets_to_delete)
            logger.debug(f"Successfully deleted {len(assets_to_delete)} check asset(s)")
        except Exception as e:
            error_msg = f"Failed to delete checks for dataset {dataset.name}: {e}"
            logger.error(error_msg)
            dataset_metrics.add_error(error_msg)
            raise
    
    @handle_api_errors
    def _execute_asset_operations(
        self, 
        assets_to_create: List[AssetCreateRequest], 
        assets_to_update: List[AssetUpdateRequest],
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Execute asset creation and update operations.
        
        Args:
            assets_to_create: Assets to create
            assets_to_update: Assets to update
            dataset_metrics: Metrics for this dataset
        """
        # Update existing assets
        if assets_to_update:
            logger.debug(f"Updating {len(assets_to_update)} existing assets in Collibra")
            updated_assets = safe_api_call(self.collibra.change_assets_bulk, assets_to_update)
            self.metrics.get_overall_metrics().checks_updated += len(updated_assets)
            dataset_metrics.checks_updated += len(updated_assets)
            logger.debug(f"Successfully updated {len(updated_assets)} assets")
            
            # Update cache with updated assets
            self._update_asset_cache_with_assets(updated_assets)
        else:
            logger.debug("No assets to update")
        
        # Create new assets
        if assets_to_create:
            logger.debug(f"Creating {len(assets_to_create)} new assets in Collibra")
            created_assets = safe_api_call(self.collibra.add_assets_bulk, assets_to_create)
            self.metrics.get_overall_metrics().checks_created += len(created_assets)
            dataset_metrics.checks_created += len(created_assets)
            logger.debug(f"Successfully created {len(created_assets)} assets")
            
            # Update cache with newly created assets
            self._update_asset_cache_with_assets(created_assets)
        else:
            logger.debug("No assets to create")
    
    def _update_asset_cache_with_assets(self, assets: List) -> None:
        """
        Update the asset cache with newly created or updated assets.
        
        Args:
            assets: List of assets to cache
        """
        for asset in assets:
            # Create cache key with domain_id (Asset has domain.id, not domainId)
            domain_id = asset.domain.id if hasattr(asset, 'domain') and asset.domain else None
            cache_key = f"{asset.name}:{domain_id}" if domain_id else asset.name
            
            # Create a mock search result to match the expected format
            from models.collibra import AssetSearchResponse
            mock_result = AssetSearchResponse(
                results=[asset],
                total=1,
                offset=0,
                limit=1
            )
            
            # Cache the result
            self._asset_cache[cache_key] = mock_result
            logger.debug(f"Updated cache for asset: {cache_key}")
    
    def _process_check_attributes_and_relations(
        self, 
        dataset: FullDataset, 
        checks: List[SodaCheck],
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process attributes and relations for all checks.
        
        Args:
            dataset: Dataset being processed
            checks: List of checks
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"Starting attribute processing for {len(checks)} check(s) in dataset: {dataset.name}")
        
        processed_count = 0
        failed_count = 0
        
        for index, check in enumerate(checks, 1):
            try:
                logger.debug(f"Processing check {index}/{len(checks)}: {check.name} (ID: {check.id})")
                self._process_single_check_attributes_and_relations(dataset, check, dataset_metrics)
                processed_count += 1
                logger.debug(f"Successfully processed check {index}/{len(checks)}: {check.name}")
            except Exception as e:
                failed_count += 1
                error_msg = f"Failed to process check {check.name}: {e}"
                logger.error(error_msg)
                self.metrics.get_overall_metrics().add_error(error_msg)
                dataset_metrics.add_error(error_msg)
        
        logger.debug(f"Completed attribute processing for dataset {dataset.name}: {processed_count} succeeded, {failed_count} failed out of {len(checks)} total")
    
    def _process_single_check_attributes_and_relations(
        self, 
        dataset: FullDataset, 
        check: SodaCheck,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process attributes and relations for a single check.
        
        Args:
            dataset: Dataset being processed
            check: Check to process
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"Processing attributes for check: {check.name}")
        
        # Get Collibra asset ID
        collibra_asset_id = self._get_check_asset_id(check, dataset)
        if not collibra_asset_id:
            return
        
        # Process attributes
        self._process_check_attributes(check, collibra_asset_id, dataset_metrics)
        
        # Process relations
        self._process_check_relations(check, dataset, collibra_asset_id, dataset_metrics)
    
    def _get_check_asset_id(self, check: SodaCheck, dataset: FullDataset) -> Optional[str]:
        """
        Get Collibra asset ID for a check.
        
        Args:
            check: Check to get asset ID for
            dataset: Dataset the check belongs to
            
        Returns:
            Asset ID or None if not found
        """
        database, schema = self._extract_database_and_schema(dataset)
        check_asset_name = generate_asset_name(
            check_name=check.name,
            dataset_name=dataset.name,
            seen_names=set(),
            database=database,
            schema=schema,
            column=check.column
        )
        logger.debug(f"  - Looking up Collibra asset: {check_asset_name}")
        
        # Get the domain ID for this dataset
        domain_id = self._get_domain_id(dataset)
        
        # Search for the asset in the specific domain
        existing_assets = self._find_check_asset(check_asset_name, domain_id)
        
        if not existing_assets or not existing_assets.results:
            error_msg = IntegrationConstants.ERR_NO_COLLIBRA_ASSET.format(check.name)
            logger.debug(f"  - {error_msg}")
            self.metrics.get_overall_metrics().add_error(error_msg)
            return None
        
        asset_id = existing_assets.results[0].id
        logger.debug(f"  - Found Collibra asset ID: {asset_id}")
        return asset_id
    
    @handle_api_errors
    def _process_check_attributes(
        self, 
        check: SodaCheck, 
        collibra_asset_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process attributes for a check.
        
        Args:
            check: Check to process attributes for
            collibra_asset_id: Collibra asset ID
            dataset_metrics: Metrics for this dataset
        """
        # Parse evaluation status to boolean
        result = check.evaluationStatus == "pass"
        logger.debug(f"  - Evaluation status: {check.evaluationStatus} -> {result}")
        
        # Find existing attributes
        logger.debug(f"  - Finding existing attributes for asset {collibra_asset_id}")
        existing_attributes = safe_api_call(self.collibra.find_attributes, asset_id=collibra_asset_id)
        logger.debug(f"  - Found {len(existing_attributes.results)} existing attributes")
        
        existing_attribute_map = {
            attr.type.id: attr.id 
            for attr in existing_attributes.results
        }
        logger.debug(f"  - Existing attribute map: {existing_attribute_map}")
        
        # Prepare attribute values
        current_timestamp = get_current_utc_midnight_timestamp()
        last_check_timestamp = convert_to_utc_midnight_timestamp(check.lastCheckRunTime) if check.lastCheckRunTime else None
        formatted_cloud_url = format_cloud_url(check.cloudUrl)
        formatted_definition = format_check_definition(check.definition) if check.definition else ""
        
        logger.debug(f"  - Current timestamp: {current_timestamp}")
        logger.debug(f"  - Last check timestamp: {last_check_timestamp}")
        logger.debug(f"  - Cloud URL: {check.cloudUrl}")
        if check.definition:
            logger.debug(f"  - Check definition length: {len(check.definition)} characters")
        else:
            logger.debug(f"  - Check definition: None (not provided by API)")
        
        # Define standard attributes
        attribute_definitions = [
            (self.config.collibra.attribute_types.check_evaluation_status_attribute, result),
            (self.config.collibra.attribute_types.check_last_sync_date_attribute, current_timestamp),
            (self.config.collibra.attribute_types.check_cloud_url_attribute, formatted_cloud_url)
        ]
        
        # Add optional attributes only if they have values
        if formatted_definition:
            attribute_definitions.append(
                (self.config.collibra.attribute_types.check_definition_attribute, formatted_definition)
            )
        
        if last_check_timestamp:
            attribute_definitions.append(
                (self.config.collibra.attribute_types.check_last_run_date_attribute, last_check_timestamp)
            )
        
        # Add diagnostic attributes if available
        self._add_diagnostic_attributes(check, attribute_definitions)
        
        logger.debug(f"  - Standard attribute definitions: {len(attribute_definitions)} attributes")
        
        # Add custom attributes
        self._add_custom_attributes(check, attribute_definitions)
        
        # Sort into create/update lists
        attributes_to_create, attributes_to_update = self._sort_attributes(
            attribute_definitions, existing_attribute_map, collibra_asset_id
        )
        
        # Execute attribute operations
        self._execute_attribute_operations(
            attributes_to_create, attributes_to_update, dataset_metrics
        )
    
    def _add_custom_attributes(self, check: SodaCheck, attribute_definitions: List) -> None:
        """
        Add custom attributes to attribute definitions.
        Handles both regular custom attributes and special attributes like "threshold".
        For threshold, only pushes percentage-based thresholds (converted to 0-1 range).
        
        Args:
            check: Check to get custom attributes from
            attribute_definitions: List to add custom attributes to
        """
        # Get cached custom attributes mapping
        if self._custom_attributes_cache is None:
            self._custom_attributes_cache = get_custom_attributes_mapping(
                self.config.soda.attributes.custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id
            )
        
        logger.debug(f"  - Custom attributes mapping: {self._custom_attributes_cache}")
        
        # Handle special attributes like "threshold"
        for soda_attr_name, collibra_type_id in self._custom_attributes_cache.items():
            if soda_attr_name == "threshold":
                logger.debug(f"  - Processing threshold attribute for check: {check.name}")
                
                # First, try to extract threshold value (we'll check if it's percentage after)
                threshold_value = self._extract_threshold_value(check)
                logger.debug(f"  - Extracted threshold value: {threshold_value}")
                
                if threshold_value:
                    # Check if it's a percentage-based threshold
                    is_percent = self._is_percentage_threshold(check)
                    
                    # Fallback: If check type typically uses percentages and we found a threshold, assume it's percentage
                    if not is_percent:
                        percentage_check_types = ['missing', 'duplicate', 'invalid', 'failed_rows']
                        if hasattr(check, 'checkType') and check.checkType:
                            check_type_lower = check.checkType.lower()
                            if any(ct in check_type_lower for ct in percentage_check_types):
                                logger.debug(f"  - Fallback: Check type '{check.checkType}' typically uses percentages - assuming percentage")
                                is_percent = True
                    
                    logger.debug(f"  - Is percentage threshold: {is_percent}")
                    
                    if is_percent:
                        # Sanitize and convert percentage to 0-1 range
                        sanitized_value = self._sanitize_threshold_value(threshold_value)
                        logger.debug(f"  - Sanitized threshold value: {sanitized_value}")
                        
                        if sanitized_value:
                            # Convert percentage to 0-1 range (e.g., 5% -> 0.05)
                            try:
                                numeric_value = float(sanitized_value)
                                logger.debug(f"  - Numeric threshold value: {numeric_value}")
                                
                                # Validate it's a reasonable percentage value (0-100)
                                if 0 <= numeric_value <= 100:
                                    decimal_value = numeric_value / 100.0
                                    # Format to avoid scientific notation and ensure proper decimal
                                    decimal_str = f"{decimal_value:.10f}".rstrip('0').rstrip('.')
                                    logger.info(f"  - ✅ Pushing percentage threshold: {threshold_value}% -> {decimal_str} (0-1 range) for check: {check.name}")
                                    attribute_definitions.append(
                                        (collibra_type_id, decimal_str)
                                    )
                                else:
                                    logger.warning(f"  - ⚠️ Threshold value '{threshold_value}' is not a valid percentage (0-100) for check: {check.name}")
                            except ValueError as e:
                                logger.warning(f"  - ⚠️ Threshold value '{threshold_value}' could not be converted to numeric: {e}")
                        else:
                            logger.warning(f"  - ⚠️ Threshold value '{threshold_value}' could not be sanitized to numeric format for check: {check.name}")
                    else:
                        logger.debug(f"  - Threshold is not percentage-based, skipping for check: {check.name}")
                else:
                    logger.debug(f"  - No threshold value found in check: {check.name}")
            elif hasattr(check, 'attributes') and check.attributes and soda_attr_name in check.attributes:
                # Regular custom attributes from check.attributes
                    logger.debug(f"  - Found custom attribute: {soda_attr_name} = {check.attributes[soda_attr_name]}")
                    attribute_definitions.append(
                        (collibra_type_id, check.attributes[soda_attr_name])
                    )
    
    def _is_percentage_threshold(self, check: SodaCheck) -> bool:
        """
        Check if the threshold is percentage-based (has 'metric: percent' in definition).
        
        Args:
            check: Check to check threshold type for
            
        Returns:
            True if threshold is percentage-based, False otherwise
        """
        logger.debug(f"  - Checking if threshold is percentage-based for check: {check.name}")
        
        if hasattr(check, 'definition') and check.definition:
            logger.debug(f"  - Check definition (first 500 chars): {check.definition[:500]}")
            # Check for 'metric: percent' or 'metric:percent' in the definition
            # Handle various formats: "metric: percent", "metric:percent", "metric:  percent", etc.
            # Also handle cases where it might be on the same line or different lines
            definition_lower = check.definition.lower()
            
            # Check for metric: percent pattern (most common)
            if re.search(r'metric\s*:\s*percent', definition_lower):
                logger.debug(f"  - Found percentage threshold in check definition (metric: percent)")
                return True
            
            # Check for metric percent without colon (less common but possible)
            if re.search(r'metric\s+percent', definition_lower):
                logger.debug(f"  - Found percentage threshold in check definition (metric percent)")
                return True
            
            # Also check if threshold section exists and look for percentage indicators nearby
            # This handles cases where the format might be slightly different
            if 'threshold' in definition_lower and 'percent' in definition_lower:
                # Check if percent appears near threshold (within reasonable distance)
                threshold_pos = definition_lower.find('threshold')
                percent_pos = definition_lower.find('percent')
                if threshold_pos != -1 and percent_pos != -1:
                    # Check if they're within 200 characters of each other
                    if abs(threshold_pos - percent_pos) < 200:
                        logger.debug(f"  - Found percentage threshold in check definition (threshold and percent nearby)")
                        return True
        
        # Also check diagnostics for percentage metric
        if check.lastCheckResultValue and check.lastCheckResultValue.diagnostics:
            diagnostics = check.lastCheckResultValue.diagnostics
            logger.debug(f"  - Checking diagnostics for percentage threshold: {diagnostics}")
            for diagnostic_type, diagnostic_data in diagnostics.items():
                if isinstance(diagnostic_data, dict):
                    # Check for metric: percent in threshold config
                    if 'threshold' in diagnostic_data:
                        threshold = diagnostic_data['threshold']
                        if isinstance(threshold, dict):
                            metric = threshold.get('metric')
                            logger.debug(f"  - Found threshold in diagnostics with metric: {metric}")
                            if metric == 'percent':
                                logger.debug(f"  - Found percentage threshold in diagnostics")
                                return True
                    # Check for metric: percent in fail config
                    if 'fail' in diagnostic_data:
                        fail_config = diagnostic_data['fail']
                        if isinstance(fail_config, dict):
                            metric = fail_config.get('metric')
                            logger.debug(f"  - Found fail config in diagnostics with metric: {metric}")
                            if metric == 'percent':
                                logger.debug(f"  - Found percentage threshold in fail config")
                                return True
        
        # Fallback: Check if this is a check type that typically uses percentages
        # and has a threshold defined. Common percentage-based check types:
        # - missing, duplicate, invalid, failed_rows (with expression)
        percentage_check_types = ['missing', 'duplicate', 'invalid', 'failed_rows']
        if hasattr(check, 'checkType') and check.checkType:
            check_type_lower = check.checkType.lower()
            if any(ct in check_type_lower for ct in percentage_check_types):
                # If there's a threshold in the definition, assume it's percentage-based
                # (since these check types typically use percentages)
                if hasattr(check, 'definition') and check.definition:
                    if 'threshold' in check.definition.lower():
                        logger.debug(f"  - Fallback: Check type '{check.checkType}' typically uses percentages and has threshold - assuming percentage")
                        return True
        
        logger.debug(f"  - Threshold is NOT percentage-based")
        return False
    
    def _extract_threshold_value(self, check: SodaCheck) -> Optional[str]:
        """
        Extract threshold value from check result.
        Only extracts numeric values (assumes percentage threshold has been verified).
        
        Args:
            check: Check to extract threshold from
            
        Returns:
            Threshold value as string (numeric), or None if not available
        """
        # Try to extract threshold from diagnostics first
        if check.lastCheckResultValue and check.lastCheckResultValue.diagnostics:
            diagnostics = check.lastCheckResultValue.diagnostics
            # Look for threshold information in diagnostics
            for diagnostic_type, diagnostic_data in diagnostics.items():
                if isinstance(diagnostic_data, dict):
                    # Check for threshold-related fields in fail config
                    if 'fail' in diagnostic_data:
                        fail_config = diagnostic_data['fail']
                        if isinstance(fail_config, dict):
                            # Extract numeric values only
                            for key, value in fail_config.items():
                                if key in ['greaterThan', 'lessThan', 'greaterThanOrEqual', 'lessThanOrEqual', 
                                          'mustBe', 'mustNotBe']:
                                    # Extract numeric value
                                    if isinstance(value, (int, float)):
                                        return str(value)
                    # Also check for threshold at top level
                    if 'threshold' in diagnostic_data:
                        threshold = diagnostic_data['threshold']
                        if isinstance(threshold, dict):
                            # Extract numeric values from threshold dict
                            for key, value in threshold.items():
                                if key in ['must_be', 'must_be_greater_than', 'must_be_less_than', 
                                          'greater_than', 'less_than']:
                                    if isinstance(value, (int, float)):
                                        return str(value)
                                    elif isinstance(value, dict):
                                        # Handle nested threshold like must_be_between - take first value
                                        for nested_key, nested_value in value.items():
                                            if isinstance(nested_value, (int, float)):
                                                return str(nested_value)
                        elif isinstance(threshold, (int, float)):
                            return str(threshold)
        
        # If no threshold in diagnostics, try to extract from definition
        # Extract numeric values from threshold patterns in YAML definition
        if hasattr(check, 'definition') and check.definition:
            logger.debug(f"  - Extracting threshold from definition: {check.definition[:500]}")
            # Look for threshold patterns and extract numeric values
            # Priority: specific threshold configs first, then generic patterns
            # Handle both multi-line YAML format and single-line format
            threshold_patterns = [
                # Multi-line YAML format with metric: percent in between:
                # threshold:\n    metric: percent\n    must_be: 0
                (r'threshold\s*:\s*\n\s+metric\s*:\s*percent\s*\n\s+must_be\s*:\s*([0-9.]+)', 1),
                (r'threshold\s*:\s*\n\s+metric\s*:\s*percent\s*\n\s+must_be_greater_than\s*:\s*([0-9.]+)', 1),
                (r'threshold\s*:\s*\n\s+metric\s*:\s*percent\s*\n\s+must_be_less_than\s*:\s*([0-9.]+)', 1),
                # Multi-line YAML format with indentation: threshold:\n      must_be: 0
                (r'threshold\s*:\s*\n\s+must_be\s*:\s*([0-9.]+)', 1),  # threshold:\n      must_be: 0
                (r'threshold\s*:\s*\n\s+must_be_greater_than\s*:\s*([0-9.]+)', 1),  # threshold:\n      must_be_greater_than: 80
                (r'threshold\s*:\s*\n\s+must_be_less_than\s*:\s*([0-9.]+)', 1),  # threshold:\n      must_be_less_than: 5
                # Multi-line YAML format without extra indentation: threshold:\n    must_be: 0
                (r'threshold\s*:\s*\n\s*must_be\s*:\s*([0-9.]+)', 1),  # threshold: must_be: 0
                (r'threshold\s*:\s*\n\s*must_be_greater_than\s*:\s*([0-9.]+)', 1),  # threshold: must_be_greater_than: 80
                (r'threshold\s*:\s*\n\s*must_be_less_than\s*:\s*([0-9.]+)', 1),  # threshold: must_be_less_than: 5
                # Single-line format: threshold: must_be: 0 or threshold: must_be_less_than: 5
                (r'threshold\s*:\s*must_be\s*:\s*([0-9.]+)', 1),
                (r'threshold\s*:\s*must_be_greater_than\s*:\s*([0-9.]+)', 1),
                (r'threshold\s*:\s*must_be_less_than\s*:\s*([0-9.]+)', 1),
                # Direct threshold values in check definition (without "threshold:" prefix)
                # Look for patterns like "  - missing:" followed by "must_be_less_than: 5"
                (r'must_be\s*:\s*([0-9.]+)', 1),  # must_be: 0
                (r'must_be_greater_than\s*:\s*([0-9.]+)', 1),  # must_be_greater_than: 80
                (r'must_be_less_than\s*:\s*([0-9.]+)', 1),  # must_be_less_than: 5
            ]
            for pattern, groups in threshold_patterns:
                match = re.search(pattern, check.definition, re.IGNORECASE | re.MULTILINE)
                if match:
                    try:
                        # Single group - return just the numeric value
                        numeric_value = match.group(groups).strip()
                        logger.debug(f"  - Extracted threshold value from definition: {numeric_value} (pattern: {pattern})")
                        # Validate it's a valid number
                        float(numeric_value)  # This will raise ValueError if not numeric
                        return numeric_value
                    except (ValueError, IndexError) as e:
                        logger.debug(f"  - Failed to extract numeric value from match: {e}")
                        # If extraction failed, try to extract number from the matched text
                        matched_text = match.group(0)
                        number_match = re.search(r'([0-9.]+)', matched_text)
                        if number_match:
                            numeric_value = number_match.group(1)
                            try:
                                float(numeric_value)
                                logger.debug(f"  - Extracted threshold value from matched text: {numeric_value}")
                                return numeric_value
                            except ValueError:
                                continue
                        continue
        
        logger.debug(f"  - No threshold value found in check definition or diagnostics")
        return None
    
    def _sanitize_threshold_value(self, threshold_value: str) -> Optional[str]:
        """
        Sanitize threshold value to extract just the numeric value.
        Removes operators and extracts just the numeric value.
        Assumes this is called only for percentage thresholds.
        
        Args:
            threshold_value: Raw threshold value string
            
        Returns:
            Sanitized numeric value as string, or None if not valid
        """
        if not threshold_value:
            return None
        
        # Remove common operators and whitespace
        sanitized = threshold_value.strip()
        
        # Extract numeric value from string (removes operators like >, <, =, etc.)
        number_match = re.search(r'([0-9.]+)', sanitized)
        if number_match:
            numeric_value = number_match.group(1)
            try:
                # Validate it's a valid number
                float(numeric_value)
                return numeric_value
            except ValueError:
                return None
        
        # If already a valid number, return as-is
        try:
            float(sanitized)
            return sanitized
        except ValueError:
            return None
    
    def _add_diagnostic_attributes(self, check: SodaCheck, attribute_definitions: List) -> None:
        """
        Add diagnostic attributes from check result value.
        Only adds attributes when actual diagnostic values are available from the API.
        Does not push default values (0) when diagnostics aren't available.
        
        Args:
            check: Check to get diagnostic attributes from
            attribute_definitions: List to add diagnostic attributes to
        """
        # Initialize with default values (0) - will be updated if diagnostics are available
        failed_rows_count = 0
        check_rows_tested = 0
        
        # Try to extract diagnostics from API
        if check.lastCheckResultValue and check.lastCheckResultValue.diagnostics:
            diagnostics = check.lastCheckResultValue.diagnostics
            logger.debug(f"  - Processing diagnostics: {diagnostics}")
            
            # Extract metrics from any diagnostic type - be flexible for future types
            for diagnostic_type, diagnostic_data in diagnostics.items():
                if not isinstance(diagnostic_data, dict):
                    continue
                    
                logger.debug(f"  - Checking diagnostic type '{diagnostic_type}' for metrics")
                
                # Look for failedRowsCount in any diagnostic type
                if failed_rows_count == 0 and 'failedRowsCount' in diagnostic_data:
                    failed_rows_count = diagnostic_data['failedRowsCount']
                    logger.debug(f"  - Found failedRowsCount in '{diagnostic_type}': {failed_rows_count}")
                
                # Look for checkRowsTested in any diagnostic type
                if check_rows_tested == 0 and 'checkRowsTested' in diagnostic_data:
                    check_rows_tested = diagnostic_data['checkRowsTested']
                    logger.debug(f"  - Found checkRowsTested in '{diagnostic_type}': {check_rows_tested}")
                
                # Fallback to datasetRowsTested if checkRowsTested not found yet
                if check_rows_tested == 0 and 'datasetRowsTested' in diagnostic_data:
                    check_rows_tested = diagnostic_data['datasetRowsTested']
                    logger.debug(f"  - Using datasetRowsTested from '{diagnostic_type}' as fallback: {check_rows_tested}")
        else:
            if not check.lastCheckResultValue:
                logger.debug(f"  - No lastCheckResultValue for check '{check.name}' - using default values (0)")
            elif not check.lastCheckResultValue.diagnostics:
                logger.debug(f"  - No diagnostics available for check '{check.name}' - using default values (0)")
                logger.debug(f"  - Check type: {check.checkType}, Metric type: {check.metricType}")
        
        logger.debug(f"  - Final values: failed_rows_count={failed_rows_count}, check_rows_tested={check_rows_tested}")
        
        # Only add diagnostic attributes if we have actual values from diagnostics
        # Don't push default values (0) when diagnostics aren't available
        if check_rows_tested > 0:
            # Add loaded rows attribute only if we have actual data
            attribute_definitions.append(
                (self.config.collibra.attribute_types.check_loaded_rows_attribute, check_rows_tested)
            )
            logger.debug(f"  - Added check_loaded_rows_attribute: {check_rows_tested}")
            
            # Add failed rows attribute only if we have actual data
            if failed_rows_count > 0:
                attribute_definitions.append(
                    (self.config.collibra.attribute_types.check_rows_failed_attribute, failed_rows_count)
                )
                logger.debug(f"  - Added check_rows_failed_attribute: {failed_rows_count}")
            
            # Calculate and add derived metrics only if we have valid values
                check_rows_passed = check_rows_tested - failed_rows_count
                check_passing_fraction_raw = check_rows_passed / check_rows_tested
                check_passing_fraction = f"{check_passing_fraction_raw * 100:.2f}"
                
                logger.debug(f"  - Calculated check_rows_passed: {check_rows_passed}")
                logger.debug(f"  - Calculated check_passing_fraction: {check_passing_fraction}")
                
                attribute_definitions.append(
                    (self.config.collibra.attribute_types.check_rows_passed_attribute, check_rows_passed)
                )
                attribute_definitions.append(
                    (self.config.collibra.attribute_types.check_passing_fraction_attribute, check_passing_fraction)
                )
        else:
            logger.debug(f"  - Skipping diagnostic attributes (no rows tested, no diagnostics available)")
    
    def _sort_attributes(
        self, 
        attribute_definitions: List, 
        existing_attribute_map: Dict, 
        collibra_asset_id: str
    ) -> Tuple[List[AttributeCreateRequest], List[AttributeUpdateRequest]]:
        """
        Sort attributes into create and update lists.
        
        Args:
            attribute_definitions: List of attribute definitions
            existing_attribute_map: Map of existing attributes
            collibra_asset_id: Asset ID
            
        Returns:
            Tuple of (attributes_to_create, attributes_to_update)
        """
        attributes_to_create = []
        attributes_to_update = []
        
        logger.debug(f"  - Sorting attributes into create/update lists")
        
        for type_id, value in attribute_definitions:
            if type_id in existing_attribute_map:
                logger.debug(f"  - Attribute {type_id} exists - will update")
                attributes_to_update.append(
                    AttributeUpdateRequest(
                        id=existing_attribute_map[type_id],
                        value=value
                    )
                )
            else:
                logger.debug(f"  - Attribute {type_id} does not exist - will create")
                attributes_to_create.append(
                    AttributeCreateRequest(
                        assetId=collibra_asset_id,
                        typeId=type_id,
                        value=value
                    )
                )
        
        logger.debug(f"  - Attributes to create: {len(attributes_to_create)}")
        logger.debug(f"  - Attributes to update: {len(attributes_to_update)}")
        
        return attributes_to_create, attributes_to_update
    
    @handle_api_errors
    def _execute_attribute_operations(
        self, 
        attributes_to_create: List[AttributeCreateRequest], 
        attributes_to_update: List[AttributeUpdateRequest],
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Execute attribute creation and update operations.
        
        Args:
            attributes_to_create: Attributes to create
            attributes_to_update: Attributes to update
            dataset_metrics: Metrics for this dataset
        """
        # Update existing attributes
        if attributes_to_update:
            logger.debug(f"  - Updating {len(attributes_to_update)} existing attributes")
            safe_api_call(self.collibra.change_attributes_bulk, attributes_to_update)
            self.metrics.get_overall_metrics().attributes_updated += len(attributes_to_update)
            dataset_metrics.attributes_processed += len(attributes_to_update)
            logger.debug(f"  - Successfully updated {len(attributes_to_update)} attributes")
        else:
            logger.debug(f"  - No attributes to update")
        
        # Create new attributes
        if attributes_to_create:
            logger.debug(f"  - Creating {len(attributes_to_create)} new attributes")
            safe_api_call(self.collibra.add_attributes_bulk, attributes_to_create)
            self.metrics.get_overall_metrics().attributes_created += len(attributes_to_create)
            dataset_metrics.attributes_processed += len(attributes_to_create)
            logger.debug(f"  - Successfully created {len(attributes_to_create)} attributes")
        else:
            logger.debug(f"  - No attributes to create")
    
    def _process_check_relations(
        self, 
        check: SodaCheck, 
        dataset: FullDataset, 
        collibra_asset_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process relations for a check.
        
        Args:
            check: Check to process relations for
            dataset: Dataset the check belongs to
            collibra_asset_id: Collibra asset ID
            dataset_metrics: Metrics for this dataset
        """
        # Process dimension relation
        self._process_dimension_relation(check, collibra_asset_id, dataset_metrics)
        
        # Process table/column relation
        self._process_table_column_relation(check, dataset, collibra_asset_id, dataset_metrics)
    
    @handle_api_errors
    def _process_dimension_relation(
        self, 
        check: SodaCheck, 
        collibra_asset_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process dimension relation for a check.
        Supports both single dimension values (strings) and multiple dimensions (comma-separated strings).
        
        Args:
            check: Check to process
            collibra_asset_id: Collibra asset ID
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"  - Processing dimension relations")
        
        if not (hasattr(check, 'attributes') and check.attributes and 
                self.config.soda.attributes.soda_dimension_attribute_name in check.attributes):
            logger.debug(f"  - No dimension attribute found - skipping dimension relation")
            return
        
        dimension_value = check.attributes[self.config.soda.attributes.soda_dimension_attribute_name]
        logger.debug(f"  - Found dimension value: {dimension_value} (type: {type(dimension_value).__name__})")
        
        # Handle both single dimension (string) and multiple dimensions (comma-separated string)
        if isinstance(dimension_value, str) and ',' in dimension_value:
            # Comma-separated string - split and trim whitespace, filter out empty values
            dimension_values = [dim.strip() for dim in dimension_value.split(',') if dim.strip()]
            logger.debug(f"  - Split comma-separated string into {len(dimension_values)} dimension(s)")
        elif isinstance(dimension_value, str):
            # Single dimension value
            dimension_values = [dimension_value.strip()] if dimension_value.strip() else []
        else:
            # Unexpected type - log and skip
            logger.warning(f"  - Unexpected dimension value type: {type(dimension_value).__name__}, expected string")
            return
        
        if not dimension_values:
            logger.debug(f"  - No valid dimensions found after parsing")
            return
        
        logger.debug(f"  - Processing {len(dimension_values)} dimension(s): {dimension_values}")
        
        # Collect all valid dimension asset IDs to create relations in batch
        valid_dimension_asset_ids = []
        
        for dim_value in dimension_values:
            logger.debug(f"  - Searching for dimension asset: {dim_value}")
            dimension_assets = safe_api_call(
                self.collibra.find_asset,
                name=dim_value,
                domain_id=self.config.collibra.domains.data_quality_dimensions_domain,
                type_id=self.config.collibra.asset_types.dimension_asset_type,
                name_match_mode="EXACT"
            )
            logger.debug(f"  - Found {len(dimension_assets.results)} dimension assets for: {dim_value}")
            
            if dimension_assets.results and len(dimension_assets.results) == 1:
                dimension_asset = dimension_assets.results[0]
                logger.debug(f"  - Found dimension asset: {dimension_asset.displayName}")
                valid_dimension_asset_ids.append(dimension_asset.id)
            elif not dimension_assets.results:
                # No dimension asset found - add warning to metrics
                error_msg = f"No dimension asset found in Collibra for dimension: {dim_value}"
                logger.debug(f"  - {error_msg}")
                dataset_metrics.add_error(error_msg)
            else:
                # Multiple dimension assets found - add warning to metrics  
                error_msg = f"Multiple dimension assets found in Collibra for dimension: {dim_value} (found {len(dimension_assets.results)})"
                logger.debug(f"  - {error_msg}")
                dataset_metrics.add_error(error_msg)
        
        # Create relations for all valid dimension assets found
        if valid_dimension_asset_ids:
            logger.debug(f"  - Creating relations to {len(valid_dimension_asset_ids)} dimension asset(s)")
            safe_api_call(
                self.collibra.set_relations,
                asset_id=collibra_asset_id,
                type_id=self.config.collibra.relation_types.check_to_dq_dimension_relation_type,
                related_asset_ids=valid_dimension_asset_ids,
                relation_direction="TO_TARGET"
            )
            
            # Update metrics for each relation created
            self.metrics.get_overall_metrics().dimension_relations_created += len(valid_dimension_asset_ids)
            dataset_metrics.relations_created += len(valid_dimension_asset_ids)
            logger.debug(f"  - Successfully created {len(valid_dimension_asset_ids)} dimension relation(s)")
    
    @handle_api_errors
    def _process_table_column_relation(
        self, 
        check: SodaCheck, 
        dataset: FullDataset, 
        collibra_asset_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Process table/column relation for a check.
        
        Args:
            check: Check to process
            dataset: Dataset the check belongs to
            collibra_asset_id: Collibra asset ID
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"  - Processing table/column relations")
        
        # Get table asset
        dataset_full_name = generate_dataset_full_name(dataset, self.config)
        logger.debug(f"  - Looking up table asset: {dataset_full_name}")
        
        collibra_assets = safe_api_call(
            self.collibra.find_asset,
            name=dataset_full_name,
            type_id=self.config.collibra.asset_types.table_asset_type
        )
        logger.debug(f"  - Found {len(collibra_assets.results)} table assets")
        
        if not collibra_assets.results:
            # Only log this error once per dataset to avoid spam
            if dataset.name not in self._datasets_without_table_assets:
                error_msg = IntegrationConstants.ERR_NO_TABLE_ASSET.format(dataset.name)
                self.metrics.get_overall_metrics().add_error(error_msg)
                self.metrics.get_overall_metrics().datasets_without_table_assets += 1
                self._datasets_without_table_assets.add(dataset.name)
                print(f"  ⚠️ {error_msg}")
            return
        elif len(collibra_assets.results) > 1:
            error_msg = IntegrationConstants.ERR_MULTIPLE_TABLE_ASSETS.format(dataset.name)
            logger.debug(f"  - {error_msg}")
            self.metrics.get_overall_metrics().add_error(error_msg)
            return
        
        collibra_table = collibra_assets.results[0]
        logger.debug(f"  - Using table asset: {collibra_table.displayName}")
        
        # Check for column relation
        if hasattr(check, 'column') and check.column:
            self._create_column_relation(check, collibra_table, collibra_asset_id, dataset_metrics)
        else:
            self._create_table_relation(collibra_table, collibra_asset_id, dataset_metrics)
    
    @handle_api_errors
    def _create_column_relation(
        self, 
        check: SodaCheck, 
        collibra_table, 
        collibra_asset_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Create column relation for a check.
        
        Args:
            check: Check to process
            collibra_table: Collibra table asset
            collibra_asset_id: Check asset ID
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"  - Check has column info: {check.column}")
        
        column_full_name = generate_column_full_name(
            collibra_table.name, check.column, self.config
        )
        logger.debug(f"  - Looking up column asset: {column_full_name}")
        
        column_assets = safe_api_call(
            self.collibra.find_asset,
            name=column_full_name,
            type_id=self.config.collibra.asset_types.column_asset_type,
            name_match_mode="EXACT"
        )
        logger.debug(f"  - Found {len(column_assets.results)} column assets")
        
        if len(column_assets.results) == 1:
            column_asset = column_assets.results[0]
            logger.debug(f"  - Creating relation to column asset: {column_asset.displayName}")
            
            safe_api_call(
                self.collibra.set_relations,
                asset_id=collibra_asset_id,
                type_id=self.config.collibra.relation_types.table_column_to_check_relation_type,
                related_asset_ids=[column_asset.id],
                relation_direction="TO_SOURCE"
            )
            
            self.metrics.get_overall_metrics().column_relations_created += 1
            dataset_metrics.relations_created += 1
            logger.debug(f"  - Successfully created column relation")
        else:
            logger.debug(f"  - Creating relation to table asset (column not found or multiple found)")
            self._create_table_relation(collibra_table, collibra_asset_id, dataset_metrics)
    
    @handle_api_errors
    def _create_table_relation(
        self, 
        collibra_table, 
        collibra_asset_id: str,
        dataset_metrics: DatasetMetrics
    ) -> None:
        """
        Create table relation for a check.
        
        Args:
            collibra_table: Collibra table asset
            collibra_asset_id: Check asset ID
            dataset_metrics: Metrics for this dataset
        """
        logger.debug(f"  - No column info - creating relation to table asset")
        
        safe_api_call(
            self.collibra.set_relations,
            asset_id=collibra_asset_id,
            type_id=self.config.collibra.relation_types.table_column_to_check_relation_type,
            related_asset_ids=[collibra_table.id],
            relation_direction="TO_SOURCE"
        )
        
        self.metrics.get_overall_metrics().table_relations_created += 1
        dataset_metrics.relations_created += 1
        logger.debug(f"  - Successfully created table relation")
    
    def _print_summary(self) -> None:
        """Print integration summary"""
        metrics = self.metrics.get_overall_metrics()
        
        print("\n" + "="*60)
        print(IntegrationConstants.MSG_INTEGRATION_COMPLETE)
        print("="*60)
        
        print(f"{IntegrationConstants.SUMMARY_DATASETS_PROCESSED}: {metrics.datasets_processed}")
        print(f"{IntegrationConstants.SUMMARY_DATASETS_SKIPPED}: {metrics.datasets_skipped}")
        print(f"{IntegrationConstants.SUMMARY_DATASETS_NO_TABLE_ASSET}: {metrics.datasets_without_table_assets}")
        print(f"{IntegrationConstants.SUMMARY_CHECKS_CREATED}: {metrics.checks_created}")
        print(f"{IntegrationConstants.SUMMARY_CHECKS_UPDATED}: {metrics.checks_updated}")
        print(f"{IntegrationConstants.SUMMARY_CHECKS_DELETED}: {metrics.checks_deleted}")
        print(f"{IntegrationConstants.SUMMARY_ATTRIBUTES_CREATED}: {metrics.attributes_created}")
        print(f"{IntegrationConstants.SUMMARY_ATTRIBUTES_UPDATED}: {metrics.attributes_updated}")
        print(f"{IntegrationConstants.SUMMARY_DIMENSION_RELATIONS}: {metrics.dimension_relations_created}")
        print(f"{IntegrationConstants.SUMMARY_TABLE_RELATIONS}: {metrics.table_relations_created}")
        print(f"{IntegrationConstants.SUMMARY_COLUMN_RELATIONS}: {metrics.column_relations_created}")
        print(f"{IntegrationConstants.SUMMARY_OWNERS_SYNCED}: {metrics.owners_synced}")
        if metrics.ownership_sync_failed > 0:
            print(f"{IntegrationConstants.SUMMARY_OWNERSHIP_SYNC_FAILED}: {metrics.ownership_sync_failed}")
        if metrics.dimension_sync_failed > 0:
            print(f"{IntegrationConstants.SUMMARY_DIMENSION_SYNC_FAILED}: {metrics.dimension_sync_failed}")
        
        # Performance summary
        print(f"\n⚡ Performance Summary:")
        print(f"   • Runtime: {metrics.duration:.2f} seconds")
        print(f"   • Datasets/second: {metrics.datasets_per_second:.2f}")  
        print(f"   • Checks/second: {metrics.checks_per_second:.2f}")
        print(f"   • API calls made: {metrics.api_calls_made}")
        if metrics.api_calls_failed > 0:
            print(f"   • API calls failed: {metrics.api_calls_failed}")
        print(f"   • Success rate: {metrics.success_rate:.1f}%")
        
        if metrics.errors:
            print(f"\n{IntegrationConstants.SUMMARY_ERRORS} ({len(metrics.errors)}):")
            for error in metrics.errors:
                print(f"   • {error}")
        
        print(f"\n{IntegrationConstants.SUMMARY_TOTAL_OPERATIONS}: {metrics.total_operations}")
        print("="*60)

    @handle_api_errors
    def _synchronize_dataset_ownership(self, dataset: FullDataset, dataset_metrics) -> None:
        """
        Synchronize dataset ownership from Collibra to Soda.
        
        Args:
            dataset: The Soda dataset to sync ownership for
            dataset_metrics: Dataset metrics tracker
        """
        logger.debug(f"Starting ownership synchronization for dataset: {dataset.name}")
        
        # Find the corresponding Collibra asset
        collibra_asset_id = self._find_collibra_asset_for_dataset(dataset)
        if not collibra_asset_id:
            logger.debug(f"No corresponding Collibra asset found for dataset: {dataset.name}")
            dataset_metrics.add_error("No Collibra asset found for ownership sync")
            return
        
        # Get responsibilities from Collibra
        responsibilities = self._get_collibra_responsibilities(collibra_asset_id)
        if not responsibilities.results:
            logger.debug(f"No owner responsibilities found in Collibra for dataset: {dataset.name}")
            print(f"  ℹ️ No owners found in Collibra for dataset: {dataset.name}")
            return
        
        # Extract owner information from responsibilities
        collibra_owners = self._extract_owners_from_responsibilities(responsibilities)
        logger.debug(f"Found {len(collibra_owners)} owners in Collibra")
        
        # Map Collibra owners to Soda users
        soda_user_ids, missing_emails = self._map_collibra_owners_to_soda_users(collibra_owners)
        
        # Add missing email errors to dataset metrics
        for email in missing_emails:
            error_msg = f"No Soda user found with email: {email}"
            dataset_metrics.add_error(error_msg)
            logger.debug(f"Added error to dataset metrics: {error_msg}")
        
        if missing_emails:
            logger.debug(f"Added {len(missing_emails)} missing email errors for dataset: {dataset.name}")
        
        if not soda_user_ids:
            error_msg = f"No matching Soda users found for Collibra owners"
            logger.error(error_msg)
            dataset_metrics.add_error(error_msg)
            return
        
        # Update dataset ownership in Soda
        self._update_soda_dataset_ownership(dataset, soda_user_ids, dataset_metrics)
        
        logger.debug(f"Ownership synchronization completed for dataset: {dataset.name}. Dataset metrics owners_synced: {dataset_metrics.owners_synced}")

    def _find_collibra_asset_for_dataset(self, dataset: FullDataset) -> Optional[str]:
        """
        Find the corresponding Collibra asset ID for a Soda dataset.
        
        Args:
            dataset: The Soda dataset
            
        Returns:
            Collibra asset ID if found, None otherwise
        """
        dataset_full_name = generate_dataset_full_name(dataset, self.config)
        logger.debug(f"Searching for Collibra asset with name: {dataset_full_name}")
        
        try:
            collibra_assets = safe_api_call(
                self.collibra.find_asset,
                name=dataset_full_name,
                type_id=self.config.collibra.asset_types.table_asset_type
            )
            
            if len(collibra_assets.results) == 1:
                asset_id = collibra_assets.results[0].id
                logger.debug(f"Found Collibra asset ID: {asset_id}")
                return asset_id
            elif len(collibra_assets.results) > 1:
                logger.warning(f"Multiple Collibra assets found for dataset: {dataset.name}")
                return None
            else:
                logger.debug(f"No Collibra asset found for dataset: {dataset.name}")
                return None
                
        except Exception as e:
            logger.error(f"Error searching for Collibra asset: {e}")
            return None

    def _get_collibra_responsibilities(self, asset_id: str) -> ResponsibilitySearchResponse:
        """
        Get owner responsibilities from Collibra for an asset.
        
        Args:
            asset_id: The Collibra asset ID
            
        Returns:
            ResponsibilitySearchResponse containing owner information
        """
        owner_role_id = self.config.collibra.responsibilities.owner_role_id
        logger.debug(f"Getting responsibilities for asset {asset_id} with role {owner_role_id}")
        
        return safe_api_call(
            self.collibra.get_responsibilities,
            resource_id=asset_id,
            role_id=owner_role_id
        )

    def _extract_owners_from_responsibilities(self, responsibilities: ResponsibilitySearchResponse) -> List[Dict]:
        """
        Extract owner information from Collibra responsibilities.
        
        Args:
            responsibilities: Responsibility search response from Collibra
            
        Returns:
            List of owner dictionaries with id and type information
        """
        owners = []
        
        for responsibility in responsibilities.results:
            owner_info = {
                'id': responsibility.owner.id,
                'type': responsibility.owner.resourceType  # 'User' or 'UserGroup'
            }
            owners.append(owner_info)
            logger.debug(f"Found owner: {owner_info}")
        
        return owners

    def _map_collibra_owners_to_soda_users(self, collibra_owners: List[Dict]) -> Tuple[List[str], List[str]]:
        """
        Map Collibra owners to Soda user IDs.
        
        Args:
            collibra_owners: List of Collibra owner information
            
        Returns:
            Tuple of (List of Soda user IDs, List of missing emails)
        """
        soda_user_ids = []
        missing_emails = []
        
        for owner in collibra_owners:
            if owner['type'] == 'User':
                # Direct user mapping
                user_emails = self._get_collibra_user_emails([owner['id']])
                soda_users, missing = self._find_soda_users_by_emails(user_emails)
                soda_user_ids.extend(soda_users)
                missing_emails.extend(missing)
                
            elif owner['type'] == 'UserGroup':
                # Get users from user group
                group_user_emails = self._get_collibra_group_user_emails(owner['id'])
                soda_users, missing = self._find_soda_users_by_emails(group_user_emails)
                soda_user_ids.extend(soda_users)
                missing_emails.extend(missing)
        
        # Remove duplicates
        soda_user_ids = list(set(soda_user_ids))
        missing_emails = list(set(missing_emails))
        logger.debug(f"Mapped to {len(soda_user_ids)} unique Soda users, {len(missing_emails)} emails not found")
        
        return soda_user_ids, missing_emails

    def _get_collibra_user_emails(self, user_ids: List[str]) -> List[str]:
        """
        Get email addresses for Collibra user IDs.
        
        Args:
            user_ids: List of Collibra user IDs
            
        Returns:
            List of email addresses
        """
        emails = []
        
        try:
            user_response = safe_api_call(
                self.collibra.get_user_information,
                user_ids=user_ids
            )
            
            for user in user_response.results:
                emails.append(user.emailAddress)
                logger.debug(f"Found user email: {user.emailAddress}")
                
        except Exception as e:
            logger.error(f"Error getting Collibra user emails: {e}")
        
        return emails

    def _get_collibra_group_user_emails(self, group_id: str) -> List[str]:
        """
        Get email addresses for users in a Collibra user group.
        
        Args:
            group_id: Collibra user group ID
            
        Returns:
            List of email addresses from group members
        """
        emails = []
        
        try:
            user_response = safe_api_call(
                self.collibra.get_user_information,
                group_id=group_id
            )
            
            for user in user_response.results:
                emails.append(user.emailAddress)
                logger.debug(f"Found group user email: {user.emailAddress}")
                
        except Exception as e:
            logger.error(f"Error getting Collibra group user emails: {e}")
        
        return emails

    def _find_soda_users_by_emails(self, emails: List[str]) -> Tuple[List[str], List[str]]:
        """
        Find Soda user IDs by email addresses.
        
        Args:
            emails: List of email addresses to search for
            
        Returns:
            Tuple of (List of Soda user IDs, List of missing emails)
        """
        soda_user_ids = []
        missing_emails = []
        
        for email in emails:
            try:
                logger.debug(f"Searching for Soda user with email: {email}")
                users = safe_api_call(self.soda.find_user, search_term=email, size=10)
                
                # Find exact email match
                found = False
                for user in users:
                    if user.email.lower() == email.lower():
                        soda_user_ids.append(user.userId)
                        logger.debug(f"Found matching Soda user: {user.fullName} ({user.userId})")
                        found = True
                        break
                
                if not found:
                    missing_emails.append(email)
                    logger.debug(f"No Soda user found with email: {email}")
                    
            except Exception as e:
                logger.error(f"Error searching for Soda user with email {email}: {e}")
                missing_emails.append(email)
        
        return soda_user_ids, missing_emails

    def _update_soda_dataset_ownership(self, dataset: FullDataset, soda_user_ids: List[str], dataset_metrics) -> None:
        """
        Update dataset ownership in Soda.
        
        Args:
            dataset: The Soda dataset to update
            soda_user_ids: List of Soda user IDs to set as owners
            dataset_metrics: Dataset metrics tracker
        """
        try:
            # Create owner update objects
            owners = [DatasetOwnerUpdate(type="user", userId=user_id) for user_id in soda_user_ids]
            
            # Create update request
            update_request = UpdateDatasetRequest(owners=owners)
            
            logger.debug(f"Updating dataset {dataset.name} with {len(owners)} owners")
            
            # Update the dataset
            updated_dataset = safe_api_call(
                self.soda.update_dataset,
                dataset_id=dataset.id,
                update_data=update_request
            )
            
            # Log success
            owner_names = []
            for owner in updated_dataset.owners:
                if owner.user:
                    owner_names.append(owner.user.fullName)
            
            success_msg = f"Updated ownership with {len(owners)} owners: {', '.join(owner_names)}"
            logger.debug(success_msg)
            
            dataset_metrics.owners_synced = len(owners)
            logger.debug(f"Set dataset metrics owners_synced to {len(owners)} for dataset {dataset.name}")
            
        except Exception as e:
            error_msg = f"Failed to update Soda dataset ownership: {e}"
            logger.error(error_msg)
            print(f"  ❌ {error_msg}")
            dataset_metrics.add_error(error_msg)
            raise 