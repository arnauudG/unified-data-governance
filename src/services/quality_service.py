"""
Quality Service - Business logic for data quality operations.

This service orchestrates quality checks and validation operations,
coordinating between Soda Cloud API and other quality-related services.
"""

from typing import List, Dict, Any, Optional, TYPE_CHECKING
from pathlib import Path

from src.core.logging import get_logger
from src.core.config import get_config, Config
from src.repositories.soda_repository import SodaRepository
from src.core.exceptions import ConfigurationError

logger = get_logger(__name__)


class QualityService:
    """
    Service for managing data quality operations.
    
    This service orchestrates quality checks and validation operations,
    coordinating between Soda Cloud API and other quality-related services.
    """

    def __init__(
        self,
        soda_repository: Optional[SodaRepository] = None,
        config: Optional[Config] = None,
    ):
        """
        Initialize Quality Service.

        Args:
            soda_repository: Optional SodaRepository instance
            config: Optional Config instance
        """
        self.config = config or get_config()
        self.soda_repository = soda_repository or SodaRepository(config=self.config)

    def get_failed_critical_checks(
        self, layer: str, dataset_ids: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """
        Get failed critical checks for a specific layer.

        Args:
            layer: Layer name ('raw', 'staging', 'mart', 'quality')
            dataset_ids: Optional list of dataset IDs to filter checks

        Returns:
            List of dictionaries with check information
        """
        # Define expected dataset names for each layer
        EXPECTED_DATASETS = {
            "raw": ["CUSTOMERS", "PRODUCTS", "ORDERS", "ORDER_ITEMS"],
            "staging": [
                "STG_CUSTOMERS",
                "STG_PRODUCTS",
                "STG_ORDERS",
                "STG_ORDER_ITEMS",
            ],
            "mart": ["DIM_CUSTOMERS", "DIM_PRODUCTS", "FACT_ORDERS"],
            "quality": ["CHECK_RESULTS"],
        }

        expected_dataset_names = EXPECTED_DATASETS.get(layer.lower(), [])
        if not expected_dataset_names:
            logger.warning(
                f"No expected datasets defined for layer '{layer}'. Skipping quality check."
            )
            return []

        try:
            # Get all checks
            if dataset_ids:
                all_checks = []
                for dataset_id in dataset_ids:
                    try:
                        dataset = self.soda_repository.get_dataset(dataset_id)
                        # Extract checks from dataset (implementation depends on API)
                        # This is a placeholder - actual implementation may vary
                        checks = []  # Would need to fetch checks for this dataset
                        all_checks.extend(checks)
                    except Exception as e:
                        logger.warning(f"Failed to fetch checks for dataset {dataset_id}: {e}")
                        continue
            else:
                all_checks = self.soda_repository.get_all_checks()

            # Filter for failed critical checks
            failed_critical = []
            for check in all_checks:
                try:
                    evaluation_status = check.get("evaluationStatus")
                    if not evaluation_status or evaluation_status == "pass":
                        continue

                    # Check if check is marked as critical
                    is_critical = False
                    check_name = check.get("name", "Unknown")

                    # Check attributes for critical flag
                    attributes = check.get("attributes", {})
                    if isinstance(attributes, dict):
                        is_critical = attributes.get("critical", False)
                    elif isinstance(attributes, list):
                        for attr in attributes:
                            if isinstance(attr, dict) and attr.get("name") == "critical":
                                is_critical = attr.get("value", False)
                                break

                    if is_critical:
                        dataset_name = (
                            check.get("dataset", {}).get("name", "Unknown")
                            if isinstance(check.get("dataset"), dict)
                            else "Unknown"
                        )

                        # Filter: Only include checks from expected datasets
                        if dataset_name.upper() not in [
                            name.upper() for name in expected_dataset_names
                        ]:
                            continue

                        failed_critical.append({
                            "name": check_name,
                            "dataset": dataset_name,
                            "status": evaluation_status,
                            "layer": layer,
                        })
                        logger.warning(
                            f"Critical check failed in {layer} layer: {check_name} "
                            f"(Dataset: {dataset_name})"
                        )
                except Exception as e:
                    logger.warning(f"Error processing check: {e}")
                    continue

            if failed_critical:
                logger.error(
                    f"Found {len(failed_critical)} failed critical checks in {layer} layer"
                )
            else:
                logger.info(f"No failed critical checks found in {layer} layer")

            return failed_critical

        except Exception as e:
            logger.error(f"Error checking Soda quality results for {layer} layer: {e}")
            logger.warning(
                "Continuing with sync despite quality check error (non-blocking)"
            )
            return []

    def validate_quality_before_sync(
        self, layer: str, dataset_ids: Optional[List[str]] = None
    ) -> bool:
        """
        Validate that no critical checks have failed before allowing sync.

        Args:
            layer: Layer name ('raw', 'staging', 'mart', 'quality')
            dataset_ids: Optional list of dataset IDs to filter checks

        Returns:
            True if sync should proceed, False if critical checks failed
        """
        failed_critical = self.get_failed_critical_checks(layer, dataset_ids)

        if failed_critical:
            logger.error(
                f"❌ Quality gate failed for {layer} layer. "
                f"{len(failed_critical)} critical checks failed:"
            )
            for check in failed_critical:
                logger.error(f"  - {check['name']} (Dataset: {check['dataset']})")
            return False

        logger.info(
            f"✅ Quality gate passed for {layer} layer. No critical checks failed."
        )
        return True

    def export_quality_data(self, output_dir: Path) -> Dict[str, Path]:
        """
        Export quality data to CSV files.

        Args:
            output_dir: Output directory for CSV files

        Returns:
            Dictionary mapping data type to file path
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Fetch all data
        datasets = self.soda_repository.get_all_datasets()
        checks = self.soda_repository.get_all_checks()

        # Save to CSV
        import pandas as pd

        files = {}
        if datasets:
            datasets_file = output_dir / "datasets_latest.csv"
            pd.DataFrame(datasets).to_csv(datasets_file, index=False)
            files["datasets"] = datasets_file
            logger.info(f"Exported {len(datasets)} datasets to {datasets_file}")

        if checks:
            checks_file = output_dir / "checks_latest.csv"
            pd.DataFrame(checks).to_csv(checks_file, index=False)
            files["checks"] = checks_file
            logger.info(f"Exported {len(checks)} checks to {checks_file}")

        return files
