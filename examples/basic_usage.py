#!/usr/bin/env python3
"""
Basic Usage Examples

This module demonstrates basic usage of the unified data governance platform.
"""

from src.core.config import get_config
from src.services.pipeline_service import PipelineService
from src.core.health import HealthChecker
from src.repositories.soda_repository import SodaRepository


def example_basic_pipeline():
    """Example: Run a basic pipeline for a single layer."""
    # Get configuration
    config = get_config()
    
    # Create pipeline service
    pipeline = PipelineService(config=config)
    
    # Run quality checks for raw layer
    result = pipeline.run_quality_checks("raw")
    print(f"Quality check result: {result}")
    
    # Sync metadata with quality gate
    sync_result = pipeline.sync_metadata_with_quality_gate("raw", strict=True)
    print(f"Sync result: {sync_result}")


def example_fetch_soda_data():
    """Example: Fetch data from Soda Cloud API."""
    config = get_config()
    
    # Create repository
    repo = SodaRepository(config=config)
    
    # Fetch all datasets
    with repo:
        datasets = repo.get_all_datasets()
        print(f"Found {len(datasets)} datasets")
        
        # Fetch all checks
        checks = repo.get_all_checks()
        print(f"Found {len(checks)} checks")


def example_health_check():
    """Example: Check platform health."""
    checker = HealthChecker()
    
    # Get health summary
    summary = checker.get_health_summary()
    print(summary)
    
    # Get detailed results
    result = checker.check_all()
    print(f"\nOverall status: {result['status']}")
    
    for check in result["checks"]:
        print(f"  {check['name']}: {check['status']}")


def example_complete_pipeline():
    """Example: Run complete pipeline for all layers."""
    config = get_config()
    pipeline = PipelineService(config=config)
    
    # Run complete pipeline
    result = pipeline.run_complete_pipeline(
        layers=["raw", "staging", "mart"],
        strict=False,  # Continue even if quality gate fails
    )
    
    print(f"Pipeline status: {result['status']}")
    for layer, layer_result in result["layers"].items():
        print(f"  {layer}: {layer_result.get('status', 'unknown')}")


if __name__ == "__main__":
    print("Basic Usage Examples")
    print("=" * 50)
    
    # Uncomment to run examples:
    # example_basic_pipeline()
    # example_fetch_soda_data()
    # example_health_check()
    # example_complete_pipeline()
