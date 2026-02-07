"""
Performance metrics and monitoring for Soda-Collibra Integration
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    """Metrics for tracking processing performance"""
    
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    # Dataset metrics
    datasets_processed: int = 0
    datasets_skipped: int = 0
    datasets_failed: int = 0
    datasets_without_table_assets: int = 0
    
    # Check metrics
    checks_processed: int = 0
    checks_created: int = 0
    checks_updated: int = 0
    checks_deleted: int = 0
    
    # Attribute metrics
    attributes_created: int = 0
    attributes_updated: int = 0
    
    # Relation metrics
    dimension_relations_created: int = 0
    table_relations_created: int = 0
    column_relations_created: int = 0
    
    # Ownership metrics
    owners_synced: int = 0
    ownership_sync_failed: int = 0
    
    # Dimension metrics  
    dimension_sync_failed: int = 0
    
    # API metrics
    api_calls_made: int = 0
    api_calls_failed: int = 0
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    
    def finish(self) -> None:
        """Mark the processing as finished"""
        self.end_time = time.time()
    
    @property
    def duration(self) -> float:
        """Get the total processing duration"""
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def datasets_per_second(self) -> float:
        """Get datasets processed per second"""
        duration = self.duration
        return self.datasets_processed / duration if duration > 0 else 0
    
    @property
    def checks_per_second(self) -> float:
        """Get checks processed per second"""
        duration = self.duration
        return self.checks_processed / duration if duration > 0 else 0
    
    @property
    def total_operations(self) -> int:
        """Get total operations performed"""
        return (self.checks_created + self.checks_updated + 
                self.attributes_created + self.attributes_updated + 
                self.dimension_relations_created + self.table_relations_created + 
                self.column_relations_created)
    
    @property
    def success_rate(self) -> float:
        """Get success rate as percentage"""
        attempted_datasets = self.datasets_processed + self.datasets_failed
        if attempted_datasets == 0:
            return 100.0
        return (self.datasets_processed / attempted_datasets) * 100
    
    def add_error(self, error: str) -> None:
        """Add an error to the metrics"""
        self.errors.append(error)
    
    def increment_api_call(self, success: bool = True) -> None:
        """Increment API call counters"""
        self.api_calls_made += 1
        if not success:
            self.api_calls_failed += 1
    
    def get_summary_dict(self) -> Dict[str, int]:
        """Get summary as dictionary for easy display"""
        return {
            'datasets_processed': self.datasets_processed,
            'datasets_skipped': self.datasets_skipped,
            'datasets_without_table_assets': self.datasets_without_table_assets,
            'checks_created': self.checks_created,
            'checks_updated': self.checks_updated,
            'checks_deleted': self.checks_deleted,
            'attributes_created': self.attributes_created,
            'attributes_updated': self.attributes_updated,
            'dimension_relations_created': self.dimension_relations_created,
            'table_relations_created': self.table_relations_created,
            'column_relations_created': self.column_relations_created,
            'owners_synced': self.owners_synced,
            'ownership_sync_failed': self.ownership_sync_failed,
            'dimension_sync_failed': self.dimension_sync_failed,
            'errors': len(self.errors)
        }
    
    def log_performance_summary(self) -> None:
        """Log a performance summary"""
        logger.info("=== PERFORMANCE SUMMARY ===")
        logger.info(f"Total duration: {self.duration:.2f} seconds")
        logger.info(f"Datasets processed: {self.datasets_processed}")
        logger.info(f"Datasets per second: {self.datasets_per_second:.2f}")
        logger.info(f"Checks processed: {self.checks_processed}")
        logger.info(f"Checks per second: {self.checks_per_second:.2f}")
        logger.info(f"Total operations: {self.total_operations}")
        logger.info(f"API calls made: {self.api_calls_made}")
        logger.info(f"API calls failed: {self.api_calls_failed}")
        logger.info(f"Success rate: {self.success_rate:.1f}%")
        
        if self.errors:
            logger.info(f"Errors encountered: {len(self.errors)}")

@dataclass
class DatasetMetrics:
    """Metrics for individual dataset processing"""
    
    dataset_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    checks_found: int = 0
    checks_created: int = 0
    checks_updated: int = 0
    checks_deleted: int = 0
    attributes_processed: int = 0
    relations_created: int = 0
    
    # Ownership metrics
    owners_synced: int = 0
    
    errors: List[str] = field(default_factory=list)
    
    def finish(self) -> None:
        """Mark dataset processing as finished"""
        self.end_time = time.time()
    
    @property
    def duration(self) -> float:
        """Get dataset processing duration"""
        end = self.end_time or time.time()
        return end - self.start_time
    
    def add_error(self, error: str) -> None:
        """Add an error for this dataset"""
        self.errors.append(error)

class MetricsCollector:
    """Collector for managing metrics across the integration"""
    
    def __init__(self):
        self.overall_metrics = ProcessingMetrics()
        self.dataset_metrics: Dict[str, DatasetMetrics] = {}
    
    def start_dataset_processing(self, dataset_name: str) -> DatasetMetrics:
        """Start tracking metrics for a dataset"""
        metrics = DatasetMetrics(dataset_name=dataset_name)
        self.dataset_metrics[dataset_name] = metrics
        return metrics
    
    def finish_dataset_processing(self, dataset_name: str) -> None:
        """Finish tracking metrics for a dataset"""
        if dataset_name in self.dataset_metrics:
            self.dataset_metrics[dataset_name].finish()
    
    def get_overall_metrics(self) -> ProcessingMetrics:
        """Get overall processing metrics"""
        return self.overall_metrics
    
    def get_dataset_metrics(self, dataset_name: str) -> Optional[DatasetMetrics]:
        """Get metrics for a specific dataset"""
        return self.dataset_metrics.get(dataset_name)
    
    def aggregate_dataset_metrics(self) -> None:
        """Aggregate dataset metrics into overall metrics"""
        logger.debug(f"Aggregating metrics from {len(self.dataset_metrics)} datasets")
        
        for dataset_metrics in self.dataset_metrics.values():
            logger.debug(f"Processing dataset {dataset_metrics.dataset_name}: "
                        f"{dataset_metrics.owners_synced} owners synced, "
                        f"{len(dataset_metrics.errors)} errors")
            
            self.overall_metrics.checks_processed += dataset_metrics.checks_found
            self.overall_metrics.checks_created += dataset_metrics.checks_created  
            self.overall_metrics.checks_updated += dataset_metrics.checks_updated
            self.overall_metrics.checks_deleted += dataset_metrics.checks_deleted
            self.overall_metrics.owners_synced += dataset_metrics.owners_synced
            
            # Debug logging for ownership metrics
            if dataset_metrics.owners_synced > 0:
                logger.debug(f"Dataset {dataset_metrics.dataset_name}: {dataset_metrics.owners_synced} owners synced")
                
            # Count datasets with ownership sync failures
            if any("ownership" in error.lower() for error in dataset_metrics.errors):
                self.overall_metrics.ownership_sync_failed += 1
                
            # Count datasets with dimension sync failures
            if any("dimension asset" in error.lower() for error in dataset_metrics.errors):
                self.overall_metrics.dimension_sync_failed += 1
                
            # Add errors to overall metrics
            if dataset_metrics.errors:
                logger.debug(f"Adding {len(dataset_metrics.errors)} errors from dataset {dataset_metrics.dataset_name}")
                for error in dataset_metrics.errors:
                    logger.debug(f"  Error: {error}")
            self.overall_metrics.errors.extend(dataset_metrics.errors)
        
        logger.debug(f"Final aggregated metrics: "
                    f"owners_synced={self.overall_metrics.owners_synced}, "
                    f"total_errors={len(self.overall_metrics.errors)}")
    
    def finish_processing(self) -> None:
        """Finish overall processing and aggregate metrics"""
        self.aggregate_dataset_metrics()
        self.overall_metrics.finish()
        self.overall_metrics.log_performance_summary() 