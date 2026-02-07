"""
Health check utilities for the unified data governance platform.

This module provides health check functionality for monitoring
the status of external services and internal components.
"""

from typing import Dict, Any, Optional, List
from enum import Enum
from datetime import datetime

from src.core.logging import get_logger
from src.core.config import get_config, Config
from src.repositories.soda_repository import SodaRepository
from src.repositories.collibra_repository import CollibraRepository

logger = get_logger(__name__)


class HealthStatus(str, Enum):
    """Health status enumeration."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class HealthCheck:
    """
    Health check result for a single component.
    
    Attributes:
        name: Component name
        status: Health status
        message: Optional status message
        details: Optional additional details
        timestamp: Check timestamp
    """

    def __init__(
        self,
        name: str,
        status: HealthStatus,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize health check result.

        Args:
            name: Component name
            status: Health status
            message: Optional status message
            details: Optional additional details
        """
        self.name = name
        self.status = status
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert health check to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
        }


class HealthChecker:
    """
    Health checker for platform components.
    
    Provides health check functionality for external services
    and internal components.
    """

    def __init__(self, config: Optional[Config] = None):
        """
        Initialize health checker.

        Args:
            config: Optional Config instance. If None, uses get_config().
        """
        self.config = config or get_config()
        self.logger = get_logger(self.__class__.__name__)

    def check_soda_cloud(self) -> HealthCheck:
        """
        Check Soda Cloud API health.

        Returns:
            HealthCheck result
        """
        try:
            repo = SodaRepository(config=self.config)
            with repo:
                # Try a simple API call
                datasets = repo.get_datasets(page=0, size=1)
                
                if datasets:
                    return HealthCheck(
                        name="soda_cloud",
                        status=HealthStatus.HEALTHY,
                        message="Soda Cloud API is accessible",
                        details={"base_url": repo.base_url},
                    )
                else:
                    return HealthCheck(
                        name="soda_cloud",
                        status=HealthStatus.DEGRADED,
                        message="Soda Cloud API accessible but returned no data",
                    )
        except Exception as e:
            self.logger.error(f"Soda Cloud health check failed: {e}", exc_info=True)
            return HealthCheck(
                name="soda_cloud",
                status=HealthStatus.UNHEALTHY,
                message=f"Soda Cloud API is not accessible: {str(e)}",
                details={"error_type": type(e).__name__},
            )

    def check_collibra(self) -> HealthCheck:
        """
        Check Collibra API health.

        Returns:
            HealthCheck result
        """
        try:
            repo = CollibraRepository(config=self.config)
            with repo:
                # Try a simple API call (get databases endpoint)
                # This is a lightweight check
                response = repo._make_request("GET", "/rest/catalogDatabase/v1/databases?limit=1")
                
                if response.status_code == 200:
                    return HealthCheck(
                        name="collibra",
                        status=HealthStatus.HEALTHY,
                        message="Collibra API is accessible",
                        details={"base_url": repo.base_url},
                    )
                else:
                    return HealthCheck(
                        name="collibra",
                        status=HealthStatus.DEGRADED,
                        message=f"Collibra API returned status {response.status_code}",
                    )
        except Exception as e:
            self.logger.error(f"Collibra health check failed: {e}", exc_info=True)
            return HealthCheck(
                name="collibra",
                status=HealthStatus.UNHEALTHY,
                message=f"Collibra API is not accessible: {str(e)}",
                details={"error_type": type(e).__name__},
            )

    def check_configuration(self) -> HealthCheck:
        """
        Check configuration validity.

        Returns:
            HealthCheck result
        """
        try:
            config = self.config
            
            # Check required configuration
            errors = []
            if not config.snowflake.account:
                errors.append("SNOWFLAKE_ACCOUNT missing")
            if not config.soda_cloud.api_key_id:
                errors.append("SODA_CLOUD_API_KEY_ID missing")
            if not config.collibra.base_url:
                errors.append("COLLIBRA_BASE_URL missing")
            
            if errors:
                return HealthCheck(
                    name="configuration",
                    status=HealthStatus.UNHEALTHY,
                    message="Configuration is incomplete",
                    details={"missing_fields": errors},
                )
            else:
                return HealthCheck(
                    name="configuration",
                    status=HealthStatus.HEALTHY,
                    message="Configuration is valid",
                )
        except Exception as e:
            self.logger.error(f"Configuration health check failed: {e}", exc_info=True)
            return HealthCheck(
                name="configuration",
                status=HealthStatus.UNHEALTHY,
                message=f"Configuration check failed: {str(e)}",
                details={"error_type": type(e).__name__},
            )

    def check_all(self) -> Dict[str, Any]:
        """
        Run all health checks.

        Returns:
            Dictionary containing all health check results and overall status
        """
        self.logger.info("Running health checks...")
        
        checks = [
            self.check_configuration(),
            self.check_soda_cloud(),
            self.check_collibra(),
        ]
        
        # Determine overall status
        statuses = [check.status for check in checks]
        if all(s == HealthStatus.HEALTHY for s in statuses):
            overall_status = HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            overall_status = HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.UNKNOWN
        
        return {
            "status": overall_status.value,
            "timestamp": datetime.utcnow().isoformat(),
            "checks": [check.to_dict() for check in checks],
        }

    def get_health_summary(self) -> str:
        """
        Get a human-readable health summary.

        Returns:
            Health summary string
        """
        result = self.check_all()
        
        summary_lines = [
            f"Overall Status: {result['status'].upper()}",
            f"Timestamp: {result['timestamp']}",
            "",
            "Component Status:",
        ]
        
        for check in result["checks"]:
            status_icon = {
                "healthy": "✅",
                "unhealthy": "❌",
                "degraded": "⚠️",
                "unknown": "❓",
            }.get(check["status"], "❓")
            
            summary_lines.append(
                f"  {status_icon} {check['name']}: {check['status']}"
            )
            if check.get("message"):
                summary_lines.append(f"     {check['message']}")
        
        return "\n".join(summary_lines)
