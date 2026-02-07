#!/usr/bin/env python3
"""
Stack Testing Script for Unified Data Governance Platform

This script tests the entire platform stack to verify all components are properly configured
and can connect to their respective services.

Usage:
    python3 scripts/test_stack.py [--component COMPONENT] [--verbose]
    
Components:
    - all (default): Test all components
    - snowflake: Test Snowflake connection
    - soda: Test Soda Cloud API connection
    - collibra: Test Collibra API connection
    - config: Test configuration loading
"""

import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config, get_config
from src.core.logging import setup_logging, get_logger
from src.core.health import HealthChecker, HealthStatus
from src.core.exceptions import ConfigurationError

# Setup logging
setup_logging(level="INFO", format_type="human")
logger = get_logger(__name__)


class StackTester:
    """Test the entire platform stack."""

    def __init__(self, config: Config):
        """
        Initialize stack tester.
        
        Args:
            config: Platform configuration
        """
        self.config = config
        self.health_checker = HealthChecker(config=config)
        self.results: Dict[str, Dict[str, Any]] = {}

    def test_configuration(self) -> bool:
        """
        Test configuration loading.
        
        Returns:
            True if configuration is valid
        """
        logger.info("Testing configuration...")
        try:
            # Configuration is already loaded, just verify it's valid
            if not self.config.snowflake.account:
                raise ConfigurationError("SNOWFLAKE_ACCOUNT is missing")
            if not self.config.snowflake.user:
                raise ConfigurationError("SNOWFLAKE_USER is missing")
            if not self.config.snowflake.password:
                raise ConfigurationError("SNOWFLAKE_PASSWORD is missing")
            
            logger.info("✅ Configuration is valid")
            self.results["config"] = {
                "status": "healthy",
                "message": "Configuration loaded and validated successfully",
            }
            return True
        except Exception as e:
            logger.error(f"❌ Configuration test failed: {e}")
            self.results["config"] = {
                "status": "unhealthy",
                "message": str(e),
            }
            return False

    def test_snowflake(self) -> bool:
        """
        Test Snowflake connection.
        
        Returns:
            True if connection succeeds
        """
        logger.info("Testing Snowflake connection...")
        try:
            from scripts.setup.setup_snowflake import SnowflakeSetup
            
            setup = SnowflakeSetup(config=self.config)
            success = setup.test_connection()
            
            if success:
                logger.info("✅ Snowflake connection test passed")
                self.results["snowflake"] = {
                    "status": "healthy",
                    "message": "Successfully connected to Snowflake",
                }
                return True
            else:
                logger.error("❌ Snowflake connection test failed")
                self.results["snowflake"] = {
                    "status": "unhealthy",
                    "message": "Failed to connect to Snowflake",
                }
                return False
        except Exception as e:
            logger.error(f"❌ Snowflake test failed: {e}")
            self.results["snowflake"] = {
                "status": "unhealthy",
                "message": str(e),
            }
            return False

    def test_soda_cloud(self) -> bool:
        """
        Test Soda Cloud API connection.
        
        Returns:
            True if connection succeeds
        """
        logger.info("Testing Soda Cloud API connection...")
        try:
            check = self.health_checker.check_soda_cloud()
            success = check.status == HealthStatus.HEALTHY
            
            if success:
                logger.info("✅ Soda Cloud connection test passed")
                self.results["soda"] = {
                    "status": "healthy",
                    "message": check.message,
                }
                return True
            else:
                logger.error(f"❌ Soda Cloud connection test failed: {check.message}")
                self.results["soda"] = {
                    "status": "unhealthy",
                    "message": check.message,
                }
                return False
        except Exception as e:
            logger.error(f"❌ Soda Cloud test failed: {e}")
            self.results["soda"] = {
                "status": "unhealthy",
                "message": str(e),
            }
            return False

    def test_collibra(self) -> bool:
        """
        Test Collibra API connection.
        
        Returns:
            True if connection succeeds
        """
        logger.info("Testing Collibra API connection...")
        try:
            check = self.health_checker.check_collibra()
            success = check.status == HealthStatus.HEALTHY
            
            if success:
                logger.info("✅ Collibra connection test passed")
                self.results["collibra"] = {
                    "status": "healthy",
                    "message": check.message,
                }
                return True
            else:
                logger.error(f"❌ Collibra connection test failed: {check.message}")
                self.results["collibra"] = {
                    "status": "unhealthy",
                    "message": check.message,
                }
                return False
        except Exception as e:
            logger.error(f"❌ Collibra test failed: {e}")
            self.results["collibra"] = {
                "status": "unhealthy",
                "message": str(e),
            }
            return False

    def test_all(self) -> Dict[str, bool]:
        """
        Test all components.
        
        Returns:
            Dictionary mapping component names to test results
        """
        logger.info("=" * 60)
        logger.info("Testing entire platform stack...")
        logger.info("=" * 60)
        
        results = {}
        
        # Test configuration first
        results["config"] = self.test_configuration()
        
        # Test Snowflake
        results["snowflake"] = self.test_snowflake()
        
        # Test Soda Cloud
        results["soda"] = self.test_soda_cloud()
        
        # Test Collibra
        results["collibra"] = self.test_collibra()
        
        return results

    def print_summary(self) -> None:
        """Print test summary."""
        print("\n" + "=" * 60)
        print("STACK TEST SUMMARY")
        print("=" * 60)
        
        for component, result in self.results.items():
            status_icon = "✅" if result["status"] == "healthy" else "❌"
            print(f"{status_icon} {component.upper()}: {result['status']}")
            if result["status"] != "healthy":
                print(f"   Message: {result['message']}")
        
        print("=" * 60)
        
        # Overall status
        all_healthy = all(r["status"] == "healthy" for r in self.results.values())
        if all_healthy:
            print("✅ All components are healthy!")
        else:
            failed = [c for c, r in self.results.items() if r["status"] != "healthy"]
            print(f"❌ Some components failed: {', '.join(failed)}")
        print("=" * 60)


def main() -> int:
    """Main test function."""
    parser = argparse.ArgumentParser(
        description="Test the Unified Data Governance Platform stack"
    )
    parser.add_argument(
        "--component",
        choices=["all", "config", "snowflake", "soda", "collibra"],
        default="all",
        help="Component to test (default: all)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    if args.verbose:
        setup_logging(level="DEBUG", format_type="human")

    try:
        # Load configuration
        config = get_config()
        logger.info("Configuration loaded successfully")

        tester = StackTester(config=config)

        # Run tests based on component selection
        if args.component == "all":
            results = tester.test_all()
        elif args.component == "config":
            results = {"config": tester.test_configuration()}
        elif args.component == "snowflake":
            results = {"snowflake": tester.test_snowflake()}
        elif args.component == "soda":
            results = {"soda": tester.test_soda_cloud()}
        elif args.component == "collibra":
            results = {"collibra": tester.test_collibra()}
        else:
            logger.error(f"Unknown component: {args.component}")
            return 1

        # Print summary
        tester.print_summary()

        # Return exit code based on results
        all_passed = all(results.values())
        return 0 if all_passed else 1

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        if e.details:
            logger.error(f"Details: {e.details}")
        return 1
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
