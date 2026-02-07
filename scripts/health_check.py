#!/usr/bin/env python3
"""
Health Check Script

This script checks the health of all platform components and services.
"""

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root / "src") not in sys.path:
    sys.path.insert(0, str(project_root / "src"))
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.core.health import HealthChecker
from src.core.logging import get_logger

logger = get_logger(__name__)


def main() -> int:
    """Main function to run health checks."""
    print("=" * 60)
    print("Platform Health Check")
    print("=" * 60)
    print()

    try:
        checker = HealthChecker()
        summary = checker.get_health_summary()
        
        print(summary)
        print()
        
        # Get detailed results
        result = checker.check_all()
        
        # Exit with appropriate code
        if result["status"] == "healthy":
            print("✅ All systems operational")
            return 0
        elif result["status"] == "degraded":
            print("⚠️  Some systems degraded")
            return 1
        else:
            print("❌ System health check failed")
            return 2

    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        print(f"\n❌ Health check error: {e}")
        return 3


if __name__ == "__main__":
    exit(main())
