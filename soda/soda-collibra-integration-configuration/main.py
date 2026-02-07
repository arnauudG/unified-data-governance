"""
Refactored Soda-Collibra Integration Main Script

This script provides both the new optimized integration class and the original test methods
for backward compatibility.
"""

import logging
import argparse
import sys
from typing import Optional

from constants import IntegrationConstants
from integration import SodaCollibraIntegration

# Configure logging - can be overridden by command line arguments
logging.basicConfig(
    level=logging.WARNING, 
    format=IntegrationConstants.LOG_FORMAT_SIMPLE
)

def setup_logging(debug: bool = False, verbose: bool = False) -> None:
    """
    Setup logging configuration.
    
    Args:
        debug: Enable debug logging
        verbose: Enable verbose (info) logging
    """
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format=IntegrationConstants.LOG_FORMAT_DEBUG,
            force=True
        )
    elif verbose:
        logging.basicConfig(
            level=logging.INFO,
            format=IntegrationConstants.LOG_FORMAT_DEBUG,
            force=True
        )

def run_integration(config_path: Optional[str] = None) -> int:
    """
    Run the main Soda-Collibra integration.
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        integration = SodaCollibraIntegration(config_path)
        results = integration.run()
        
        # Log final results for debugging
        logger = logging.getLogger(__name__)
        logger.info(f"Integration completed successfully: {results}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        logger = logging.getLogger(__name__)
        logger.error(f"Integration failed with error: {e}", exc_info=True)
        return 1

def run_soda_tests() -> int:
    """
    Run Soda client tests (legacy function for backward compatibility).
    
    Returns:
        Exit code
    """
    try:
        from legacy_tests import soda_test_methods
        soda_test_methods()
        return 0
    except ImportError:
        print("❌ Legacy test methods not available")
        return 1
    except Exception as e:
        print(f"❌ Soda tests failed: {e}")
        return 1

def run_collibra_tests() -> int:
    """
    Run Collibra client tests (legacy function for backward compatibility).
    
    Returns:
        Exit code
    """
    try:
        from legacy_tests import collibra_test_methods
        collibra_test_methods()
        return 0
    except ImportError:
        print("❌ Legacy test methods not available")
        return 1
    except Exception as e:
        print(f"❌ Collibra tests failed: {e}")
        return 1

def main():
    """Main entry point with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Soda-Collibra Integration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Run integration with default config
  %(prog)s --config custom.yaml  # Run with custom config file
  %(prog)s --debug            # Run with debug logging
  %(prog)s --test-soda        # Run Soda client tests
  %(prog)s --test-collibra    # Run Collibra client tests
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--test-soda',
        action='store_true',
        help='Run Soda client tests instead of integration'
    )
    
    parser.add_argument(
        '--test-collibra',
        action='store_true',
        help='Run Collibra client tests instead of integration'
    )
    
    args = parser.parse_args()
    
    # Setup logging based on arguments
    setup_logging(debug=args.debug, verbose=args.verbose)
    
    # Run appropriate function based on arguments
    if args.test_soda:
        return run_soda_tests()
    elif args.test_collibra:
        return run_collibra_tests()
    else:
        return run_integration(args.config)

if __name__ == "__main__":
    sys.exit(main()) 