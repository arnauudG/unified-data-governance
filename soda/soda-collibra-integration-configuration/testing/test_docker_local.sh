#!/bin/bash

# Local Docker Testing Script for Kubernetes Setup
# This script tests the Docker image as it would run in Kubernetes

set -e  # Exit on any error

echo "🐳 Local Docker Testing for Kubernetes Setup"
echo "=============================================="

# Change to parent directory for correct relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PARENT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "INFO")
            echo -e "${BLUE}ℹ️  ${message}${NC}"
            ;;
        "SUCCESS")
            echo -e "${GREEN}✅ ${message}${NC}"
            ;;
        "WARNING")
            echo -e "${YELLOW}⚠️  ${message}${NC}"
            ;;
        "ERROR")
            echo -e "${RED}❌ ${message}${NC}"
            ;;
    esac
}

# Check if Docker is running
check_docker() {
    print_status "INFO" "Checking Docker availability..."
    
    if ! command -v docker &> /dev/null; then
        print_status "ERROR" "Docker is not installed"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_status "ERROR" "Docker daemon is not running. Please start Docker."
        exit 1
    fi
    
    print_status "SUCCESS" "Docker is available"
}

# Build the Docker image
build_image() {
    print_status "INFO" "Building Docker image..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    if docker build -t "$IMAGE_NAME" .; then
        print_status "SUCCESS" "Docker image built successfully: $IMAGE_NAME"
    else
        print_status "ERROR" "Docker build failed"
        exit 1
    fi
}

# Test basic container functionality
test_basic_container() {
    print_status "INFO" "Testing basic container functionality..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    # Test help command
    if docker run --rm "$IMAGE_NAME" python main.py --help > /dev/null; then
        print_status "SUCCESS" "Container help command works"
    else
        print_status "ERROR" "Container help command failed"
        return 1
    fi
    
    # Test import functionality
    if docker run --rm "$IMAGE_NAME" python -c "from integration import SodaCollibraIntegration; print('Imports work')" > /dev/null; then
        print_status "SUCCESS" "Container imports work correctly"
    else
        print_status "ERROR" "Container imports failed"
        return 1
    fi
}

# Test with environment variables (simulating Kubernetes secrets)
test_with_env_vars() {
    print_status "INFO" "Testing with environment variables (simulating K8s secrets)..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    # Test with environment variable overrides
    docker run --rm \
        -e SODA_API_KEY_ID="test-key-id" \
        -e SODA_API_KEY_SECRET="test-key-secret" \
        -e COLLIBRA_USERNAME="test-username" \
        -e COLLIBRA_PASSWORD="test-password" \
        "$IMAGE_NAME" \
        python -c "
from config import load_config
config = load_config()
assert config.soda.api_key_id == 'test-key-id', 'Environment override failed'
print('Environment variable overrides working')
" > /dev/null
    
    if [ $? -eq 0 ]; then
        print_status "SUCCESS" "Environment variable overrides work correctly"
    else
        print_status "ERROR" "Environment variable overrides failed"
        return 1
    fi
}

# Test different CLI modes
test_cli_modes() {
    print_status "INFO" "Testing different CLI modes..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    # Test verbose mode
    if docker run --rm "$IMAGE_NAME" python main.py --help --verbose > /dev/null 2>&1; then
        print_status "SUCCESS" "Verbose mode works"
    else
        print_status "WARNING" "Verbose mode test failed (may be expected)"
    fi
    
    # Test debug mode
    if docker run --rm "$IMAGE_NAME" python main.py --help --debug > /dev/null 2>&1; then
        print_status "SUCCESS" "Debug mode works"
    else
        print_status "WARNING" "Debug mode test failed (may be expected)"
    fi
}

# Test with mounted config (simulating ConfigMap)
test_with_mounted_config() {
    print_status "INFO" "Testing with mounted config (simulating K8s ConfigMap)..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    # Create a temporary config file
    TEMP_CONFIG=$(mktemp)
    cp config.yaml "$TEMP_CONFIG"
    
    # Test with mounted config
    if docker run --rm \
        -v "$TEMP_CONFIG:/app/config.yaml:ro" \
        "$IMAGE_NAME" \
        python -c "from config import load_config; config = load_config(); print('Config loaded from mount')" > /dev/null; then
        print_status "SUCCESS" "Mounted config works correctly"
    else
        print_status "ERROR" "Mounted config failed"
        rm "$TEMP_CONFIG"
        return 1
    fi
    
    rm "$TEMP_CONFIG"
}

# Test resource usage
test_resource_usage() {
    print_status "INFO" "Testing resource usage..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    # Test with memory limit (simulating K8s resource limits)
    if docker run --rm \
        --memory="512m" \
        --cpus="0.5" \
        "$IMAGE_NAME" \
        python main.py --help > /dev/null; then
        print_status "SUCCESS" "Container works within resource limits"
    else
        print_status "ERROR" "Container failed with resource limits"
        return 1
    fi
}

# Simulate full Kubernetes environment
simulate_k8s_environment() {
    print_status "INFO" "Simulating full Kubernetes environment..."
    
    IMAGE_NAME="soda-collibra-integration:local-test"
    
    # Create temporary config
    TEMP_CONFIG=$(mktemp)
    cp config.yaml "$TEMP_CONFIG"
    
    print_status "INFO" "Running container with K8s-like configuration..."
    
    # Run with full K8s simulation
    docker run --rm \
        --name "soda-collibra-k8s-test" \
        --memory="1g" \
        --cpus="1.0" \
        -e SODA_API_KEY_ID="k8s-test-key-id" \
        -e SODA_API_KEY_SECRET="k8s-test-key-secret" \
        -e COLLIBRA_USERNAME="k8s-test-username" \
        -e COLLIBRA_PASSWORD="k8s-test-password" \
        -v "$TEMP_CONFIG:/app/config.yaml:ro" \
        "$IMAGE_NAME" \
        python main.py --help --verbose
    
    if [ $? -eq 0 ]; then
        print_status "SUCCESS" "Full K8s simulation successful"
    else
        print_status "ERROR" "Full K8s simulation failed"
        rm "$TEMP_CONFIG"
        return 1
    fi
    
    rm "$TEMP_CONFIG"
}

# Cleanup function
cleanup() {
    print_status "INFO" "Cleaning up test resources..."
    
    # Remove test image
    if docker images -q soda-collibra-integration:local-test > /dev/null 2>&1; then
        docker rmi soda-collibra-integration:local-test > /dev/null 2>&1 || true
        print_status "SUCCESS" "Test image cleaned up"
    fi
}

# Main execution
main() {
    # Set up cleanup trap
    trap cleanup EXIT
    
    # Run all tests
    check_docker
    build_image
    test_basic_container
    test_with_env_vars
    test_cli_modes
    test_with_mounted_config
    test_resource_usage
    simulate_k8s_environment
    
    echo ""
    print_status "SUCCESS" "All Docker tests passed! 🎉"
    echo ""
    print_status "INFO" "Your Docker setup is ready for Kubernetes deployment."
    echo ""
    echo "📋 Next steps:"
    echo "1. Push your image to a container registry"
    echo "2. Update image URLs in k8s/cronjob.yaml and k8s/debug.yaml"
    echo "3. Replace <customer-name> placeholders in YAML files"
    echo "4. Deploy to your Kubernetes cluster"
}

# Run main function
main "$@" 