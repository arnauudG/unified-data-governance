# Local Kubernetes Testing Guide

This guide provides multiple ways to test your Kubernetes setup locally before deploying to production.

## 🎯 Testing Options

### **Option 1: Comprehensive Python Testing (Recommended)**

This tests everything without requiring Docker to be running:

```bash
# Run all tests with test credentials (safe)
python testing/test_k8s_local.py

# Run tests with real credentials from config.yaml
python testing/test_k8s_local.py --real-credentials
```

**What it tests:**
- ✅ Environment variable overrides (simulating K8s secrets)
- ✅ Configuration loading and validation
- ✅ CLI functionality and all modes
- ✅ Docker build process (if Docker is available)
- ✅ Kubernetes YAML validation
- ✅ Different deployment scenarios

### **Option 2: Docker-Only Testing**

If you want to test the containerized version specifically:

```bash
# Make sure Docker is running first
docker --version

# Run comprehensive Docker tests
./testing/test_docker_local.sh
```

**What it tests:**
- ✅ Docker image build
- ✅ Container functionality
- ✅ Environment variable injection
- ✅ Config file mounting (simulating ConfigMaps)
- ✅ Resource limits
- ✅ Full Kubernetes environment simulation

### **Option 3: Manual Testing**

For step-by-step testing:

```bash
# 1. Test basic functionality
python main.py --help

# 2. Test with environment variables (simulating K8s secrets)
SODA_API_KEY_ID=test-id COLLIBRA_USERNAME=test-user python main.py --help

# 3. Test different logging modes
python main.py --help --verbose
python main.py --help --debug

# 4. Test Docker build (if Docker is available)
docker build -t soda-collibra-integration:test .

# 5. Test container with environment variables
docker run --rm \
  -e SODA_API_KEY_ID=test-id \
  -e COLLIBRA_USERNAME=test-user \
  soda-collibra-integration:test \
  python main.py --help
```

## 🔧 Local Environment Setup

### **Simulating Kubernetes Secrets**

Set these environment variables to simulate how Kubernetes would inject secrets:

```bash
export SODA_API_KEY_ID="your-soda-api-key-id"
export SODA_API_KEY_SECRET="your-soda-api-key-secret"
export COLLIBRA_USERNAME="your-collibra-username"
export COLLIBRA_PASSWORD="your-collibra-password"
export COLLIBRA_BASE_URL="https://your-collibra.com/rest/2.0"  # Optional
```

### **Simulating Kubernetes ConfigMaps**

The `config.yaml` file simulates a Kubernetes ConfigMap. Test with different configs:

```bash
# Test with custom config
python main.py --config custom-config.yaml

# Test with Docker volume mount (simulating ConfigMap)
docker run --rm \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  soda-collibra-integration:test \
  python main.py --help
```

## 📊 Expected Test Results

### **Successful Python Test Output:**
```
🧪 Starting Kubernetes Local Testing
============================================================
🔧 Setting up Kubernetes-like environment...
✅ Environment variables set

📋 Testing configuration loading...
✅ SODA_API_KEY_ID override working: test-api-key-id
✅ COLLIBRA_USERNAME override working: test-username
✅ Configuration loading test passed

🖥️  Testing CLI functionality...
✅ Command succeeded: python main.py --help
✅ CLI functionality test passed

🐳 Testing Docker build...
✅ Docker available: Docker version X.X.X
✅ Docker build successful
✅ Container run test successful

☸️  Validating Kubernetes YAML files...
✅ k8s/cronjob.yaml: Valid YAML structure
✅ k8s/debug.yaml: Valid YAML structure

🚀 Simulating Kubernetes deployment scenario...
✅ All scenarios passed

📈 Overall: 5/5 tests passed
🎉 All tests passed! Kubernetes deployment is ready.
```

### **Successful Docker Test Output:**
```
🐳 Local Docker Testing for Kubernetes Setup
==============================================
✅ Docker is available
✅ Docker image built successfully
✅ Container help command works
✅ Container imports work correctly
✅ Environment variable overrides work correctly
✅ Mounted config works correctly
✅ Container works within resource limits
✅ Full K8s simulation successful

All Docker tests passed! 🎉
```

## 🚀 Testing Different Scenarios

### **Production Scenario**
```bash
# Minimal logging (production-ready)
python main.py --help
```

### **Development Scenario**
```bash
# Verbose logging for development
python main.py --help --verbose
```

### **Debugging Scenario**
```bash
# Full debug logging for troubleshooting
python main.py --help --debug
```

### **Custom Configuration**
```bash
# Test with different config file
python main.py --config test-config.yaml --help
```

## 🔍 Troubleshooting

### **Common Issues and Solutions**

#### **"Docker daemon not running"**
```bash
# Start Docker Desktop or Docker service
# On macOS: Start Docker Desktop application
# On Linux: sudo systemctl start docker
```

#### **"ModuleNotFoundError"**
```bash
# Install dependencies
pip install -r requirements.txt
```

#### **"Config file not found"**
```bash
# Make sure config.yaml exists
ls -la config.yaml

# Or specify custom config path
python main.py --config /path/to/config.yaml --help
```

#### **Environment Variable Override Not Working**
```bash
# Test the override functionality
SODA_API_KEY_ID=test-override python -c "
from config import load_config
config = load_config()
print(f'API Key ID: {config.soda.api_key_id}')
"
```

## 📋 Pre-Deployment Checklist

Before deploying to Kubernetes, ensure:

- [ ] ✅ All Python tests pass (`python testing/test_k8s_local.py`)
- [ ] ✅ Docker tests pass (`./testing/test_docker_local.sh`) 
- [ ] ✅ Environment variables work correctly
- [ ] ✅ Config file loads properly
- [ ] ✅ All CLI modes function (`--help`, `--verbose`, `--debug`)
- [ ] ✅ Docker image builds successfully
- [ ] ✅ Container runs with resource limits
- [ ] ✅ Kubernetes YAML files are valid

## 🎯 Next Steps

Once local testing passes:

1. **Push Docker Image**:
   ```bash
   docker tag soda-collibra-integration:test your-registry/soda-collibra-integration:latest
   docker push your-registry/soda-collibra-integration:latest
   ```

2. **Update Kubernetes Files**:
   - Replace `<customer-name>` in YAML files
   - Update image URLs to your registry
   - Adjust resource limits if needed

3. **Deploy to Kubernetes**:
   ```bash
   kubectl apply -f k8s/cronjob.yaml
   kubectl apply -f k8s/debug.yaml
   ```

4. **Monitor Deployment**:
   ```bash
   kubectl get cronjobs -n your-namespace
   kubectl logs -f -n your-namespace -l app=soda-collibra-integration
   ```

## 💡 Tips

- **Use test credentials** for initial testing to avoid API rate limits
- **Test with real credentials** once basic functionality is confirmed
- **Monitor resource usage** during local Docker tests to optimize K8s limits
- **Test different logging levels** to choose the right one for production
- **Validate YAML files** before deploying to catch syntax errors early

Happy testing! 🎉 