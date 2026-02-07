# Testing Directory

This directory contains all testing and validation scripts for the Soda-Collibra integration Kubernetes deployment.

## 📁 Files Overview

### **Core Testing Scripts**

- **`test_k8s_local.py`** - Comprehensive Python-based testing script
  - Tests configuration loading with environment variable overrides
  - Validates CLI functionality and all modes
  - Tests Docker build process (if Docker is available)
  - Validates Kubernetes YAML files
  - Simulates different deployment scenarios

- **`test_docker_local.sh`** - Docker-specific testing script
  - Tests Docker image build and run
  - Validates environment variable injection
  - Tests config file mounting (simulating ConfigMaps)
  - Tests resource limits
  - Full Kubernetes environment simulation

- **`validate_k8s.py`** - Basic validation script
  - Quick validation of Docker setup
  - Kubernetes files validation
  - Configuration structure validation
  - Environment variable override testing

### **Documentation**

- **`LOCAL_TESTING_GUIDE.md`** - Comprehensive testing guide
  - Step-by-step testing instructions
  - Troubleshooting guide
  - Expected test outputs
  - Pre-deployment checklist

## 🚀 Quick Start

### **Option 1: Comprehensive Testing (Recommended)**
```bash
# From project root directory
python testing/test_k8s_local.py
```

### **Option 2: Docker-Only Testing**
```bash
# From project root directory (requires Docker)
./testing/test_docker_local.sh
```

### **Option 3: Basic Validation**
```bash
# From project root directory
python testing/validate_k8s.py
```

## 📋 What Gets Tested

### **Configuration System**
- ✅ YAML config file loading
- ✅ Environment variable overrides (simulating K8s secrets)
- ✅ Config validation and error handling

### **Application Functionality**
- ✅ CLI interface (`--help`, `--verbose`, `--debug`)
- ✅ Module imports and dependencies
- ✅ Integration class initialization

### **Containerization**
- ✅ Docker image build process
- ✅ Container startup and functionality
- ✅ Environment variable injection
- ✅ Config file mounting (ConfigMap simulation)
- ✅ Resource limits (memory/CPU constraints)

### **Kubernetes Compatibility**
- ✅ YAML manifest validation
- ✅ Deployment scenario simulation
- ✅ Secret and ConfigMap integration
- ✅ Different logging modes

## 🎯 Usage Examples

### **Test with Test Credentials (Safe)**
```bash
python testing/test_k8s_local.py
```

### **Test with Real Credentials**
```bash
python testing/test_k8s_local.py --real-credentials
```

### **Docker Testing with Full Simulation**
```bash
./testing/test_docker_local.sh
```

### **Quick Validation Check**
```bash
python testing/validate_k8s.py
```

## 📊 Expected Results

### **Successful Test Run**
```
🧪 Starting Kubernetes Local Testing
============================================================
✅ Configuration Loading
✅ CLI Functionality  
✅ Docker Build
✅ K8s YAML Validation
✅ K8s Deployment Simulation

📈 Overall: 5/5 tests passed
🎉 All tests passed! Kubernetes deployment is ready.
```

## 🔧 Requirements

### **Python Testing**
- Python 3.8+
- All dependencies from `requirements.txt`
- Access to `config.yaml` in project root

### **Docker Testing**
- Docker installed and running
- Sufficient disk space for image build
- Access to project files for mounting

## 💡 Tips

- **Run from project root**: All scripts expect to be run from the main project directory
- **Use test credentials first**: Avoid API rate limits during initial testing
- **Check Docker status**: Ensure Docker is running before Docker-specific tests
- **Review logs**: Use `--debug` mode for detailed troubleshooting

## 🚨 Troubleshooting

### **Common Issues**

#### **"No such file or directory: config.yaml"**
- Ensure you're running from the project root directory
- Check that `config.yaml` exists in the project root

#### **"Docker daemon not running"**
- Start Docker Desktop (macOS) or Docker service (Linux)
- Verify with `docker --version`

#### **"ModuleNotFoundError"**
- Install dependencies: `pip install -r requirements.txt`
- Ensure you're in the correct Python environment

#### **Permission denied on shell script**
- Make script executable: `chmod +x testing/test_docker_local.sh`

## 📋 Integration with CI/CD

These scripts can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions step
- name: Test Kubernetes Setup
  run: |
    python testing/test_k8s_local.py
    ./testing/test_docker_local.sh
```

For more detailed information, see `LOCAL_TESTING_GUIDE.md` in this directory. 