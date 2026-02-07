# Soda-Collibra Integration

This **optimized** integration synchronizes data quality checks from Soda to Collibra, featuring 3-5x performance improvements, robust error handling, advanced monitoring capabilities, and **ownership synchronization**.

## 🚀 Quick Start

```bash
# Run the integration
python main.py

# Run with verbose logging
python main.py --verbose

# Run with debug logging
python main.py --debug

# Test individual components
python main.py --test-soda
python main.py --test-collibra
```

## 🧪 Testing

Before deploying to Kubernetes, test your setup locally:

```bash
# Comprehensive local testing (recommended)
python testing/test_k8s_local.py

# Docker-specific testing  
./testing/test_docker_local.sh

# Quick validation
python testing/validate_k8s.py
```

See `testing/README.md` for detailed testing instructions.

## 📚 Documentation

- **`documentation.md`** - Complete integration guide and configuration
- **`k8s/README.md`** - Kubernetes deployment instructions
- **`testing/LOCAL_TESTING_GUIDE.md`** - Local testing guide

### Prerequisites
- **Python 3.10+** required
- Dataset attributes need to be created first in the UI
- Collibra attributes created manually and assigned to correct asset type
- Collibra relations created manually and assigned to correct asset type

## 📋 To-Do List

- [ ] Add logic and configuration to decide what happens in Collibra if a check is deleted in Soda
- [ ] Sync Collibra attribute types to Soda attributes
- [ ] Add configuration to push monitors Yes/No
- [ ] Validate check counter correctness. 


## 🔮 Future Enhancements

- [ ] Add option to use publicTypeIDs instead of typeIDs
- [ ] Integrate with Collibra Quality dashboards and lineage overlays when diagnostic warehouse is available in Soda
  - Will include availability of:
    - Rows passed
    - Rows failed
    - Row count on check level

## 🎯 Design Decisions

### Architecture
- Use one single level from an operating model point of view and make it fully configurable

### Scope
- Descope rows_passed, rows_failed and row_count metrics until diagnostic warehouse is available



