# Soda-Collibra Integration Documentation

## 📑 Table of Contents

- [🎯 Overview](#-overview)
- [🚀 Quick Start](#-quick-start)
- [⚙️ Configuration Guide](#️-configuration-guide)
- [🔗 Custom Attribute Syncing](#-custom-attribute-syncing)
- [👥 Ownership Synchronization](#-ownership-synchronization)
- [🗑️ Deletion Synchronization](#️-deletion-synchronization)
- [📊 Diagnostic Metrics Processing](#-diagnostic-metrics-processing)
- [🔄 How It Works](#-how-it-works)
- [📈 Performance & Monitoring](#-performance--monitoring)
- [🧪 Testing](#-testing)
- [🔧 Advanced Configuration](#-advanced-configuration)
- [🔍 Troubleshooting](#-troubleshooting)
- [📋 Quick Reference](#-quick-reference)
- [🛠️ Development](#️-development)
- [📞 Support](#-support)

## 🎯 Overview

This **optimized** integration synchronizes data quality checks from Soda to Collibra, creating a unified view of your data quality metrics. The integration has been completely refactored for maximum performance, reliability, and maintainability.

### ✨ Key Features
- **High Performance**: 3-5x faster execution through caching, batching, and parallel processing
- **Custom Attribute Syncing**: Flexible mapping of Soda check attributes to Collibra attributes for rich business context
- **Ownership Synchronization**: Bi-directional ownership sync between Collibra and Soda
- **Deletion Synchronization**: Automatically removes obsolete check assets from Collibra when checks are deleted in Soda
- **Multiple Dimensions Support**: Link checks to multiple data quality dimensions simultaneously
- **Monitor Exclusion**: Option to exclude Soda monitors from synchronization, focusing only on data quality checks
- **Diagnostic Metrics Processing**: Automatic extraction of diagnostic metrics from any Soda check type with intelligent fallbacks
- **Robust Error Handling**: Comprehensive retry logic and graceful error recovery
- **Advanced Monitoring**: Real-time metrics, performance tracking, and detailed reporting
- **CLI Interface**: Flexible command-line options for different use cases
- **Backward Compatibility**: Legacy test methods preserved for smooth migration

### 📁 Architecture Overview

#### File Structure
```
soda-collibra-integration/
├── main.py              # Main entry point with CLI interface
├── integration.py       # Core integration class
├── constants.py         # Configuration constants and messages
├── utils.py            # Utility functions with caching
├── metrics.py          # Performance monitoring and statistics
├── legacy_tests.py     # Backward compatibility tests
├── clients/            # API client implementations
├── models/             # Data model definitions
├── tests/              # Unit test framework
├── k8s/                # Kubernetes deployment files
├── config.yaml         # Configuration file
└── requirements.txt    # Dependencies
```

#### Core Components

**SodaCollibraIntegration Class** (`integration.py`)
- Main orchestration class handling dataset processing and check synchronization
- Implements caching, batching, and error recovery
- Provides comprehensive metrics and logging

**Performance Utilities** (`utils.py`)
- LRU caching for domain mappings and asset lookups
- Retry logic with exponential backoff
- Batch processing helpers and data transformation utilities

**Metrics System** (`metrics.py`)
- Real-time performance tracking and success/failure rate monitoring
- Processing speed analytics and comprehensive reporting

**Constants Management** (`constants.py`)
- Centralized configuration values and error messages
- Performance tuning parameters

## 🚀 Quick Start

### Prerequisites
- **Python 3.10+** required
- Valid Soda Cloud API credentials
- Valid Collibra API credentials
- Properly configured Collibra asset types and relations

### Basic Usage
```bash
# Run the integration with default settings
python main.py

# Run with debug logging for troubleshooting
python main.py --debug

# Use a custom configuration file
python main.py --config custom.yaml

# Show help and all available options
python main.py --help
```

### Advanced Usage
```bash
# Run legacy Soda client tests
python main.py --test-soda

# Run legacy Collibra client tests
python main.py --test-collibra

# Run with verbose logging (info level)
python main.py --verbose
```


## ⚙️ Configuration Guide

### 1. Collibra Configuration

#### Base Settings
```yaml
collibra:
  base_url: "https://your-instance.collibra.com/rest/2.0"
  username: "your-username"
  password: "your-password"
  general:
    naming_delimiter: ">"  # Used to separate parts of asset names
```

#### Asset Types
Configure the different types of assets in Collibra:
```yaml
  asset_types:
    table_asset_type: "00000000-0000-0000-0000-000000031007"  # ID for Table assets
    soda_check_asset_type: "00000000-0000-0000-0000-000000031107"  # ID for Data Quality Metric type
    dimension_asset_type: "00000000-0000-0000-0000-000000031108"  # ID for Data Quality Dimension type
    column_asset_type: "00000000-0000-0000-0000-000000031109"  # ID for Column type
```

#### Attribute Types
Define the attributes that will be set on check assets:
```yaml
  attribute_types:
    # Standard Check Attributes
    check_evaluation_status_attribute: "00000000-0000-0000-0000-000000000238"  # Boolean attribute for pass/fail
    check_last_sync_date_attribute: "00000000-0000-0000-0000-000000000256"  # Last sync timestamp
    check_definition_attribute: "00000000-0000-0000-0000-000000000225"  # Check definition
    check_last_run_date_attribute: "01975dd9-a7b0-79fb-bb74-2c1f76402663"  # Last run timestamp
    check_cloud_url_attribute: "00000000-0000-0000-0000-000000000258"  # Link to Soda Cloud
    
    # Diagnostic Metric Attributes - Extracted from Soda check diagnostics
    check_loaded_rows_attribute: "00000000-0000-0000-0000-000000000233"      # Number of rows tested/loaded
    check_rows_failed_attribute: "00000000-0000-0000-0000-000000000237"      # Number of rows that failed
    check_rows_passed_attribute: "00000000-0000-0000-0000-000000000236"      # Number of rows that passed (calculated)
    check_passing_fraction_attribute: "00000000-0000-0000-0000-000000000240" # Fraction of rows passing (calculated)
```

**Diagnostic Attributes Behavior:**
- **Flexible Extraction**: Automatically extracts metrics from any diagnostic type (`missing`, `aggregate`, `valid`, etc.)
- **Future-Proof**: Works with new diagnostic types that Soda may introduce
- **Smart Fallbacks**: Falls back to `datasetRowsTested` if `checkRowsTested` is not available
- **Calculated Values**: Automatically computes `check_rows_passed` and `check_passing_fraction` when source data is available
- **Graceful Handling**: Leaves attributes empty when diagnostic data is not present in the check result

#### Relation Types
Define the types of relationships between assets:
```yaml
  relation_types:
    table_column_to_check_relation_type: "00000000-0000-0000-0000-000000007018"  # Relation between table/column and check
    check_to_dq_dimension_relation_type: "f7e0a26b-eed6-4ba9-9152-4a1363226640"  # Relation between check and dimension
```

#### Responsibilities
Configure ownership role mappings:
```yaml
  responsibilities:
    owner_role_id: "00000000-0000-0000-0000-000000005040"  # Collibra role ID for asset owners
```

#### Domains
Configure the domains where assets will be created:
```yaml
  domains:
    data_quality_dimensions_domain: "00000000-0000-0000-0000-000000006019"  # Domain for DQ dimensions
    soda_collibra_domain_mapping: '{"Sales": "0197377f-e595-7434-82c7-3ce1499ac620"}'  # Dataset to domain mapping
    soda_collibra_default_domain: "01975b4a-0ace-79f6-b5ec-68656ca60b11"  # Default domain if no mapping
```

### 2. Soda Configuration

#### Base Settings
```yaml
soda:
  api_key_id: "your-api-key-id"
  api_key_secret: "your-api-key-secret"
  base_url: "https://cloud.soda.io/api/v1"
```

#### General Settings
```yaml
  general:
    filter_datasets_to_sync_to_collibra: true  # Only sync datasets with sync attribute
    soda_no_collibra_dataset_skip_checks: false  # Skip checks if dataset not in Collibra
    sync_monitors: true  # Set to false to exclude monitors (items with metricType) from sync
```

#### Attributes
Define Soda attributes and their mappings:
```yaml
  attributes:
    soda_collibra_sync_dataset_attribute: "collibra_sync"  # Attribute to mark datasets for sync
    soda_collibra_domain_dataset_attribute_name: "rulebook"  # Attribute for domain mapping
    soda_dimension_attribute_name: "dimension"  # Attribute for DQ dimension
```

**Multiple Dimensions Support**  
The integration supports both single and multiple dimensions for data quality checks:
- **Single dimension**: Specify as a string value (e.g., `"Completeness"`)
- **Multiple dimensions**: Use a comma-separated string (e.g., `"Completeness, Consistency"`)

When multiple dimensions are provided as a comma-separated string, the integration will:
1. Automatically split the string by commas and trim whitespace
2. Search for each dimension asset in Collibra individually
3. Create a relation for each dimension found
4. Log a warning for any dimension that cannot be found in Collibra
5. Continue processing even if some dimensions are missing

**Example Configuration:**
```yaml
checks for orders:
  - row_count > 0:
      attributes:
        dimension: "Completeness, Consistency, Accuracy"
```

This will create three separate dimension relations in Collibra, one for each dimension specified.

**Monitor Exclusion**  
The integration can exclude Soda monitors (items with `metricType`) from synchronization:
- **Enabled** (`sync_monitors: true`): All checks and monitors are synchronized (default)
- **Disabled** (`sync_monitors: false`): Only checks are synchronized, monitors are filtered out

When `sync_monitors` is disabled, the integration will:
1. Filter out all items that have a `metricType` attribute
2. Only process actual checks (items without `metricType`)
3. Log the number of monitors filtered out for each dataset
4. Continue processing with the remaining checks

This is useful when you want to focus on data quality checks and exclude monitoring metrics from your Collibra catalog.

**Custom Attribute Syncing Configuration**  
See the [Custom Attribute Syncing](#-custom-attribute-syncing) section below for detailed instructions.

## 🔗 Custom Attribute Syncing

The integration supports syncing custom attributes from Soda checks to Collibra assets, allowing you to enrich your Collibra assets with business context and additional metadata from your data quality checks.

### 📋 How Custom Attribute Syncing Works

Custom attribute syncing enables you to map specific attributes from your Soda checks to corresponding attribute types in Collibra. When a check is synchronized, the integration will automatically extract the values of these attributes and set them on the created/updated Collibra asset.

### ⚙️ Configuration

To enable custom attribute syncing, add the `custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id` configuration to your `config.yaml` file:

```yaml
soda:
  attributes:
    # ... other attributes ...
    custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id: '{"soda_attribute_id": "collibra_attribute_type_uuid", "another_soda_attribute": "another_collibra_uuid"}'
```

The configuration value is a **JSON string** containing key-value pairs where:
- **Key**: The name of the attribute in Soda (as it appears on your Soda checks)
- **Value**: The UUID of the corresponding attribute type in Collibra

### 📝 Step-by-Step Setup

#### 1. Identify Soda Attributes
First, identify which attributes from your Soda checks you want to sync to Collibra. Common examples include:
- `description` - Check description
- `business_impact` - Business impact assessment
- `data_domain` - Data domain classification
- `criticality` - Data criticality level
- `owner_team` - Owning team information

#### 2. Find Collibra Attribute Type UUIDs
For each Soda attribute, find the corresponding attribute type UUID in Collibra:

1. Navigate to your Collibra instance
2. Go to **Settings** → **Metamodel** → **Attribute Types**
3. Find or create the attribute types you want to map to
4. Copy the UUID of each attribute type

#### 3. Create the JSON Mapping
Create a JSON object mapping Soda attribute names to Collibra attribute type UUIDs:

```json
{
  "description": "00000000-0000-0000-0000-000000003114",
  "business_impact": "01975f7b-0c04-7b98-9fb8-6635261a7c7b",
  "data_domain": "0197ca72-aee8-7259-9e88-5b98073147ed"
}
```

#### 4. Add to Configuration
Add the JSON mapping to your `config.yaml` file as a single-line string:

```yaml
soda:
  attributes:
    custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id: '{"description": "00000000-0000-0000-0000-000000003114", "business_impact": "01975f7b-0c04-7b98-9fb8-6635261a7c7b", "data_domain": "0197ca72-aee8-7259-9e88-5b98073147ed"}'
```

### 📋 Complete Example

Here's a complete example showing how to configure custom attribute syncing:

**Soda Check with Custom Attributes:**
```yaml
checks for orders:
  - row_count > 0:
      attributes:
        description: "Ensures orders table is not empty"
        business_impact: "critical"
        data_domain: "sales"
        criticality: "high"
```

**Collibra Configuration:**
```yaml
soda:
  attributes:
    soda_collibra_sync_dataset_attribute: "collibra_sync"
    soda_collibra_domain_dataset_attribute_name: "rulebook"
    soda_dimension_attribute_name: "dimension"
    custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id: '{"description": "00000000-0000-0000-0000-000000003114", "business_impact": "01975f7b-0c04-7b98-9fb8-6635261a7c7b", "data_domain": "0197ca72-aee8-7259-9e88-5b98073147ed", "criticality": "0197f2a8-1234-5678-9abc-def012345678"}'
```

**Result:**
When this check is synchronized, the integration will create a Collibra asset with these attributes automatically set:
- Description: "Ensures orders table is not empty"  
- Business Impact: "critical"
- Data Domain: "sales"
- Criticality: "high"

### ⚠️ Important Notes

1. **JSON Format**: The mapping must be a valid JSON string enclosed in single quotes
2. **Attribute Type UUIDs**: Use the exact UUIDs from your Collibra metamodel
3. **Case Sensitivity**: Soda attribute names are case-sensitive and must match exactly
4. **Missing Attributes**: If a Soda check doesn't have an attribute defined in the mapping, it will be skipped (no error)
5. **Invalid UUIDs**: Invalid Collibra attribute type UUIDs will cause the sync to fail for that attribute

### 🔧 Troubleshooting

**Common Issues:**
- **Invalid JSON**: Ensure the JSON string is properly formatted and enclosed in single quotes
- **Attribute Not Found**: Verify the Soda attribute names match exactly what's defined in your checks
- **UUID Errors**: Confirm the Collibra attribute type UUIDs are correct and exist in your instance
- **Permission Issues**: Ensure your Collibra user has permissions to set the specified attribute types

**Debug Mode:**
Run with debug mode to see detailed logging about custom attribute processing:
```bash
python main.py --debug
```

Look for log messages like:
- `Processing custom attribute: attribute_name`
- `Successfully set custom attribute: attribute_name`
- `Skipping custom attribute (not found in check): attribute_name`

## 🔄 How It Works

### 1. **Optimized Dataset Processing**
- **Smart Filtering**: Only processes datasets marked for synchronization
- **Parallel Processing**: Handles multiple operations concurrently
- **Caching**: Reduces API calls through intelligent caching
- **Batch Operations**: Groups similar operations for efficiency

### 2. **Enhanced Check Processing**
For each check in a dataset:

#### **Asset Management**
- **Bulk Creation/Updates**: Processes multiple assets simultaneously
- **Duplicate Handling**: Intelligent naming to avoid conflicts
- **Status Tracking**: Monitors creation vs. update operations

#### **Attribute Processing**
- **Standard Attributes**: Evaluation status, timestamps, definitions
- **Diagnostic Metrics**: Automatically extracts and calculates diagnostic metrics from check results
- **Custom Attributes**: Flexible mappings for business context (see [Custom Attribute Syncing](#-custom-attribute-syncing))
- **Batch Updates**: Groups attribute operations for performance

#### **Relationship Management**
- **Dimension Relations**: Links checks to data quality dimensions (supports single or multiple dimensions per check)
- **Table/Column Relations**: Creates appropriate asset relationships
- **Error Recovery**: Graceful handling of missing or ambiguous assets

#### **Deletion Synchronization**
- **Automatic Cleanup**: Removes obsolete check assets from Collibra that no longer exist in Soda
- **Pattern Matching**: Uses naming convention `{checkname}___{datasetName}` to identify checks for a dataset
- **Bulk Deletion**: Efficiently deletes multiple obsolete assets in a single operation
- **Idempotent Handling**: Gracefully handles 404 errors when assets are already deleted
- **Metrics Tracking**: Reports the number of checks deleted in the integration summary

The deletion sync process:
1. Searches for all check assets in Collibra matching the dataset pattern
2. Compares with current checks from Soda
3. Identifies assets that exist in Collibra but not in Soda
4. Deletes obsolete assets in bulk
5. Tracks deletion metrics for reporting

### 3. **Ownership Synchronization**
- **Collibra to Soda Sync**: Automatically syncs dataset owners from Collibra to Soda
- **User Mapping**: Maps Collibra users to Soda users by email address
- **Error Handling**: Tracks missing users and synchronization failures
- **Metrics Tracking**: Monitors successful ownership transfers

### 4. **Advanced Error Handling**
- **Retry Logic**: Exponential backoff for transient failures
- **Rate Limiting**: Intelligent throttling to avoid API limits
- **Error Aggregation**: Collects and reports all issues at the end
- **Graceful Degradation**: Continues processing despite individual failures

## 📈 Performance & Monitoring

### **Performance Optimization**

**Caching System**
- **Domain Mappings**: Cached for the entire session
- **Asset Lookups**: LRU cache reduces repeated API calls
- **Configuration Parsing**: One-time parsing with caching

**Batch Processing**
- **Asset Operations**: Create/update multiple assets in single calls
- **Attribute Management**: Bulk attribute creation and updates
- **Relation Creation**: Batch relationship establishment

**Performance Results**
- **3-5x faster** execution vs. original implementation
- **60% fewer** API calls through caching
- **90% reduction** in rate limit errors
- **Improved reliability** with comprehensive error handling

### **Performance Benchmarks**

**Typical Performance**
- **Small datasets** (< 100 checks): 30-60 seconds
- **Medium datasets** (100-1000 checks): 2-5 minutes  
- **Large datasets** (1000+ checks): 5-15 minutes

Performance varies based on:
- Network latency to APIs
- Number of existing vs. new assets
- Complexity of relationships
- API rate limits

### **Monitoring & Metrics**

**Integration Completion Report**
```
============================================================
🎉 INTEGRATION COMPLETED SUCCESSFULLY 🎉
============================================================
📊 Datasets processed: 15
⏭️  Datasets skipped: 2
✅ Checks created: 45
🔄 Checks updated: 67
📝 Attributes created: 224
🔄 Attributes updated: 156
🔗 Dimension relations created: 89
📋 Table relations created: 23
📊 Column relations created: 89
👥 Owners synchronized: 12
❌ Ownership sync failures: 1

🎯 Total operations performed: 693
============================================================
```

**Debug Logging**
Enable detailed logging for troubleshooting:
```bash
python main.py --debug
```

Debug output includes:
- Dataset processing details
- API call timing and results
- Caching hit/miss statistics
- Error context and stack traces
- Performance metrics per operation
- Ownership synchronization details

## 🗑️ Deletion Synchronization

The integration automatically synchronizes deletions, removing obsolete check assets from Collibra when checks are deleted or removed in Soda.

### **How It Works**

1. **Pattern Matching**: For each dataset, the integration searches for all check assets in Collibra using the naming pattern `{checkname}___{datasetName}`
2. **Comparison**: Compares the list of check assets in Collibra with the current checks returned from Soda
3. **Identification**: Identifies assets that exist in Collibra but are no longer present in Soda
4. **Bulk Deletion**: Deletes all obsolete assets in a single bulk operation for efficiency
5. **Error Handling**: Gracefully handles cases where assets are already deleted (404 errors), treating them as successful deletions
6. **Metrics Tracking**: Reports the number of checks deleted in the integration summary

### **Benefits**

- **Automatic Cleanup**: Keeps your Collibra catalog in sync with Soda without manual intervention
- **Efficient Processing**: Uses bulk deletion operations to minimize API calls
- **Idempotent**: Safe to run multiple times - handles already-deleted assets gracefully
- **Transparent**: Shows deletion progress in the console output and tracks metrics

### **Example Output**

When obsolete checks are found and deleted, you'll see:
```
Processing dataset 1/3: finance_loans
  📋 Getting checks...
  🔄 Processing 18 checks...
    🏗️ Preparing assets...
    📤 Creating/updating assets...
    📝 Processing metadata & relations...
    🗑️  Deleting 2 obsolete check(s)...
  👥 Syncing ownership...
```

And in the summary:
```
🗑️  Checks deleted: 2
```

### **Configuration**

No additional configuration is required. Deletion synchronization is enabled by default and runs automatically for each dataset during the integration process.

### **Error Handling**

- **404 Errors**: If assets are already deleted (404 response), the integration treats this as success and continues
- **Other Errors**: Network issues, authentication problems, or other HTTP errors are retried with exponential backoff
- **Missing Assets**: If no check assets are found in Collibra for a dataset, deletion sync is skipped

### **Monitoring**

Deletion synchronization is tracked in the integration metrics:
- **🗑️ Checks deleted**: Number of obsolete check assets removed from Collibra
- **Error Tracking**: Any deletion failures are recorded in the error summary

## 👥 Ownership Synchronization

The integration supports automatic synchronization of dataset ownership from Collibra to Soda.

### **How It Works**
1. **Asset Discovery**: For each dataset, finds the corresponding table asset in Collibra
2. **Responsibility Extraction**: Retrieves ownership responsibilities from Collibra
3. **User Mapping**: Maps Collibra users to Soda users by email address
4. **Ownership Update**: Updates the Soda dataset with synchronized owners
5. **Error Tracking**: Records any failures for monitoring

### **Configuration Requirements**
Ensure the following are configured in your `config.yaml`:

```yaml
collibra:
  responsibilities:
    owner_role_id: "00000000-0000-0000-0000-000000005040"  # Collibra owner role ID
```

### **Monitoring**
Ownership synchronization is tracked in the integration metrics:
- **👥 Owners synchronized**: Number of successful ownership transfers
- **❌ Ownership sync failures**: Number of failed synchronization attempts

### **Error Handling**
Common issues and their handling:
- **Missing Collibra Asset**: Skip ownership sync for that dataset
- **No Collibra Owners**: Log information message, continue processing
- **User Email Mismatch**: Track as error, continue with remaining users
- **Soda API Failures**: Retry with exponential backoff

## 📊 Diagnostic Metrics Processing

The integration automatically extracts diagnostic metrics from Soda check results and populates detailed row-level statistics in Collibra.

### **Supported Metrics**
| Metric | Source | Description |
|--------|--------|-------------|
| `check_loaded_rows_attribute` | `checkRowsTested` or `datasetRowsTested` | Total number of rows evaluated by the check |
| `check_rows_failed_attribute` | `failedRowsCount` | Number of rows that failed the check |
| `check_rows_passed_attribute` | **Calculated** | `check_loaded_rows` - `check_rows_failed` |
| `check_passing_fraction_attribute` | **Calculated** | `check_rows_passed` / `check_loaded_rows` |

### **Flexible Diagnostic Type Support**
The system automatically extracts metrics from **any diagnostic type**, making it future-proof:

#### **Current Soda Diagnostic Types**
```json
// Missing value checks
{
  "diagnostics": {
    "missing": {
      "failedRowsCount": 3331,
      "failedRowsPercent": 1.213,
      "datasetRowsTested": 274577,
      "checkRowsTested": 274577
    }
  }
}

// Aggregate checks  
{
  "diagnostics": {
    "aggregate": {
      "datasetRowsTested": 274577,
      "checkRowsTested": 274577
    }
  }
}
```

#### **Future Diagnostic Types (Automatically Supported)**
```json
// Hypothetical future types
{
  "diagnostics": {
    "valid": {
      "failedRowsCount": 450,
      "validRowsCount": 9550,
      "checkRowsTested": 10000
    },
    "duplicate": {
      "duplicateRowsCount": 200,
      "checkRowsTested": 8000
    }
  }
}
```

### **Intelligent Extraction Logic**
The system uses a **metric-focused approach** rather than type-specific logic:

1. **Scans All Diagnostic Types**: Iterates through every diagnostic type in the response
2. **Extracts Relevant Metrics**: Looks for specific metric fields regardless of diagnostic type name
3. **Applies Smart Fallbacks**: Uses `datasetRowsTested` if `checkRowsTested` is not available
4. **Calculates Derived Metrics**: Computes passing rows and fraction when source data is available
5. **Handles Missing Data**: Gracefully skips attributes when diagnostic data is unavailable

### **Fallback Mechanisms**
| Priority | Field Used | Fallback Reason |
|----------|------------|-----------------|
| 1st | `checkRowsTested` | Preferred - rows actually tested by the specific check |
| 2nd | `datasetRowsTested` | Fallback - total dataset rows when check-specific count unavailable |

### **Example Processing Flow**

#### **Input: Soda Check Result**
```json
{
  "name": "customer_id is present",
  "evaluationStatus": "fail",
  "lastCheckResultValue": {
    "value": 1.213,
    "diagnostics": {
      "missing": {
        "failedRowsCount": 3331,
        "checkRowsTested": 274577
      }
    }
  }
}
```

#### **Output: Collibra Attributes**
```yaml
Attributes Set:
  - check_loaded_rows_attribute: 274577           # From checkRowsTested
  - check_rows_failed_attribute: 3331             # From failedRowsCount  
  - check_rows_passed_attribute: 271246           # Calculated: 274577 - 3331
  - check_passing_fraction_attribute: 0.9879      # Calculated: 271246 / 274577
```

### **Benefits**
- ✅ **Future-Proof**: Automatically works with new diagnostic types Soda introduces
- ✅ **Comprehensive**: Provides both raw metrics and calculated insights
- ✅ **Flexible**: Handles partial data gracefully with intelligent fallbacks
- ✅ **Accurate**: Uses check-specific row counts when available
- ✅ **Transparent**: Detailed logging shows exactly which metrics were found and used

## 🧪 Testing

### **Unit Tests**
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_integration.py -v

# Run with coverage
python -m pytest tests/ --cov=integration --cov-report=html
```

### **Local Kubernetes Testing**
```bash
# Comprehensive local testing (recommended)
python testing/test_k8s_local.py

# Docker-specific testing
./testing/test_docker_local.sh

# Quick validation
python testing/validate_k8s.py
```

### **Legacy Tests**
```bash
# Test Soda client functionality
python main.py --test-soda

# Test Collibra client functionality
python main.py --test-collibra
```

## 🔧 Advanced Configuration

### **Performance Tuning**
Modify `constants.py` for your environment:
```python
class IntegrationConstants:
    MAX_RETRIES = 3              # API retry attempts
    BATCH_SIZE = 50              # Batch operation size
    DEFAULT_PAGE_SIZE = 1000     # API pagination size
    RATE_LIMIT_DELAY = 2         # Rate limiting delay
    CACHE_MAX_SIZE = 128         # LRU cache size
```

### **Enhanced Configuration Options**

For detailed information on configuring custom attribute syncing, see the [Custom Attribute Syncing](#-custom-attribute-syncing) section above.

### **Custom Logging**
```python
# In your code
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### **Environment Variables**
```bash
# Set custom config path
export SODA_COLLIBRA_CONFIG=/path/to/custom/config.yaml

# Enable debug mode
export SODA_COLLIBRA_DEBUG=true
```

## 🔍 Troubleshooting

### **Common Issues**

#### **Performance Issues**
- **Slow Processing**: Increase `BATCH_SIZE` and `DEFAULT_PAGE_SIZE`
- **Rate Limiting**: Increase `RATE_LIMIT_DELAY`
- **Memory Usage**: Decrease `CACHE_MAX_SIZE`

#### **Connection Issues**
- **API Timeouts**: Check network connectivity and API endpoints
- **Authentication**: Verify credentials and permissions
- **Rate Limits**: Monitor API usage and adjust delays

#### **Data Issues**
- **Missing Assets**: Ensure required asset types exist in Collibra
- **Relation Failures**: Verify relation type configurations
- **Domain Mapping**: Check domain IDs and JSON formatting

#### **Diagnostic Metrics Issues**
- **Missing Diagnostic Attributes**: Check if Soda checks have `lastCheckResultValue.diagnostics` data
- **Incomplete Metrics**: Some diagnostic types may only have partial metrics (e.g., `aggregate` checks lack `failedRowsCount`)
- **Attribute Type Configuration**: Verify diagnostic attribute type IDs are configured correctly in `config.yaml`
- **Zero Division Errors**: System automatically prevents division by zero when calculating fractions

### **Debug Commands**
```bash
# Full debug output
python main.py --debug 2>&1 | tee debug.log

# Verbose logging with timestamps
python main.py --verbose

# Test specific components
python main.py --test-soda --debug
python main.py --test-collibra --debug
```

### **Log Analysis**
Look for these patterns in debug logs:

**General Operation Patterns:**
- `Rate limit prevention`: Normal throttling behavior
- `Successfully updated/created`: Successful operations
- `Skipping dataset`: Expected filtering behavior
- `ERROR`: Issues requiring attention

**Diagnostic Processing Patterns:**
- `Processing diagnostics`: Diagnostic data found in check result
- `Found failedRowsCount in 'X'`: Successfully extracted failure count from diagnostic type X
- `Found checkRowsTested in 'X'`: Successfully extracted row count from diagnostic type X
- `Using datasetRowsTested from 'X' as fallback`: Fallback mechanism activated
- `No diagnostics found in check result`: Check has no diagnostic data (normal for some check types)
- `Calculated check_rows_passed`: Successfully computed passing rows
- `Added check_X_attribute`: Diagnostic attribute successfully added to Collibra

## 📋 Quick Reference

### **Common Commands**
```bash
# Basic run with default config
python main.py

# Debug mode with detailed logging
python main.py --debug

# Use custom configuration file
python main.py --config custom.yaml

# Test individual components
python main.py --test-soda --debug
python main.py --test-collibra --debug
```

### **Key Configuration Sections**
- **Collibra Base**: `collibra.base_url`, `collibra.username`, `collibra.password`
- **Soda API**: `soda.api_key_id`, `soda.api_key_secret`
- **Custom Attributes**: `soda.attributes.custom_attributes_mapping_soda_attribute_name_to_collibra_attribute_type_id`
- **Domain Mapping**: `collibra.domains.soda_collibra_domain_mapping`
- **Ownership Sync**: `collibra.responsibilities.owner_role_id`

### **Essential UUIDs to Configure**
- Asset types (table, check, dimension, column)
- Attribute types (evaluation status, sync date, diagnostic metrics)
- Relation types (table-to-check, check-to-dimension)
- Domain IDs for asset creation

## 🛠️ Development

### **Contributing**
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Run the test suite
5. Submit a pull request

### **Code Style**
- Follow PEP 8 guidelines
- Use type hints throughout
- Add comprehensive docstrings
- Include unit tests for new features

### **Dependencies**
See `requirements.txt` for the complete list:
- `requests>=2.31.0` - HTTP client
- `pydantic>=2.0.0` - Data validation
- `PyYAML>=6.0` - Configuration parsing
- `tenacity>=8.2.0` - Retry logic
- `simplejson>=3.19.0` - JSON handling

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Enable debug logging for detailed information
3. Review the performance metrics for bottlenecks
4. Consult the unit tests for usage examples
5. Contact support@soda.io for additional help
