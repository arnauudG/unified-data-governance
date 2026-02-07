# Soda Data Quality Configuration

This directory contains the comprehensive Soda data quality monitoring configuration, featuring complete integration with Soda Cloud and Collibra Data Intelligence Cloud for unified data governance and quality management.

## Complete Integration

### Advanced Features Implemented
- **Dataset Discovery**: Automatic table and column discovery across all layers
- **Column Profiling**: Comprehensive statistical analysis with smart exclusions
- **Sample Data Collection**: 100 sample rows per dataset for analysis
- **Failed Row Sampling**: Detailed failure analysis with custom SQL queries
- **API Integration**: Automated metadata extraction for external reporting
- **Collibra Integration**: 
  - Automatic synchronization of quality results to governance catalog
  - Automatic metadata synchronization after each pipeline layer
- **Uppercase Standardization**: Consistent naming across all layers

### Quality Coverage
- **RAW Layer**: 4 tables with initial data quality assessment
- **STAGING Layer**: 4 tables with transformation validation
- **MART Layer**: 2 business-ready tables with strict quality standards
- **QUALITY Layer**: Quality monitoring and check results tracking
- **Total Checks**: 50+ data quality checks across all layers
- **Data Quality Dimensions**: All checks properly categorized with standardized dimensions

## Directory Structure

```
soda/
├── configuration/           # Soda configuration files by layer
│   ├── configuration_raw.yml      # RAW layer configuration
│   ├── configuration_staging.yml  # STAGING layer configuration
│   ├── configuration_mart.yml     # MART layer configuration
│   └── configuration_quality.yml  # QUALITY layer configuration
├── checks/                 # Data quality checks organized by layer
│   ├── raw/               # RAW layer checks (lenient thresholds)
│   ├── staging/           # STAGING layer checks (stricter thresholds)
│   ├── mart/              # MART layer checks (strictest thresholds)
│   └── quality/           # QUALITY layer checks (monitoring)
├── soda-collibra-integration-configuration/
│   └── configuration-collibra.yml  # Collibra integration configuration
├── helpers.py              # Helper functions for data source name derivation
├── update_data_source_names.py  # Script to update config files when DB name changes
└── README.md              # This file
```

## Data Source Name Parameterization

**Data source names are derived from the database name** to ensure consistency across the platform.

### Naming Convention

Data source names follow the pattern: `<database_name_lowercase>_<layer>`

- Database name: `DATA PLATFORM XYZ` (from `SNOWFLAKE_DATABASE` env var)
- Data source names:
  - `data_platform_xyz_raw`
  - `data_platform_xyz_staging`
  - `data_platform_xyz_mart`
  - `data_platform_xyz_quality`

### Automatic Derivation

The Airflow DAG automatically derives data source names from the database name using `soda/helpers.py`. This means:
- ✅ **No manual updates needed** in the DAG when database name changes
- ✅ **Consistent naming** across all layers
- ✅ **Single source of truth** (SNOWFLAKE_DATABASE environment variable)

### Updating Configuration Files

**Automatic Updates**: The YAML configuration files are automatically updated when you run:
- `just setup` - During initial setup
- `just airflow-up` - Before starting Airflow
- `just all-up` - Before starting all services
- `just airflow-trigger-init` - Before triggering initialization
- `just airflow-trigger-pipeline` - Before triggering pipeline

**Manual Update** (if needed):
```bash
# Update all configuration files with new data source names
just soda-update-datasources
# or
python3 soda/update_data_source_names.py
```

This script will:
- Read the `SNOWFLAKE_DATABASE` environment variable
- Generate the appropriate data source names
- Update all configuration files automatically

**Note**: The Airflow DAG uses dynamic data source names, so it will work automatically. The YAML files are kept in sync automatically via justfile targets.

## Collibra Integration

### Overview

The Collibra integration automatically synchronizes data quality results from Soda Cloud to Collibra Data Intelligence Cloud, creating a unified view where quality metrics are linked to data assets (tables and columns) in the governance catalog.

### Integration Flow

```
Soda Quality Checks
    ↓
Soda Cloud (Results Storage)
    ↓
Collibra Integration (Automatic Sync)
    ├──→ Quality Metrics → Table Assets
    ├──→ Check Results → Column Assets
    ├──→ Quality Dimensions → Governance Framework
    └──→ Metadata Sync → Schema & Table Updates
    ↓
Collibra Data Catalog (Unified View)
```

**Note**: Metadata synchronization is handled separately in the `collibra/` directory and runs automatically after each pipeline layer. See [Collibra Integration README](../collibra/README.md) for details.

### Key Features

**Automatic Asset Mapping**
- Tables automatically mapped to Collibra Table assets
- Columns automatically mapped to Collibra Column assets
- Quality checks created as Data Quality Metric assets
- Quality dimensions linked via governance relationships

**Selective Synchronization**
- Only datasets marked with `push_to_collibra_dic` attribute are synced
- Configurable filtering to control which datasets appear in Collibra
- Option to skip checks for datasets not in Collibra

**Quality Metrics Synchronized**
- Check evaluation status (pass/fail)
- Last run and sync timestamps
- Check definitions and configurations
- Diagnostic metrics:
  - Rows tested/loaded
  - Rows passed
  - Rows failed
  - Passing fraction
- Links to Soda Cloud for detailed analysis

**Governance Relationships**
- Table/Column to Check relationships
- Check to Quality Dimension relationships
- Ownership and responsibility tracking
- Domain organization

### Configuration

The Collibra integration is configured in `soda-collibra-integration-configuration/configuration-collibra.yml`.

**Soda Cloud Configuration**
```yaml
soda_cloud:
  general:
    filter_datasets_to_sync_to_collibra: true  # Only sync marked datasets
    soda_no_collibra_dataset_skip_checks: true # Skip if not in Collibra
  attributes:
    soda_collibra_sync_dataset_attribute: "push_to_collibra_dic"
    soda_dimension_attribute_name: "dimension"
```

**Collibra Configuration**
```yaml
collibra:
  base_url: ${COLLIBRA_BASE_URL}
  username: ${COLLIBRA_USERNAME}
  password: ${COLLIBRA_PASSWORD}
  asset_types:
    table_asset_type: "..."  # Collibra Table asset type ID
    column_asset_type: "..." # Collibra Column asset type ID
    soda_check_asset_type: "..." # Data Quality Metric type ID
    dimension_asset_type: "..."  # Data Quality Dimension type ID
```

### Marking Datasets for Collibra Sync

To enable synchronization of a dataset to Collibra, add the sync attribute in your Soda check file:

```yaml
discover datasets:
  datasets:
    - include TABLE_NAME
      attributes:
        push_to_collibra_dic: true
```

### Domain Mapping

Quality results can be organized by Collibra domains:
- Configure domain mapping via `soda_collibra_domain_dataset_attribute_name`
- Set default domain via `soda_collibra_default_domain`
- Quality assets are created in appropriate governance domains

### Required Environment Variables

```bash
# Collibra Configuration
COLLIBRA_BASE_URL=https://your-instance.collibra.com
COLLIBRA_USERNAME=your_username
COLLIBRA_PASSWORD=your_password
```

## Complete Soda Cloud Configuration

### Dataset Discovery
```yaml
discover datasets:
  datasets:
    - include TABLE_NAME
```

### Column Profiling
```yaml
profile columns:
  columns:
    - TABLE_NAME.%
    - exclude TABLE_NAME.CREATED_AT
    - exclude TABLE_NAME.UPDATED_AT
```

### Sample Data Collection
```yaml
sample datasets:
  datasets:
    - include TABLE_NAME
```

### Layer-Specific Quality Standards
- **RAW Layer**: Relaxed thresholds for initial assessment
- **STAGING Layer**: Stricter validation after transformation
- **MARTS Layer**: Business-ready data with strictest requirements
- **Uppercase Naming**: All tables use consistent uppercase column names (CUSTOMER_ID, FIRST_NAME, etc.)

## Usage

### Complete Pipeline Execution

The pipeline runs with full Soda Cloud and Collibra integration:

```bash
# Trigger the complete pipeline with profiling and sampling
just airflow-trigger-pipeline
```

**This executes:**
1. **RAW Layer**: Data initialization + quality checks + profiling + sampling
2. **STAGING Layer**: dbt transformations + quality checks + profiling + sampling
3. **MART Layer**: dbt models + quality checks + profiling + sampling
4. **QUALITY Layer**: Quality monitoring + check results tracking
5. **Soda Cloud**: All results sent to cloud dashboard
6. **Collibra**: Quality results automatically synchronized to governance catalog

### Run Individual Layer Checks

**Note**: Data source names are derived from `SNOWFLAKE_DATABASE`. Use `soda/helpers.py` to get the correct names:

```bash
# Get data source names for your database
python3 -c "from soda.helpers import get_all_data_source_names; import json; print(json.dumps(get_all_data_source_names(), indent=2))"
```

Or use the default names (if `SNOWFLAKE_DATABASE="DATA PLATFORM XYZ"`):

```bash
# RAW layer
soda scan -d data_platform_xyz_raw -c soda/configuration/configuration_raw.yml soda/checks/raw/

# STAGING layer
soda scan -d data_platform_xyz_staging -c soda/configuration/configuration_staging.yml soda/checks/staging/

# MART layer
soda scan -d data_platform_xyz_mart -c soda/configuration/configuration_mart.yml soda/checks/mart/

# QUALITY layer
soda scan -d data_platform_xyz_quality -c soda/configuration/configuration_quality.yml soda/checks/quality/
```

### Test Individual Tables
```bash
# Test specific table (replace data source name if using different database)
soda scan -d data_platform_xyz_raw -c soda/configuration/configuration_raw.yml soda/checks/raw/customers.yml

# Test connection
soda test-connection -d data_platform_xyz_raw -c soda/configuration/configuration_raw.yml
```

## Soda Cloud Integration

All configuration files include Soda Cloud integration:
- **Centralized Monitoring**: Results sent to Soda Cloud platform
- **Automated Alerting**: Get notified when issues occur
- **Historical Trends**: Track data quality over time
- **Team Collaboration**: Share insights with stakeholders

## Configuration

Each layer configuration includes:
- **Data source connection** (database, schema, warehouse)
- **Performance settings** (timeouts, parallel execution)
- **Soda Cloud integration** (API keys, monitoring)
- **Collibra integration** (governance synchronization)

## Data Quality Dimensions

All Soda checks are categorized using standardized data quality dimensions. Each check must include an `attributes` section with a `dimension` field.

### Dimension Mapping

| Check Type | Dimension | Description |
|------------|-----------|-------------|
| `schema` | **Accuracy** | Schema validation ensures table structure integrity |
| `row_count` | **Accuracy** | Row count checks validate data volume correctness |
| `missing_count` | **Completeness** | Missing value checks validate data completeness |
| `duplicate_count` | **Uniqueness** | Duplicate checks ensure record uniqueness |
| `invalid_count` | **Validity** | Format/constraint validation ensures data conforms to rules |
| `freshness` | **Timeliness** | Freshness checks monitor data recency |
| `min` / `max` / `avg` | **Accuracy** | Range and statistical checks validate data correctness |
| `failed rows` | **Accuracy** | General data quality validation |
| `invalid_count` (referential) | **Consistency** | Referential integrity checks ensure cross-table consistency |

### Required Dimensions

All checks must use one of these six dimensions:
- **Accuracy**: Data correctness, schema validation, range checks, business rules
- **Completeness**: Missing value detection and validation
- **Consistency**: Referential integrity and cross-table consistency
- **Uniqueness**: Duplicate detection and prevention
- **Validity**: Format validation, constraint checking, data type validation
- **Timeliness**: Data freshness and recency monitoring

### Example Check with Dimension

```yaml
checks for TABLE_NAME:
  # Completeness check
  - missing_count(CUSTOMER_ID) = 0:
      name: "No missing customer IDs"
      attributes:
        dimension: Completeness
  
  # Uniqueness check
  - duplicate_count(CUSTOMER_ID) = 0:
      name: "Customer IDs are unique"
      attributes:
        dimension: Uniqueness
  
  # Validity check
  - invalid_count(EMAIL) < 100:
      name: "Valid email formats"
      valid regex: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
      attributes:
        dimension: Validity
  
  # Accuracy check (range validation)
  - min(PRICE) >= 0:
      name: "All prices are non-negative"
      attributes:
        dimension: Accuracy
  
  # Timeliness check
  - freshness(CREATED_AT) < 1d:
      name: "Data is fresh"
      attributes:
        dimension: Timeliness
  
  # Consistency check (referential integrity)
  - invalid_count(CUSTOMER_ID) < 100:
      name: "Valid customer references"
      valid values: ['CUST_001', 'CUST_002', 'CUST_003']
      attributes:
        dimension: Consistency
```

### Verification

All checks in this project have been verified to:
- Include the `attributes` section with `dimension` field
- Use one of the six required dimensions
- Follow the standard dimension mapping based on check type

## Maintenance

### Adding New Checks
1. Create check file in appropriate layer directory
2. Follow naming convention: `{table_name}.yml`
3. Use layer-appropriate thresholds
4. **Always include `attributes` section with correct `dimension`**
5. Follow the dimension mapping table above
6. **Add `push_to_collibra_dic: true` attribute if you want to sync to Collibra**
7. Test with `soda scan` command

### Troubleshooting
1. Check connection with `soda test-connection`
2. Validate YAML syntax
3. Review check logic and thresholds
4. Check Snowflake permissions
5. Monitor Soda Cloud connectivity
6. Verify Collibra credentials and asset type IDs
7. Check that datasets are marked for Collibra sync if needed

## Best Practices

1. **Layer Progression**: Start with RAW, progress to MART
2. **Threshold Strategy**: Lenient → Stricter → Strictest
3. **Check Organization**: Group by table and layer
4. **Performance**: Use appropriate warehouse sizes
5. **Monitoring**: Set up automated alerts in Soda Cloud
6. **Governance**: Mark important datasets for Collibra sync
7. **Documentation**: Keep checks well-documented
8. **Testing**: Validate changes before deployment

## Code Quality & Architecture

### Repository Pattern
The Soda integration uses the Repository pattern for clean API access:
- `SodaRepository` handles all API calls
- Automatic retry logic with exponential backoff
- Comprehensive error handling
- Rate limit management

### Testing
- Comprehensive test coverage for repository
- Mock-based testing for API interactions
- Error scenario testing

---

**Last Updated**: February 6, 2026  
**Version**: 2.1.0
