# Scripts Directory - Utility Scripts

This directory contains utility scripts for environment setup, data management, Soda Cloud API integration, and data quality visualization.

## Directory Structure

```
scripts/
├── setup/                          # Environment setup scripts
│   ├── requirements.txt            # Python dependencies for setup
│   ├── setup_snowflake.py         # Snowflake database and table creation
│   └── reset_snowflake.py         # Snowflake database cleanup
├── soda_dump_api.py               # Soda Cloud API data extraction
├── run_soda_dump.sh               # Soda Cloud data dump runner
├── requirements_dump.txt          # API extraction dependencies
├── organize_soda_data.py          # Organize Soda data in user-friendly structure
├── upload_soda_data_docker.py     # Upload Soda data to Superset database
└── README.md                      # This file
```

## Soda Cloud API Integration

### Automated Metadata Extraction

The Soda Cloud API integration provides automated extraction of dataset and check metadata for external reporting and dashboard creation. The enhanced environment loader now supports dynamic loading of all variables from your .env file.

## Data Organization & Upload

### Data Organization Script
- **`organize_soda_data.py`**: Validates and updates Soda dump data in `superset/data/`
- **Features**: 
  - Validates that required files exist in `superset/data/`
  - Updates `*_latest.csv` files with most recent timestamped data
  - Automatically finds and uses latest files from `soda_dump_output/` if present
  - Falls back to finding latest files in `superset/data/` itself
  - Cleans up temporary `soda_dump_output/` folder automatically
  - Updates `analysis_summary.csv` if available
  - Removes old timestamped files, keeping only latest files

### Data Upload Script
- **`upload_soda_data_docker.py`**: Uploads organized Soda data to Superset PostgreSQL database
- **Features**:
  - Creates dedicated `soda` schema in PostgreSQL
  - Uploads latest data to dedicated tables (`soda.datasets_latest`, `soda.checks_latest`, `soda.analysis_summary`)
  - Handles historical data upload
  - Cleans up temporary files after successful upload

### Usage
```bash
# Complete workflow (recommended)
just superset-upload-data
# This automatically:
# 1. Updates Soda data source names to match SNOWFLAKE_DATABASE
# 2. Extracts data from Soda Cloud
# 3. Organizes data (keeps only latest files)
# 4. Uploads to Superset

# Individual steps
just soda-dump           # Extract from Soda Cloud
just organize-soda-data  # Organize data
just superset-upload-data # Upload to Superset (includes data source name update)

# Database Management
just dump-databases      # Dump all databases (Superset, Airflow, Soda data)
just dump-superset      # Dump Superset database only
just dump-airflow       # Dump Airflow database only
just dump-soda          # Dump Soda data only
```

### Features:
- **Dataset Metadata**: Extract table information, health status, and statistics
- **Check Results**: Retrieve check outcomes, pass/fail rates, and detailed results
- **CSV Export**: Structured data export for external tools
- **Visualization Ready**: Ready-to-use data for dashboard creation
- **API Rate Limiting**: Optimized for efficient data extraction
- **Automatic Cleanup**: Removes old timestamped files, keeps only latest

### Usage:
```bash
# Extract Soda Cloud metadata
just soda-dump

# Manual execution
./scripts/run_soda_dump.sh
```

### Output Files:
- `superset/data/datasets_latest.csv` - Latest dataset metadata (all data sources from Soda Cloud)
- `superset/data/checks_latest.csv` - Latest check results metadata (all data sources from Soda Cloud)
- `superset/data/analysis_summary.csv` - Analysis summary data

**Note**: 
- Old timestamped files are automatically cleaned up. Only the latest files are kept.
- The script fetches ALL data from Soda Cloud API. When creating Superset dashboards, filter by your configured data source names (derived from `SNOWFLAKE_DATABASE`) to focus on your project data.
- Data source names in Soda configuration files are automatically updated before extraction to match your `SNOWFLAKE_DATABASE` environment variable.

## Environment Setup Scripts

### Snowflake Setup (`setup_snowflake.py`)

Creates the complete Snowflake infrastructure with:
- **Database**: Configured via `SNOWFLAKE_DATABASE` environment variable (default: `DATA_GOVERNANCE_PLATFORM`)
- **Schemas**: `RAW`, `STAGING`, `MART`, `QUALITY`
- **Tables**: 4 RAW tables with uppercase column names
- **Sample Data**: 10,000+ customers, 1,000+ products, 20,000+ orders, 50,000+ order items

#### Table Schema (Uppercase Standardization):
```sql
-- CUSTOMERS table
CUSTOMER_ID VARCHAR(50) PRIMARY KEY,
FIRST_NAME VARCHAR(100),
LAST_NAME VARCHAR(100),
EMAIL VARCHAR(255),
PHONE VARCHAR(50),
-- ... other columns

-- PRODUCTS table  
PRODUCT_ID VARCHAR(50) PRIMARY KEY,
PRODUCT_NAME VARCHAR(255),
CATEGORY VARCHAR(100),
-- ... other columns

-- ORDERS table
ORDER_ID VARCHAR(50) PRIMARY KEY,
CUSTOMER_ID VARCHAR(50),
ORDER_DATE DATE,
-- ... other columns

-- ORDER_ITEMS table
ORDER_ITEM_ID VARCHAR(50) PRIMARY KEY,
ORDER_ID VARCHAR(50),
PRODUCT_ID VARCHAR(50),
-- ... other columns
```

### Snowflake Reset (`reset_snowflake.py`)

Cleans up the Snowflake environment:
- Drops all tables and schemas
- Removes sample data
- Resets to clean state

## Configuration

### Environment Variables

Required environment variables in `.env`:
```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DATA_GOVERNANCE_PLATFORM  # Database name (default: DATA_GOVERNANCE_PLATFORM if not set)
SNOWFLAKE_SCHEMA=RAW

# Soda Cloud Configuration
SODA_CLOUD_HOST=https://cloud.soda.io
SODA_CLOUD_API_KEY_ID=your_api_key_id
SODA_CLOUD_API_KEY_SECRET=your_api_key_secret

# Collibra Configuration (for governance integration)
COLLIBRA_BASE_URL=https://your-instance.collibra.com
COLLIBRA_USERNAME=your_username
COLLIBRA_PASSWORD=your_password
```

### Dependencies

#### Setup Dependencies (`setup/requirements.txt`):
```
snowflake-connector-python
faker
python-dotenv
pandas
numpy
```

#### API Dependencies (`requirements_dump.txt`):
```
pandas
requests
python-dotenv
```

## Usage Examples

### Complete Environment Setup
```bash
# 1. Start Airflow services
just airflow-up

# 2. Initialize Snowflake (creates tables with uppercase columns)
just airflow-trigger-init

# 3. Run data quality pipeline
just airflow-trigger-pipeline

# 4. Extract Soda Cloud metadata
just soda-dump
```

### Manual Script Execution
```bash
# Setup Snowflake manually
python3 scripts/setup/setup_snowflake.py

# Reset Snowflake manually  
python3 scripts/setup/reset_snowflake.py

# Extract Soda Cloud data manually
python3 scripts/soda_dump_api.py
```

## Data Quality Features

### Sample Data Generation
- **Realistic Data**: Faker-generated realistic customer and product data
- **Quality Issues**: Intentionally introduced data quality problems for testing
- **Volume**: Production-scale data volumes (10K+ customers, 50K+ order items)
- **Relationships**: Proper foreign key relationships between tables

### Data Quality Issues Introduced
- **Missing Values**: 10% of records have missing email/phone
- **Invalid Formats**: Invalid email formats and negative prices
- **Duplicate Data**: Intentional duplicates for uniqueness testing
- **Future Dates**: Invalid future timestamps for freshness testing

## Troubleshooting

### Common Issues

1. **Snowflake Connection**
   - Verify credentials in `.env`
   - Check warehouse is running
   - Ensure proper permissions

2. **Soda Cloud API**
   - Verify API keys are correct
   - Check network connectivity
   - Monitor rate limits

3. **Data Generation**
   - Ensure sufficient warehouse compute
   - Check for memory issues with large datasets
   - Verify table creation permissions

### Log Locations
- Setup logs: Available in Airflow UI
- API logs: Console output and CSV files
- Error logs: Check Airflow task logs

## Best Practices

1. **Environment Setup**: Always use the initialization DAG for consistent setup
2. **Data Reset**: Use reset script for clean environment testing
3. **API Usage**: Monitor rate limits and implement appropriate delays
4. **Error Handling**: Check logs for detailed error information
5. **Testing**: Validate setup before running full pipeline
6. **Data Management**: Use the automated cleanup to keep only latest files

## Dynamic File Finding & Smart Filtering

The `soda_dump_api.py` script fetches ALL data from Soda Cloud, and provides intelligent filtering:

### API Script Features:
- **Complete Data Fetch**: Retrieves ALL datasets and checks from Soda Cloud
- **Latest Files Only**: Creates only `_latest.csv` files (no timestamped files)
- **Automatic Cleanup**: Removes old timestamped files automatically
- **Enhanced Logging**: Better error messages and progress tracking

### Notebook Filtering Features:
- **Dynamic File Discovery**: Automatically finds latest files without hardcoding
- **Smart Data Source Filtering**: Filters for your configured data sources (derived from `SNOWFLAKE_DATABASE`)
- **Flexible Analysis**: Can analyze different subsets by changing filter criteria
- **Enhanced Error Handling**: Clear messages when files are missing

### Usage Example:
```python
import sys
sys.path.append('scripts')
from soda_dump_api import SodaCloudDump
import pandas as pd

# Find latest files dynamically (defaults to superset/data/)
datasets_file = SodaCloudDump.get_latest_datasets_file('superset/data')
checks_file = SodaCloudDump.get_latest_checks_file('superset/data')

# Load all data
datasets_df = pd.read_csv(datasets_file)
checks_df = pd.read_csv(checks_file)

# Filter for your project (data source names derived from SNOWFLAKE_DATABASE)
# Example: If SNOWFLAKE_DATABASE=DATA_GOVERNANCE_PLATFORM, data sources are:
from soda.helpers import get_all_data_source_names

# Get data source names dynamically from your database name
data_source_names = get_all_data_source_names()
# Returns: {'raw': 'data_governance_platform_raw', 'staging': 'data_governance_platform_staging', ...}

# Filter datasets for your configured data sources
filtered_datasets = datasets_df[datasets_df['datasource'].isin(data_source_names.values())]
```

### Benefits:
- **Complete Data Access**: All Soda Cloud data available for analysis
- **Smart Filtering**: Notebook automatically filters for your project
- **Flexible Analysis**: Can analyze different data sources by changing filters
- **No Hardcoded Timestamps**: Always finds the most recent data
- **Production Ready**: Perfect for notebooks and automated scripts
- **Error Handling**: Gracefully handles missing files
- **Clean Data Directory**: Only latest files kept, old files automatically removed

### Available Methods:
- `SodaCloudDump.get_latest_datasets_file(output_dir='superset/data')` - Find latest datasets CSV
- `SodaCloudDump.get_latest_checks_file(output_dir='superset/data')` - Find latest checks CSV
- `SodaCloudDump.find_latest_file(pattern, output_dir='superset/data')` - Find latest file by pattern

## Success Metrics

- **Complete Environment**: Snowflake database with all tables and data
- **Uppercase Standardization**: Consistent naming across all layers
- **API Integration**: Successful metadata extraction from Soda Cloud
- **Complete Data Access**: All Soda Cloud data available for analysis
- **Smart Filtering**: Intelligent filtering for project-specific data
- **Dynamic File Finding**: No hardcoded timestamps, always finds latest data
- **Data Quality**: Comprehensive test data with quality issues
- **Automation**: One-command setup and execution
- **Clean Data Management**: Automatic cleanup of old files

---

**Last Updated**: January 2025  
**Version**: 2.0.0
