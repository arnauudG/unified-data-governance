# Scripts Directory - Utility Scripts

This directory contains utility scripts for environment setup, data management, and platform testing.

## Directory Structure

```
scripts/
├── setup/                          # Environment setup scripts
│   ├── requirements.txt            # Python dependencies for setup
│   ├── setup_snowflake.py         # Snowflake database and table creation (enhanced)
│   └── reset_snowflake.py         # Snowflake database cleanup
├── test_stack.py                  # Platform stack testing script
└── README.md                      # This file
```

## Stack Testing

### Platform Stack Testing Script

**`test_stack.py`** - Comprehensive platform stack testing

Test all platform components to verify configuration and connectivity.

**Usage**:
```bash
# Test all components
python3 scripts/test_stack.py --component all

# Test specific component
python3 scripts/test_stack.py --component snowflake
python3 scripts/test_stack.py --component soda
python3 scripts/test_stack.py --component collibra
python3 scripts/test_stack.py --component config

# Using justfile
just test-stack                    # Test all components
just test-stack-component snowflake  # Test specific component
```

**Features**:
- ✅ Configuration validation
- ✅ Snowflake connection testing
- ✅ Soda Cloud API connection testing
- ✅ Collibra API connection testing
- ✅ Comprehensive test summary
- ✅ Exit codes for automation (0=success, 1=failure)

**Components Tested**:
1. **Configuration**: Validates all required environment variables
2. **Snowflake**: Tests database connection and basic queries
3. **Soda Cloud**: Tests API authentication and connectivity
4. **Collibra**: Tests API authentication and connectivity

---

## Snowflake Setup

### Enhanced Snowflake Setup Script

**`setup/setup_snowflake.py`** - Snowflake infrastructure setup (enhanced)

**Key Improvements**:
- ✅ Uses centralized `Config` class from platform
- ✅ Integrated with platform logging system
- ✅ Better error handling with `ConfigurationError`
- ✅ Connection testing mode (`--test-only`)
- ✅ Type hints and comprehensive docstrings
- ✅ Can be used standalone or from Airflow DAGs

**Usage**:
```bash
# Test connection only
python3 scripts/setup/setup_snowflake.py --test-only

# Full setup
python3 scripts/setup/setup_snowflake.py

# Reset and setup (drops existing database)
python3 scripts/setup/setup_snowflake.py --reset

# Using justfile
just test-snowflake              # Test connection only
just setup-snowflake             # Full setup
just setup-snowflake-reset      # Reset and setup
```

**What It Does**:
1. **Connects to Snowflake** using platform configuration
2. **Creates database** (`DATA PLATFORM XYZ` by default, or from `SNOWFLAKE_DATABASE` env var)
3. **Creates schemas**: RAW, STAGING, MART, QUALITY
4. **Creates warehouse** (COMPUTE_WH if needed)
5. **Creates tables**: CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS, CHECK_RESULTS
6. **Generates sample data** with intentional quality issues for testing
7. **Verifies setup** with data quality report

**Integration with Airflow**:
- Used by `soda_initialization` DAG for one-time setup
- Reads configuration from environment variables
- Can be triggered manually or scheduled

---

## Health Check Script

### Platform Health Monitoring (`health_check.py`)

**Purpose**: Check health of all platform components

**Features**:
- Soda Cloud API health check
- Collibra API health check
- Configuration validation
- Human-readable and machine-readable output

**Usage**:
```bash
just health-check
# or
python3 scripts/health_check.py
```

**Exit Codes**:
- `0` - All systems healthy
- `1` - Some systems degraded
- `2` - System health check failed
- `3` - Health check error

## Environment Setup Scripts

### Snowflake Setup (`setup_snowflake.py`)

Creates the complete Snowflake infrastructure with:
- **Database**: Configured via `SNOWFLAKE_DATABASE` environment variable (default: `DATA PLATFORM XYZ`)
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
SNOWFLAKE_DATABASE="DATA PLATFORM XYZ"  # Database name (default: DATA PLATFORM XYZ if not set)
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


## Usage Examples

### Complete Environment Setup
```bash
# 1. Start Airflow services
just airflow-up

# 2. Initialize Snowflake (creates tables with uppercase columns)
just airflow-trigger-init

# 3. Run data quality pipeline
just airflow-trigger-pipeline

```

### Manual Script Execution
```bash
# Setup Snowflake manually
python3 scripts/setup/setup_snowflake.py

# Reset Snowflake manually  
python3 scripts/setup/reset_snowflake.py

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

## Success Metrics

- **Complete Environment**: Snowflake database with all tables and data
- **Uppercase Standardization**: Consistent naming across all layers
- **Data Quality**: Comprehensive test data with quality issues
- **Automation**: One-command setup and execution

## Code Quality & Architecture

### Repository Pattern
Scripts use the Repository pattern for clean API access:
- `SodaRepository` for Soda Cloud API
- Clean error handling and retry logic

### Testing
- Mock-based testing
- Error scenario handling

---

**Last Updated**: February 7, 2026  
**Version**: 2.1.0
