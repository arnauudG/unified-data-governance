# Great Expectations (GX) Project

This directory contains a Great Expectations project that **mimics the purpose** of Soda quality checks - checking data quality with one checkpoint per table, organized by layer.

## Structure

```
gx/
├── great_expectations/
│   ├── great_expectations.yml          # Main GX configuration
│   ├── datasources/                     # Datasource configurations
│   │   ├── raw_datasource.yml
│   │   ├── staging_datasource.yml
│   │   ├── mart_datasource.yml
│   │   └── quality_datasource.yml
│   ├── expectations/                    # Expectation suites (one per table)
│   │   ├── raw/
│   │   │   ├── customers.json
│   │   │   ├── products.json
│   │   │   ├── orders.json
│   │   │   └── order_items.json
│   │   ├── staging/
│   │   │   ├── stg_customers.json
│   │   │   ├── stg_products.json
│   │   │   ├── stg_orders.json
│   │   │   └── stg_order_items.json
│   │   ├── mart/
│   │   │   ├── dim_customers.json
│   │   │   ├── dim_products.json
│   │   │   └── fact_orders.json
│   │   └── quality/
│   │       └── check_results.json
│   └── checkpoints/                     # Checkpoints (one per table)
│       ├── raw/
│       │   ├── customers.yml
│       │   ├── products.yml
│       │   ├── orders.yml
│       │   └── order_items.yml
│       ├── staging/
│       │   ├── stg_customers.yml
│       │   ├── stg_products.yml
│       │   ├── stg_orders.yml
│       │   └── stg_order_items.yml
│       ├── mart/
│       │   ├── dim_customers.yml
│       │   ├── dim_products.yml
│       │   └── fact_orders.yml
│       └── quality/
│           └── check_results.yml
├── scripts/
│   ├── run_gx_check.py                  # Run a single checkpoint
│   ├── run_gx_layer.py                  # Run all checkpoints for a layer
│   └── setup_gx_project.py             # Initialize GX project
└── README.md                            # This file
```

## Design Philosophy

**One Checkpoint Per Table** - Just like Soda has one check file per table, GX has one checkpoint per table.

**Layer-Based Organization** - Checks are organized by data layer (raw, staging, mart, quality), matching the Soda structure.

**Decision-Driven Quality** - Expectations mirror the decision-driven quality framework applied to Soda checks.

## Setup

### Prerequisites

- Python 3.8+ (recommended: 3.11)
- [uv](https://github.com/astral-sh/uv) - Fast Python package installer

Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or with pip: pip install uv
```

### 1. Set Up Environment

```bash
cd gx

# Create virtual environment and install dependencies with uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .

# Or use the setup script
bash scripts/setup_environment.sh
```

> 💡 **Tip**: See [UV_SETUP.md](UV_SETUP.md) for detailed uv usage guide and `uv run` commands (no activation needed).

### 2. Initialize GX Project

```bash
# Initialize GX project structure
python scripts/setup_gx_project.py
```

## Usage

### Test Project Setup

```bash
# Validate project structure and configuration
uv run python scripts/test_gx_setup.py
# Or if venv is activated:
python scripts/test_gx_setup.py
```

### Set Up Datasources

```bash
# Configure Snowflake connections for each layer
uv run python scripts/setup_datasources.py
```

### Run a Single Checkpoint (One Table)

```bash
# Run checks for a specific table (like: soda scan -d datasource checks/mart/fact_orders.yml)
uv run python scripts/run_gx_check.py --layer mart --table fact_orders
```

### Run All Checks for a Layer

```bash
# Run all checkpoints for a layer (like: soda scan -d datasource checks/mart/)
uv run python scripts/run_gx_layer.py --layer mart
```

### Run All Checks (All Layers)

```bash
# Run all checkpoints for all layers
uv run python scripts/run_gx_layer.py --layer all
```

## Quick Test (Without Full GX Setup)

```bash
# Validate checkpoint configuration without running
uv run python scripts/run_gx_check_simple.py --layer mart --table fact_orders
```

## Mapping: Soda → GX

| Soda Concept | GX Concept |
|-------------|------------|
| Check file (`customers.yml`) | Expectation Suite (`customers.json`) + Checkpoint (`customers.yml`) |
| `soda scan -d datasource -c config.yml checks/raw/` | `great_expectations checkpoint run raw.customers` |
| Data source (`data_platform_xyz_raw`) | Datasource (`raw_datasource`) |
| Check (`duplicate_count(ORDER_ID) = 0`) | Expectation (`expect_column_values_to_be_unique`) |
| Layer (`raw`, `staging`, `mart`) | Namespace in checkpoint names (`raw.customers`) |

## Example: GX Expectations (Decision-Driven Quality)

Expectations follow the same decision-driven quality framework as Soda checks:

### Example: MART Layer (fact_orders)

**Decision**: "Can we trust this for financial reporting?"  
**Risk**: Duplicate orders = inflated revenue, wrong amounts = financial errors

```json
{
  "expectation_suite_name": "mart.fact_orders",
  "expectations": [
    {
      "expectation_type": "expect_table_columns_to_match_ordered_list",
      "kwargs": {
        "column_list": ["ORDER_ID", "CUSTOMER_ID", "ORDER_TOTAL_AMOUNT", "ORDER_STATUS"]
      },
      "meta": {
        "notes": "Schema validation - structure matters for consumption"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {"column": "ORDER_ID"},
      "meta": {
        "notes": "Uniqueness - duplicate orders = inflated revenue"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "ORDER_TOTAL_AMOUNT",
        "min_value": 0,
        "max_value": 10000,
        "strictly_min": true
      },
      "meta": {
        "notes": "Accuracy - wrong amounts = financial errors"
      }
    }
  ]
}
```

### Example: RAW Layer (customers)

**Decision**: "Should we ingest this data without breaking the pipeline?"  
**Risk**: Pipeline breaks, downstream transformations fail

```json
{
  "expectation_suite_name": "raw.customers",
  "expectations": [
    {
      "expectation_type": "expect_table_columns_to_match_ordered_list",
      "kwargs": {
        "column_list": ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", ...]
      },
      "meta": {
        "notes": "Schema validation - prevents transformation failures"
      }
    },
    {
      "expectation_type": "expect_column_values_to_match_regex",
      "kwargs": {
        "column": "EMAIL",
        "regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
      },
      "meta": {
        "notes": "Validity checks - format errors break pipelines"
      }
    }
  ]
}
```

## Integration with Airflow

GX checkpoints can be integrated into Airflow DAGs similar to Soda scans:

```python
gx_check_raw = BashOperator(
    task_id="gx_check_raw",
    bash_command="python gx/scripts/run_gx_layer.py --layer raw"
)
```
