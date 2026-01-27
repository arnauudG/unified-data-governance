# Running GX Tests

## Current Status

✅ **Project Structure**: Complete  
✅ **Expectation Suites**: 3 examples created (fact_orders, customers, stg_customers)  
✅ **Checkpoints**: 3 examples created  
✅ **Configuration**: Valid  
✅ **Environment Variables**: Set  

⚠️ **GX Import Issue**: There's a segmentation fault when importing GX (likely Python version/environment issue)

## Quick Test (No GX Required)

You can validate the configuration without running actual tests:

```bash
cd gx
python scripts/test_gx_setup.py
```

This validates:
- Project structure
- Expectation suite JSON files
- Checkpoint YAML files
- Datasource templates
- Environment variables

## Running Actual Tests

Once GX import issue is resolved, run tests like this:

### 1. Set Up Datasources

```bash
python scripts/setup_datasources.py
```

This configures Snowflake connections for each layer using environment variables.

### 2. Run a Single Checkpoint

```bash
# Run checks for one table (like: soda scan -d datasource checks/mart/fact_orders.yml)
python scripts/run_gx_check.py --layer mart --table fact_orders
```

### 3. Run All Checks for a Layer

```bash
# Run all checkpoints for a layer (like: soda scan -d datasource checks/mart/)
python scripts/run_gx_layer.py --layer mart
```

### 4. Run All Checks

```bash
# Run all checkpoints for all layers
python scripts/run_gx_layer.py --layer all
```

## Setup with uv

This project uses `uv` for dependency management:

```bash
cd gx

# Create virtual environment
uv venv

# Activate it
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .

# Or use uv run directly (no activation needed)
uv run python scripts/test_gx_setup.py
```

## Troubleshooting GX Import Issue

If you encounter segmentation faults when importing GX:

1. **Check Python version**: GX requires Python 3.8+
   ```bash
   python3 --version
   ```

2. **Reinstall GX with uv**:
   ```bash
   uv pip uninstall great-expectations
   uv pip install "great-expectations[snowflake]>=0.16.0"
   ```

3. **Use uv run** (handles environment automatically):
   ```bash
   uv run python scripts/run_gx_check.py --layer mart --table fact_orders
   ```

4. **Use GX CLI directly**:
   ```bash
   cd great_expectations
   uv run great_expectations checkpoint run mart.fact_orders
   ```

## Project Structure

```
gx/
├── great_expectations/
│   ├── great_expectations.yml          ✅ Created
│   ├── datasources/                     ✅ Templates ready
│   ├── expectations/                    ✅ 3 example suites
│   │   ├── mart/fact_orders.json
│   │   ├── raw/customers.json
│   │   └── staging/stg_customers.json
│   └── checkpoints/                     ✅ 3 example checkpoints
│       ├── mart/fact_orders.yml
│       ├── raw/customers.yml
│       └── staging/stg_customers.yml
└── scripts/
    ├── test_gx_setup.py                 ✅ Works (validates config)
    ├── setup_datasources.py            ⚠️  Needs GX import
    ├── run_gx_check.py                 ⚠️  Needs GX import
    └── run_gx_layer.py                 ⚠️  Needs GX import
```

## Example: What a Test Run Looks Like

When working, running `python scripts/run_gx_check.py --layer mart --table fact_orders` will:

1. Connect to Snowflake using `mart_datasource`
2. Query the `FACT_ORDERS` table in the `MART` schema
3. Run 3 expectations:
   - Schema validation (required columns)
   - Uniqueness check (ORDER_ID)
   - Accuracy check (ORDER_TOTAL_AMOUNT range)
4. Report pass/fail for each expectation
5. Store validation results
6. Update data docs

This mirrors what Soda does with: `soda scan -d datasource -c config.yml checks/mart/fact_orders.yml`
