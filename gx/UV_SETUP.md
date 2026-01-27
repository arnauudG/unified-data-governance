# Using uv with GX Project

This project uses [uv](https://github.com/astral-sh/uv) for fast Python package management instead of traditional `pip` and `venv`.

## Quick Start

```bash
cd gx

# Create virtual environment (uv creates .venv by default)
uv venv

# Activate virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .
```

## Using uv run (No Activation Needed)

You can use `uv run` to execute commands without activating the virtual environment:

```bash
# Run scripts directly
uv run python scripts/test_gx_setup.py
uv run python scripts/run_gx_check.py --layer mart --table fact_orders

# Run GX CLI
uv run great_expectations checkpoint run mart.fact_orders
```

## Common Commands

### Setup
```bash
# Create venv
uv venv

# Install dependencies
uv pip install -e .

# Install with dev dependencies
uv pip install -e ".[dev]"
```

### Running Scripts
```bash
# With uv run (recommended - no activation needed)
uv run python scripts/test_gx_setup.py
uv run python scripts/setup_datasources.py
uv run python scripts/run_gx_check.py --layer mart --table fact_orders

# Or activate venv first
source .venv/bin/activate
python scripts/test_gx_setup.py
```

### Managing Dependencies
```bash
# Add a new dependency
uv pip install package-name

# Update dependencies
uv pip install --upgrade great-expectations

# List installed packages
uv pip list

# Remove a package
uv pip uninstall package-name
```

### Sync Dependencies
```bash
# Sync environment with pyproject.toml
uv pip sync pyproject.toml  # Note: uv sync is coming, for now use uv pip install
```

## Benefits of uv

- **Fast**: Written in Rust, much faster than pip
- **Reliable**: Better dependency resolution
- **Simple**: Drop-in replacement for pip/venv
- **Modern**: Supports pyproject.toml natively

## Project Structure

```
gx/
├── pyproject.toml          # Dependencies defined here
├── .python-version         # Python version (3.11)
├── .venv/                  # Virtual environment (created by uv)
└── scripts/
    └── setup_environment.sh # Setup script using uv
```

## Troubleshooting

### uv not found
```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or with pip
pip install uv
```

### Python version issues
```bash
# Check Python version
python --version

# uv will use the Python version specified in .python-version
# Or specify explicitly:
uv venv --python 3.11
```

### Permission errors
```bash
# If you get permission errors, ensure .venv is writable
chmod -R u+w .venv
```
