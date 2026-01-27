#!/usr/bin/env python3
"""
Create example expectation suites and checkpoints for GX.
This creates example files following the decision-driven quality framework.
"""

import json
import yaml
from pathlib import Path

GX_ROOT = Path(__file__).parent.parent
EXPECTATIONS_DIR = GX_ROOT / "great_expectations" / "expectations"
CHECKPOINTS_DIR = GX_ROOT / "great_expectations" / "checkpoints"

def create_expectation_suite(layer: str, table: str, expectations: list):
    """Create an expectation suite JSON file."""
    suite = {
        "data_asset_type": "SqlAlchemyDataset",
        "expectation_suite_name": f"{layer}.{table}",
        "expectations": expectations,
        "meta": {
            "great_expectations_version": "0.18.0",
            "notes": {
                "content": f"Expectations for {layer}.{table} - Decision-driven quality checks",
                "format": "markdown"
            }
        }
    }
    
    suite_path = EXPECTATIONS_DIR / layer / f"{table}.json"
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(suite_path, 'w') as f:
        json.dump(suite, f, indent=2)
    
    print(f"✅ Created expectation suite: {suite_path}")
    return suite_path

def create_checkpoint(layer: str, table: str):
    """Create a checkpoint YAML file."""
    checkpoint = {
        "name": f"{layer}.{table}",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S",
        "expectation_suite_name": f"{layer}.{table}",
        "batch_request": {
            "datasource_name": f"{layer}_datasource",
            "data_connector_name": "default_runtime_data_connector",
            "data_asset_name": table.upper(),
            "batch_identifiers": {
                "default_identifier_name": "default_identifier"
            }
        },
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction"
                }
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction"
                }
            }
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {}
    }
    
    checkpoint_path = CHECKPOINTS_DIR / layer / f"{table}.yml"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(checkpoint_path, 'w') as f:
        yaml.dump({checkpoint["name"]: checkpoint}, f, default_flow_style=False, sort_keys=False)
    
    print(f"✅ Created checkpoint: {checkpoint_path}")
    return checkpoint_path

def create_mart_fact_orders_example():
    """Create example expectations for MART.fact_orders following decision-driven quality."""
    # DECISION: "Can we trust this for financial reporting, revenue analytics, and business decisions?"
    # RISK: Duplicate orders = inflated revenue, wrong amounts = financial errors
    # QUALITY DIMENSIONS: Uniqueness, Accuracy, Schema
    
    expectations = [
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {
                "column_list": ["ORDER_ID", "CUSTOMER_ID", "ORDER_TOTAL_AMOUNT", "ORDER_STATUS"]
            },
            "meta": {
                "notes": {
                    "content": "Schema validation - structure matters for consumption",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {
                "column": "ORDER_ID"
            },
            "meta": {
                "notes": {
                    "content": "Uniqueness - duplicate orders = inflated revenue, wrong metrics",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "ORDER_TOTAL_AMOUNT",
                "min_value": 0,
                "max_value": 10000,
                "strictly_min": True
            },
            "meta": {
                "notes": {
                    "content": "Accuracy - wrong amounts = financial errors",
                    "format": "markdown"
                }
            }
        }
    ]
    
    create_expectation_suite("mart", "fact_orders", expectations)
    create_checkpoint("mart", "fact_orders")

def create_raw_customers_example():
    """Create example expectations for RAW.customers following decision-driven quality."""
    # DECISION: "Should we ingest this data without breaking the pipeline?"
    # RISK: Pipeline breaks, downstream transformations fail
    # QUALITY DIMENSIONS: Validity (format/type errors), Schema (missing columns)
    
    expectations = [
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {
                "column_list": [
                    "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE",
                    "ADDRESS", "CITY", "STATE", "ZIP_CODE", "COUNTRY",
                    "CREATED_AT", "UPDATED_AT"
                ]
            },
            "meta": {
                "notes": {
                    "content": "Schema validation - prevents transformation failures",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "EMAIL",
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            },
            "meta": {
                "notes": {
                    "content": "Validity checks - format errors break pipelines",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "PHONE",
                "regex": r"^[0-9\-\+\(\)\s]+$"
            },
            "meta": {
                "notes": {
                    "content": "Validity checks - format errors break pipelines",
                    "format": "markdown"
                }
            }
        }
    ]
    
    create_expectation_suite("raw", "customers", expectations)
    create_checkpoint("raw", "customers")

def create_staging_stg_customers_example():
    """Create example expectations for STAGING.stg_customers following decision-driven quality."""
    # DECISION: "Is the transformation successful and can we proceed to MART?"
    # RISK: Bad transformations, inconsistent data, missing required fields for downstream
    # QUALITY DIMENSIONS: Schema, Validity, Completeness, Consistency
    
    expectations = [
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {
                "column_list": ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "DATA_QUALITY_SCORE"]
            },
            "meta": {
                "notes": {
                    "content": "Schema validation - structure matters for downstream",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "CUSTOMER_ID"
            },
            "meta": {
                "notes": {
                    "content": "Completeness - critical for downstream joins",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "EMAIL",
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            },
            "meta": {
                "notes": {
                    "content": "Validity - transformation rules",
                    "format": "markdown"
                }
            }
        },
        {
            "expectation_type": "expect_column_mean_to_be_between",
            "kwargs": {
                "column": "DATA_QUALITY_SCORE",
                "min_value": 80,
                "max_value": None
            },
            "meta": {
                "notes": {
                    "content": "Consistency - transformation quality indicator",
                    "format": "markdown"
                }
            }
        }
    ]
    
    create_expectation_suite("staging", "stg_customers", expectations)
    create_checkpoint("staging", "stg_customers")

def main():
    """Create example expectation suites and checkpoints."""
    print("🚀 Creating example GX expectations and checkpoints...")
    print("   Following decision-driven quality framework\n")
    
    create_mart_fact_orders_example()
    print()
    create_raw_customers_example()
    print()
    create_staging_stg_customers_example()
    
    print("\n✅ Example files created!")
    print("\nNext steps:")
    print("1. Review the created expectation suites")
    print("2. Create expectations for remaining tables")
    print("3. Run: python scripts/run_gx_check.py --layer mart --table fact_orders")

if __name__ == "__main__":
    main()
