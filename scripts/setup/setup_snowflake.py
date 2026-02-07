#!/usr/bin/env python3
"""
Snowflake Setup Script for Data Governance Platform

This script sets up the complete Snowflake infrastructure and populates it with sample data.
It integrates with the platform's centralized configuration system and can be used both
standalone and from Airflow DAGs.

Usage:
    python3 scripts/setup/setup_snowflake.py [--reset] [--test-only]
"""

import sys
import argparse
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
from faker import Faker

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config, get_config
from src.core.logging import setup_logging, get_logger
from src.core.exceptions import ConfigurationError

# Setup logging
setup_logging(level="INFO", format_type="human")
logger = get_logger(__name__)

# Initialize Faker
fake = Faker()


def _normalize_account(account: str) -> str:
    """Normalize Snowflake account identifier."""
    if not account:
        return account
    account = account.strip().lower()
    if account.endswith(".snowflakecomputing.com"):
        account = account.split(".snowflakecomputing.com")[0]
    return account


class SnowflakeSetup:
    """
    Snowflake infrastructure setup and data population.
    
    Uses centralized configuration from the platform's Config system.
    """

    def __init__(self, config: Optional[Config] = None):
        """
        Initialize Snowflake setup.
        
        Args:
            config: Optional Config instance. If None, loads from environment.
        """
        self.config = config or get_config()
        self.conn: Optional[snowflake.connector.SnowflakeConnection] = None
        self.cursor: Optional[snowflake.connector.cursor.SnowflakeCursor] = None

    def connect_to_snowflake(self) -> None:
        """
        Connect to Snowflake using configuration.
        
        Raises:
            ConfigurationError: If required configuration is missing
            Exception: If connection fails
        """
        try:
            logger.info("Connecting to Snowflake...")

            account = _normalize_account(self.config.snowflake.account)
            user = self.config.snowflake.user
            password = self.config.snowflake.password
            warehouse = self.config.snowflake.warehouse
            database = self.config.snowflake.database
            schema = self.config.snowflake.schema_name
            role = self.config.snowflake.role

            if not account or not user or not password:
                raise ConfigurationError(
                    "Missing required Snowflake configuration",
                    details={
                        "missing_fields": [
                            f for f, v in [
                                ("SNOWFLAKE_ACCOUNT", account),
                                ("SNOWFLAKE_USER", user),
                                ("SNOWFLAKE_PASSWORD", password),
                            ] if not v
                        ]
                    },
                )

            # Build connection parameters (only include role if specified)
            conn_params = {
                "account": account,
                "user": user,
                "password": password,
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
                "insecure_mode": True,  # Disable SSL certificate validation for internal stages
            }
            
            # Try with role first if specified, fallback to no role if it fails
            if role:
                conn_params["role"] = role
                try:
                    self.conn = snowflake.connector.connect(**conn_params)
                except Exception as role_error:
                    if "Role" in str(role_error) and "not granted" in str(role_error):
                        logger.warning(
                            f"Role '{role}' not available, connecting without role: {role_error}"
                        )
                        # Retry without role
                        conn_params.pop("role", None)
                        self.conn = snowflake.connector.connect(**conn_params)
                    else:
                        raise
            else:
                self.conn = snowflake.connector.connect(**conn_params)
            self.cursor = self.conn.cursor()
            logger.info(
                f"Successfully connected to Snowflake "
                f"(account={account}, user={user}, database={database}, schema={schema})"
            )
        except ConfigurationError:
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test Snowflake connection with a simple query.
        
        Returns:
            True if connection test succeeds, False otherwise
        """
        try:
            logger.info("Testing Snowflake connection...")
            self.connect_to_snowflake()
            
            # Test query
            self.cursor.execute("SELECT CURRENT_VERSION()")
            version = self.cursor.fetchone()[0]
            logger.info(f"✅ Connection test successful! Snowflake version: {version}")
            
            # Test database access
            self.cursor.execute(f"SELECT CURRENT_DATABASE()")
            current_db = self.cursor.fetchone()[0]
            logger.info(f"✅ Current database: {current_db}")
            
            # Test warehouse
            self.cursor.execute(f"SELECT CURRENT_WAREHOUSE()")
            current_wh = self.cursor.fetchone()[0]
            logger.info(f"✅ Current warehouse: {current_wh}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None
                self.cursor = None

    def setup_infrastructure(self) -> None:
        """Set up database, schemas, warehouse, and tables."""
        logger.info("Setting up Snowflake infrastructure...")

        database_name = self.config.snowflake.database
        # Quote database name if it contains spaces or special characters
        quoted_db_name = f'"{database_name}"' if ' ' in database_name or '-' in database_name else database_name

        setup_queries = [
            # Create database
            f"CREATE DATABASE IF NOT EXISTS {quoted_db_name}",
            # Use the database
            f"USE DATABASE {quoted_db_name}",
            # Create schemas
            "CREATE SCHEMA IF NOT EXISTS RAW",
            "CREATE SCHEMA IF NOT EXISTS STAGING",
            "CREATE SCHEMA IF NOT EXISTS MART",
            "CREATE SCHEMA IF NOT EXISTS QUALITY",
            # Create warehouse (if needed)
            """
            CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
                WITH WAREHOUSE_SIZE = 'X-SMALL'
                AUTO_SUSPEND = 60
                AUTO_RESUME = TRUE
                INITIALLY_SUSPENDED = TRUE
            """,
            # Use RAW schema
            "USE SCHEMA RAW",
            # Create customers table
            """
            CREATE TABLE IF NOT EXISTS CUSTOMERS (
                CUSTOMER_ID VARCHAR(50) PRIMARY KEY,
                FIRST_NAME VARCHAR(100),
                LAST_NAME VARCHAR(100),
                EMAIL VARCHAR(255),
                PHONE VARCHAR(50),
                ADDRESS VARCHAR(500),
                CITY VARCHAR(100),
                STATE VARCHAR(50),
                ZIP_CODE VARCHAR(20),
                COUNTRY VARCHAR(100),
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """,
            # Create products table
            """
            CREATE TABLE IF NOT EXISTS PRODUCTS (
                PRODUCT_ID VARCHAR(50) PRIMARY KEY,
                PRODUCT_NAME VARCHAR(255),
                CATEGORY VARCHAR(100),
                SUBCATEGORY VARCHAR(100),
                PRICE DECIMAL(10,2),
                CURRENCY VARCHAR(3),
                DESCRIPTION TEXT,
                BRAND VARCHAR(100),
                SKU VARCHAR(100),
                WEIGHT DECIMAL(8,2),
                DIMENSIONS VARCHAR(100),
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """,
            # Create orders table
            """
            CREATE TABLE IF NOT EXISTS ORDERS (
                ORDER_ID VARCHAR(50) PRIMARY KEY,
                CUSTOMER_ID VARCHAR(50),
                ORDER_DATE DATE,
                ORDER_STATUS VARCHAR(50),
                TOTAL_AMOUNT DECIMAL(10,2),
                CURRENCY VARCHAR(3),
                SHIPPING_ADDRESS VARCHAR(500),
                PAYMENT_METHOD VARCHAR(50),
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """,
            # Create order_items table
            """
            CREATE TABLE IF NOT EXISTS ORDER_ITEMS (
                ORDER_ITEM_ID VARCHAR(50) PRIMARY KEY,
                ORDER_ID VARCHAR(50),
                PRODUCT_ID VARCHAR(50),
                QUANTITY INTEGER,
                UNIT_PRICE DECIMAL(10,2),
                TOTAL_PRICE DECIMAL(10,2),
                DISCOUNT_PERCENT DECIMAL(5,2),
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """,
            # Create data quality results table
            """
            CREATE TABLE IF NOT EXISTS QUALITY.CHECK_RESULTS (
                check_id VARCHAR(100),
                table_name VARCHAR(100),
                schema_name VARCHAR(100),
                check_type VARCHAR(50),
                check_name VARCHAR(255),
                check_status VARCHAR(20),
                check_message TEXT,
                check_timestamp TIMESTAMP_NTZ,
                execution_time_ms INTEGER,
                rows_checked INTEGER,
                rows_failed INTEGER,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """,
        ]

        for query in setup_queries:
            try:
                self.cursor.execute(query)
                logger.debug(f"Executed: {query[:50]}...")
            except Exception as e:
                logger.warning(f"Warning executing query: {e}")

        self.conn.commit()
        logger.info("✅ Infrastructure setup completed successfully")

    def reset_database(self) -> None:
        """Drop and recreate the project database."""
        database_name = self.config.snowflake.database
        # Quote database name if it contains spaces or special characters
        quoted_db_name = f'"{database_name}"' if ' ' in database_name or '-' in database_name else database_name
        logger.info(f"Resetting Snowflake database {database_name} (drop & create)...")
        cur = self.conn.cursor()
        try:
            cur.execute(f"DROP DATABASE IF EXISTS {quoted_db_name} CASCADE")
            cur.execute(f"CREATE DATABASE {quoted_db_name}")
            logger.info("✅ Database reset complete")
        finally:
            cur.close()

    def generate_sample_data(self) -> None:
        """Generate and insert sample e-commerce data with quality issues."""
        logger.info("Generating sample data...")

        # Generate customers
        customers_data = []
        for i in range(10000):
            customer_id = f"CUST_{i+1:06d}"
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.email()
            phone = fake.phone_number()
            address = fake.street_address()
            city = fake.city()
            state = fake.state()
            zip_code = fake.zipcode()
            country = fake.country()
            created_at = fake.date_time_between(start_date="-1y", end_date="now")
            updated_at = created_at + timedelta(days=random.randint(0, 30))

            # Introduce data quality issues (10% of records)
            if random.random() < 0.1:
                issue_type = random.choice(
                    ["missing_email", "invalid_email", "missing_phone", "duplicate_email"]
                )
                if issue_type == "missing_email":
                    email = ""
                elif issue_type == "invalid_email":
                    email = "invalid-email-format"
                elif issue_type == "missing_phone":
                    phone = ""
                elif issue_type == "duplicate_email":
                    email = "duplicate@example.com"

            customers_data.append(
                {
                    "CUSTOMER_ID": customer_id,
                    "FIRST_NAME": first_name,
                    "LAST_NAME": last_name,
                    "EMAIL": email,
                    "PHONE": phone,
                    "ADDRESS": address,
                    "CITY": city,
                    "STATE": state,
                    "ZIP_CODE": zip_code,
                    "COUNTRY": country,
                    "CREATED_AT": created_at,
                    "UPDATED_AT": updated_at,
                }
            )

        # Generate products
        categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
        subcategories = {
            "Electronics": ["Smartphones", "Laptops", "Headphones"],
            "Clothing": ["Men's", "Women's", "Kids'"],
            "Home & Garden": ["Furniture", "Kitchen", "Decor"],
            "Sports": ["Fitness", "Outdoor", "Team Sports"],
            "Books": ["Fiction", "Non-Fiction", "Educational"],
        }

        products_data = []
        for i in range(1000):
            product_id = f"PROD_{i+1:06d}"
            category = random.choice(categories)
            subcategory = random.choice(subcategories[category])
            product_name = fake.catch_phrase()
            price = round(random.uniform(10.00, 500.00), 2)
            currency = "USD"
            description = fake.text(max_nb_chars=200)
            brand = fake.company()
            sku = f"{brand[:3].upper()}-{random.randint(1000, 9999)}"
            weight = round(random.uniform(0.1, 10.0), 2)
            dimensions = (
                f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(1, 20)} cm"
            )
            created_at = fake.date_time_between(start_date="-6m", end_date="now")
            updated_at = created_at + timedelta(days=random.randint(0, 30))

            # Introduce data quality issues (5% of records)
            if random.random() < 0.05:
                if random.choice([True, False]):
                    price = -price  # Negative price

            products_data.append(
                {
                    "PRODUCT_ID": product_id,
                    "PRODUCT_NAME": product_name,
                    "CATEGORY": category,
                    "SUBCATEGORY": subcategory,
                    "PRICE": price,
                    "CURRENCY": currency,
                    "DESCRIPTION": description,
                    "BRAND": brand,
                    "SKU": sku,
                    "WEIGHT": weight,
                    "DIMENSIONS": dimensions,
                    "CREATED_AT": created_at,
                    "UPDATED_AT": updated_at,
                }
            )

        # Generate orders
        orders_data = []
        customer_ids = [c["CUSTOMER_ID"] for c in customers_data]
        for i in range(20000):
            order_id = f"ORD_{i+1:08d}"
            customer_id = random.choice(customer_ids)
            order_date = fake.date_between(start_date="-6m", end_date="today")
            order_status = random.choice(
                ["pending", "processing", "shipped", "delivered", "cancelled"]
            )
            total_amount = round(random.uniform(25.00, 1000.00), 2)
            currency = "USD"
            shipping_address = fake.street_address()
            payment_method = random.choice(
                ["credit_card", "debit_card", "paypal", "apple_pay"]
            )
            created_at = fake.date_time_between(start_date="-6m", end_date="now")
            updated_at = created_at + timedelta(days=random.randint(0, 7))

            # Introduce data quality issues (8% of records)
            if random.random() < 0.08:
                issue_type = random.choice(
                    ["negative_amount", "invalid_status", "future_date"]
                )
                if issue_type == "negative_amount":
                    total_amount = -total_amount
                elif issue_type == "invalid_status":
                    order_status = "invalid_status"
                elif issue_type == "future_date":
                    order_date = fake.date_between(start_date="today", end_date="+1y")

            orders_data.append(
                {
                    "ORDER_ID": order_id,
                    "CUSTOMER_ID": customer_id,
                    "ORDER_DATE": order_date,
                    "ORDER_STATUS": order_status,
                    "TOTAL_AMOUNT": total_amount,
                    "CURRENCY": currency,
                    "SHIPPING_ADDRESS": shipping_address,
                    "PAYMENT_METHOD": payment_method,
                    "CREATED_AT": created_at,
                    "UPDATED_AT": updated_at,
                }
            )

        # Generate order items
        order_items_data = []
        order_ids = [o["ORDER_ID"] for o in orders_data]
        product_ids = [p["PRODUCT_ID"] for p in products_data]
        for i in range(50000):
            order_item_id = f"ITEM_{i+1:08d}"
            order_id = random.choice(order_ids)
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(5.00, 200.00), 2)
            total_price = round(unit_price * quantity, 2)
            discount_percent = round(random.uniform(0, 25), 2)
            created_at = fake.date_time_between(start_date="-6m", end_date="now")
            updated_at = created_at + timedelta(days=random.randint(0, 3))

            # Introduce data quality issues (5% of records)
            if random.random() < 0.05:
                if random.choice([True, False]):
                    quantity = -quantity  # Negative quantity

            order_items_data.append(
                {
                    "ORDER_ITEM_ID": order_item_id,
                    "ORDER_ID": order_id,
                    "PRODUCT_ID": product_id,
                    "QUANTITY": quantity,
                    "UNIT_PRICE": unit_price,
                    "TOTAL_PRICE": total_price,
                    "DISCOUNT_PERCENT": discount_percent,
                    "CREATED_AT": created_at,
                    "UPDATED_AT": updated_at,
                }
            )

        # Upload data to Snowflake
        self.upload_data(pd.DataFrame(customers_data), "CUSTOMERS")
        self.upload_data(pd.DataFrame(products_data), "PRODUCTS")
        self.upload_data(pd.DataFrame(orders_data), "ORDERS")
        self.upload_data(pd.DataFrame(order_items_data), "ORDER_ITEMS")

        logger.info("✅ Sample data generation completed")

    def upload_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Upload DataFrame to Snowflake table.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
        """
        try:
            # Ensure Snowflake receives uppercase, unquoted-friendly column names
            df = df.copy()
            df.columns = [str(c).upper() for c in df.columns]
            # Coerce known temporal columns to appropriate types
            for col in ["CREATED_AT", "UPDATED_AT"]:
                if col in df.columns:
                    dt = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
                    # Use ISO string to let Snowflake parse into TIMESTAMP_NTZ
                    df[col] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
            if "ORDER_DATE" in df.columns:
                od = pd.to_datetime(df["ORDER_DATE"], errors="coerce")
                df["ORDER_DATE"] = od.dt.strftime("%Y-%m-%d")
            logger.info(f"Uploading {len(df)} records to {table_name}...")
            success, nchunks, nrows, _ = write_pandas(
                self.conn,
                df,
                table_name,
                schema="RAW",
                auto_create_table=False,
                overwrite=True,
            )
            if success:
                logger.info(f"✅ Successfully uploaded {nrows} rows to {table_name}")
            else:
                logger.error(f"❌ Failed to upload data to {table_name}")
        except Exception as e:
            logger.error(f"Error uploading to {table_name}: {e}")
            raise

    def verify_setup(self) -> None:
        """Verify the setup by checking table counts and data quality issues."""
        logger.info("Verifying setup...")

        verification_queries = [
            "SELECT COUNT(*) as customer_count FROM RAW.CUSTOMERS",
            "SELECT COUNT(*) as product_count FROM RAW.PRODUCTS",
            "SELECT COUNT(*) as order_count FROM RAW.ORDERS",
            "SELECT COUNT(*) as order_item_count FROM RAW.ORDER_ITEMS",
            "SELECT COUNT(*) as negative_prices FROM RAW.PRODUCTS WHERE PRICE < 0",
            "SELECT COUNT(*) as negative_amounts FROM RAW.ORDERS WHERE TOTAL_AMOUNT < 0",
            "SELECT COUNT(*) as missing_emails FROM RAW.CUSTOMERS WHERE EMAIL IS NULL OR EMAIL = ''",
            "SELECT COUNT(*) as invalid_statuses FROM RAW.ORDERS WHERE ORDER_STATUS NOT IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')",
        ]

        print("\n" + "=" * 60)
        print("SETUP VERIFICATION REPORT")
        print("=" * 60)

        for query in verification_queries:
            try:
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                print(f"{query.split('FROM')[1].strip()}: {result[0]:,}")
            except Exception as e:
                print(f"Error executing {query}: {e}")

        print("=" * 60)
        print("✅ Setup completed successfully!")
        print("Data quality issues intentionally included for Soda testing:")
        print("- Negative prices and amounts")
        print("- Missing email addresses")
        print("- Invalid order statuses")
        print("- Future dates")
        print("=" * 60)

    def close_connection(self) -> None:
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            logger.info("Snowflake connection closed")


def main() -> int:
    """Main setup function."""
    parser = argparse.ArgumentParser(
        description="Snowflake Setup for Data Governance Platform"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate database (from SNOWFLAKE_DATABASE env var) before setup",
    )
    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Only test connection, don't set up infrastructure",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Snowflake Setup for Data Governance Platform")
    print("=" * 60)

    try:
        # Load configuration
        config = get_config()
        logger.info("Configuration loaded successfully")

        setup = SnowflakeSetup(config=config)

        # Test connection first
        if args.test_only:
            success = setup.test_connection()
            return 0 if success else 1

        # Full setup
        setup.connect_to_snowflake()
        if args.reset:
            setup.reset_database()
        setup.setup_infrastructure()
        setup.generate_sample_data()
        setup.verify_setup()
        setup.close_connection()

        print("\n" + "=" * 60)
        print("Next steps:")
        print("1. Set up dbt models")
        print("2. Configure Soda checks")
        print("3. Set up Airflow orchestration")
        print("=" * 60)

        return 0

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        if e.details:
            logger.error(f"Details: {e.details}")
        return 1
    except Exception as e:
        logger.error(f"Setup failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())
