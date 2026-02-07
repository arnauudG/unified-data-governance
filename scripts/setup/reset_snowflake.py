#!/usr/bin/env python3
"""
Reset Snowflake environment for Data Governance Platform project.

Drops and recreates the database (from SNOWFLAKE_DATABASE env var, default: DATA PLATFORM XYZ) to start from a clean state.

Usage:
  python3 scripts/setup/reset_snowflake.py [--force]

Requires Snowflake env vars to be loaded.
"""
import os
import sys
import argparse
import logging
import snowflake.connector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _normalize_account(acc: str) -> str:
    if not acc:
        return acc
    acc = acc.strip().lower()
    if acc.endswith(".snowflakecomputing.com"):
        acc = acc.split(".snowflakecomputing.com")[0]
    return acc


def connect_snowflake():
    account = _normalize_account(os.getenv("SNOWFLAKE_ACCOUNT"))
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    role = os.getenv("SNOWFLAKE_ROLE")

    if not all([account, user, password]):
        raise RuntimeError("Missing Snowflake env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")

    logger.info("Connecting to Snowflake (reset)...")
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
        insecure_mode=True,
    )
    return conn


def reset_database(conn):
    # Get database name from environment variable
    database_name = os.getenv("SNOWFLAKE_DATABASE", "DATA PLATFORM XYZ")
    # Quote database name if it contains spaces or special characters
    quoted_db_name = f'"{database_name}"' if ' ' in database_name or '-' in database_name else database_name
    cur = conn.cursor()
    try:
        logger.info(f"Dropping database {database_name} if exists (cascade)...")
        cur.execute(f"DROP DATABASE IF EXISTS {quoted_db_name} CASCADE")
        logger.info(f"Creating database {database_name}...")
        cur.execute(f"CREATE DATABASE {quoted_db_name}")
        logger.info("Reset complete.")
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Do not prompt for confirmation")
    args = parser.parse_args()

    database_name = os.getenv("SNOWFLAKE_DATABASE", "DATA PLATFORM XYZ")
    if not args.force:
        reply = input(f"This will DROP the {database_name} database. Continue? [y/N]: ").strip().lower()
        if reply not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    conn = connect_snowflake()
    try:
        reset_database(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()