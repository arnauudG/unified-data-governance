#!/usr/bin/env python3
"""
Script to upload Soda dump CSV data to Superset database
This version runs inside the Superset container with direct database access
"""

import pandas as pd
import psycopg2
import os
import sys
import shutil
from pathlib import Path

# Database connection settings (running inside container)
DB_CONFIG = {
    'host': 'superset-db',
    'port': 5432,
    'database': 'superset',
    'user': 'superset',
    'password': 'superset'
}

def connect_to_db():
    """Connect to Superset PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✅ Connected to Superset database")
        return conn
    except psycopg2.Error as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)

def quote_table_name(table_name):
    """Quote table name properly for PostgreSQL (handles schema.table format)"""
    if '.' in table_name:
        schema, table = table_name.split('.', 1)
        return f'"{schema}"."{table}"'
    else:
        return f'"{table_name}"'

def extract_timestamp_from_filename(filename):
    """Extract timestamp from filename patterns like datasets_2025-12-11.csv or datasets_20251211_174641.csv"""
    import re
    from datetime import datetime
    
    # Try YYYY-MM-DD pattern
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d')
        except:
            pass
    
    # Try YYYYMMDD_HHMMSS pattern
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S')
        except:
            pass
    
    # Try YYYYMMDD pattern
    match = re.search(r'(\d{8})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y%m%d')
        except:
            pass
    
    # Default to current timestamp if no pattern matches
    return datetime.now()

def upload_csv_to_table(csv_file, table_name, conn, is_latest=False, capture_timestamp=None):
    """Upload CSV data to PostgreSQL table"""
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        print(f"📊 Reading {csv_file.name}: {len(df)} rows")
        
        # Create table if it doesn't exist
        cursor = conn.cursor()
        
        # Generate CREATE TABLE statement
        columns = []
        for col, dtype in df.dtypes.items():
            if dtype == 'object':
                col_type = 'TEXT'
            elif dtype == 'int64':
                col_type = 'INTEGER'
            elif dtype == 'float64':
                col_type = 'FLOAT'
            elif dtype == 'bool':
                col_type = 'BOOLEAN'
            else:
                col_type = 'TEXT'
            
            columns.append(f'"{col}" {col_type}')
        
        # Add metadata columns for tracking
        if is_latest:
            columns.append('upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            columns.append('data_source TEXT DEFAULT \'soda_latest\'')
        else:
            # For historical data, add capture_timestamp to track when data was captured
            columns.append('capture_timestamp TIMESTAMP')
            columns.append('upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        # Quote table name properly for PostgreSQL
        quoted_table_name = quote_table_name(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {quoted_table_name} (
            {', '.join(columns)}
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        
        # For latest tables, clear existing data and insert fresh
        if is_latest:
            cursor.execute(f"DELETE FROM {quoted_table_name}")
            conn.commit()
            print(f"🔄 Cleared existing data from {table_name}")
        
        # Insert data
        if is_latest:
            # Latest tables don't have capture_timestamp
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * len(row))
                columns_str = ', '.join([f'"{col}"' for col in df.columns])
                insert_sql = f"INSERT INTO {quoted_table_name} ({columns_str}) VALUES ({placeholders})"
                cursor.execute(insert_sql, tuple(row))
        else:
            # Historical tables include capture_timestamp
            if capture_timestamp is None:
                capture_timestamp = extract_timestamp_from_filename(csv_file.name)
            
            for _, row in df.iterrows():
                placeholders = ', '.join(['%s'] * (len(row) + 1))  # +1 for capture_timestamp
                columns_str = ', '.join([f'"{col}"' for col in df.columns] + ['capture_timestamp'])
                values = tuple(row) + (capture_timestamp,)
                insert_sql = f"INSERT INTO {quoted_table_name} ({columns_str}) VALUES ({placeholders})"
                cursor.execute(insert_sql, values)
        
        conn.commit()
        cursor.close()
        
        print(f"✅ Uploaded {len(df)} rows to table '{table_name}'")
        
    except Exception as e:
        print(f"❌ Error uploading {csv_file}: {e}")
        conn.rollback()

def refresh_latest_data(soda_data_dir):
    """Refresh latest CSV files with most recent data"""
    print("🔄 Refreshing latest data files...")
    
    # Find the most recent datasets and checks files
    datasets_files = list(soda_data_dir.glob("datasets_*.csv"))
    checks_files = list(soda_data_dir.glob("checks_*.csv"))
    
    if datasets_files:
        latest_datasets = max(datasets_files, key=lambda x: x.stat().st_mtime)
        latest_datasets_path = soda_data_dir / "datasets_latest.csv"
        if latest_datasets != latest_datasets_path:
            shutil.copy2(latest_datasets, latest_datasets_path)
            print(f"✅ Updated datasets_latest.csv from {latest_datasets.name}")
        else:
            print(f"✅ datasets_latest.csv is already the latest")
    
    if checks_files:
        latest_checks = max(checks_files, key=lambda x: x.stat().st_mtime)
        latest_checks_path = soda_data_dir / "checks_latest.csv"
        if latest_checks != latest_checks_path:
            shutil.copy2(latest_checks, latest_checks_path)
            print(f"✅ Updated checks_latest.csv from {latest_checks.name}")
        else:
            print(f"✅ checks_latest.csv is already the latest")
    
    # Also update analysis_summary if it exists
    analysis_files = list(soda_data_dir.glob("analysis_summary*.csv"))
    if analysis_files:
        latest_analysis = max(analysis_files, key=lambda x: x.stat().st_mtime)
        analysis_summary_path = soda_data_dir / "analysis_summary.csv"
        if latest_analysis != analysis_summary_path:
            shutil.copy2(latest_analysis, analysis_summary_path)
            print(f"✅ Updated analysis_summary.csv from {latest_analysis.name}")
        else:
            print(f"✅ analysis_summary.csv is already the latest")

def cleanup_temp_folder():
    """Clean up temporary soda_dump_output folder after successful upload"""
    import shutil
    
    # Path to the temporary folder (relative to project root)
    temp_folder = Path("../soda_dump_output")
    
    if temp_folder.exists():
        try:
            shutil.rmtree(temp_folder)
            print(f"✅ Removed temporary folder: {temp_folder}")
        except Exception as e:
            print(f"⚠️  Could not remove temporary folder {temp_folder}: {e}")
    else:
        print(f"ℹ️  Temporary folder {temp_folder} not found (already cleaned up)")

def main(data_dir=None):
    """Main function to upload all Soda dump data
    
    Args:
        data_dir: Optional path to data directory. Defaults to /app/soda_data (Superset container path)
    """
    # Check if superset/data directory exists
    if data_dir is None:
        # Default to Superset container path
        soda_data_dir = Path("/app/soda_data")
    else:
        soda_data_dir = Path(data_dir)
    
    if not soda_data_dir.exists():
        print(f"❌ Data directory not found: {soda_data_dir}")
        print("Please ensure the directory exists and contains the Soda data files")
        sys.exit(1)
    
    # Refresh latest data files
    refresh_latest_data(soda_data_dir)
    
    # Connect to database
    print("\n🔌 Connecting to Superset database...")
    conn = connect_to_db()
    
    # Create soda schema if it doesn't exist
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS soda;")
    conn.commit()
    cursor.close()
    print("✅ Created/verified 'soda' schema")
    
    # Upload latest files to dedicated tables
    latest_files = {
        "datasets_latest.csv": "soda.datasets_latest",
        "checks_latest.csv": "soda.checks_latest",
        "analysis_summary.csv": "soda.analysis_summary"
    }
    
    print("\n📤 Uploading latest data to dedicated tables...")
    for csv_file, table_name in latest_files.items():
        file_path = soda_data_dir / csv_file
        if file_path.exists():
            print(f"\n📤 Uploading {csv_file} to table '{table_name}'...")
            upload_csv_to_table(file_path, table_name, conn, is_latest=True)
        else:
            print(f"⚠️  {csv_file} not found, skipping...")
    
    # Upload historical data to aggregated tables
    print("\n📤 Uploading historical data to aggregated tables...")
    historical_files = [f for f in soda_data_dir.glob("*.csv") if "latest" not in f.name and f.name != "analysis_summary.csv"]
    
    # Separate datasets and checks files
    datasets_files = [f for f in historical_files if f.name.startswith("datasets_")]
    checks_files = [f for f in historical_files if f.name.startswith("checks_")]
    
    # Upload historical datasets to aggregated table
    if datasets_files:
        print(f"\n📊 Aggregating {len(datasets_files)} historical datasets files into soda.datasets_historical...")
        for csv_file in sorted(datasets_files):
            capture_timestamp = extract_timestamp_from_filename(csv_file.name)
            print(f"  📥 Adding data from {csv_file.name} (captured: {capture_timestamp})...")
            upload_csv_to_table(csv_file, "soda.datasets_historical", conn, is_latest=False, capture_timestamp=capture_timestamp)
    
    # Upload historical checks to aggregated table
    if checks_files:
        print(f"\n✅ Aggregating {len(checks_files)} historical checks files into soda.checks_historical...")
        for csv_file in sorted(checks_files):
            capture_timestamp = extract_timestamp_from_filename(csv_file.name)
            print(f"  📥 Adding data from {csv_file.name} (captured: {capture_timestamp})...")
            upload_csv_to_table(csv_file, "soda.checks_historical", conn, is_latest=False, capture_timestamp=capture_timestamp)
    
    conn.close()
    
    # Clean up temporary soda_dump_output folder
    print("\n🧹 Cleaning up temporary files...")
    cleanup_temp_folder()
    
    print("\n🎉 All Soda dump data uploaded successfully!")
    print("\n📊 Database Tables Created:")
    print("  - soda.datasets_latest      - Latest dataset information (refreshed on each upload)")
    print("  - soda.checks_latest       - Latest check results (refreshed on each upload)")
    print("  - soda.analysis_summary    - Analysis summary data")
    print("  - soda.datasets_historical - All historical dataset snapshots (with capture_timestamp)")
    print("  - soda.checks_historical   - All historical check snapshots (with capture_timestamp)")
    print("\nNext steps:")
    print("1. Access Superset UI: http://localhost:8089")
    print("2. Go to Data > Databases and add your PostgreSQL connection")
    print("3. Create datasets from the uploaded tables")
    print("4. Build dashboards with your data quality insights")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Upload Soda data to Superset database")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to directory containing Soda data CSV files (default: /app/soda_data)"
    )
    args = parser.parse_args()
    main(data_dir=args.data_dir)
