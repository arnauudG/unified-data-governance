# Superset Integration for Data Quality Visualization

This directory contains the Apache Superset configuration and dashboard templates for visualizing data quality metrics and governance information.

## Overview

Apache Superset provides powerful data visualization capabilities for your integrated data engineering, governance, and quality insights. It integrates seamlessly with your existing Airflow and PostgreSQL setup, enabling visualization of quality metrics alongside governance information.

## Quick Start

### Start Superset
```bash
just superset-up
```
**Note**: Environment variables are automatically loaded with dynamic validation. The enhanced loader supports any variables in your .env file with intelligent sensitivity detection.

### Start All Services (Airflow + Superset)
```bash
just all-up
```
**Note**: Environment variables are automatically loaded with dynamic validation. The enhanced loader supports any variables in your .env file with intelligent sensitivity detection.

### Access Superset UI
- URL: http://localhost:8089
- Username: admin
- Password: admin

## Available Commands

- `just superset-up` - Start Superset services
- `just superset-down` - Stop Superset services
- `just superset-status` - Check Superset status
- `just superset-logs` - View Superset logs
- `just superset-reset` - Reset Superset database
- `just superset-clean-restart` - Clean restart Superset (removes all data)
- `just superset-reset-data` - Reset only Superset data (keep containers)
- `just superset-reset-schema` - Reset only soda schema (fixes table structure issues)
- `just superset-upload-data` - Complete Soda workflow: updates data source names + dump + organize + upload to Superset

## Configuration

### Environment Variables
Superset automatically loads and validates environment variables when started with `just superset-up`. The following variables are required:

**Required Variables:**
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
- `SODA_CLOUD_API_KEY_ID`, `SODA_CLOUD_API_KEY_SECRET`

**Optional Variables:**
- `SODA_CLOUD_HOST`, `SODA_CLOUD_REGION`
- `COLLIBRA_BASE_URL`, `COLLIBRA_USERNAME`, `COLLIBRA_PASSWORD` (for governance integration)

For complete setup instructions, see the main project README.

### Database Connection
Superset uses its own PostgreSQL database. The Soda data is automatically uploaded to the following tables:

- **soda.datasets_latest** - Latest dataset information from Soda Cloud (filtered for your configured data sources)
- **soda.checks_latest** - Latest check results from Soda Cloud (filtered for your configured data sources)
- **soda.analysis_summary** - Analysis summary data

**Important**: The Superset upload workflow automatically ensures your Soda configuration files are synchronized with your database name before extracting data. Only the latest data for your configured data sources (derived from `SNOWFLAKE_DATABASE`) is relevant for visualization.

**Data Source Filtering**: When creating dashboards in Superset, filter the data by your data source names (e.g., `data_governance_platform_raw`, `data_governance_platform_staging`, etc.) to focus on your specific project data.

To connect to additional data sources:

1. Access Superset UI at http://localhost:8089
2. Go to Settings > Database Connections
3. Add your connections (e.g., Snowflake, PostgreSQL, etc.)

### Data Persistence
Your Superset dashboards, charts, and configurations are automatically preserved using Docker volumes:

- **`superset-data`** - Preserves dashboards, charts, datasets, and user configurations
- **`superset-postgres-data`** - Preserves database data and metadata
- **`superset-logs`** - Preserves application logs

**Your work is automatically saved** - no need to recreate dashboards after restarts!

### Data Quality Dashboards

Create your own dashboards in Superset using the uploaded Soda data:

- **Data Quality Score Over Time** - Track overall data quality trends
- **Failed Checks by Table** - Identify tables with most quality issues
- **Check Results Distribution** - Overview of pass/fail rates
- **Quality Issues by Severity** - Categorize issues by severity level
- **Quality by Dimension** - Analyze quality across Accuracy, Completeness, Consistency, Uniqueness, Validity, Timeliness
- **Layer Comparison** - Compare quality metrics across RAW, STAGING, and MART layers

## Integration with Data Quality and Governance

Superset can visualize data from:
- Soda Cloud check results
- Airflow DAG execution logs
- Custom data quality metrics
- Snowflake data warehouse tables
- Collibra governance metadata (if integrated)

### Automatic Pipeline Integration

**Superset upload is automatically integrated into the Airflow pipeline** as the final step. After all data quality checks and governance syncs complete, the pipeline automatically:

1. **Checks Superset availability** (container and database health)
2. **Updates Soda data source names** to match your database configuration
3. **Extracts latest data** from Soda Cloud API
4. **Organizes data** (keeps only latest files, removes old timestamped files)
5. **Uploads to Superset** PostgreSQL database

**Pipeline Flow**: `RAW → STAGING → MART → QUALITY → Cleanup → Superset Upload`

**Requirements**: Superset must be running before the pipeline completes. The task performs health checks and provides clear error messages if Superset is not available.

**Manual Upload**: You can also upload data manually at any time:
```bash
just superset-upload-data
```

### Quality-Gated Metadata Sync

The pipeline implements quality-gated metadata synchronization:
- **Build Phase**: dbt models materialize data in Snowflake
- **Validation Phase**: Soda quality checks validate the data
- **Governance Phase**: Collibra metadata sync (only after quality validation)
- **Visualization Phase**: Superset upload (after all validation completes)

This ensures Superset visualizations reflect validated, committed data that has passed quality gates, not just data that exists in Snowflake.

### Quality Metrics Visualization

The uploaded Soda data includes (filtered for your configured data sources):
- Dataset health status and quality metrics for your database layers
- Check evaluation results (pass/fail) for your data sources
- Quality dimensions (Accuracy, Completeness, Consistency, Uniqueness, Validity, Timeliness)
- Diagnostic metrics (rows tested, passed, failed, passing fraction)
- Latest data only - historical timestamped files are automatically cleaned up

**Data Source Configuration**: Data source names are automatically derived from your `SNOWFLAKE_DATABASE` environment variable. The update script runs automatically before data extraction to ensure consistency.

### Governance Integration

If Collibra integration is configured, you can visualize:
- Quality metrics linked to data assets
- Governance metadata alongside quality metrics
- Asset ownership and responsibility
- Domain-based quality analysis

## Troubleshooting

### Superset Won't Start
```bash
just superset-reset
```

### Database Schema Issues (Duplicate Tables)
If you see errors like "column does not exist" or duplicate tables:
```bash
just superset-reset-schema  # Reset only the soda schema
just superset-upload-data  # Re-upload data with correct schema
```

### Check Logs
```bash
just superset-logs
```

### Verify Status
```bash
just superset-status
```

## Next Steps

1. **Connect Data Sources**: Add your Snowflake and PostgreSQL connections
2. **Create Dashboards**: Use the templates in `dashboards/` as starting points
3. **Set Up Alerts**: Configure notifications for data quality issues
4. **Schedule Reports**: Automate dashboard generation and sharing
5. **Integrate Governance Views**: Create dashboards that combine quality and governance information

---

**Last Updated**: January 2025  
**Version**: 2.0.0
