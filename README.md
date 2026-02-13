# Databricks Unity Catalog Practice

This repository contains comprehensive examples and practice code for learning Databricks Unity Catalog features.

## Overview

Unity Catalog is Databricks' unified governance solution for data and AI assets. This repository covers all major features including:

- Catalog Management
- Schema Management
- Table Management
- Access Control & Permissions
- Data Sharing
- Lineage & Data Quality
- External Tables & Volumes

## Structure

- `01_catalog_management.py` - Creating and managing catalogs
- `02_schema_management.py` - Working with schemas (databases)
- `03_table_management.py` - Table creation and management
- `04_access_control.py` - Permissions and security
- `05_data_sharing.py` - Sharing data across workspaces
- `06_lineage.py` - Data lineage tracking
- `07_data_quality.py` - Data quality checks
- `08_external_tables.py` - Working with external data sources
- `09_volumes.py` - Managing volumes for unstructured data
- `10_best_practices.py` - Best practices and common patterns

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI or SDK installed
- Proper authentication configured

## Setup

1. Install required packages:
```bash
pip install databricks-sdk databricks-sql-connector
```

2. Configure authentication:
```bash
databricks configure --token
```

3. Update catalog/schema names in scripts to match your workspace

## Usage

Each script can be run independently to practice specific Unity Catalog features. Start with catalog management and work through each module sequentially.

### Additional Modules

- `11_autoloader.py` - Databricks Auto Loader patterns and best practices
- `12_medallion_architecture.py` - Bronze/Silver/Gold medallion design
- `13_data_quality.py` - Reusable data quality checks and scoring
- `14_delta_lake_optimization.py` - Delta Lake performance and maintenance

## Notes

- These are practice scripts - adjust catalog/schema names for your environment
- Some operations require specific permissions
- Always test in a development workspace first

