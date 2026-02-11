"""
Unity Catalog - Data Quality Integration
This module provides Unity Catalog-specific data quality functions.
Integrates with the data quality module for Unity Catalog tables.
"""

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
import logging

# Import data quality functions
try:
    from data_quality import (
        check_null_percentage, check_duplicates, check_data_range,
        check_data_quality_score, generate_quality_report
    )
except ImportError:
    # Fallback if module not found
    from data_quality import (
        check_null_percentage, check_duplicates, check_data_range,
        check_data_quality_score, generate_quality_report
    )

logger = logging.getLogger(__name__)
workspace = WorkspaceClient()

# Initialize Spark (in Databricks, this is already available as 'spark')
# For local: spark = SparkSession.builder.appName("DataQuality").getOrCreate()

def validate_unity_catalog_table(table_name: str, key_columns: list = None):
    """
    Validates data quality for a Unity Catalog table.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        key_columns: Optional list of key columns for duplicate checking
    """
    print(f"=== Validating Unity Catalog Table: {table_name} ===")
    
    try:
        # Read table
        df = spark.table(table_name)
        
        # Generate quality report
        report = generate_quality_report(df, "unity_catalog", key_columns)
        
        print(f"\n📊 Quality Report Summary:")
        print(f"  Total Records: {report['summary']['total_records']}")
        print(f"  Total Columns: {report['summary']['total_columns']}")
        print(f"  Duplicates: {report['summary']['duplicates']}")
        
        if report['recommendations']:
            print(f"\n💡 Recommendations:")
            for rec in report['recommendations']:
                print(f"  - {rec}")
        
        return report
    except Exception as e:
        print(f"Error validating table: {str(e)}")
        return None

def check_table_quality_rules(table_name: str, rules: dict):
    """
    Checks data quality rules against a Unity Catalog table.
    
    Args:
        table_name: Full table name
        rules: Dictionary of quality rules (see data_quality module)
    """
    print(f"=== Checking Quality Rules for: {table_name} ===")
    
    try:
        df = spark.table(table_name)
        quality_score = check_data_quality_score(df, rules)
        
        print(f"\n✓ Quality Score: {quality_score:.2%}")
        return quality_score
    except Exception as e:
        print(f"Error checking quality rules: {str(e)}")
        return None

def validate_medallion_layer(layer: str, catalog_name: str, schema_name: str, table_name: str):
    """
    Validates data quality for a specific medallion layer table.
    
    Args:
        layer: Layer name (bronze, silver, gold)
        catalog_name: Catalog name
        schema_name: Schema name
        table_name: Table name
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    print(f"=== Validating {layer.upper()} Layer: {full_table_name} ===")
    
    try:
        df = spark.table(full_table_name)
        
        # Import layer-specific validation
        from data_quality import validate_bronze_data, validate_silver_data, validate_gold_data
        
        if layer.lower() == "bronze":
            result = validate_bronze_data(df)
        elif layer.lower() == "silver":
            key_cols = ["id"]  # Adjust based on your schema
            result = validate_silver_data(df, key_cols)
        elif layer.lower() == "gold":
            result = validate_gold_data(df)
        else:
            print(f"Unknown layer: {layer}")
            return None
        
        print(f"\n✓ Validation complete")
        print(f"  Quality Score: {result['quality_score']:.2%}")
        return result
    except Exception as e:
        print(f"Error validating layer: {str(e)}")
        return None

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Data Quality Integration ===\n")
    
    catalog = "practice_catalog"
    schema = "analytics"
    table = "customers"
    
    # Validate a Unity Catalog table
    validate_unity_catalog_table(f"{catalog}.{schema}.{table}", ["customer_id"])
    
    # Check quality rules
    rules = {
        "no_null_email": {
            "type": "null_check",
            "column": "email",
            "max_null_percentage": 0.0
        },
        "no_duplicates": {
            "type": "duplicate_check",
            "key_columns": ["customer_id"],
            "max_duplicates": 0
        }
    }
    check_table_quality_rules(f"{catalog}.{schema}.{table}", rules)
    
    print("\n=== Practice Complete ===")
