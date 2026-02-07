"""
Delta Lake Optimization - Best Practices and Performance Tuning
This module demonstrates Delta Lake optimization techniques for better performance and cost efficiency.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import logging

logger = logging.getLogger(__name__)

# Initialize Spark (in Databricks, this is already available as 'spark')
# For local: spark = SparkSession.builder.appName("DeltaOptimization").getOrCreate()

def optimize_delta_table(table_name: str, zorder_columns: list = None):
    """
    Optimizes a Delta table by compacting small files and optionally applying Z-ORDER.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        zorder_columns: Optional list of columns for Z-ORDER clustering
    """
    print(f"=== Optimizing Delta Table: {table_name} ===")
    
    if zorder_columns:
        zorder_clause = f"ZORDER BY ({', '.join(zorder_columns)})"
        optimize_sql = f"OPTIMIZE {table_name} {zorder_clause}"
        print(f"  Applying Z-ORDER on: {', '.join(zorder_columns)}")
    else:
        optimize_sql = f"OPTIMIZE {table_name}"
        print(f"  Compacting small files only")
    
    print(f"\nSQL Command:")
    print(f"  {optimize_sql}")
    
    # Execute optimization
    try:
        result = spark.sql(optimize_sql)
        result.show(truncate=False)
        print(f"\n✓ Optimization completed for {table_name}")
        return result
    except Exception as e:
        print(f"✗ Error optimizing table: {str(e)}")
        return None

def vacuum_delta_table(table_name: str, retention_hours: int = 168):
    """
    Vacuums a Delta table to remove old files beyond retention period.
    
    Args:
        table_name: Full table name
        retention_hours: Retention period in hours (default: 168 = 7 days)
    """
    print(f"=== Vacuuming Delta Table: {table_name} ===")
    print(f"  Retention: {retention_hours} hours ({retention_hours/24:.1f} days)")
    
    vacuum_sql = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
    
    print(f"\nSQL Command:")
    print(f"  {vacuum_sql}")
    print(f"\n⚠️  Warning: This will permanently delete files older than retention period")
    
    try:
        result = spark.sql(vacuum_sql)
        print(f"\n✓ Vacuum completed for {table_name}")
        return result
    except Exception as e:
        print(f"✗ Error vacuuming table: {str(e)}")
        return None

def get_table_file_stats(table_name: str):
    """
    Gets file statistics for a Delta table.
    Shows number of files, total size, and average file size.
    """
    print(f"=== File Statistics for: {table_name} ===")
    
    stats_sql = f"""
    DESCRIBE DETAIL {table_name}
    """
    
    try:
        detail_df = spark.sql(stats_sql)
        detail_df.show(truncate=False)
        
        # Get file count and size
        files_sql = f"""
        SELECT 
            COUNT(*) as file_count,
            SUM(size) as total_size_bytes,
            AVG(size) as avg_file_size_bytes,
            MIN(size) as min_file_size_bytes,
            MAX(size) as max_file_size_bytes
        FROM (
            SELECT size 
            FROM delta.`{table_name}`
        )
        """
        
        file_stats = spark.sql(files_sql)
        file_stats.show(truncate=False)
        
        print(f"\n✓ Statistics retrieved")
        return detail_df, file_stats
    except Exception as e:
        print(f"✗ Error getting statistics: {str(e)}")
        return None, None

def analyze_delta_table(table_name: str):
    """
    Analyzes a Delta table to collect statistics for query optimization.
    """
    print(f"=== Analyzing Delta Table: {table_name} ===")
    
    analyze_sql = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
    
    print(f"\nSQL Command:")
    print(f"  {analyze_sql}")
    
    try:
        result = spark.sql(analyze_sql)
        print(f"\n✓ Analysis completed - statistics computed for all columns")
        return result
    except Exception as e:
        print(f"✗ Error analyzing table: {str(e)}")
        return None

def set_table_properties(table_name: str, properties: dict):
    """
    Sets table properties for Delta Lake optimization.
    
    Common properties:
    - delta.autoOptimize.optimizeWrite: Auto-optimize writes (true/false)
    - delta.autoOptimize.autoCompact: Auto-compact small files (true/false)
    - delta.logRetentionDuration: How long to keep transaction log (interval string)
    - delta.deletedFileRetentionDuration: How long to keep deleted files (interval string)
    """
    print(f"=== Setting Table Properties for: {table_name} ===")
    
    for key, value in properties.items():
        set_prop_sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{key}' = '{value}')"
        print(f"\n  Setting {key} = {value}")
        print(f"  SQL: {set_prop_sql}")
        
        try:
            spark.sql(set_prop_sql)
            print(f"  ✓ Property set successfully")
        except Exception as e:
            print(f"  ✗ Error setting property: {str(e)}")
    
    print(f"\n✓ All properties configured")
