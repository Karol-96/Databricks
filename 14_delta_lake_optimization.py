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

def enable_auto_optimize(table_name: str, optimize_write: bool = True, auto_compact: bool = True):
    """
    Enables auto-optimization for a Delta table.
    Auto-optimization automatically compacts small files during writes.
    
    Args:
        table_name: Full table name
        optimize_write: Enable automatic file optimization during writes
        auto_compact: Enable automatic compaction of small files
    """
    print(f"=== Enabling Auto-Optimize for: {table_name} ===")
    
    properties = {
        'delta.autoOptimize.optimizeWrite': str(optimize_write).lower(),
        'delta.autoOptimize.autoCompact': str(auto_compact).lower()
    }
    
    set_table_properties(table_name, properties)
    print(f"\n✓ Auto-optimization enabled:")
    print(f"  - Optimize Write: {optimize_write}")
    print(f"  - Auto Compact: {auto_compact}")

def create_bloom_filter_index(table_name: str, column: str):
    """
    Creates a Bloom filter index on a column for faster point lookups.
    Bloom filters are useful for high-cardinality columns used in WHERE clauses.
    
    Args:
        table_name: Full table name
        column: Column name to create Bloom filter on
    """
    print(f"=== Creating Bloom Filter Index ===")
    print(f"  Table: {table_name}")
    print(f"  Column: {column}")
    
    # Note: Bloom filter creation syntax may vary by Databricks version
    # This is a conceptual example
    bloom_sql = f"""
    CREATE BLOOM FILTER INDEX
    ON TABLE {table_name}
    FOR COLUMNS({column})
    """
    
    print(f"\nSQL Command:")
    print(f"  {bloom_sql}")
    print(f"\n⚠️  Note: Bloom filters are available in Databricks Runtime 10.4+")
    
    try:
        result = spark.sql(bloom_sql)
        print(f"\n✓ Bloom filter index created on {column}")
        return result
    except Exception as e:
        print(f"✗ Error creating Bloom filter: {str(e)}")
        print(f"  This feature may not be available in your Databricks version")
        return None

def optimize_for_queries(table_name: str, filter_columns: list, zorder_columns: list = None):
    """
    Optimizes a table specifically for query performance.
    Combines Z-ORDER with Bloom filters for optimal query speed.
    
    Args:
        table_name: Full table name
        filter_columns: Columns frequently used in WHERE clauses (for Bloom filters)
        zorder_columns: Columns for Z-ORDER clustering (optional)
    """
    print(f"=== Optimizing Table for Query Performance ===")
    print(f"  Table: {table_name}")
    print(f"  Filter columns: {', '.join(filter_columns)}")
    
    # Step 1: Create Bloom filters on filter columns
    for col_name in filter_columns:
        create_bloom_filter_index(table_name, col_name)
    
    # Step 2: Apply Z-ORDER if specified
    if zorder_columns:
        print(f"\n  Applying Z-ORDER on: {', '.join(zorder_columns)}")
        optimize_delta_table(table_name, zorder_columns)
    else:
        optimize_delta_table(table_name)
    
    # Step 3: Analyze table for statistics
    analyze_delta_table(table_name)
    
    print(f"\n✓ Table optimized for query performance")

def compact_small_files(table_name: str, target_file_size: str = "128MB"):
    """
    Compacts small files in a Delta table to improve read performance.
    
    Args:
        table_name: Full table name
        target_file_size: Target size for compacted files (e.g., "128MB", "256MB")
    """
    print(f"=== Compacting Small Files ===")
    print(f"  Table: {table_name}")
    print(f"  Target file size: {target_file_size}")
    
    # OPTIMIZE automatically compacts small files
    optimize_sql = f"OPTIMIZE {table_name}"
    
    print(f"\nSQL Command:")
    print(f"  {optimize_sql}")
    print(f"\n  Note: OPTIMIZE automatically compacts files to optimal size")
    
    try:
        result = spark.sql(optimize_sql)
        result.show(truncate=False)
        print(f"\n✓ Files compacted")
        return result
    except Exception as e:
        print(f"✗ Error compacting files: {str(e)}")
        return None

def get_table_history(table_name: str, limit: int = 20):
    """
    Gets the transaction history of a Delta table.
    Useful for auditing and understanding table changes.
    
    Args:
        table_name: Full table name
        limit: Number of recent operations to show
    """
    print(f"=== Table History for: {table_name} ===")
    
    history_sql = f"DESCRIBE HISTORY {table_name} LIMIT {limit}"
    
    print(f"\nSQL Command:")
    print(f"  {history_sql}")
    
    try:
        history_df = spark.sql(history_sql)
        history_df.show(truncate=False)
        print(f"\n✓ History retrieved ({limit} most recent operations)")
        return history_df
    except Exception as e:
        print(f"✗ Error getting history: {str(e)}")
        return None

def restore_table_to_version(table_name: str, version: int):
    """
    Restores a Delta table to a specific version.
    Useful for rolling back changes.
    
    Args:
        table_name: Full table name
        version: Version number to restore to
    """
    print(f"=== Restoring Table to Version ===")
    print(f"  Table: {table_name}")
    print(f"  Version: {version}")
    
    restore_sql = f"RESTORE TABLE {table_name} TO VERSION AS OF {version}"
    
    print(f"\nSQL Command:")
    print(f"  {restore_sql}")
    print(f"\n⚠️  Warning: This will restore the table to a previous state")
    
    try:
        result = spark.sql(restore_sql)
        print(f"\n✓ Table restored to version {version}")
        return result
    except Exception as e:
        print(f"✗ Error restoring table: {str(e)}")
        return None
