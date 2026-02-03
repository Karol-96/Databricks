"""
Databricks Auto Loader - Incremental Data Loading Practice
Auto Loader is a Databricks feature that automatically processes new files as they arrive in cloud storage.
This script demonstrates various Auto Loader patterns and best practices.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# Initialize Spark session
# Note: In Databricks notebooks, SparkSession is already available as 'spark'
# For local testing, you would create: spark = SparkSession.builder.appName("AutoLoader").getOrCreate()

def autoloader_basic_json(source_path: str, target_table: str, checkpoint_path: str):
    """
    Basic Auto Loader example for JSON files.
    Auto Loader automatically detects and processes new files incrementally.
    
    Args:
        source_path: Cloud storage path (s3://, abfss://, gs://, etc.)
        target_table: Target Delta table name
        checkpoint_path: Location for Auto Loader checkpoint (tracks processed files)
    """
    print(f"=== Auto Loader: Loading JSON from {source_path} ===")
    
    # Basic Auto Loader query
    # cloudFiles is the format for Auto Loader
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .load(source_path))
    
    # Write to Delta table
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .option("mergeSchema", "true")  # Automatically merge schema changes
             .table(target_table))
    
    print(f"✓ Started streaming to table: {target_table}")
    print(f"✓ Checkpoint location: {checkpoint_path}")
    return query

def autoloader_with_schema(source_path: str, target_table: str, checkpoint_path: str, schema: StructType):
    """
    Auto Loader with explicit schema definition.
    Use this when you know the schema and want better performance/type control.
    
    Args:
        source_path: Source data path
        target_table: Target table name
        checkpoint_path: Checkpoint location
        schema: Explicit schema definition
    """
    print(f"=== Auto Loader: Loading with explicit schema ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .schema(schema)  # Explicit schema
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .table(target_table))
    
    print(f"✓ Using explicit schema with {len(schema.fields)} fields")
    return query

def autoloader_csv(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader for CSV files with common options.
    """
    print(f"=== Auto Loader: Loading CSV files ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("header", "true")  # First row contains headers
          .option("inferSchema", "true")
          .option("delimiter", ",")
          .option("quote", '"')
          .option("escape", '"')
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .table(target_table))
    
    print(f"✓ CSV Auto Loader configured")
    return query

def autoloader_parquet(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader for Parquet files.
    Parquet is columnar format - great for analytics workloads.
    """
    print(f"=== Auto Loader: Loading Parquet files ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "parquet")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .table(target_table))
    
    print(f"✓ Parquet Auto Loader configured")
    return query

def autoloader_with_transformations(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with data transformations.
    You can apply transformations before writing to target.
    """
    print(f"=== Auto Loader: With transformations ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .load(source_path))
    
    # Apply transformations
    transformed_df = (df
                      .withColumn("load_timestamp", current_timestamp())
                      .withColumn("year", col("date").substr(1, 4))
                      .withColumn("month", col("date").substr(6, 2))
                      .filter(col("status") == "active"))  # Filter records
    
    query = (transformed_df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .table(target_table))
    
    print(f"✓ Applied transformations: timestamp, date parsing, filtering")
    return query

def autoloader_file_notification_mode(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with file notification mode (faster, requires queue setup).
    File notification mode uses cloud-native queues (SQS, Event Grid, Pub/Sub).
    This is faster than directory listing mode for large directories.
    """
    print(f"=== Auto Loader: File notification mode ===")
    
    # For AWS S3
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.useNotifications", "true")  # Enable notification mode
          .option("cloudFiles.region", "us-east-1")  # Your AWS region
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .table(target_table))
    
    print(f"✓ File notification mode enabled (requires SQS/Event Grid/Pub/Sub setup)")
    return query

def autoloader_batch_mode(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader in batch mode (one-time load instead of streaming).
    Useful for backfilling or one-time data loads.
    """
    print(f"=== Auto Loader: Batch mode (one-time load) ===")
    
    df = (spark.read
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .load(source_path))
    
    # Write as batch (not streaming)
    df.write.format("delta").mode("append").saveAsTable(target_table)
    
    print(f"✓ Batch load completed to table: {target_table}")
    return df

def autoloader_with_options(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with advanced options and configurations.
    """
    print(f"=== Auto Loader: Advanced options ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.maxFilesPerTrigger", "1000")  # Process up to 1000 files per trigger
          .option("cloudFiles.maxBytesPerTrigger", "10g")  # Or limit by size
          .option("cloudFiles.includeExistingFiles", "true")  # Process existing files on first run
          .option("cloudFiles.allowOverwrites", "false")  # Don't reprocess files
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle schema changes
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .option("mergeSchema", "true")
             .option("maxFilesPerTrigger", "1000")
             .trigger(processingTime='10 seconds')  # Process every 10 seconds
             .table(target_table))
    
    print(f"✓ Advanced options configured")
    print(f"  - Max files per trigger: 1000")
    print(f"  - Schema evolution: addNewColumns")
    print(f"  - Trigger interval: 10 seconds")
    return query

def autoloader_multipath(source_paths: list, target_table: str, checkpoint_path: str):
    """
    Auto Loader reading from multiple source paths.
    Useful when data is partitioned across multiple directories.
    """
    print(f"=== Auto Loader: Multiple source paths ===")
    
    # Read from multiple paths
    dfs = []
    for path in source_paths:
        df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{checkpoint_path}/path_{source_paths.index(path)}")
              .load(path))
        dfs.append(df)
    
    # Union all dataframes
    from functools import reduce
    from pyspark.sql import DataFrame
    combined_df = reduce(DataFrame.unionByName, dfs)
    
    query = (combined_df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .table(target_table))
    
    print(f"✓ Reading from {len(source_paths)} source paths")
    return query

def autoloader_error_handling(source_path: str, target_table: str, checkpoint_path: str, error_table: str):
    """
    Auto Loader with error handling and dead letter queue.
    Captures records that fail to parse for investigation.
    """
    print(f"=== Auto Loader: With error handling ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .option("badRecordsPath", f"{checkpoint_path}/bad_records")  # Store bad records
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .option("mergeSchema", "true")
             .table(target_table))
    
    print(f"✓ Error handling enabled - bad records saved to: {checkpoint_path}/bad_records")
    return query

# Example schema definitions
def get_customer_schema():
    """Example schema for customer data"""
    return StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("status", StringType(), True)
    ])

def get_transaction_schema():
    """Example schema for transaction data"""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("category", StringType(), True)
    ])

# Example usage
if __name__ == "__main__":
    print("=== Databricks Auto Loader Practice ===\n")
    
    # Example paths (adjust to your environment)
    source_path = "s3://my-bucket/data/landing/"
    target_table = "my_catalog.my_schema.raw_data"
    checkpoint_path = "s3://my-bucket/checkpoints/autoloader/"
    
    print("Auto Loader Key Concepts:")
    print("1. Automatically detects new files in cloud storage")
    print("2. Tracks processed files using checkpoints")
    print("3. Supports schema inference and evolution")
    print("4. Works with JSON, CSV, Parquet, and other formats")
    print("5. Can run in streaming or batch mode")
    print("\nExample configurations:")
    print("- Basic JSON loading: autoloader_basic_json()")
    print("- CSV with headers: autoloader_csv()")
    print("- With transformations: autoloader_with_transformations()")
    print("- File notification mode: autoloader_file_notification_mode()")
    print("- Batch mode: autoloader_batch_mode()")
    print("\nBest Practices:")
    print("- Use file notification mode for large directories")
    print("- Define explicit schemas when possible for better performance")
    print("- Use separate checkpoint paths for different streams")
    print("- Enable schema evolution for changing data structures")
    print("- Monitor bad records path for data quality issues")
    print("\n=== Practice Complete ===")
