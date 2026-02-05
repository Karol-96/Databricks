"""
Databricks Auto Loader - Incremental Data Loading Practice
Auto Loader is a Databricks feature that automatically processes new files as they arrive in cloud storage.
This script demonstrates various Auto Loader patterns and best practices.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, current_timestamp, when

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

def autoloader_with_deduplication(source_path: str, target_table: str, checkpoint_path: str, 
                                  unique_key: str, watermark_column: str = "ingestion_timestamp"):
    """
    Auto Loader with deduplication using Delta MERGE.
    Prevents duplicate records from being inserted.
    
    Args:
        source_path: Source data path
        target_table: Target Delta table
        checkpoint_path: Checkpoint location
        unique_key: Column name that should be unique
        watermark_column: Column for watermarking (default: ingestion_timestamp)
    """
    print(f"=== Auto Loader: With deduplication ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .load(source_path))
    
    # Add watermark for deduplication
    df_with_watermark = df.withWatermark(watermark_column, "10 minutes")
    
    def foreach_batch_function(batch_df, batch_id):
        """Function to handle each batch with deduplication"""
        batch_df.createOrReplaceTempView("updates")
        
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING updates AS source
        ON target.{unique_key} = source.{unique_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        spark.sql(merge_sql)
    
    query = (df_with_watermark.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .foreachBatch(foreach_batch_function)
             .outputMode("update")
             .trigger(processingTime='10 seconds')
             .start())
    
    print(f"✓ Deduplication enabled using MERGE on key: {unique_key}")
    return query

def autoloader_schema_evolution(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with automatic schema evolution.
    Handles schema changes gracefully by adding new columns.
    """
    print(f"=== Auto Loader: With schema evolution ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Add new columns automatically
          .option("cloudFiles.schemaHints", "id:long,name:string")  # Optional: provide hints
          .load(source_path))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .option("mergeSchema", "true")  # Merge schema changes
             .option("overwriteSchema", "false")  # Don't overwrite, add columns
             .table(target_table))
    
    print(f"✓ Schema evolution enabled - new columns will be added automatically")
    return query

def autoloader_with_partitioning(source_path: str, target_table: str, checkpoint_path: str,
                                 partition_columns: list):
    """
    Auto Loader with partitioned writes for better performance.
    
    Args:
        partition_columns: List of columns to partition by (e.g., ["year", "month"])
    """
    print(f"=== Auto Loader: With partitioning ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .load(source_path))
    
    # Add partition columns if they don't exist
    for partition_col in partition_columns:
        if partition_col not in df.columns:
            # Extract from date or other column
            if "date" in df.columns:
                df = df.withColumn(partition_col, col("date").substr(1, 4) if partition_col == "year" 
                                  else col("date").substr(6, 2))
    
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .partitionBy(*partition_columns)  # Partition by specified columns
             .table(target_table))
    
    print(f"✓ Partitioning enabled on columns: {', '.join(partition_columns)}")
    return query

def autoloader_checkpoint_recovery(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with checkpoint recovery handling.
    Useful for recovering from failures or resuming processing.
    """
    print(f"=== Auto Loader: Checkpoint recovery ===")
    
    # Check if checkpoint exists
    try:
        # Try to recover from existing checkpoint
        df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", checkpoint_path)
              .option("cloudFiles.inferColumnTypes", "true")
              .load(source_path))
        
        query = (df.writeStream
                 .format("delta")
                 .option("checkpointLocation", checkpoint_path)
                 .option("failOnDataLoss", "false")  # Don't fail if some data is lost
                 .table(target_table))
        
        print(f"✓ Checkpoint recovery enabled - will resume from last checkpoint")
        print(f"  Checkpoint location: {checkpoint_path}")
    except Exception as e:
        print(f"⚠ Error recovering checkpoint: {str(e)}")
        print("  Starting fresh checkpoint...")
        return None
    
    return query

def autoloader_with_metrics(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with custom metrics tracking.
    Tracks files processed, records ingested, and processing time.
    """
    print(f"=== Auto Loader: With metrics tracking ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          .load(source_path))
    
    # Add metrics columns
    df_with_metrics = (df
                       .withColumn("_file_name", col("_metadata.file_path"))
                       .withColumn("_ingestion_timestamp", current_timestamp())
                       .withColumn("_record_count", col("_metadata.file_size")))  # Approximate
    
    def foreach_batch_with_metrics(batch_df, batch_id):
        """Process batch and log metrics"""
        record_count = batch_df.count()
        file_count = batch_df.select("_file_name").distinct().count()
        
        print(f"Batch {batch_id}: Processed {file_count} files, {record_count} records")
        
        # Write to target table
        batch_df.drop("_file_name", "_record_count").write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
    
    query = (df_with_metrics.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .foreachBatch(foreach_batch_with_metrics)
             .trigger(processingTime='10 seconds')
             .start())
    
    print(f"✓ Metrics tracking enabled")
    return query

def autoloader_bronze_to_silver(source_path: str, bronze_table: str, silver_table: str, 
                                checkpoint_path: str):
    """
    Auto Loader pattern for medallion architecture: Bronze to Silver.
    Ingests raw data to bronze, then transforms to silver.
    """
    print(f"=== Auto Loader: Bronze to Silver Pattern ===")
    
    # Step 1: Load to Bronze (raw data)
    bronze_df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("cloudFiles.schemaLocation", f"{checkpoint_path}/bronze")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .load(source_path))
    
    # Add metadata
    bronze_df_with_meta = (bronze_df
                           .withColumn("_ingestion_timestamp", current_timestamp())
                           .withColumn("_source_file", col("_metadata.file_path")))
    
    # Write to Bronze
    bronze_query = (bronze_df_with_meta.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{checkpoint_path}/bronze")
                    .table(bronze_table))
    
    # Step 2: Transform Bronze to Silver
    silver_df = (spark.readStream
                 .table(bronze_table)
                 .select(
                     col("id").alias("customer_id"),
                     trim(upper(col("name"))).alias("name"),
                     lower(trim(col("email"))).alias("email"),
                     col("_ingestion_timestamp").alias("ingestion_timestamp"),
                     col("_source_file").alias("source_file")
                 )
                 .filter(col("email").isNotNull())
                 .filter(col("email").contains("@"))
                 .withColumn("data_quality_score", 
                            when(col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"), 1.0)
                            .otherwise(0.5)))
    
    silver_query = (silver_df.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{checkpoint_path}/silver")
                    .option("mergeSchema", "true")
                    .table(silver_table))
    
    print(f"✓ Bronze to Silver pipeline configured")
    print(f"  Bronze table: {bronze_table}")
    print(f"  Silver table: {silver_table}")
    return bronze_query, silver_query

def autoloader_performance_tuning(source_path: str, target_table: str, checkpoint_path: str):
    """
    Auto Loader with performance optimization settings.
    Optimized for high-throughput scenarios.
    """
    print(f"=== Auto Loader: Performance tuning ===")
    
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", checkpoint_path)
          .option("cloudFiles.inferColumnTypes", "true")
          # Performance options
          .option("cloudFiles.maxFilesPerTrigger", "10000")  # Process more files per batch
          .option("cloudFiles.maxBytesPerTrigger", "50g")  # Larger batch size
          .option("cloudFiles.useNotifications", "true")  # Use notification mode if available
          .option("cloudFiles.includeExistingFiles", "true")
          .load(source_path))
    
    # Repartition for better parallelism
    num_partitions = spark.sparkContext.defaultParallelism * 2
    df_repartitioned = df.repartition(num_partitions)
    
    query = (df_repartitioned.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .option("mergeSchema", "true")
             .option("maxFilesPerTrigger", "10000")
             .option("txnAppId", "autoloader_high_throughput")  # Transaction ID for monitoring
             .trigger(processingTime='5 seconds')  # More frequent triggers
             .table(target_table))
    
    print(f"✓ Performance optimizations applied:")
    print(f"  - Max files per trigger: 10000")
    print(f"  - Repartitioned to {num_partitions} partitions")
    print(f"  - Trigger interval: 5 seconds")
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

def autoloader_monitor_streaming_query(query):
    """
    Monitor a running Auto Loader streaming query.
    Provides status and metrics.
    """
    print(f"\n=== Monitoring Streaming Query ===")
    print(f"Query ID: {query.id}")
    print(f"Status: {query.status}")
    
    if hasattr(query, 'lastProgress'):
        progress = query.lastProgress
        if progress:
            print(f"Files processed: {progress.get('numInputRows', 0)}")
            print(f"Processing rate: {progress.get('inputRowsPerSecond', 0):.2f} rows/sec")
            print(f"Batch ID: {progress.get('batchId', 'N/A')}")
    
    return query

def autoloader_stop_query(query):
    """
    Safely stop a running Auto Loader query.
    """
    print(f"\n=== Stopping Streaming Query ===")
    try:
        query.stop()
        print("✓ Query stopped successfully")
    except Exception as e:
        print(f"Error stopping query: {str(e)}")

# Example usage
if __name__ == "__main__":
    print("=== Databricks Auto Loader Practice ===\n")
    
    # Example paths (adjust to your environment)
    source_path = "s3://my-bucket/data/landing/"
    target_table = "my_catalog.my_schema.raw_data"
    bronze_table = "my_catalog.bronze.raw_data"
    silver_table = "my_catalog.silver.cleaned_data"
    checkpoint_path = "s3://my-bucket/checkpoints/autoloader/"
    
    print("Auto Loader Key Concepts:")
    print("1. Automatically detects new files in cloud storage")
    print("2. Tracks processed files using checkpoints")
    print("3. Supports schema inference and evolution")
    print("4. Works with JSON, CSV, Parquet, and other formats")
    print("5. Can run in streaming or batch mode")
    print("\nBasic Examples:")
    print("- Basic JSON loading: autoloader_basic_json()")
    print("- CSV with headers: autoloader_csv()")
    print("- Parquet files: autoloader_parquet()")
    print("- With transformations: autoloader_with_transformations()")
    print("\nAdvanced Examples:")
    print("- File notification mode: autoloader_file_notification_mode()")
    print("- Batch mode: autoloader_batch_mode()")
    print("- With deduplication: autoloader_with_deduplication()")
    print("- Schema evolution: autoloader_schema_evolution()")
    print("- With partitioning: autoloader_with_partitioning()")
    print("- Checkpoint recovery: autoloader_checkpoint_recovery()")
    print("- Metrics tracking: autoloader_with_metrics()")
    print("- Bronze to Silver: autoloader_bronze_to_silver()")
    print("- Performance tuning: autoloader_performance_tuning()")
    print("\nBest Practices:")
    print("1. Use file notification mode for large directories (>10K files)")
    print("2. Define explicit schemas when possible for better performance")
    print("3. Use separate checkpoint paths for different streams")
    print("4. Enable schema evolution for changing data structures")
    print("5. Monitor bad records path for data quality issues")
    print("6. Use deduplication for idempotent processing")
    print("7. Partition target tables for better query performance")
    print("8. Set appropriate maxFilesPerTrigger based on cluster size")
    print("9. Use checkpoint recovery for fault tolerance")
    print("10. Monitor streaming queries regularly for performance")
    print("\nPerformance Tips:")
    print("- Repartition to 2-4× total cores for optimal parallelism")
    print("- Use notification mode instead of directory listing for large dirs")
    print("- Set maxFilesPerTrigger based on cluster capacity")
    print("- Enable Photon for SQL transformations")
    print("- Use partitioning on frequently filtered columns")
    print("\n=== Practice Complete ===")
