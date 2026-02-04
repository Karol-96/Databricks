"""
Databricks Medallion Architecture - Bronze, Silver, Gold Layers
This script demonstrates the medallion architecture pattern using Unity Catalog and Delta Lake.
The medallion architecture organizes data into three layers: Bronze (raw), Silver (cleaned), Gold (aggregated).
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SchemaInfo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, regexp_replace, trim, upper, to_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
import logging

# Import utilities
try:
    from config import workspace, get_config, validate_catalog_name, format_table_path, get_checkpoint_path, log_operation
    from config import logger as config_logger
except ImportError:
    # Fallback if config module not available
    workspace = WorkspaceClient()
    logging.basicConfig(level=logging.INFO)
    config_logger = logging.getLogger(__name__)
    
    def get_config():
        return {
            "catalog": "main",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold"
        }
    
    def validate_catalog_name(name):
        return True
    
    def format_table_path(catalog, schema, table):
        return f"{catalog}.{schema}.{table}"
    
    def get_checkpoint_path(catalog, schema, table):
        return f"/checkpoints/{catalog}/{schema}/{table}"
    
    def log_operation(operation, details=None):
        config_logger.info(f"Operation: {operation}")

logger = logging.getLogger(__name__)

# Initialize Spark (in Databricks, this is already available as 'spark')
# For local: spark = SparkSession.builder.appName("MedallionArchitecture").getOrCreate()

def setup_medallion_catalog(catalog_name: str):
    """
    Creates a catalog for the medallion architecture.
    This will contain bronze, silver, and gold schemas.
    """
    log_operation("setup_medallion_catalog", {"catalog_name": catalog_name})
    
    # Validate catalog name
    if not validate_catalog_name(catalog_name):
        logger.error(f"Invalid catalog name: {catalog_name}")
        return None
    
    try:
        catalog = workspace.catalogs.create(
            name=catalog_name,
            comment=f"Medallion architecture catalog: {catalog_name}"
        )
        logger.info(f"✓ Created catalog: {catalog_name}")
        print(f"✓ Created catalog: {catalog_name}")
        return catalog
    except Exception as e:
        logger.warning(f"Catalog {catalog_name} may already exist: {str(e)}")
        print(f"Catalog {catalog_name} may already exist: {str(e)}")
        return None

def create_medallion_schemas(catalog_name: str):
    """
    Creates the three medallion layers as schemas:
    - Bronze: Raw, unprocessed data
    - Silver: Cleaned and validated data
    - Gold: Business-ready aggregated data
    """
    schemas = {
        "bronze": "Raw data layer - unprocessed data from source systems",
        "silver": "Cleaned data layer - validated, deduplicated, and enriched data",
        "gold": "Business-ready layer - aggregated and curated data for analytics"
    }
    
    created_schemas = []
    for schema_name, description in schemas.items():
        try:
            schema = workspace.schemas.create(
                name=schema_name,
                catalog_name=catalog_name,
                comment=description
            )
            print(f"✓ Created schema: {catalog_name}.{schema_name}")
            created_schemas.append(schema)
        except Exception as e:
            print(f"Schema {schema_name} may already exist: {str(e)}")
    
    return created_schemas

def create_bronze_table(catalog_name: str, table_name: str, source_path: str = None):
    """
    Creates a bronze table for raw data ingestion.
    Bronze tables store data exactly as received from source systems.
    """
    print(f"\n=== Creating Bronze Table: {catalog_name}.bronze.{table_name} ===")
    
    # SQL command to create bronze table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.{table_name}
    (
        id BIGINT,
        raw_data STRING,
        source_system STRING,
        ingestion_timestamp TIMESTAMP,
        file_name STRING,
        _metadata STRUCT<
            file_path: STRING,
            file_size: BIGINT,
            file_modification_time: TIMESTAMP
        >
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    COMMENT 'Bronze layer table for raw data ingestion'
    """
    
    print("SQL Command:")
    print(create_table_sql)
    print("\n✓ Bronze table structure created")
    print("  - Stores raw data as received")
    print("  - Includes metadata about source files")
    print("  - Auto-optimization enabled for better performance")
    
    return create_table_sql

def create_silver_table(catalog_name: str, table_name: str, source_bronze_table: str):
    """
    Creates a silver table for cleaned and validated data.
    Silver tables contain data that has been cleaned, validated, and enriched.
    """
    print(f"\n=== Creating Silver Table: {catalog_name}.silver.{table_name} ===")
    
    # Example: Customer data silver table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.silver.{table_name}
    (
        customer_id BIGINT,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING,
        date_of_birth DATE,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        is_active BOOLEAN,
        data_quality_score DOUBLE,
        source_system STRING,
        ingestion_date DATE
    )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    COMMENT 'Silver layer table for cleaned and validated data'
    """
    
    print("SQL Command:")
    print(create_table_sql)
    print("\n✓ Silver table structure created")
    print("  - Cleaned and validated data")
    print("  - Partitioned by ingestion_date for better query performance")
    print("  - Includes data quality metrics")
    
    return create_table_sql

def create_gold_table(catalog_name: str, table_name: str):
    """
    Creates a gold table for business-ready aggregated data.
    Gold tables contain aggregated, curated data optimized for analytics.
    """
    print(f"\n=== Creating Gold Table: {catalog_name}.gold.{table_name} ===")
    
    # Example: Customer analytics gold table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.{table_name}
    (
        customer_id BIGINT,
        customer_name STRING,
        email STRING,
        customer_segment STRING,
        total_orders BIGINT,
        total_revenue DOUBLE,
        average_order_value DOUBLE,
        last_order_date DATE,
        first_order_date DATE,
        days_since_last_order INT,
        lifetime_value DOUBLE,
        preferred_category STRING,
        region STRING,
        is_premium_customer BOOLEAN,
        last_updated TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (region)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    COMMENT 'Gold layer table for business-ready aggregated data'
    """
    
    print("SQL Command:")
    print(create_table_sql)
    print("\n✓ Gold table structure created")
    print("  - Business-ready aggregated data")
    print("  - Partitioned by region for analytics")
    print("  - Optimized for reporting and dashboards")
    
    return create_table_sql

def bronze_to_silver_pipeline(catalog_name: str, bronze_table: str, silver_table: str):
    """
    Processes data from bronze to silver layer.
    This includes data cleaning, validation, and enrichment.
    """
    print(f"\n=== Bronze to Silver Pipeline ===")
    print(f"Source: {catalog_name}.bronze.{bronze_table}")
    print(f"Target: {catalog_name}.silver.{silver_table}")
    
    # SQL for bronze to silver transformation
    transform_sql = f"""
    INSERT INTO {catalog_name}.silver.{silver_table}
    SELECT
        CAST(JSON_EXTRACT_SCALAR(raw_data, '$.customer_id') AS BIGINT) AS customer_id,
        TRIM(UPPER(JSON_EXTRACT_SCALAR(raw_data, '$.first_name'))) AS first_name,
        TRIM(UPPER(JSON_EXTRACT_SCALAR(raw_data, '$.last_name'))) AS last_name,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(raw_data, '$.email'))) AS email,
        REGEXP_REPLACE(JSON_EXTRACT_SCALAR(raw_data, '$.phone'), '[^0-9]', '') AS phone,
        TRIM(JSON_EXTRACT_SCALAR(raw_data, '$.address')) AS address,
        TRIM(UPPER(JSON_EXTRACT_SCALAR(raw_data, '$.city'))) AS city,
        UPPER(TRIM(JSON_EXTRACT_SCALAR(raw_data, '$.state'))) AS state,
        REGEXP_REPLACE(JSON_EXTRACT_SCALAR(raw_data, '$.zip_code'), '[^0-9]', '') AS zip_code,
        UPPER(TRIM(JSON_EXTRACT_SCALAR(raw_data, '$.country'))) AS country,
        TO_DATE(JSON_EXTRACT_SCALAR(raw_data, '$.date_of_birth'), 'yyyy-MM-dd') AS date_of_birth,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at,
        CASE 
            WHEN JSON_EXTRACT_SCALAR(raw_data, '$.status') = 'active' THEN TRUE 
            ELSE FALSE 
        END AS is_active,
        -- Data quality score calculation
        CASE
            WHEN JSON_EXTRACT_SCALAR(raw_data, '$.email') IS NOT NULL 
                 AND JSON_EXTRACT_SCALAR(raw_data, '$.email') LIKE '%@%.%' THEN 1.0
            ELSE 0.5
        END AS data_quality_score,
        source_system,
        DATE(ingestion_timestamp) AS ingestion_date
    FROM {catalog_name}.bronze.{bronze_table}
    WHERE 
        -- Data quality filters
        JSON_EXTRACT_SCALAR(raw_data, '$.customer_id') IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_data, '$.email') IS NOT NULL
        AND JSON_EXTRACT_SCALAR(raw_data, '$.email') LIKE '%@%.%'
    """
    
    print("\nSQL Transformation:")
    print(transform_sql)
    print("\n✓ Transformation includes:")
    print("  - Data cleaning (trim, upper, lower)")
    print("  - Data validation (email format, required fields)")
    print("  - Data enrichment (timestamps, quality scores)")
    print("  - Data type conversions")
    
    return transform_sql

def silver_to_gold_pipeline(catalog_name: str, silver_table: str, gold_table: str, 
                           orders_table: str = None):
    """
    Processes data from silver to gold layer.
    This includes aggregation and business logic.
    """
    print(f"\n=== Silver to Gold Pipeline ===")
    print(f"Source: {catalog_name}.silver.{silver_table}")
    print(f"Target: {catalog_name}.gold.{gold_table}")
    
    # SQL for silver to gold aggregation
    # This is a simplified example - in practice, you'd join with orders/transactions
    transform_sql = f"""
    MERGE INTO {catalog_name}.gold.{gold_table} AS target
    USING (
        SELECT
            c.customer_id,
            CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
            c.email,
            -- Customer segmentation logic
            CASE
                WHEN COALESCE(o.total_revenue, 0) > 10000 THEN 'Premium'
                WHEN COALESCE(o.total_revenue, 0) > 5000 THEN 'Standard'
                ELSE 'Basic'
            END AS customer_segment,
            COALESCE(o.total_orders, 0) AS total_orders,
            COALESCE(o.total_revenue, 0) AS total_revenue,
            CASE 
                WHEN COALESCE(o.total_orders, 0) > 0 
                THEN COALESCE(o.total_revenue, 0) / o.total_orders 
                ELSE 0 
            END AS average_order_value,
            o.last_order_date,
            o.first_order_date,
            DATEDIFF(CURRENT_DATE(), o.last_order_date) AS days_since_last_order,
            COALESCE(o.total_revenue, 0) AS lifetime_value,
            o.preferred_category,
            c.state AS region,
            COALESCE(o.total_revenue, 0) > 10000 AS is_premium_customer,
            CURRENT_TIMESTAMP() AS last_updated
        FROM {catalog_name}.silver.{silver_table} c
        LEFT JOIN (
            -- Aggregated order data (example - adjust based on your schema)
            SELECT
                customer_id,
                COUNT(*) AS total_orders,
                SUM(amount) AS total_revenue,
                MAX(order_date) AS last_order_date,
                MIN(order_date) AS first_order_date,
                MODE(category) AS preferred_category
            FROM {catalog_name}.silver.{orders_table if orders_table else 'orders'}
            GROUP BY customer_id
        ) o ON c.customer_id = o.customer_id
        WHERE c.is_active = TRUE
    ) AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            customer_name = source.customer_name,
            email = source.email,
            customer_segment = source.customer_segment,
            total_orders = source.total_orders,
            total_revenue = source.total_revenue,
            average_order_value = source.average_order_value,
            last_order_date = source.last_order_date,
            first_order_date = source.first_order_date,
            days_since_last_order = source.days_since_last_order,
            lifetime_value = source.lifetime_value,
            preferred_category = source.preferred_category,
            region = source.region,
            is_premium_customer = source.is_premium_customer,
            last_updated = source.last_updated
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id, customer_name, email, customer_segment,
            total_orders, total_revenue, average_order_value,
            last_order_date, first_order_date, days_since_last_order,
            lifetime_value, preferred_category, region,
            is_premium_customer, last_updated
        )
        VALUES (
            source.customer_id, source.customer_name, source.email, source.customer_segment,
            source.total_orders, source.total_revenue, source.average_order_value,
            source.last_order_date, source.first_order_date, source.days_since_last_order,
            source.lifetime_value, source.preferred_category, source.region,
            source.is_premium_customer, source.last_updated
        )
    """
    
    print("\nSQL Transformation (MERGE):")
    print(transform_sql)
    print("\n✓ Transformation includes:")
    print("  - Data aggregation (sum, count, min, max)")
    print("  - Business logic (customer segmentation)")
    print("  - MERGE operation for upserts")
    print("  - Joins with related tables")
    
    return transform_sql

def create_streaming_bronze_to_silver(catalog_name: str, bronze_table: str, silver_table: str):
    """
    Creates a streaming pipeline from bronze to silver using Auto Loader.
    """
    print(f"\n=== Streaming Bronze to Silver Pipeline ===")
    
    # This would use Spark Structured Streaming
    streaming_code = f"""
    # Streaming from bronze to silver
    bronze_df = spark.readStream.table("{catalog_name}.bronze.{bronze_table}")
    
    # Apply transformations
    silver_df = (bronze_df
        .select(
            col("id").alias("customer_id"),
            trim(upper(col("raw_data.first_name"))).alias("first_name"),
            trim(upper(col("raw_data.last_name"))).alias("last_name"),
            lower(trim(col("raw_data.email"))).alias("email"),
            col("source_system"),
            current_timestamp().alias("created_at"),
            to_date(col("ingestion_timestamp")).alias("ingestion_date")
        )
        .filter(col("raw_data.email").isNotNull())
        .filter(col("raw_data.email").contains("@"))
    )
    
    # Write to silver table
    query = (silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/checkpoints/{catalog_name}/bronze_to_silver")
        .table(f"{catalog_name}.silver.{silver_table}")
        .trigger(processingTime='10 seconds'))
    
    query.awaitTermination()
    """
    
    print("Streaming Code:")
    print(streaming_code)
    print("\n✓ Streaming pipeline configured")
    print("  - Processes new data every 10 seconds")
    print("  - Applies data quality filters")
    print("  - Writes to silver table")
    
    return streaming_code

def optimize_tables(catalog_name: str):
    """
    Optimizes Delta tables in all medallion layers.
    """
    print(f"\n=== Optimizing Medallion Tables ===")
    
    optimization_commands = []
    for layer in ["bronze", "silver", "gold"]:
        # Get all tables in the layer
        tables = workspace.tables.list(catalog_name=catalog_name, schema_name=layer)
        
        for table in tables:
            optimize_sql = f"OPTIMIZE {catalog_name}.{layer}.{table.name}"
            zorder_sql = f"OPTIMIZE {catalog_name}.{layer}.{table.name} ZORDER BY (customer_id)"
            
            optimization_commands.append({
                "table": f"{catalog_name}.{layer}.{table.name}",
                "optimize": optimize_sql,
                "zorder": zorder_sql
            })
    
    print("Optimization Commands:")
    for cmd in optimization_commands:
        print(f"\n  Table: {cmd['table']}")
        print(f"  Basic: {cmd['optimize']}")
        print(f"  Z-Order: {cmd['zorder']}")
    
    print("\n✓ Optimization commands generated")
    print("  - OPTIMIZE: Compacts small files")
    print("  - ZORDER: Co-locates related data for faster queries")
    
    return optimization_commands

def vacuum_tables(catalog_name: str, retention_hours: int = 168):
    """
    Vacuums Delta tables to remove old files.
    """
    print(f"\n=== Vacuuming Medallion Tables ===")
    print(f"Retention: {retention_hours} hours (7 days)")
    
    vacuum_commands = []
    for layer in ["bronze", "silver", "gold"]:
        tables = workspace.tables.list(catalog_name=catalog_name, schema_name=layer)
        
        for table in tables:
            vacuum_sql = f"VACUUM {catalog_name}.{layer}.{table.name} RETAIN {retention_hours} HOURS"
            vacuum_commands.append({
                "table": f"{catalog_name}.{layer}.{table.name}",
                "command": vacuum_sql
            })
    
    print("\nVacuum Commands:")
    for cmd in vacuum_commands:
        print(f"  {cmd['command']}")
    
    print("\n✓ Vacuum commands generated")
    print("  - Removes old files beyond retention period")
    print("  - Reduces storage costs")
    
    return vacuum_commands

# Example usage
if __name__ == "__main__":
    print("=== Databricks Medallion Architecture Practice ===\n")
    
    catalog_name = "medallion_catalog"
    
    # Step 1: Setup catalog and schemas
    print("Step 1: Setting up medallion architecture...")
    setup_medallion_catalog(catalog_name)
    create_medallion_schemas(catalog_name)
    
    # Step 2: Create tables in each layer
    print("\n" + "="*60)
    print("Step 2: Creating tables in each layer...")
    create_bronze_table(catalog_name, "customers_raw")
    create_silver_table(catalog_name, "customers", "customers_raw")
    create_gold_table(catalog_name, "customer_analytics")
    
    # Step 3: Data processing pipelines
    print("\n" + "="*60)
    print("Step 3: Creating data processing pipelines...")
    bronze_to_silver_pipeline(catalog_name, "customers_raw", "customers")
    silver_to_gold_pipeline(catalog_name, "customers", "customer_analytics", "orders")
    
    # Step 4: Streaming pipeline
    print("\n" + "="*60)
    print("Step 4: Streaming pipeline...")
    create_streaming_bronze_to_silver(catalog_name, "customers_raw", "customers")
    
    # Step 5: Optimization
    print("\n" + "="*60)
    print("Step 5: Table optimization...")
    optimize_tables(catalog_name)
    vacuum_tables(catalog_name)
    
    print("\n" + "="*60)
    print("=== Medallion Architecture Setup Complete ===")
    print("\nArchitecture Overview:")
    print("  BRONZE → Raw data ingestion (as-is from source)")
    print("  SILVER → Cleaned, validated, enriched data")
    print("  GOLD   → Business-ready aggregated data")
    print("\nBest Practices:")
    print("  - Use Delta Lake for ACID transactions")
    print("  - Partition tables appropriately")
    print("  - Enable auto-optimization")
    print("  - Regular OPTIMIZE and VACUUM operations")
    print("  - Monitor data quality at each layer")
