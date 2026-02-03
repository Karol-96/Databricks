"""
Unity Catalog - External Tables Practice
This script demonstrates working with external tables in Unity Catalog.
External tables point to data stored outside Unity Catalog managed storage.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo

workspace = WorkspaceClient()

def create_external_table_s3(catalog_name: str, schema_name: str, table_name: str,
                             s3_path: str, columns: list, format_type: str = "DELTA",
                             comment: str = None):
    """
    Creates an external table pointing to S3 data.
    
    Args:
        catalog_name: Catalog name
        schema_name: Schema name
        table_name: Table name
        s3_path: S3 path (e.g., s3://bucket/path/)
        columns: List of column definitions
        format_type: Data format (DELTA, PARQUET, CSV, JSON)
        comment: Optional description
    """
    try:
        column_defs = []
        for col in columns:
            column_defs.append({
                "name": col["name"],
                "type_text": col.get("type", "string"),
                "type_name": col.get("type", "string"),
                "position": len(column_defs),
                "comment": col.get("comment", "")
            })
        
        table = workspace.tables.create(
            name=f"{catalog_name}.{schema_name}.{table_name}",
            columns=column_defs,
            comment=comment or f"External table: {table_name}",
            table_type="EXTERNAL",
            storage_location=s3_path,
            data_source_format=format_type
        )
        print(f"✓ Created external table: {catalog_name}.{schema_name}.{table_name}")
        print(f"  Location: {s3_path}")
        print(f"  Format: {format_type}")
        return table
    except Exception as e:
        print(f"Error creating external table: {str(e)}")
        return None

def create_external_table_adls(catalog_name: str, schema_name: str, table_name: str,
                               adls_path: str, columns: list, format_type: str = "DELTA",
                               comment: str = None):
    """
    Creates an external table pointing to Azure Data Lake Storage (ADLS).
    
    Args:
        adls_path: ADLS path (e.g., abfss://container@account.dfs.core.windows.net/path/)
    """
    try:
        column_defs = []
        for col in columns:
            column_defs.append({
                "name": col["name"],
                "type_text": col.get("type", "string"),
                "type_name": col.get("type", "string"),
                "position": len(column_defs)
            })
        
        table = workspace.tables.create(
            name=f"{catalog_name}.{schema_name}.{table_name}",
            columns=column_defs,
            comment=comment or f"External table: {table_name}",
            table_type="EXTERNAL",
            storage_location=adls_path,
            data_source_format=format_type
        )
        print(f"✓ Created external table: {catalog_name}.{schema_name}.{table_name}")
        print(f"  Location: {adls_path}")
        return table
    except Exception as e:
        print(f"Error creating external table: {str(e)}")
        return None

def create_external_table_gcs(catalog_name: str, schema_name: str, table_name: str,
                              gcs_path: str, columns: list, format_type: str = "PARQUET",
                              comment: str = None):
    """
    Creates an external table pointing to Google Cloud Storage (GCS).
    
    Args:
        gcs_path: GCS path (e.g., gs://bucket/path/)
    """
    try:
        column_defs = []
        for col in columns:
            column_defs.append({
                "name": col["name"],
                "type_text": col.get("type", "string"),
                "type_name": col.get("type", "string"),
                "position": len(column_defs)
            })
        
        table = workspace.tables.create(
            name=f"{catalog_name}.{schema_name}.{table_name}",
            columns=column_defs,
            comment=comment or f"External table: {table_name}",
            table_type="EXTERNAL",
            storage_location=gcs_path,
            data_source_format=format_type
        )
        print(f"✓ Created external table: {catalog_name}.{schema_name}.{table_name}")
        print(f"  Location: {gcs_path}")
        return table
    except Exception as e:
        print(f"Error creating external table: {str(e)}")
        return None

def create_external_table_with_partitions(catalog_name: str, schema_name: str, table_name: str,
                                         location: str, columns: list, partition_columns: list,
                                         format_type: str = "PARQUET", comment: str = None):
    """
    Creates a partitioned external table.
    Partitioning improves query performance for large datasets.
    
    Args:
        partition_columns: List of column names to partition by (e.g., ["year", "month"])
    """
    try:
        column_defs = []
        for col in columns:
            is_partition = col["name"] in partition_columns
            column_defs.append({
                "name": col["name"],
                "type_text": col.get("type", "string"),
                "type_name": col.get("type", "string"),
                "position": len(column_defs),
                "comment": col.get("comment", ""),
                "partition_index": partition_columns.index(col["name"]) if is_partition else None
            })
        
        table = workspace.tables.create(
            name=f"{catalog_name}.{schema_name}.{table_name}",
            columns=column_defs,
            comment=comment or f"Partitioned external table: {table_name}",
            table_type="EXTERNAL",
            storage_location=location,
            data_source_format=format_type
        )
        print(f"✓ Created partitioned external table: {catalog_name}.{schema_name}.{table_name}")
        print(f"  Partitions: {', '.join(partition_columns)}")
        return table
    except Exception as e:
        print(f"Error creating partitioned external table: {str(e)}")
        return None

def refresh_external_table(catalog_name: str, schema_name: str, table_name: str):
    """
    Refreshes the metadata of an external table.
    Useful when new files are added to the external location.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        # In Databricks SQL, you would use: REFRESH TABLE table_name
        # Using SDK, you might need to use SQL execution
        print(f"✓ Refreshed external table: {full_name}")
        print("  Note: Use SQL command 'REFRESH TABLE' for actual refresh")
        return True
    except Exception as e:
        print(f"Error refreshing table: {str(e)}")
        return False

def get_external_table_location(catalog_name: str, schema_name: str, table_name: str):
    """
    Retrieves the storage location of an external table.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        table = workspace.tables.get(full_name)
        
        if table.table_type == "EXTERNAL":
            print(f"\n=== External Table Location: {full_name} ===")
            if hasattr(table, 'storage_location'):
                print(f"Storage Location: {table.storage_location}")
            if hasattr(table, 'data_source_format'):
                print(f"Data Format: {table.data_source_format}")
            return table.storage_location
        else:
            print(f"Table {full_name} is not an external table")
            return None
    except Exception as e:
        print(f"Error retrieving table location: {str(e)}")
        return None

def convert_to_managed_table(catalog_name: str, schema_name: str, table_name: str):
    """
    Converts an external table to a managed table.
    This moves data to Unity Catalog managed storage.
    Note: This is typically done via SQL: CREATE TABLE AS SELECT FROM external_table
    """
    print(f"\n=== Converting External to Managed Table ===")
    print(f"Table: {catalog_name}.{schema_name}.{table_name}")
    print("Note: Use SQL command:")
    print(f"  CREATE TABLE {catalog_name}.{schema_name}.{table_name}_managed")
    print(f"  AS SELECT * FROM {catalog_name}.{schema_name}.{table_name}")

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - External Tables Practice ===\n")
    
    catalog = "practice_catalog"
    schema = "external_data"
    
    # Example: Create external table from S3
    columns = [
        {"name": "id", "type": "bigint", "comment": "Record ID"},
        {"name": "name", "type": "string", "comment": "Name"},
        {"name": "value", "type": "double", "comment": "Value"},
        {"name": "created_date", "type": "date", "comment": "Creation date"}
    ]
    
    # Example S3 external table
    # create_external_table_s3(
    #     catalog, schema, "s3_data",
    #     "s3://my-bucket/data/",
    #     columns,
    #     format_type="PARQUET",
    #     comment="External table from S3"
    # )
    
    # Example: Partitioned external table
    partition_columns = ["created_date"]
    # create_external_table_with_partitions(
    #     catalog, schema, "partitioned_data",
    #     "s3://my-bucket/partitioned/",
    #     columns,
    #     partition_columns,
    #     format_type="PARQUET"
    # )
    
    print("\n=== Practice Complete ===")
    print("Key points:")
    print("- External tables point to data in your own storage")
    print("- No data movement - query data in place")
    print("- Supports S3, ADLS, GCS, and other cloud storage")
    print("- Can be partitioned for better performance")
    print("- Use REFRESH TABLE to update metadata when new files arrive")
