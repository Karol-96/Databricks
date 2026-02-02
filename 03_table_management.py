"""
Unity Catalog - Table Management Practice
This script demonstrates creating, managing, and working with tables in Unity Catalog.
Tables are the core data objects that store structured data.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, DataSourceFormat
import json

workspace = WorkspaceClient()

def create_managed_table(catalog_name: str, schema_name: str, table_name: str, 
                        columns: list, comment: str = None):
    """
    Creates a managed table in Unity Catalog.
    Managed tables are fully managed by Databricks - storage and metadata.
    
    Args:
        catalog_name: Catalog where table will be created
        schema_name: Schema (database) name
        table_name: Name of the table
        columns: List of column definitions, e.g., [{"name": "id", "type": "int"}, ...]
        comment: Optional table description
    
    Returns:
        TableInfo object if successful
    """
    try:
        # Build column definitions for the table
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
            comment=comment or f"Managed table: {table_name}",
            table_type="MANAGED"
        )
        print(f"✓ Created managed table: {catalog_name}.{schema_name}.{table_name}")
        return table
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        return None

def create_external_table(catalog_name: str, schema_name: str, table_name: str,
                          location: str, columns: list, data_source_format: str = "DELTA",
                          comment: str = None):
    """
    Creates an external table pointing to data stored outside Unity Catalog.
    Useful for data that's already stored in cloud storage.
    
    Args:
        catalog_name: Catalog name
        schema_name: Schema name
        table_name: Table name
        location: External storage location (S3, ADLS, etc.)
        columns: Column definitions
        data_source_format: Format like "DELTA", "PARQUET", "CSV"
        comment: Optional description
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
            storage_location=location,
            data_source_format=data_source_format
        )
        print(f"✓ Created external table: {catalog_name}.{schema_name}.{table_name}")
        return table
    except Exception as e:
        print(f"Error creating external table: {str(e)}")
        return None

def list_tables(catalog_name: str, schema_name: str):
    """
    Lists all tables in a given schema.
    Shows both managed and external tables.
    """
    print(f"\n=== Tables in {catalog_name}.{schema_name} ===")
    try:
        tables = workspace.tables.list(
            catalog_name=catalog_name,
            schema_name=schema_name
        )
        
        if not tables:
            print("  No tables found")
            return []
        
        for table in tables:
            print(f"  - {table.name}")
            if hasattr(table, 'table_type'):
                print(f"    Type: {table.table_type}")
            if hasattr(table, 'comment') and table.comment:
                print(f"    Description: {table.comment}")
        
        return tables
    except Exception as e:
        print(f"Error listing tables: {str(e)}")
        return []

def get_table_details(catalog_name: str, schema_name: str, table_name: str):
    """
    Retrieves detailed information about a table.
    Shows columns, properties, storage location, etc.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        table = workspace.tables.get(full_name)
        
        print(f"\n=== Table Details: {full_name} ===")
        print(f"Name: {table.name}")
        print(f"Type: {table.table_type}")
        if hasattr(table, 'comment'):
            print(f"Comment: {table.comment}")
        if hasattr(table, 'storage_location'):
            print(f"Storage Location: {table.storage_location}")
        if hasattr(table, 'data_source_format'):
            print(f"Data Format: {table.data_source_format}")
        
        if hasattr(table, 'columns') and table.columns:
            print(f"\nColumns ({len(table.columns)}):")
            for col in table.columns:
                print(f"  - {col.name}: {col.type_text}")
        
        return table
    except Exception as e:
        print(f"Error retrieving table: {str(e)}")
        return None

def update_table(catalog_name: str, schema_name: str, table_name: str,
                new_comment: str = None, owner: str = None):
    """
    Updates table properties like description or ownership.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        update_request = {}
        
        if new_comment:
            update_request['comment'] = new_comment
        if owner:
            update_request['owner'] = owner
        
        if update_request:
            workspace.tables.update(full_name, **update_request)
            print(f"✓ Updated table: {full_name}")
        else:
            print("No updates specified")
    except Exception as e:
        print(f"Error updating table: {str(e)}")

def delete_table(catalog_name: str, schema_name: str, table_name: str):
    """
    Deletes a table. For managed tables, this also deletes the data!
    For external tables, only metadata is removed.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        workspace.tables.delete(full_name)
        print(f"✓ Deleted table: {full_name}")
    except Exception as e:
        print(f"Error deleting table: {str(e)}")

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Table Management Practice ===\n")
    
    catalog = "practice_catalog"
    schema = "raw_data"
    
    # Create a sample managed table
    columns = [
        {"name": "id", "type": "bigint", "comment": "Unique identifier"},
        {"name": "name", "type": "string", "comment": "Customer name"},
        {"name": "email", "type": "string", "comment": "Email address"},
        {"name": "created_at", "type": "timestamp", "comment": "Creation timestamp"}
    ]
    
    create_managed_table(
        catalog, schema, "customers",
        columns,
        comment="Customer data table"
    )
    
    # List all tables in the schema
    list_tables(catalog, schema)
    
    # Get table details
    get_table_details(catalog, schema, "customers")
    
    # Update table description
    update_table(catalog, schema, "customers", 
                new_comment="Updated: Customer master data")
    
    print("\n=== Practice Complete ===")
    print("Note: Managed tables store data in Unity Catalog managed storage")
    print("External tables point to data in your own cloud storage")

