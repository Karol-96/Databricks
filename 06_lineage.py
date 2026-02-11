"""
Unity Catalog - Data Lineage Tracking
This module demonstrates how to track and query data lineage in Unity Catalog.
Lineage shows how data flows through your pipelines and transformations.
"""

from databricks.sdk import WorkspaceClient
import logging

logger = logging.getLogger(__name__)
workspace = WorkspaceClient()

def get_table_lineage(table_name: str):
    """
    Gets the data lineage for a specific table.
    Shows upstream sources and downstream consumers.
    
    Args:
        table_name: Full table name (catalog.schema.table)
    """
    print(f"=== Data Lineage for: {table_name} ===")
    
    # Note: Lineage API may vary by Databricks version
    # This is a conceptual example
    print(f"\nQuerying lineage information...")
    print(f"  Table: {table_name}")
    
    try:
        # In Databricks, you can query lineage using SQL or API
        # Example SQL approach:
        lineage_sql = f"""
        -- Lineage information is available through system tables
        -- This is a conceptual example - actual implementation depends on Databricks version
        SELECT * FROM system.information_schema.table_lineage
        WHERE table_name = '{table_name}'
        """
        
        print(f"\nSQL Query (conceptual):")
        print(f"  {lineage_sql}")
        print(f"\n✓ Lineage information retrieved")
        print(f"\nNote: Use Databricks UI or Lineage API for detailed lineage graphs")
        
        return None
    except Exception as e:
        print(f"Error getting lineage: {str(e)}")
        return None

def track_data_flow(source_tables: list, target_table: str):
    """
    Documents the data flow from source tables to target table.
    Useful for documenting pipeline lineage.
    
    Args:
        source_tables: List of source table names
        target_table: Target table name
    """
    print(f"=== Tracking Data Flow ===")
    print(f"\nSources:")
    for i, source in enumerate(source_tables, 1):
        print(f"  {i}. {source}")
    
    print(f"\nTarget:")
    print(f"  → {target_table}")
    
    print(f"\nData Flow:")
    print(f"  {' + '.join(source_tables)} → {target_table}")
    
    # In practice, you would:
    # 1. Document this in metadata
    # 2. Use Databricks Lineage API
    # 3. Track in a lineage catalog
    
    return {
        "sources": source_tables,
        "target": target_table,
        "flow": f"{' + '.join(source_tables)} → {target_table}"
    }

def get_downstream_dependencies(table_name: str):
    """
    Gets all tables/views that depend on this table (downstream).
    """
    print(f"=== Downstream Dependencies for: {table_name} ===")
    
    print(f"\nFinding tables that use: {table_name}")
    print(f"\nNote: Use Databricks Lineage API or UI to get actual dependencies")
    print(f"  - Check 'Used By' section in table details")
    print(f"  - Query system tables for dependency information")
    
    return []

def get_upstream_sources(table_name: str):
    """
    Gets all tables/views that this table depends on (upstream).
    """
    print(f"=== Upstream Sources for: {table_name} ===")
    
    print(f"\nFinding source tables for: {table_name}")
    print(f"\nNote: Use Databricks Lineage API or UI to get actual sources")
    print(f"  - Check 'Dependencies' section in table details")
    print(f"  - Query system tables for source information")
    
    return []

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Data Lineage Practice ===\n")
    
    catalog = "practice_catalog"
    schema = "analytics"
    table = "customer_analytics"
    
    # Get lineage for a table
    get_table_lineage(f"{catalog}.{schema}.{table}")
    
    # Track data flow
    sources = [
        f"{catalog}.silver.customers",
        f"{catalog}.silver.orders"
    ]
    track_data_flow(sources, f"{catalog}.{schema}.{table}")
    
    print("\n=== Practice Complete ===")
    print("Key points:")
    print("- Lineage shows data flow through your pipelines")
    print("- Track upstream sources and downstream consumers")
    print("- Use Databricks UI or Lineage API for detailed graphs")
    print("- Document data flows for governance and debugging")
