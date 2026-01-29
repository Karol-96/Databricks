"""
Unity Catalog - Schema Management Practice
Schemas (also called databases) are containers within catalogs that organize tables and views.
This script shows how to create, manage, and organize schemas.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SchemaInfo

workspace = WorkspaceClient()

def create_schema(catalog_name: str, schema_name: str, comment: str = None):
    """
    Creates a new schema within a catalog.
    Think of schemas as databases - they organize related tables together.
    
    Args:
        catalog_name: The catalog where schema will be created
        schema_name: Name of the schema to create
        comment: Optional description
    
    Returns:
        SchemaInfo object if successful
    """
    try:
        schema = workspace.schemas.create(
            name=schema_name,
            catalog_name=catalog_name,
            comment=comment or f"Schema for {schema_name} data"
        )
        print(f"✓ Created schema '{schema_name}' in catalog '{catalog_name}'")
        return schema
    except Exception as e:
        print(f"Error creating schema {schema_name}: {str(e)}")
        return None

def list_schemas(catalog_name: str):
    """
    Lists all schemas in a given catalog.
    Useful for exploring the structure of your data organization.
    """
    print(f"\n=== Schemas in Catalog: {catalog_name} ===")
    try:
        schemas = workspace.schemas.list(catalog_name=catalog_name)
        
        if not schemas:
            print("  No schemas found in this catalog")
            return []
        
        for schema in schemas:
            print(f"  - {schema.name}")
            if hasattr(schema, 'comment') and schema.comment:
                print(f"    Description: {schema.comment}")
            if hasattr(schema, 'owner'):
                print(f"    Owner: {schema.owner}")
        
        return schemas
    except Exception as e:
        print(f"Error listing schemas: {str(e)}")
        return []

def get_schema_details(catalog_name: str, schema_name: str):
    """
    Gets detailed information about a specific schema.
    Shows metadata, ownership, and properties.
    """
    try:
        schema = workspace.schemas.get(
            full_name=f"{catalog_name}.{schema_name}"
        )
        print(f"\n=== Schema Details: {catalog_name}.{schema_name} ===")
        print(f"Full Name: {schema.full_name}")
        if hasattr(schema, 'comment'):
            print(f"Comment: {schema.comment}")
        if hasattr(schema, 'owner'):
            print(f"Owner: {schema.owner}")
        if hasattr(schema, 'properties'):
            print(f"Properties: {schema.properties}")
        return schema
    except Exception as e:
        print(f"Error retrieving schema: {str(e)}")
        return None

def update_schema(catalog_name: str, schema_name: str, new_comment: str = None, owner: str = None):
    """
    Updates schema properties.
    Common use case: updating description or transferring ownership.
    """
    try:
        update_request = {}
        if new_comment:
            update_request['comment'] = new_comment
        if owner:
            update_request['owner'] = owner
        
        if update_request:
            workspace.schemas.update(
                full_name=f"{catalog_name}.{schema_name}",
                **update_request
            )
            print(f"✓ Updated schema {catalog_name}.{schema_name}")
        else:
            print("No updates specified")
    except Exception as e:
        print(f"Error updating schema: {str(e)}")

def delete_schema(catalog_name: str, schema_name: str, force: bool = False):
    """
    Deletes a schema. Be careful - this removes the container and all its contents!
    
    Args:
        catalog_name: Catalog containing the schema
        schema_name: Schema to delete
        force: If True, deletes even if schema contains tables/views
    """
    try:
        workspace.schemas.delete(
            full_name=f"{catalog_name}.{schema_name}",
            force=force
        )
        print(f"✓ Deleted schema {catalog_name}.{schema_name}")
    except Exception as e:
        print(f"Error deleting schema: {str(e)}")
        print("Note: Schema must be empty or use force=True")

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Schema Management Practice ===\n")
    
    # Assuming we have a catalog called 'practice_catalog'
    catalog = "practice_catalog"
    
    # Create a few schemas for different purposes
    create_schema(catalog, "raw_data", comment="Schema for raw, unprocessed data")
    create_schema(catalog, "staging", comment="Schema for staging/transformed data")
    create_schema(catalog, "analytics", comment="Schema for analytics-ready data")
    
    # List all schemas in the catalog
    list_schemas(catalog)
    
    # Get details of a specific schema
    get_schema_details(catalog, "raw_data")
    
    # Update schema description
    update_schema(catalog, "raw_data", new_comment="Updated: Raw data landing zone")
    
    print("\n=== Practice Complete ===")
    print("Schemas help organize tables logically within catalogs")

