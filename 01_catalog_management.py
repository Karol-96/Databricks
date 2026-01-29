"""
Unity Catalog - Catalog Management Practice
This script demonstrates how to create, manage, and work with catalogs in Unity Catalog.
Catalogs are the top-level container for organizing data assets.
"""

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo

# Initialize the workspace client - this connects to your Databricks workspace
# Make sure you have proper authentication configured (token or OAuth)
workspace = WorkspaceClient()

def create_catalog(catalog_name: str, comment: str = None, provider: str = "LOCAL"):
    """
    Creates a new catalog in Unity Catalog.
    
    Args:
        catalog_name: Name of the catalog to create
        comment: Optional description for the catalog
        provider: Storage provider - "LOCAL" for managed storage, or external provider name
    
    Returns:
        CatalogInfo object if successful
    """
    try:
        # Create catalog with basic configuration
        # Using LOCAL provider means Databricks manages the storage location
        catalog = workspace.catalogs.create(
            name=catalog_name,
            comment=comment or f"Catalog for {catalog_name} data assets",
            provider=provider
        )
        print(f"✓ Successfully created catalog: {catalog_name}")
        return catalog
    except Exception as e:
        # Handle case where catalog might already exist
        print(f"Error creating catalog {catalog_name}: {str(e)}")
        return None

def list_all_catalogs():
    """
    Lists all catalogs accessible in the workspace.
    This includes both user-created catalogs and system catalogs.
    """
    print("\n=== Available Catalogs ===")
    catalogs = workspace.catalogs.list()
    
    for catalog in catalogs:
        # Print catalog details - some might be system catalogs (like 'hive_metastore')
        print(f"  - {catalog.name}")
        if hasattr(catalog, 'comment') and catalog.comment:
            print(f"    Description: {catalog.comment}")
        if hasattr(catalog, 'provider'):
            print(f"    Provider: {catalog.provider}")
    
    return catalogs

def get_catalog_details(catalog_name: str):
    """
    Retrieves detailed information about a specific catalog.
    Useful for checking permissions, storage location, and metadata.
    """
    try:
        catalog = workspace.catalogs.get(catalog_name)
        print(f"\n=== Catalog Details: {catalog_name} ===")
        print(f"Name: {catalog.name}")
        print(f"Provider: {catalog.provider}")
        if hasattr(catalog, 'comment'):
            print(f"Comment: {catalog.comment}")
        if hasattr(catalog, 'storage_root'):
            print(f"Storage Root: {catalog.storage_root}")
        return catalog
    except Exception as e:
        print(f"Error retrieving catalog {catalog_name}: {str(e)}")
        return None

def update_catalog(catalog_name: str, new_comment: str = None, owner: str = None):
    """
    Updates catalog properties like description or ownership.
    Note: You need proper permissions to update catalogs.
    """
    try:
        update_request = {}
        if new_comment:
            update_request['comment'] = new_comment
        if owner:
            update_request['owner'] = owner
        
        if update_request:
            workspace.catalogs.update(catalog_name, **update_request)
            print(f"✓ Successfully updated catalog: {catalog_name}")
        else:
            print("No updates specified")
    except Exception as e:
        print(f"Error updating catalog {catalog_name}: {str(e)}")

def delete_catalog(catalog_name: str, force: bool = False):
    """
    Deletes a catalog. Use with caution!
    
    Args:
        catalog_name: Name of catalog to delete
        force: If True, deletes even if catalog contains schemas/tables
    """
    try:
        workspace.catalogs.delete(catalog_name, force=force)
        print(f"✓ Successfully deleted catalog: {catalog_name}")
    except Exception as e:
        print(f"Error deleting catalog {catalog_name}: {str(e)}")
        print("Note: Make sure catalog is empty or use force=True")

# Example usage - practicing catalog operations
if __name__ == "__main__":
    print("=== Unity Catalog - Catalog Management Practice ===\n")
    
    # Create a practice catalog
    practice_catalog = "practice_catalog"
    create_catalog(practice_catalog, comment="Practice catalog for learning Unity Catalog")
    
    # List all available catalogs
    list_all_catalogs()
    
    # Get details of our practice catalog
    get_catalog_details(practice_catalog)
    
    # Update the catalog description
    update_catalog(practice_catalog, new_comment="Updated: Practice catalog for Unity Catalog features")
    
    print("\n=== Practice Complete ===")
    print("Note: In production, be careful with catalog deletion!")
    print("The catalog can be deleted later if needed using delete_catalog()")

