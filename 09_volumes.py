"""
Unity Catalog - Volumes Management Practice
Volumes are Unity Catalog objects that represent cloud storage locations for unstructured data.
This script demonstrates creating, managing, and working with volumes in Unity Catalog.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeInfo, VolumeType
import logging

logger = logging.getLogger(__name__)
workspace = WorkspaceClient()

def create_volume(catalog_name: str, schema_name: str, volume_name: str, 
                 volume_type: str = "MANAGED", comment: str = None):
    """
    Creates a volume in Unity Catalog.
    Volumes are used for storing unstructured data like images, documents, videos, etc.
    
    Args:
        catalog_name: Catalog where volume will be created
        schema_name: Schema (database) where volume will be created
        volume_name: Name of the volume
        volume_type: "MANAGED" (Databricks manages storage) or "EXTERNAL" (your storage)
        comment: Optional description
    
    Returns:
        VolumeInfo object if successful
    """
    try:
        volume = workspace.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.MANAGED if volume_type == "MANAGED" else VolumeType.EXTERNAL,
            comment=comment or f"Volume for {volume_name} unstructured data"
        )
        print(f"✓ Created volume: {catalog_name}.{schema_name}.{volume_name}")
        print(f"  Type: {volume_type}")
        if hasattr(volume, 'storage_location'):
            print(f"  Storage Location: {volume.storage_location}")
        return volume
    except Exception as e:
        print(f"Error creating volume: {str(e)}")
        return None

def create_external_volume(catalog_name: str, schema_name: str, volume_name: str,
                           storage_location: str, comment: str = None):
    """
    Creates an external volume pointing to existing cloud storage.
    
    Args:
        catalog_name: Catalog name
        schema_name: Schema name
        volume_name: Volume name
        storage_location: External storage path (s3://, abfss://, gs://)
        comment: Optional description
    """
    try:
        volume = workspace.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            volume_type=VolumeType.EXTERNAL,
            storage_location=storage_location,
            comment=comment or f"External volume: {volume_name}"
        )
        print(f"✓ Created external volume: {catalog_name}.{schema_name}.{volume_name}")
        print(f"  Storage Location: {storage_location}")
        return volume
    except Exception as e:
        print(f"Error creating external volume: {str(e)}")
        return None

def list_volumes(catalog_name: str, schema_name: str = None):
    """
    Lists all volumes in a catalog or schema.
    
    Args:
        catalog_name: Catalog name
        schema_name: Optional schema name to filter
    """
    print(f"\n=== Volumes in {catalog_name}" + (f".{schema_name}" if schema_name else "") + " ===")
    try:
        volumes = workspace.volumes.list(
            catalog_name=catalog_name,
            schema_name=schema_name
        )
        
        if not volumes:
            print("  No volumes found")
            return []
        
        for volume in volumes:
            print(f"  - {volume.name}")
            if hasattr(volume, 'volume_type'):
                print(f"    Type: {volume.volume_type}")
            if hasattr(volume, 'comment') and volume.comment:
                print(f"    Description: {volume.comment}")
            if hasattr(volume, 'storage_location'):
                print(f"    Location: {volume.storage_location}")
        
        return volumes
    except Exception as e:
        print(f"Error listing volumes: {str(e)}")
        return []

def get_volume_details(catalog_name: str, schema_name: str, volume_name: str):
    """
    Gets detailed information about a specific volume.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{volume_name}"
        volume = workspace.volumes.get(full_name)
        
        print(f"\n=== Volume Details: {full_name} ===")
        print(f"Name: {volume.name}")
        if hasattr(volume, 'volume_type'):
            print(f"Type: {volume.volume_type}")
        if hasattr(volume, 'storage_location'):
            print(f"Storage Location: {volume.storage_location}")
        if hasattr(volume, 'comment'):
            print(f"Comment: {volume.comment}")
        if hasattr(volume, 'owner'):
            print(f"Owner: {volume.owner}")
        
        return volume
    except Exception as e:
        print(f"Error retrieving volume: {str(e)}")
        return None

def update_volume(catalog_name: str, schema_name: str, volume_name: str,
                 new_comment: str = None, owner: str = None):
    """
    Updates volume properties like description or ownership.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{volume_name}"
        update_request = {}
        
        if new_comment:
            update_request['comment'] = new_comment
        if owner:
            update_request['owner'] = owner
        
        if update_request:
            workspace.volumes.update(full_name, **update_request)
            print(f"✓ Updated volume: {full_name}")
        else:
            print("No updates specified")
    except Exception as e:
        print(f"Error updating volume: {str(e)}")

def delete_volume(catalog_name: str, schema_name: str, volume_name: str):
    """
    Deletes a volume. Use with caution!
    For managed volumes, this also deletes the data.
    For external volumes, only metadata is removed.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{volume_name}"
        workspace.volumes.delete(full_name)
        print(f"✓ Deleted volume: {full_name}")
    except Exception as e:
        print(f"Error deleting volume: {str(e)}")

def grant_volume_permission(catalog_name: str, schema_name: str, volume_name: str,
                            principal: str, privileges: list):
    """
    Grants permissions on a volume to a user or group.
    
    Common volume privileges:
    - READ VOLUME: Can read files from the volume
    - WRITE VOLUME: Can write files to the volume
    - CREATE EXTERNAL TABLE: Can create external tables from volume data
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{volume_name}"
        from databricks.sdk.service.catalog import Privilege, SecurableType
        
        for privilege in privileges:
            workspace.grants.update(
                securable_type=SecurableType.VOLUME,
                full_name=full_name,
                principal=principal,
                privileges=[Privilege(privilege)]
            )
        print(f"✓ Granted {privileges} on volume '{full_name}' to {principal}")
    except Exception as e:
        print(f"Error granting volume permission: {str(e)}")

def list_volume_permissions(catalog_name: str, schema_name: str, volume_name: str):
    """
    Lists all permissions granted on a volume.
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{volume_name}"
        from databricks.sdk.service.catalog import SecurableType
        
        grants = workspace.grants.get(
            securable_type=SecurableType.VOLUME,
            full_name=full_name
        )
        
        print(f"\n=== Permissions on Volume: {full_name} ===")
        if hasattr(grants, 'privilege_assignments') and grants.privilege_assignments:
            for assignment in grants.privilege_assignments:
                print(f"  Principal: {assignment.principal}")
                print(f"  Privileges: {[p.privilege for p in assignment.privileges]}")
                print()
        else:
            print("  No permissions found")
        
        return grants
    except Exception as e:
        print(f"Error listing permissions: {str(e)}")
        return None

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Volumes Management Practice ===\n")
    
    catalog = "practice_catalog"
    schema = "unstructured_data"
    
    # Create a managed volume for storing unstructured data
    create_volume(
        catalog, schema, "documents",
        volume_type="MANAGED",
        comment="Volume for document storage (PDFs, Word docs, etc.)"
    )
    
    # Create another volume for images
    create_volume(
        catalog, schema, "images",
        volume_type="MANAGED",
        comment="Volume for image files (JPG, PNG, etc.)"
    )
    
    # Example: Create external volume (uncomment and adjust path)
    # create_external_volume(
    #     catalog, schema, "external_data",
    #     storage_location="s3://my-bucket/unstructured/",
    #     comment="External volume pointing to S3"
    # )
    
    # List all volumes in the schema
    list_volumes(catalog, schema)
    
    # Get details of a specific volume
    get_volume_details(catalog, schema, "documents")
    
    # Update volume description
    update_volume(catalog, schema, "documents", 
                 new_comment="Updated: Document storage volume for all file types")
    
    # Example: Grant permissions (uncomment and adjust)
    # grant_volume_permission(
    #     catalog, schema, "documents",
    #     "data-engineers@company.com",
    #     ["READ VOLUME", "WRITE VOLUME"]
    # )
    
    # List permissions
    # list_volume_permissions(catalog, schema, "documents")
    
    print("\n=== Practice Complete ===")
    print("Key points:")
    print("- Volumes are for unstructured data (files, images, documents)")
    print("- Managed volumes: Databricks manages storage")
    print("- External volumes: Point to your own cloud storage")
    print("- Use volumes for ML model artifacts, raw files, etc.")
    print("- Volumes support fine-grained access control")
