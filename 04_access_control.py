"""
Unity Catalog - Access Control & Permissions Practice
This script demonstrates how to manage permissions and access control in Unity Catalog.
Unity Catalog provides fine-grained access control at catalog, schema, and table levels.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import Privilege, SecurableType

workspace = WorkspaceClient()

def grant_catalog_permission(catalog_name: str, principal: str, privileges: list):
    """
    Grants permissions on a catalog to a user or group.
    
    Args:
        catalog_name: Name of the catalog
        principal: User email or group name (e.g., "user@example.com" or "data-engineers")
        privileges: List of privileges like ["ALL_PRIVILEGES", "USE CATALOG", "CREATE SCHEMA"]
    
    Common privileges:
        - ALL_PRIVILEGES: Full access
        - USE CATALOG: Can use the catalog
        - CREATE SCHEMA: Can create schemas
    """
    try:
        for privilege in privileges:
            workspace.grants.update(
                securable_type=SecurableType.CATALOG,
                full_name=catalog_name,
                principal=principal,
                privileges=[Privilege(privilege)]
            )
        print(f"✓ Granted {privileges} on catalog '{catalog_name}' to {principal}")
    except Exception as e:
        print(f"Error granting catalog permission: {str(e)}")

def grant_schema_permission(catalog_name: str, schema_name: str, principal: str, privileges: list):
    """
    Grants permissions on a schema to a user or group.
    
    Common schema privileges:
        - ALL_PRIVILEGES: Full access
        - USE SCHEMA: Can use the schema
        - CREATE TABLE: Can create tables
        - CREATE FUNCTION: Can create functions
    """
    try:
        full_name = f"{catalog_name}.{schema_name}"
        for privilege in privileges:
            workspace.grants.update(
                securable_type=SecurableType.SCHEMA,
                full_name=full_name,
                principal=principal,
                privileges=[Privilege(privilege)]
            )
        print(f"✓ Granted {privileges} on schema '{full_name}' to {principal}")
    except Exception as e:
        print(f"Error granting schema permission: {str(e)}")

def grant_table_permission(catalog_name: str, schema_name: str, table_name: str,
                          principal: str, privileges: list):
    """
    Grants permissions on a table to a user or group.
    
    Common table privileges:
        - ALL_PRIVILEGES: Full access
        - SELECT: Can read data
        - MODIFY: Can insert/update/delete data
        - READ FILES: Can read underlying files (for external tables)
    """
    try:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        for privilege in privileges:
            workspace.grants.update(
                securable_type=SecurableType.TABLE,
                full_name=full_name,
                principal=principal,
                privileges=[Privilege(privilege)]
            )
        print(f"✓ Granted {privileges} on table '{full_name}' to {principal}")
    except Exception as e:
        print(f"Error granting table permission: {str(e)}")

def revoke_permission(securable_type: str, full_name: str, principal: str, privileges: list):
    """
    Revokes permissions from a user or group.
    
    Args:
        securable_type: "CATALOG", "SCHEMA", or "TABLE"
        full_name: Full name of the securable (e.g., "catalog.schema.table")
        principal: User or group to revoke from
        privileges: List of privileges to revoke
    """
    try:
        sec_type = getattr(SecurableType, securable_type)
        for privilege in privileges:
            workspace.grants.delete(
                securable_type=sec_type,
                full_name=full_name,
                principal=principal,
                privileges=[Privilege(privilege)]
            )
        print(f"✓ Revoked {privileges} on {securable_type} '{full_name}' from {principal}")
    except Exception as e:
        print(f"Error revoking permission: {str(e)}")

def list_permissions(securable_type: str, full_name: str):
    """
    Lists all permissions granted on a securable (catalog, schema, or table).
    Shows who has what access.
    """
    try:
        sec_type = getattr(SecurableType, securable_type)
        grants = workspace.grants.get(securable_type=sec_type, full_name=full_name)
        
        print(f"\n=== Permissions on {securable_type}: {full_name} ===")
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

def set_owner(securable_type: str, full_name: str, owner: str):
    """
    Sets the owner of a catalog, schema, or table.
    Owners have full control over the object.
    """
    try:
        if securable_type == "CATALOG":
            workspace.catalogs.update(full_name, owner=owner)
        elif securable_type == "SCHEMA":
            workspace.schemas.update(full_name, owner=owner)
        elif securable_type == "TABLE":
            workspace.tables.update(full_name, owner=owner)
        
        print(f"✓ Set owner of {securable_type} '{full_name}' to {owner}")
    except Exception as e:
        print(f"Error setting owner: {str(e)}")

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Access Control Practice ===\n")
    
    catalog = "practice_catalog"
    schema = "analytics"
    table = "customers"
    
    # Example: Grant catalog access to a data engineering group
    # grant_catalog_permission(
    #     catalog,
    #     "data-engineers@company.com",
    #     ["USE CATALOG", "CREATE SCHEMA"]
    # )
    
    # Example: Grant schema access
    # grant_schema_permission(
    #     catalog, schema,
    #     "analysts@company.com",
    #     ["USE SCHEMA", "SELECT"]
    # )
    
    # Example: Grant table read access
    # grant_table_permission(
    #     catalog, schema, table,
    #     "readonly-user@company.com",
    #     ["SELECT"]
    # )
    
    # List permissions on a table
    # list_permissions("TABLE", f"{catalog}.{schema}.{table}")
    
    print("\n=== Practice Complete ===")
    print("Key points:")
    print("- Permissions cascade: catalog → schema → table")
    print("- Use groups for easier management")
    print("- Principle of least privilege: grant only what's needed")
    print("- Owners have full control automatically")

