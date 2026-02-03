"""
Unity Catalog - Data Sharing Practice
This script demonstrates how to share data across workspaces and organizations using Unity Catalog.
Data sharing enables secure data collaboration without copying data.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sharing import ShareInfo, RecipientInfo

workspace = WorkspaceClient()

def create_share(share_name: str, comment: str = None):
    """
    Creates a new share for data sharing.
    Shares allow you to securely share data with other workspaces or organizations.
    
    Args:
        share_name: Name of the share to create
        comment: Optional description
    
    Returns:
        ShareInfo object if successful
    """
    try:
        share = workspace.shares.create(
            name=share_name,
            comment=comment or f"Share for {share_name} data"
        )
        print(f"✓ Created share: {share_name}")
        return share
    except Exception as e:
        print(f"Error creating share {share_name}: {str(e)}")
        return None

def add_table_to_share(share_name: str, catalog_name: str, schema_name: str, table_name: str):
    """
    Adds a table to a share.
    The table becomes available to recipients of the share.
    
    Args:
        share_name: Name of the share
        catalog_name: Catalog containing the table
        schema_name: Schema containing the table
        table_name: Table to add to share
    """
    try:
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        workspace.shares.update(
            name=share_name,
            updates=[{
                "action": "ADD",
                "data_object": {
                    "name": full_table_name,
                    "data_object_type": "TABLE"
                }
            }]
        )
        print(f"✓ Added table {full_table_name} to share {share_name}")
    except Exception as e:
        print(f"Error adding table to share: {str(e)}")

def remove_table_from_share(share_name: str, catalog_name: str, schema_name: str, table_name: str):
    """
    Removes a table from a share.
    """
    try:
        full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
        workspace.shares.update(
            name=share_name,
            updates=[{
                "action": "REMOVE",
                "data_object": {
                    "name": full_table_name,
                    "data_object_type": "TABLE"
                }
            }]
        )
        print(f"✓ Removed table {full_table_name} from share {share_name}")
    except Exception as e:
        print(f"Error removing table from share: {str(e)}")

def list_shares():
    """
    Lists all shares in the workspace.
    """
    print("\n=== Available Shares ===")
    try:
        shares = workspace.shares.list()
        for share in shares:
            print(f"  - {share.name}")
            if hasattr(share, 'comment') and share.comment:
                print(f"    Description: {share.comment}")
        return shares
    except Exception as e:
        print(f"Error listing shares: {str(e)}")
        return []

def get_share_details(share_name: str):
    """
    Gets detailed information about a share, including shared objects.
    """
    try:
        share = workspace.shares.get(share_name)
        print(f"\n=== Share Details: {share_name} ===")
        print(f"Name: {share.name}")
        if hasattr(share, 'comment'):
            print(f"Comment: {share.comment}")
        if hasattr(share, 'objects') and share.objects:
            print(f"\nShared Objects ({len(share.objects)}):")
            for obj in share.objects:
                print(f"  - {obj.name} ({obj.data_object_type})")
        return share
    except Exception as e:
        print(f"Error retrieving share: {str(e)}")
        return None

def create_recipient(recipient_name: str, comment: str = None, sharing_id: str = None):
    """
    Creates a recipient for data sharing.
    Recipients can be other Databricks workspaces or external organizations.
    
    Args:
        recipient_name: Name of the recipient
        comment: Optional description
        sharing_id: Sharing identifier (for external recipients)
    """
    try:
        recipient = workspace.recipients.create(
            name=recipient_name,
            comment=comment or f"Recipient: {recipient_name}",
            sharing_id=sharing_id
        )
        print(f"✓ Created recipient: {recipient_name}")
        return recipient
    except Exception as e:
        print(f"Error creating recipient: {str(e)}")
        return None

def grant_share_to_recipient(share_name: str, recipient_name: str):
    """
    Grants access to a share for a recipient.
    This allows the recipient to access the shared data.
    """
    try:
        workspace.shares.update(
            name=share_name,
            updates=[{
                "action": "ADD",
                "recipient": recipient_name
            }]
        )
        print(f"✓ Granted share '{share_name}' to recipient '{recipient_name}'")
    except Exception as e:
        print(f"Error granting share access: {str(e)}")

def revoke_share_from_recipient(share_name: str, recipient_name: str):
    """
    Revokes access to a share from a recipient.
    """
    try:
        workspace.shares.update(
            name=share_name,
            updates=[{
                "action": "REMOVE",
                "recipient": recipient_name
            }]
        )
        print(f"✓ Revoked share '{share_name}' from recipient '{recipient_name}'")
    except Exception as e:
        print(f"Error revoking share access: {str(e)}")

def list_recipients():
    """
    Lists all recipients configured in the workspace.
    """
    print("\n=== Available Recipients ===")
    try:
        recipients = workspace.recipients.list()
        for recipient in recipients:
            print(f"  - {recipient.name}")
            if hasattr(recipient, 'comment') and recipient.comment:
                print(f"    Description: {recipient.comment}")
        return recipients
    except Exception as e:
        print(f"Error listing recipients: {str(e)}")
        return []

def delete_share(share_name: str):
    """
    Deletes a share. Use with caution!
    """
    try:
        workspace.shares.delete(share_name)
        print(f"✓ Deleted share: {share_name}")
    except Exception as e:
        print(f"Error deleting share: {str(e)}")

# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Data Sharing Practice ===\n")
    
    # Create a share
    share_name = "customer_data_share"
    create_share(share_name, comment="Share for customer analytics data")
    
    # Add tables to the share
    catalog = "practice_catalog"
    schema = "analytics"
    add_table_to_share(share_name, catalog, schema, "customers")
    add_table_to_share(share_name, catalog, schema, "transactions")
    
    # List all shares
    list_shares()
    
    # Get share details
    get_share_details(share_name)
    
    # Create a recipient (example - adjust for your environment)
    # recipient_name = "partner_workspace"
    # create_recipient(recipient_name, comment="Partner organization workspace")
    
    # Grant share access to recipient
    # grant_share_to_recipient(share_name, recipient_name)
    
    print("\n=== Practice Complete ===")
    print("Key points:")
    print("- Shares enable secure data collaboration")
    print("- No data copying required - shared directly from source")
    print("- Fine-grained access control at share level")
    print("- Recipients can be Databricks workspaces or external organizations")
