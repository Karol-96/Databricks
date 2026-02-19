"""
Unity Catalog - Row-Level and Column-Level Security
This module demonstrates how to implement row-level security (RLS)
and basic column masking patterns using Unity Catalog.

Note: Exact syntax and features depend on your Databricks Runtime
and Unity Catalog version. This script focuses on concepts and
example SQL patterns you can adapt.
"""

from databricks.sdk import WorkspaceClient
import logging

logger = logging.getLogger(__name__)
workspace = WorkspaceClient()


def create_rls_view(base_table: str, rls_view: str, predicate: str):
    """
    Creates a row-level security view on top of a base table.

    Common RLS patterns:
    - Filter rows by current_user()
    - Filter by group membership
    - Filter by region / business unit

    Args:
        base_table: Full base table name (catalog.schema.table)
        rls_view: Full view name (catalog.schema.view_name)
        predicate: SQL predicate to enforce (e.g., "region = 'EU'")
    """
    print(f"=== Creating Row-Level Security View ===")
    print(f"Base table : {base_table}")
    print(f"RLS view   : {rls_view}")
    print(f"Predicate  : {predicate}")

    create_view_sql = f"""
    CREATE OR REPLACE VIEW {rls_view} AS
    SELECT *
    FROM {base_table}
    WHERE {predicate}
    """

    print("\nSQL Command:")
    print(create_view_sql)
    print("\nNote: Run this SQL in Databricks (Python cell with spark.sql or SQL cell).")

    return create_view_sql


def rls_by_current_user(base_table: str, rls_view: str, user_column: str = "owner_email"):
    """
    RLS pattern: each user sees only their own rows, based on email/username.

    Assumes the base table has a column (e.g., owner_email) that matches current_user().
    """
    predicate = f"{user_column} = current_user()"
    return create_rls_view(base_table, rls_view, predicate)


def rls_by_region(base_table: str, rls_view: str, region_column: str = "region"):
    """
    RLS pattern: users see only rows for regions they are allowed to access.

    Simple example: map user to region via a CASE expression.
    In production you would typically join to a mapping table of user → region(s).
    """
    predicate = f"""
    {region_column} IN (
      CASE
        WHEN current_user() LIKE '%@emea%' THEN 'EU'
        WHEN current_user() LIKE '%@apac%' THEN 'APAC'
        ELSE 'US'
      END
    )
    """
    return create_rls_view(base_table, rls_view, predicate)


def create_masked_view(base_table: str, masked_view: str, column_masks: dict):
    """
    Creates a column-masked view on top of a base table.

    Args:
        base_table: Full base table name
        masked_view: Full view name
        column_masks: Dict[column_name] = mask_expression
            Example:
              {
                "email": "CASE WHEN is_member('pii_readers') THEN email ELSE '*** MASKED ***' END",
                "phone": "CASE WHEN is_member('pii_readers') THEN phone ELSE NULL END"
              }
    """
    print(f"=== Creating Column-Masked View ===")
    print(f"Base table  : {base_table}")
    print(f"Masked view : {masked_view}")

    # Build SELECT list with masks
    select_expressions = []
    for col_name, mask_expr in column_masks.items():
        select_expressions.append(f"{mask_expr} AS {col_name}")

    # Everything else passes through as-is using '*'
    # In production you may want to list all columns explicitly.
    select_clause = ",\n       ".join(select_expressions)

    create_view_sql = f"""
    CREATE OR REPLACE VIEW {masked_view} AS
    SELECT
       *,
       {select_clause}
    FROM {base_table}
    """

    print("\nSQL Command:")
    print(create_view_sql)
    print("\nNote: This pattern overlays masked columns on top of the base table.")
    print("      Adjust column list if you want stricter control.")

    return create_view_sql


def example_mask_expressions():
    """
    Returns common masking expressions you can use in column_masks.
    """
    return {
        "email_mask": "CASE WHEN is_member('pii_readers') THEN email ELSE '*** MASKED ***' END",
        "phone_mask": "CASE WHEN is_member('pii_readers') THEN phone ELSE NULL END",
        "cc_mask": "CASE WHEN is_member('pci_readers') THEN credit_card ELSE CONCAT('****-****-****-', RIGHT(credit_card, 4)) END"
    }


def grant_rls_view_access(rls_view: str, principal: str, privileges: list = None):
    """
    Grants access to an RLS/masked view, not the base table.

    Args:
        rls_view: Full view name (catalog.schema.view)
        principal: User or group (e.g., 'analysts', 'user@company.com')
        privileges: List of privileges (default: ['SELECT'])
    """
    from databricks.sdk.service.catalog import Privilege, SecurableType

    if privileges is None:
        privileges = ["SELECT"]

    print(f"=== Granting Access to Secure View ===")
    print(f"View      : {rls_view}")
    print(f"Principal : {principal}")
    print(f"Privileges: {privileges}")

    try:
        for privilege in privileges:
            workspace.grants.update(
                securable_type=SecurableType.VIEW,
                full_name=rls_view,
                principal=principal,
                privileges=[Privilege(privilege)]
            )
        print("✓ Access granted")
    except Exception as e:
        print(f"Error granting access: {str(e)}")


# Example usage
if __name__ == "__main__":
    print("=== Unity Catalog - Row-Level & Column-Level Security Practice ===\n")

    catalog = "practice_catalog"
    schema = "analytics"
    base_table = f"{catalog}.{schema}.customers"

    # Example: RLS by current user
    rls_view_user = f"{catalog}.{schema}.customers_rls_user"
    rls_by_current_user(base_table, rls_view_user, user_column="owner_email")

    # Example: RLS by region
    rls_view_region = f"{catalog}.{schema}.customers_rls_region"
    rls_by_region(base_table, rls_view_region, region_column="region")

    # Example: column masking
    column_masks = {
        "email": "CASE WHEN is_member('pii_readers') THEN email ELSE '*** MASKED ***' END",
        "phone": "CASE WHEN is_member('pii_readers') THEN phone ELSE NULL END"
    }
    masked_view = f"{catalog}.{schema}.customers_masked"
    create_masked_view(base_table, masked_view, column_masks)

    print("\nBest Practices:")
    print("- Grant analysts access to RLS/masked views, not raw base tables.")
    print("- Use groups (like 'pii_readers') instead of individual users.")
    print("- Keep RLS logic in views or policies, not in application code.")
    print("- Test policies with different users before production.")

