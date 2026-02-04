"""
Configuration and Utilities for Databricks Unity Catalog and Data Processing
This module provides common configuration, error handling, and utility functions.
"""

import logging
from typing import Optional, Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize workspace client
try:
    workspace = WorkspaceClient()
    logger.info("Workspace client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize workspace client: {str(e)}")
    workspace = None

# Default configuration
DEFAULT_CONFIG = {
    "catalog": "main",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold",
    "checkpoint_base_path": "/checkpoints/",
    "retention_days": 7,
    "auto_optimize": True,
    "auto_compact": True
}

def get_config() -> Dict[str, Any]:
    """
    Returns the default configuration dictionary.
    Can be extended to read from environment variables or config files.
    """
    return DEFAULT_CONFIG.copy()

def handle_databricks_error(func):
    """
    Decorator for handling Databricks API errors gracefully.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabricksError as e:
            logger.error(f"Databricks API error in {func.__name__}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
            return None
    return wrapper

def validate_table_name(table_name: str) -> bool:
    """
    Validates that a table name follows Databricks naming conventions.
    
    Args:
        table_name: Table name to validate
    
    Returns:
        True if valid, False otherwise
    """
    if not table_name:
        logger.warning("Table name cannot be empty")
        return False
    
    if len(table_name) > 255:
        logger.warning("Table name exceeds 255 characters")
        return False
    
    # Check for invalid characters
    invalid_chars = [' ', ';', ',', '(', ')', '[', ']', '{', '}', '|', '\\', '/', '?', '*']
    if any(char in table_name for char in invalid_chars):
        logger.warning(f"Table name contains invalid characters: {table_name}")
        return False
    
    return True

def validate_catalog_name(catalog_name: str) -> bool:
    """
    Validates that a catalog name follows Databricks naming conventions.
    """
    if not catalog_name:
        logger.warning("Catalog name cannot be empty")
        return False
    
    if len(catalog_name) > 255:
        logger.warning("Catalog name exceeds 255 characters")
        return False
    
    # Catalog names should be lowercase
    if catalog_name != catalog_name.lower():
        logger.warning(f"Catalog name should be lowercase: {catalog_name}")
        return False
    
    return True

def format_table_path(catalog: str, schema: str, table: str) -> str:
    """
    Formats a full table path from components.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
    
    Returns:
        Formatted table path (catalog.schema.table)
    """
    return f"{catalog}.{schema}.{table}"

def parse_table_path(full_path: str) -> Dict[str, str]:
    """
    Parses a full table path into components.
    
    Args:
        full_path: Full table path (catalog.schema.table)
    
    Returns:
        Dictionary with catalog, schema, and table keys
    """
    parts = full_path.split('.')
    if len(parts) != 3:
        raise ValueError(f"Invalid table path format: {full_path}. Expected: catalog.schema.table")
    
    return {
        "catalog": parts[0],
        "schema": parts[1],
        "table": parts[2]
    }

def get_checkpoint_path(catalog: str, schema: str, table: str, base_path: Optional[str] = None) -> str:
    """
    Generates a checkpoint path for streaming operations.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        base_path: Base path for checkpoints (optional)
    
    Returns:
        Checkpoint path string
    """
    if base_path is None:
        base_path = DEFAULT_CONFIG["checkpoint_base_path"]
    
    return f"{base_path}{catalog}/{schema}/{table}"

def log_operation(operation: str, details: Optional[Dict[str, Any]] = None):
    """
    Logs an operation with details.
    
    Args:
        operation: Name of the operation
        details: Optional dictionary with operation details
    """
    message = f"Operation: {operation}"
    if details:
        message += f" | Details: {details}"
    logger.info(message)

def format_bytes(bytes_value: int) -> str:
    """
    Formats bytes into human-readable format.
    
    Args:
        bytes_value: Size in bytes
    
    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"

# Example usage
if __name__ == "__main__":
    print("=== Configuration and Utilities Module ===\n")
    
    # Test configuration
    config = get_config()
    print("Default Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    # Test validation
    print("\nValidation Tests:")
    print(f"  Valid table name 'customers': {validate_table_name('customers')}")
    print(f"  Invalid table name 'customers table': {validate_table_name('customers table')}")
    print(f"  Valid catalog name 'main': {validate_catalog_name('main')}")
    
    # Test path formatting
    print("\nPath Formatting:")
    path = format_table_path("main", "bronze", "customers")
    print(f"  Formatted path: {path}")
    parsed = parse_table_path(path)
    print(f"  Parsed components: {parsed}")
    
    # Test checkpoint path
    checkpoint = get_checkpoint_path("main", "bronze", "customers")
    print(f"\nCheckpoint Path: {checkpoint}")
    
    print("\n=== Module Ready ===")
