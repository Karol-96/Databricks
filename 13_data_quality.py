"""
Data Quality Checks and Validation
This module provides data quality functions for validating data at each medallion layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, avg, min as spark_min, max as spark_max
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

def check_null_percentage(df: DataFrame, column_name: str) -> float:
    """
    Calculates the percentage of null values in a column.
    
    Args:
        df: Spark DataFrame
        column_name: Name of the column to check
    
    Returns:
        Percentage of null values (0.0 to 100.0)
    """
    total_count = df.count()
    if total_count == 0:
        return 0.0
    
    null_count = df.filter(col(column_name).isNull()).count()
    null_percentage = (null_count / total_count) * 100.0
    
    logger.info(f"Column '{column_name}': {null_percentage:.2f}% null values")
    return null_percentage

def check_duplicates(df: DataFrame, key_columns: List[str]) -> int:
    """
    Checks for duplicate records based on key columns.
    
    Args:
        df: Spark DataFrame
        key_columns: List of column names that should be unique
    
    Returns:
        Number of duplicate records
    """
    total_count = df.count()
    unique_count = df.select(key_columns).distinct().count()
    duplicates = total_count - unique_count
    
    logger.info(f"Duplicate records based on {key_columns}: {duplicates}")
    return duplicates

def check_data_range(df: DataFrame, column_name: str, min_value: Optional[float] = None, 
                     max_value: Optional[float] = None) -> Dict[str, any]:
    """
    Checks if values in a numeric column are within expected range.
    
    Args:
        df: Spark DataFrame
        column_name: Name of the column to check
        min_value: Minimum expected value (optional)
        max_value: Maximum expected value (optional)
    
    Returns:
        Dictionary with range statistics and violations
    """
    stats = df.select(
        spark_min(col(column_name)).alias("min"),
        spark_max(col(column_name)).alias("max"),
        avg(col(column_name)).alias("avg")
    ).collect()[0]
    
    result = {
        "min": stats["min"],
        "max": stats["max"],
        "avg": stats["avg"],
        "violations": 0
    }
    
    if min_value is not None:
        violations = df.filter(col(column_name) < min_value).count()
        result["violations"] += violations
        result["min_violations"] = violations
        logger.info(f"Values below minimum ({min_value}): {violations}")
    
    if max_value is not None:
        violations = df.filter(col(column_name) > max_value).count()
        result["violations"] += violations
        result["max_violations"] = violations
        logger.info(f"Values above maximum ({max_value}): {violations}")
    
    return result

def check_data_quality_score(df: DataFrame, rules: Dict[str, Dict]) -> float:
    """
    Calculates an overall data quality score based on multiple rules.
    
    Args:
        df: Spark DataFrame
        rules: Dictionary of quality rules to check
    
    Returns:
        Data quality score (0.0 to 1.0)
    """
    total_rules = len(rules)
    passed_rules = 0
    
    for rule_name, rule_config in rules.items():
        rule_type = rule_config.get("type")
        
        if rule_type == "null_check":
            column = rule_config["column"]
            max_null_pct = rule_config.get("max_null_percentage", 10.0)
            null_pct = check_null_percentage(df, column)
            if null_pct <= max_null_pct:
                passed_rules += 1
                logger.info(f"Rule '{rule_name}' passed: {null_pct:.2f}% nulls (max: {max_null_pct}%)")
            else:
                logger.warning(f"Rule '{rule_name}' failed: {null_pct:.2f}% nulls (max: {max_null_pct}%)")
        
        elif rule_type == "duplicate_check":
            key_columns = rule_config["key_columns"]
            max_duplicates = rule_config.get("max_duplicates", 0)
            duplicates = check_duplicates(df, key_columns)
            if duplicates <= max_duplicates:
                passed_rules += 1
                logger.info(f"Rule '{rule_name}' passed: {duplicates} duplicates (max: {max_duplicates})")
            else:
                logger.warning(f"Rule '{rule_name}' failed: {duplicates} duplicates (max: {max_duplicates})")
        
        elif rule_type == "range_check":
            column = rule_config["column"]
            min_val = rule_config.get("min_value")
            max_val = rule_config.get("max_value")
            range_result = check_data_range(df, column, min_val, max_val)
            if range_result["violations"] == 0:
                passed_rules += 1
                logger.info(f"Rule '{rule_name}' passed: all values in range")
            else:
                logger.warning(f"Rule '{rule_name}' failed: {range_result['violations']} violations")
    
    quality_score = passed_rules / total_rules if total_rules > 0 else 0.0
    logger.info(f"Overall data quality score: {quality_score:.2%} ({passed_rules}/{total_rules} rules passed)")
    
    return quality_score

def validate_bronze_data(df: DataFrame) -> Dict[str, any]:
    """
    Validates data quality for bronze layer.
    Bronze layer checks are minimal - mainly ensuring data was ingested.
    
    Args:
        df: Spark DataFrame from bronze layer
    
    Returns:
        Dictionary with validation results
    """
    logger.info("=== Validating Bronze Layer Data ===")
    
    total_records = df.count()
    has_data = total_records > 0
    
    result = {
        "layer": "bronze",
        "total_records": total_records,
        "has_data": has_data,
        "quality_score": 1.0 if has_data else 0.0,
        "checks": {
            "data_ingested": has_data,
            "record_count": total_records
        }
    }
    
    logger.info(f"Bronze validation: {total_records} records ingested")
    return result

def validate_silver_data(df: DataFrame, key_columns: List[str]) -> Dict[str, any]:
    """
    Validates data quality for silver layer.
    Silver layer checks include null checks, duplicates, and basic validations.
    
    Args:
        df: Spark DataFrame from silver layer
        key_columns: Columns that should be unique
    
    Returns:
        Dictionary with validation results
    """
    logger.info("=== Validating Silver Layer Data ===")
    
    total_records = df.count()
    
    # Define quality rules
    rules = {}
    
    # Add null checks for key columns
    for col_name in key_columns:
        rules[f"null_check_{col_name}"] = {
            "type": "null_check",
            "column": col_name,
            "max_null_percentage": 0.0  # Key columns should not have nulls
        }
    
    # Add duplicate check
    rules["duplicate_check"] = {
        "type": "duplicate_check",
        "key_columns": key_columns,
        "max_duplicates": 0
    }
    
    quality_score = check_data_quality_score(df, rules)
    duplicates = check_duplicates(df, key_columns)
    
    result = {
        "layer": "silver",
        "total_records": total_records,
        "quality_score": quality_score,
        "duplicates": duplicates,
        "checks": {
            "no_duplicates": duplicates == 0,
            "no_null_keys": all(check_null_percentage(df, col) == 0.0 for col in key_columns)
        }
    }
    
    logger.info(f"Silver validation: Quality score = {quality_score:.2%}")
    return result

def validate_gold_data(df: DataFrame) -> Dict[str, any]:
    """
    Validates data quality for gold layer.
    Gold layer checks include business logic validation and aggregation accuracy.
    
    Args:
        df: Spark DataFrame from gold layer
    
    Returns:
        Dictionary with validation results
    """
    logger.info("=== Validating Gold Layer Data ===")
    
    total_records = df.count()
    
    # Check for negative values in metrics (if applicable)
    negative_revenue = 0
    if "total_revenue" in df.columns:
        negative_revenue = df.filter(col("total_revenue") < 0).count()
    
    negative_orders = 0
    if "total_orders" in df.columns:
        negative_orders = df.filter(col("total_orders") < 0).count()
    
    result = {
        "layer": "gold",
        "total_records": total_records,
        "quality_score": 1.0 if (negative_revenue == 0 and negative_orders == 0) else 0.8,
        "checks": {
            "no_negative_revenue": negative_revenue == 0,
            "no_negative_orders": negative_orders == 0,
            "has_aggregations": total_records > 0
        },
        "violations": {
            "negative_revenue": negative_revenue,
            "negative_orders": negative_orders
        }
    }
    
    logger.info(f"Gold validation: Quality score = {result['quality_score']:.2%}")
    return result

def generate_quality_report(df: DataFrame, layer: str, key_columns: Optional[List[str]] = None) -> Dict[str, any]:
    """
    Generates a comprehensive data quality report for a DataFrame.
    
    Args:
        df: Spark DataFrame to analyze
        layer: Layer name (bronze, silver, gold)
        key_columns: Key columns for duplicate checking (optional)
    
    Returns:
        Dictionary with comprehensive quality report
    """
    logger.info(f"=== Generating Quality Report for {layer} Layer ===")
    
    total_records = df.count()
    total_columns = len(df.columns)
    
    # Column-level statistics
    column_stats = {}
    for col_name in df.columns:
        null_pct = check_null_percentage(df, col_name)
        column_stats[col_name] = {
            "null_percentage": null_pct,
            "has_nulls": null_pct > 0
        }
    
    # Duplicate check if key columns provided
    duplicates = 0
    if key_columns:
        duplicates = check_duplicates(df, key_columns)
    
    report = {
        "layer": layer,
        "summary": {
            "total_records": total_records,
            "total_columns": total_columns,
            "duplicates": duplicates
        },
        "column_statistics": column_stats,
        "recommendations": []
    }
    
    # Generate recommendations
    if duplicates > 0:
        report["recommendations"].append(f"Found {duplicates} duplicate records. Consider deduplication.")
    
    high_null_cols = [col for col, stats in column_stats.items() if stats["null_percentage"] > 50]
    if high_null_cols:
        report["recommendations"].append(f"Columns with >50% nulls: {', '.join(high_null_cols)}")
    
    logger.info(f"Quality report generated: {total_records} records, {total_columns} columns")
    return report

# Example usage
if __name__ == "__main__":
    print("=== Data Quality Checks Module ===\n")
    print("This module provides:")
    print("  - Null value checks")
    print("  - Duplicate detection")
    print("  - Range validation")
    print("  - Layer-specific validation (bronze, silver, gold)")
    print("  - Quality score calculation")
    print("  - Comprehensive quality reports")
    print("\n=== Module Ready ===")
