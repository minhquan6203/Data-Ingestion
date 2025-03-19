"""
Silver layer transformation module
"""
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr

from src.config.config import BRONZE_BUCKET, SILVER_BUCKET, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from src.utils.spark_utils import create_spark_session, apply_transformations, write_dataframe_to_parquet
from src.utils.storage_utils import create_table_if_not_exists, load_data_from_minio_to_postgres, get_postgres_connection, execute_sql
from src.audit.audit import ETLAuditor
from src.notification.notification import ETLNotifier


def transform_to_silver(pipeline_config, source_config, audit=True, notify=True):
    """
    Transform data from bronze to silver layer
    
    Args:
        pipeline_config: Pipeline configuration from metadata
        source_config: Source configuration from metadata
        audit: Whether to create audit records
        notify: Whether to send notifications
        
    Returns:
        DataFrame containing the transformed data
        Number of records processed
    """
    bronze_table = source_config.get("bronze_table")
    silver_table = source_config.get("silver_table")
    schema_dict = source_config.get("schema", {})
    primary_key = source_config.get("primary_key")
    transformations = pipeline_config.get("transformations", [])
    quality_checks = pipeline_config.get("quality_checks", [])
    
    logger.info(f"Transforming data from {bronze_table} to {silver_table}")
    
    # Create SparkSession
    spark = create_spark_session()
    
    # Set up auditing if enabled
    auditor = None
    if audit:
        auditor = ETLAuditor(
            pipeline_id=pipeline_config.get("source"),
            source_name=f"bronze.{bronze_table}",
            destination_name=f"silver.{silver_table}"
        )
        
        if notify:
            ETLNotifier.notify_pipeline_start(
                pipeline_id=pipeline_config.get("source"),
                source_name=f"bronze.{bronze_table}",
                destination_name=f"silver.{silver_table}"
            )
    
    try:
        # Read data from bronze layer
        bronze_path = f"s3a://{BRONZE_BUCKET}/{bronze_table}"
        df = spark.read.parquet(bronze_path)
        
        # Apply transformations
        if transformations:
            df = apply_transformations(df, transformations)
        
        # Apply data quality checks
        failed_checks = []
        if quality_checks:
            df, failed_checks = apply_quality_checks(df, quality_checks)
            
            # Report data quality issues
            if failed_checks and notify:
                for check in failed_checks:
                    ETLNotifier.notify_data_quality_issue(
                        pipeline_id=pipeline_config.get("source"),
                        source_name=bronze_table,
                        quality_check=check["message"],
                        failed_records=check["failed_count"]
                    )
        
        # Get record count
        count = df.count()
        logger.info(f"Transformed {count} records from {bronze_table} to {silver_table}")
        
        if auditor:
            auditor.set_records_processed(count)
        
        # Write to silver layer
        silver_path = f"s3a://{SILVER_BUCKET}/{silver_table}"
        write_dataframe_to_parquet(df, silver_path)
        
        # Create or update PostgreSQL table for the silver layer
        create_table_if_not_exists(
            silver_table,
            schema_dict,
            schema_name="silver",
            primary_key=primary_key
        )
        
        # Use JDBC to write data directly to PostgreSQL
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        logger.info(f"Writing data to PostgreSQL via JDBC: {jdbc_url}")
        
        try:
            # Delete existing data
            execute_sql(f"DELETE FROM silver.{silver_table}")
            
            # Write DataFrame to PostgreSQL
            df.write \
               .format("jdbc") \
               .option("url", jdbc_url) \
               .option("dbtable", f"silver.{silver_table}") \
               .option("user", POSTGRES_USER) \
               .option("password", POSTGRES_PASSWORD) \
               .option("driver", "org.postgresql.Driver") \
               .mode("append") \
               .save()
               
            logger.info(f"Successfully wrote data to silver.{silver_table} via JDBC")
        except Exception as e:
            logger.error(f"Error writing data to PostgreSQL via JDBC: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        
        # Update audit status
        if auditor:
            auditor.complete_successfully(count)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_success(
                    pipeline_id=pipeline_config.get("source"),
                    source_name=f"bronze.{bronze_table}",
                    destination_name=f"silver.{silver_table}",
                    records_processed=count,
                    duration_seconds=duration
                )
        
        return df, count
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error transforming data to silver layer: {error_msg}")
        import traceback
        logger.error(traceback.format_exc())
        
        if auditor:
            auditor.complete_with_error(error_msg)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_failure(
                    pipeline_id=pipeline_config.get("source"),
                    source_name=f"bronze.{bronze_table}",
                    destination_name=f"silver.{silver_table}",
                    error_message=error_msg,
                    duration_seconds=duration
                )
        
        raise


def apply_quality_checks(df, quality_checks):
    """
    Apply data quality checks to a DataFrame
    
    Args:
        df: Spark DataFrame
        quality_checks: List of quality check definitions
        
    Returns:
        DataFrame with quality checks applied
        List of failed quality checks
    """
    import traceback
    failed_checks = []
    
    logger.debug(f"Applying quality checks: {quality_checks}")
    
    for check in quality_checks:
        try:
            column = check.get("column")
            rule_type = check.get("rule_type")
            
            logger.debug(f"Processing check: column={column}, rule_type={rule_type}")
            
            if rule_type == "not_null":
                # Count records where column is null
                logger.debug(f"Checking for NULL values in column {column}")
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    message = f"Column '{column}' contains {null_count} NULL values"
                    logger.warning(message)
                    failed_checks.append({
                        "column": column,
                        "rule_type": rule_type,
                        "message": message,
                        "failed_count": null_count
                    })
                    
                    # Filter out records with NULL values
                    df = df.filter(col(column).isNotNull())
                    
            elif rule_type == "greater_than":
                value = check.get("value")
                logger.debug(f"Checking if values in column {column} are greater than {value}")
                # Count records where column is not greater than value
                invalid_count = df.filter(col(column) <= value).count()
                if invalid_count > 0:
                    message = f"Column '{column}' contains {invalid_count} values not greater than {value}"
                    logger.warning(message)
                    failed_checks.append({
                        "column": column,
                        "rule_type": rule_type,
                        "message": message,
                        "failed_count": invalid_count
                    })
                    
                    # Filter out invalid records
                    df = df.filter(col(column) > value)
                    
            elif rule_type == "greater_than_equal":
                value = check.get("value")
                logger.debug(f"Checking if values in column {column} are greater than or equal to {value}")
                # Count records where column is less than value
                invalid_count = df.filter(col(column) < value).count()
                if invalid_count > 0:
                    message = f"Column '{column}' contains {invalid_count} values less than {value}"
                    logger.warning(message)
                    failed_checks.append({
                        "column": column,
                        "rule_type": rule_type,
                        "message": message,
                        "failed_count": invalid_count
                    })
                    
                    # Filter out invalid records
                    df = df.filter(col(column) >= value)
                    
            elif rule_type == "regex":
                pattern = check.get("pattern")
                logger.debug(f"Checking if values in column {column} match pattern {pattern}")
                # Count records where column doesn't match the regex pattern
                try:
                    # Use a simpler approach to avoid Column object issues
                    df_valid = df.filter(col(column).rlike(pattern))
                    invalid_count = df.count() - df_valid.count()
                    
                    if invalid_count > 0:
                        message = f"Column '{column}' contains {invalid_count} values not matching pattern '{pattern}'"
                        logger.warning(message)
                        failed_checks.append({
                            "column": column,
                            "rule_type": rule_type,
                            "message": message,
                            "failed_count": invalid_count
                        })
                        
                        # Keep only valid records
                        df = df_valid
                except Exception as e:
                    logger.error(f"Error applying regex check: {str(e)}")
                    logger.error(traceback.format_exc())
                    # Skip this check if there's an error
                    continue
                    
            elif rule_type == "date_format":
                date_format = check.get("format")
                logger.debug(f"Checking date format for column {column}")
                # This is a simplification; in practice, would need more robust date validation
                # For now, just log that we're checking this
                logger.info(f"Date format check for column {column} with format {date_format} is simplified")
                
        except Exception as e:
            logger.error(f"Error in quality check {check}: {str(e)}")
            logger.error(traceback.format_exc())
            # Continue with the next check if there's an error
            continue
            
    return df, failed_checks 