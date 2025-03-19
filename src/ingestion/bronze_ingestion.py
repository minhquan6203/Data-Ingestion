"""
Bronze layer ingestion module
"""
from loguru import logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
import os

from src.config.config import BRONZE_BUCKET
from src.utils.spark_utils import create_spark_session, read_csv_to_dataframe, write_dataframe_to_parquet
from src.utils.storage_utils import upload_file_to_minio
from src.audit.audit import ETLAuditor
from src.notification.notification import ETLNotifier


def ingest_to_bronze(source_config, audit=True, notify=True):
    """
    Ingest data from source to bronze layer
    
    Args:
        source_config: Source configuration from metadata
        audit: Whether to create audit records
        notify: Whether to send notifications
        
    Returns:
        DataFrame containing the ingested data
        Number of records processed
    """
    source_type = source_config.get("source_type")
    source_path = source_config.get("source_path")
    schema_dict = source_config.get("schema", {})
    bronze_table = source_config.get("bronze_table")
    
    logger.info(f"Ingesting {source_type} data from {source_path} to bronze layer")
    
    # Create SparkSession
    spark = create_spark_session()
    
    # Set up auditing if enabled
    auditor = None
    if audit:
        auditor = ETLAuditor(
            pipeline_id=f"bronze_{bronze_table}",
            source_name=source_path,
            destination_name=f"bronze.{bronze_table}"
        )
        
        if notify:
            ETLNotifier.notify_pipeline_start(
                pipeline_id=f"bronze_{bronze_table}",
                source_name=source_path,
                destination_name=f"bronze.{bronze_table}"
            )
    
    try:
        # Create Spark schema from schema_dict
        spark_schema = None
        if schema_dict:
            spark_schema = create_spark_schema(schema_dict)
        
        # Read data from source
        if source_type == "csv":
            df = read_csv_to_dataframe(spark, source_path, schema=spark_schema)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Get number of records
        count = df.count()
        logger.info(f"Read {count} records from {source_path}")
        
        if auditor:
            auditor.set_records_processed(count)
        
        # Write to bronze layer
        bronze_path = f"s3a://{BRONZE_BUCKET}/{bronze_table}"
        write_dataframe_to_parquet(df, bronze_path)
        
        # Upload source file to MinIO if it's a local file
        if os.path.isfile(source_path):
            try:
                upload_file_to_minio(source_path, BRONZE_BUCKET, f"raw/{os.path.basename(source_path)}")
            except Exception as e:
                logger.warning(f"Failed to upload source file to MinIO: {e}")
        
        # Update audit status
        if auditor:
            auditor.complete_successfully(count)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_success(
                    pipeline_id=f"bronze_{bronze_table}",
                    source_name=source_path,
                    destination_name=f"bronze.{bronze_table}",
                    records_processed=count,
                    duration_seconds=duration
                )
        
        return df, count
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error ingesting data to bronze layer: {error_msg}")
        
        if auditor:
            auditor.complete_with_error(error_msg)
            
            if notify:
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_failure(
                    pipeline_id=f"bronze_{bronze_table}",
                    source_name=source_path,
                    destination_name=f"bronze.{bronze_table}",
                    error_message=error_msg,
                    duration_seconds=duration
                )
        
        raise


def create_spark_schema(schema_dict):
    """
    Create a Spark schema from a schema dictionary
    
    Args:
        schema_dict: Dictionary mapping column names to data types
        
    Returns:
        Spark StructType schema
    """
    type_mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": IntegerType(),  # Using IntegerType for simplicity
        "double": DoubleType(),
        "float": DoubleType(),  # Using DoubleType for simplicity
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType()
    }
    
    fields = []
    for column_name, data_type in schema_dict.items():
        spark_type = type_mapping.get(data_type.lower(), StringType())
        fields.append(StructField(column_name, spark_type, True))
    
    return StructType(fields) 