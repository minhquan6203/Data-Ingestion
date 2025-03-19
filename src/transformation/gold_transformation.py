"""
Gold layer transformation module
"""
from loguru import logger
from pyspark.sql.functions import col

from src.config.config import SILVER_BUCKET, GOLD_BUCKET, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
from src.utils.spark_utils import create_spark_session, write_dataframe_to_parquet
from src.utils.storage_utils import create_table_if_not_exists, execute_sql
from src.audit.audit import ETLAuditor
from src.notification.notification import ETLNotifier


def transform_to_gold(pipeline_config, audit=True, notify=True):
    """
    Transform data from silver to gold layer using SQL
    
    Args:
        pipeline_config: Pipeline configuration from metadata
        audit: Whether to create audit records
        notify: Whether to send notifications
        
    Returns:
        DataFrame containing the transformed data
        Number of records processed
    """
    sources = pipeline_config.get("sources", [])
    destination = pipeline_config.get("destination")
    sql_transform = pipeline_config.get("sql_transform")
    
    if not sql_transform:
        raise ValueError("SQL transform is required for gold layer transformation")
    
    logger.info(f"Transforming data to gold layer: {destination}")
    
    # Create SparkSession
    spark = create_spark_session()
    
    # Set up auditing if enabled
    auditor = None
    if audit:
        source_name = ", ".join(f"silver.{s}" for s in sources)
        
        auditor = ETLAuditor(
            pipeline_id=pipeline_config.get("id", destination),
            source_name=source_name,
            destination_name=f"gold.{destination}"
        )
        
        if notify:
            ETLNotifier.notify_pipeline_start(
                pipeline_id=pipeline_config.get("id", destination),
                source_name=source_name,
                destination_name=f"gold.{destination}"
            )
    
    try:
        # Register each silver table as a temporary view
        for silver_table in sources:
            silver_path = f"s3a://{SILVER_BUCKET}/{silver_table}"
            silver_df = spark.read.format("parquet").load(silver_path)
            silver_df.createOrReplaceTempView(silver_table)
            logger.info(f"Registered silver table as view: {silver_table}")
        
        # Execute the SQL transformation
        logger.info(f"Executing SQL transformation for {destination}")
        result_df = spark.sql(sql_transform)
        
        # Get record count
        count = result_df.count()
        logger.info(f"Transformed {count} records to gold.{destination}")
        
        if auditor:
            auditor.set_records_processed(count)
        
        # Write to gold layer
        gold_path = f"s3a://{GOLD_BUCKET}/{destination}"
        write_dataframe_to_parquet(result_df, gold_path)
        
        # Create or update PostgreSQL table for the gold layer
        # For simplicity, we'll infer the schema from the DataFrame
        schema_dict = {}
        for field in result_df.schema.fields:
            field_type = field.dataType.simpleString()
            # Map Spark types to our simplified type system
            if "string" in field_type.lower():
                schema_dict[field.name] = "string"
            elif "int" in field_type.lower():
                schema_dict[field.name] = "integer"
            elif "double" in field_type.lower() or "float" in field_type.lower():
                schema_dict[field.name] = "double"
            elif "boolean" in field_type.lower():
                schema_dict[field.name] = "boolean"
            elif "date" in field_type.lower():
                schema_dict[field.name] = "date"
            elif "timestamp" in field_type.lower():
                schema_dict[field.name] = "timestamp"
            else:
                schema_dict[field.name] = "string"
                
        create_table_if_not_exists(
            destination, 
            schema_dict,
            schema_name="gold"
        )
        
        # Use JDBC to write data directly to PostgreSQL
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        logger.info(f"Writing data to PostgreSQL via JDBC: {jdbc_url}")
        
        try:
            # Delete existing data
            execute_sql(f"DELETE FROM gold.{destination}")
            
            # Write DataFrame to PostgreSQL
            result_df.write \
               .format("jdbc") \
               .option("url", jdbc_url) \
               .option("dbtable", f"gold.{destination}") \
               .option("user", POSTGRES_USER) \
               .option("password", POSTGRES_PASSWORD) \
               .option("driver", "org.postgresql.Driver") \
               .mode("append") \
               .save()
               
            logger.info(f"Successfully wrote data to gold.{destination} via JDBC")
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
                    pipeline_id=pipeline_config.get("id", destination),
                    source_name=source_name,
                    destination_name=f"gold.{destination}",
                    records_processed=count,
                    duration_seconds=duration
                )
        
        return result_df, count
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error transforming data to gold layer: {error_msg}")
        
        if auditor:
            auditor.complete_with_error(error_msg)
            
            if notify:
                source_name = ", ".join(f"silver.{s}" for s in sources)
                duration = (auditor.end_time - auditor.start_time).total_seconds()
                ETLNotifier.notify_pipeline_failure(
                    pipeline_id=pipeline_config.get("id", destination),
                    source_name=source_name,
                    destination_name=f"gold.{destination}",
                    error_message=error_msg,
                    duration_seconds=duration
                )
        
        raise 