"""
Audit module for tracking ETL processes
"""
import time
from datetime import datetime
from loguru import logger

from src.utils.storage_utils import execute_sql


class ETLAuditor:
    """
    Class for auditing ETL pipeline executions
    """
    
    def __init__(self, pipeline_id, source_name, destination_name):
        """
        Initialize an ETL audit record
        
        Args:
            pipeline_id: Identifier for the pipeline
            source_name: Name of the source
            destination_name: Name of the destination
        """
        self.pipeline_id = pipeline_id
        self.source_name = source_name
        self.destination_name = destination_name
        self.start_time = datetime.now()
        self.end_time = None
        self.records_processed = 0
        self.status = "RUNNING"
        self.error_message = None
        self.audit_id = None
        
        # Create the audit record
        self._create_audit_record()
    
    def _create_audit_record(self):
        """
        Create an audit record in the database
        """
        sql = """
        INSERT INTO public.etl_audit 
            (pipeline_id, source_name, destination_name, start_time, status)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING audit_id;
        """
        
        params = (
            self.pipeline_id,
            self.source_name,
            self.destination_name,
            self.start_time,
            self.status
        )
        
        try:
            logger.debug(f"Creating audit record for pipeline: {self.pipeline_id}")
            result = execute_sql(sql, params, fetch=True)
            
            if result and len(result) > 0 and len(result[0]) > 0:
                self.audit_id = result[0][0]
                logger.info(f"Created audit record with ID: {self.audit_id}")
                
                # Verify record exists
                verify_sql = "SELECT COUNT(*) FROM public.etl_audit WHERE audit_id = %s;"
                verify_result = execute_sql(verify_sql, (self.audit_id,), fetch=True)
                record_count = verify_result[0][0] if verify_result and len(verify_result) > 0 else 0
                
                if record_count == 0:
                    logger.error(f"Audit record with ID {self.audit_id} was not found in the database after creation!")
                else:
                    logger.debug(f"Verified audit record with ID {self.audit_id} exists in database")
            else:
                logger.error("Failed to create audit record: No audit_id returned")
        except Exception as e:
            logger.error(f"Failed to create audit record: {e}")
            # Don't raise the exception - allow the ETL process to continue even if auditing fails
    
    def update_status(self, status, error_message=None):
        """
        Update the status of the audit record
        
        Args:
            status: New status (RUNNING, COMPLETED, FAILED)
            error_message: Error message if status is FAILED
        """
        self.status = status
        self.error_message = error_message
        
        if status in ["COMPLETED", "FAILED"]:
            self.end_time = datetime.now()
        
        self._update_audit_record()
    
    def set_records_processed(self, records_processed):
        """
        Set the number of records processed
        
        Args:
            records_processed: Number of records processed
        """
        self.records_processed = records_processed
        self._update_audit_record()
    
    def _update_audit_record(self):
        """
        Update the audit record in the database
        """
        if self.audit_id is None:
            logger.error("Cannot update audit record: No audit_id available")
            return
            
        sql = """
        UPDATE public.etl_audit
        SET status = %s,
            records_processed = %s,
            end_time = %s,
            error_message = %s
        WHERE audit_id = %s;
        """
        
        params = (
            self.status,
            self.records_processed,
            self.end_time,
            self.error_message,
            self.audit_id
        )
        
        try:
            logger.debug(f"Updating audit record with ID: {self.audit_id}, status: {self.status}")
            execute_sql(sql, params)
            logger.info(f"Updated audit record {self.audit_id} with status: {self.status}")
            
            # Verify the update
            verify_sql = "SELECT status FROM public.etl_audit WHERE audit_id = %s;"
            verify_result = execute_sql(verify_sql, (self.audit_id,), fetch=True)
            
            if not verify_result or len(verify_result) == 0:
                logger.error(f"Audit record with ID {self.audit_id} not found after update!")
            else:
                db_status = verify_result[0][0] if verify_result and len(verify_result) > 0 else None
                if db_status != self.status:
                    logger.error(f"Audit record status mismatch: Expected {self.status}, found {db_status}")
                else:
                    logger.debug(f"Verified audit record {self.audit_id} status is {db_status}")
        except Exception as e:
            logger.error(f"Failed to update audit record: {e}")
            # Don't raise the exception - allow the ETL process to continue even if auditing fails
    
    def complete_successfully(self, records_processed):
        """
        Mark the audit record as completed successfully
        
        Args:
            records_processed: Number of records processed
        """
        self.records_processed = records_processed
        self.update_status("COMPLETED")
        
        duration = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Pipeline {self.pipeline_id} completed successfully. " \
                  f"Processed {records_processed} records in {duration:.2f} seconds")
    
    def complete_with_error(self, error_message, records_processed=0):
        """
        Mark the audit record as failed
        
        Args:
            error_message: Error message
            records_processed: Number of records processed before failure
        """
        self.records_processed = records_processed
        self.update_status("FAILED", error_message)
        
        duration = (self.end_time - self.start_time).total_seconds()
        logger.error(f"Pipeline {self.pipeline_id} failed after {duration:.2f} seconds. " \
                   f"Error: {error_message}")


def get_pipeline_execution_history(pipeline_id=None, limit=10):
    """
    Get the execution history for a pipeline or all pipelines
    
    Args:
        pipeline_id: Pipeline ID to filter by, or None for all pipelines
        limit: Maximum number of records to return
        
    Returns:
        List of audit records
    """
    if pipeline_id:
        sql = """
        SELECT * FROM public.etl_audit
        WHERE pipeline_id = %s
        ORDER BY start_time DESC
        LIMIT %s;
        """
        params = (pipeline_id, limit)
    else:
        sql = """
        SELECT * FROM public.etl_audit
        ORDER BY start_time DESC
        LIMIT %s;
        """
        params = (limit,)
    
    try:
        results = execute_sql(sql, params, fetch=True)
        return results
    except Exception as e:
        logger.error(f"Failed to get pipeline execution history: {e}")
        return [] 