import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.audit.audit import ETLAuditor
from loguru import logger

def main():
    # Initialize an auditor object
    logger.info("Creating test audit record")
    auditor = ETLAuditor(
        pipeline_id="manual_test_pipeline",
        source_name="test_source",
        destination_name="test_destination"
    )
    
    # Record some records processed
    logger.info(f"Setting records processed to 1000 for audit ID: {auditor.audit_id}")
    auditor.set_records_processed(1000)
    
    # Mark as completed
    logger.info(f"Marking audit record {auditor.audit_id} as completed")
    auditor.complete_successfully(1000)
    
    logger.info("Test completed successfully")
    
    # Print the audit ID for verification
    print(f"Created audit record with ID: {auditor.audit_id}")

if __name__ == "__main__":
    main() 