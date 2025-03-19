"""
Main script for the metadata-driven ETL framework demo
"""
import os
import sys
import argparse
from loguru import logger

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.controller.etl_controller import ETLController
from src.config.metadata import PIPELINES
from src.audit.audit import get_pipeline_execution_history, ETLAuditor


def setup_logging():
    """
    Set up logging configuration
    """
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="DEBUG")
    logger.add("logs/etl_{time}.log", rotation="500 MB", level="DEBUG")


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Metadata-driven ETL Framework Demo")
    
    parser.add_argument(
        "--run-pipeline",
        help="Run a specific pipeline by ID",
    )
    
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Run all pipelines"
    )
    
    parser.add_argument(
        "--list-pipelines",
        action="store_true",
        help="List all available pipelines"
    )
    
    parser.add_argument(
        "--show-history",
        action="store_true",
        help="Show pipeline execution history"
    )
    
    parser.add_argument(
        "--schedule",
        help="Schedule a pipeline to run at regular intervals (pipeline ID)"
    )
    
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Interval in seconds for scheduled runs (default: 3600)"
    )
    
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Run pipelines sequentially instead of in parallel"
    )
    
    # Add test-audit argument
    parser.add_argument(
        "--test-audit",
        action="store_true",
        help="Test audit functionality by creating a test record"
    )
    
    return parser.parse_args()


def list_pipelines():
    """
    List all available pipelines
    """
    print("\nAvailable Pipelines:")
    print("===================")
    
    for pid, config in PIPELINES.items():
        pipeline_type = config.get("type", "bronze_to_silver")
        
        if pipeline_type == "gold":
            sources = ", ".join(config.get("sources", []))
            destination = config.get("destination", "")
            print(f"ID: {pid} (Gold) - Sources: [{sources}], Destination: {destination}")
        else:
            source = config.get("source", "")
            destination = config.get("destination", "")
            print(f"ID: {pid} (Bronze to Silver) - Source: {source}, Destination: {destination}")
    
    print()


def show_execution_history():
    """
    Show pipeline execution history
    """
    history = get_pipeline_execution_history(limit=20)
    
    if not history:
        print("\nNo pipeline execution history found.")
        return
    
    print("\nPipeline Execution History:")
    print("==========================")
    
    for record in history:
        audit_id, pipeline_id, source, destination, start_time, end_time, records, status, error = record
        
        duration = "N/A"
        if end_time and start_time:
            duration = f"{(end_time - start_time).total_seconds():.2f}s"
        
        print(f"Pipeline: {pipeline_id}")
        print(f"  Status: {status}")
        print(f"  Start: {start_time}")
        print(f"  Duration: {duration}")
        print(f"  Records: {records or 0}")
        
        if status == "FAILED" and error:
            print(f"  Error: {error}")
        
        print()


def main():
    """
    Main function
    """
    # Set up logging
    setup_logging()
    
    # Parse command line arguments
    args = parse_args()
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create ETL controller
    controller = ETLController()
    
    # Test audit functionality if requested
    if args.test_audit:
        logger.info("Running audit test")
        
        # Create test audit record
        auditor = ETLAuditor(
            pipeline_id="test_audit_pipeline",
            source_name="test_source",
            destination_name="test_destination"
        )
        
        # Update record count and complete
        auditor.set_records_processed(1000)
        auditor.complete_successfully(1000)
        
        print(f"Created and completed audit record with ID: {auditor.audit_id}")
        return
    
    if args.list_pipelines:
        list_pipelines()
        return
    
    if args.show_history:
        show_execution_history()
        return
    
    if args.run_pipeline:
        pipeline_id = args.run_pipeline
        
        if pipeline_id not in PIPELINES:
            logger.error(f"Pipeline {pipeline_id} not found")
            print(f"Pipeline '{pipeline_id}' not found. Use --list-pipelines to see available pipelines.")
            return
        
        print(f"Running pipeline: {pipeline_id}")
        success = controller.run_pipeline(pipeline_id)
        
        if success:
            print(f"Pipeline {pipeline_id} completed successfully")
        else:
            print(f"Pipeline {pipeline_id} failed")
        
        return
    
    if args.schedule:
        pipeline_id = args.schedule
        interval = args.interval
        
        if pipeline_id not in PIPELINES:
            logger.error(f"Pipeline {pipeline_id} not found")
            print(f"Pipeline '{pipeline_id}' not found. Use --list-pipelines to see available pipelines.")
            return
        
        print(f"Scheduling pipeline {pipeline_id} to run every {interval} seconds.")
        print("Press Ctrl+C to stop.")
        
        controller.schedule_pipeline(pipeline_id, interval)
        return
    
    if args.run_all:
        parallel = not args.sequential
        print(f"Running all pipelines {'sequentially' if not parallel else 'in parallel'}")
        
        results = controller.run_all_pipelines(parallel=parallel)
        
        successful = sum(1 for result in results.values() if result)
        failed = len(results) - successful
        
        print(f"\nPipeline execution summary: {successful} successful, {failed} failed")
        
        if failed > 0:
            failed_pipelines = [pid for pid, result in results.items() if not result]
            print(f"Failed pipelines: {', '.join(failed_pipelines)}")
        
        return
    
    # If no specific action was requested, print usage
    print("No action specified. Use one of the following options:")
    print("  --run-pipeline PIPELINE_ID : Run a specific pipeline")
    print("  --run-all                  : Run all pipelines")
    print("  --list-pipelines           : List all available pipelines")
    print("  --show-history             : Show pipeline execution history")
    print("  --schedule PIPELINE_ID     : Schedule a pipeline to run at regular intervals")
    print("  --help                     : Show all available options")


if __name__ == "__main__":
    main() 