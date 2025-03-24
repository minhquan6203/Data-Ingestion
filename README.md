# Data Ingestion Pipeline

A modern metadata-driven ETL framework that processes data through bronze, silver, and gold layers using Docker, Apache Spark, MinIO (S3-compatible storage), and PostgreSQL.

## Architecture

The pipeline follows a medallion architecture:

- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleansed and validated data
- **Gold Layer**: Business-aggregated data ready for analytics

## Features

- Metadata-driven ETL processes defined in YAML configuration
- Data quality validation during transformations
- Audit logging of all pipeline operations
- Error handling and notifications
- Containerized deployment with Docker

## Prerequisites

- Docker and Docker Compose
- Git

## Data Generation

To avoid memory issues when running the demo, generate the sample data separately:

### Generate data directly (Recommended)

```bash
# Run the script directly with Python, specifying number of records (e.g., 1000)
python src/utils/generate_sample_data.py 1000
```

You can adjust the number of records to generate based on your machine's memory capacity:
- 1000 records is good for testing (low memory usage)
- 5000 records for medium-sized datasets
- 10000 records for full-scale testing (requires more memory)

## Quick Start

1. Clone the repository
2. Generate sample data (see data generation section below)
3. Run the demo script:

```bash
# For Linux/Mac
./run_docker_demo.sh

# For Windows (Command Prompt)
run_docker_demo.bat
```

## Manual Setup and Execution

1. Start the containers:
   ```
   docker-compose up -d
   ```

2. Generate sample data (as explained in the Data Generation section)

3. Run the ETL pipelines:
   ```
   docker-compose run --rm etl-app --run-all
   ```

4. Verify data in PostgreSQL:
   ```
   docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM gold.gold_order_summary;"
   ```

## Individual Pipelines

The ETL framework consists of the following pipelines that can be run individually:

### Bronze to Silver Pipelines

```bash
# Ingest and transform customer data
docker-compose run --rm etl-app --run-pipeline customer_pipeline

# Ingest and transform order data
docker-compose run --rm etl-app --run-pipeline order_pipeline

# Ingest and transform product data
docker-compose run --rm etl-app --run-pipeline product_pipeline
```

### Silver to Gold Pipelines

```bash
# Generate gold layer order summary (requires silver_orders and silver_customers)
docker-compose run --rm etl-app --run-pipeline order_summary_pipeline
```

### Run All Pipelines

This will run all pipelines in the optimal order (bronze to silver, then gold):

```bash
docker-compose run --rm etl-app --run-all
```

## Project Structure

```
├── data/                  # Sample data files
├── src/
│   ├── audit/             # Audit logging functionality
│   ├── config/            # Pipeline configurations (YAML)
│   ├── controller/        # Main ETL controller
│   ├── ingestion/         # Bronze layer ingestion logic
│   ├── notification/      # Email and alert notifications
│   ├── transformation/    # Silver and gold transformations
│   └── utils/             # Utility functions
├── docker-compose.yml     # Docker services configuration
├── Dockerfile             # ETL application container
├── run_docker_demo.bat    # Windows batch script for demo
├── run_docker_demo.ps1    # PowerShell script for demo
├── run_docker_demo.sh     # Unix/Linux shell script for demo
└── requirements.txt       # Python dependencies
```

## Data Flow

1. CSV files are ingested into the bronze layer (MinIO)
2. Bronze data is transformed and validated into the silver layer (PostgreSQL)
3. Silver data is aggregated into business views in the gold layer (PostgreSQL)

## Verifying Results

After running the ETL pipelines, you can verify the data in each layer:

### Bronze Layer Data (Raw ingested data)

Bronze data is primarily stored in MinIO/S3 storage and may not be directly accessible through PostgreSQL tables. To check the bronze layer:

```bash
# Initialize MinIO client (first time only)
docker-compose exec minio mc alias set local http://localhost:9000 minioadmin minioadmin

# List files in the bronze bucket
docker-compose exec minio mc ls local/bronze/

# List specific directories within bronze bucket
docker-compose exec minio mc ls local/bronze/bronze_customers/
docker-compose exec minio mc ls local/bronze/bronze_products/
docker-compose exec minio mc ls local/bronze/bronze_orders/

# Check contents of a specific file (first find the exact filename)
docker-compose exec minio mc cat local/bronze/bronze_customers/<filename>.parquet | head
```

### Silver Layer Data (Transformed and validated data)

Check the record counts in silver tables:

```bash
# Check customer records in silver layer
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM silver.silver_customers;"

# Check product records in silver layer
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM silver.silver_products;"

# Check order records in silver layer
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM silver.silver_orders;"

# Get counts for all tables in one query
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT 'Customers' as table_name, COUNT(*) FROM silver.silver_customers UNION ALL SELECT 'Products', COUNT(*) FROM silver.silver_products UNION ALL SELECT 'Orders', COUNT(*) FROM silver.silver_orders;"
```

### Gold Layer Data (Business-ready aggregated data)

Check the aggregated business data:

```bash
# Check order summary records in gold layer
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM gold.gold_order_summary;"

# View sample data from gold layer
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT * FROM gold.gold_order_summary LIMIT 5;"
```

## Notes

- MinIO UI is accessible at http://localhost:9001 (login: minioadmin/minioadmin)
- PostgreSQL is exposed on port 5432

## Audit Tracking

The ETL pipeline includes a robust audit tracking system that logs the execution details of each pipeline. This helps in monitoring pipeline execution, diagnosing issues, and maintaining data lineage.

### Key Features of Audit Tracking:

- **Record-level tracking**: Each pipeline execution creates a record in the `etl_audit` table
- **Status monitoring**: Pipeline status is updated throughout execution (RUNNING, COMPLETED, FAILED)
- **Performance metrics**: Records processed count and execution duration are logged
- **Error capture**: Detailed error messages are stored when failures occur
- **Transaction verification**: Audit records include verification steps to ensure persistence


### Viewing Audit Records:

```bash
# View the most recent audit records
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT audit_id, pipeline_id, status, records_processed, start_time, end_time FROM public.etl_audit ORDER BY audit_id DESC LIMIT 5;"

# Get summary of pipeline execution times
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT pipeline_id, AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds, AVG(records_processed) as avg_records FROM public.etl_audit WHERE status = 'COMPLETED' GROUP BY pipeline_id;"
```

## Full Load vs Incremental Load

The ETL pipeline supports both full and incremental loading strategies:

- **Full Load**: Processes all data regardless of previous runs
- **Incremental Load**: Only processes new or changed data since the previous run

### Running Full vs Incremental Loads

```bash
# For Windows (Command Prompt)
# Run full load (default)
run_docker_demo.bat

# Run incremental load
run_docker_demo.bat incremental
```

### Checking Load Types and Comparing Results

You can examine the audit records to compare full vs incremental loads:

```bash
# View full load records and record counts
docker exec -i data-ingestion-postgres-1 psql -U postgres -d datawarehouse -c "SELECT audit_id, pipeline_id, load_type, status, records_processed FROM etl_audit WHERE load_type = 'full' ORDER BY audit_id DESC LIMIT 5;"

# View incremental load records and record counts
docker exec -i data-ingestion-postgres-1 psql -U postgres -d datawarehouse -c "SELECT audit_id, pipeline_id, load_type, status, records_processed FROM etl_audit WHERE load_type = 'incremental' ORDER BY audit_id DESC LIMIT 5;"

# View incremental load metadata (shows the watermark values used)
docker exec -i data-ingestion-postgres-1 psql -U postgres -d datawarehouse -c "SELECT audit_id, pipeline_id, load_type, metadata FROM etl_audit WHERE load_type = 'incremental' AND metadata IS NOT NULL ORDER BY audit_id DESC LIMIT 5;"
```

### Understanding Incremental Load Metadata

The metadata column in the audit records contains important information about incremental loads:
- `incremental_column`: The column used to track changes (e.g., timestamps, IDs)
- `last_timestamp`: The high watermark value from the previous run
- `filtered_records`: Indicates whether records were filtered based on incremental criteria

### Expected Behavior

When running sequential loads:
1. First run (full load): Processes all available records
2. Second run (incremental): Processes only new records since the first run
3. Third run (incremental, no new data): Processes 0 records if no new data was added

This pattern confirms that the incremental loading strategy is working correctly, only processing data that has changed since the last run.
