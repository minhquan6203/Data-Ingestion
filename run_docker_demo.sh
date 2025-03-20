#!/bin/bash

# Determine demo type
if [ "$1" == "incremental" ]; then
    DEMO_TYPE="incremental"
else
    DEMO_TYPE="standard"
fi

# Data Ingestion Pipeline Demo Script
echo "=========================================="
echo "   Data Ingestion Pipeline Demo"
if [ "$DEMO_TYPE" == "incremental" ]; then
    echo "   [Incremental Load Mode]"
else
    echo "   [Standard Mode]"
fi
echo "=========================================="
echo ""

# Stop and remove existing containers
echo "Step 1: Cleaning up any existing containers..."
docker-compose down
echo "Done"
echo ""

# Build the ETL application
echo "Step 2: Building the ETL application..."
docker-compose build etl-app
echo "Done"
echo ""

# Start all services
echo "Step 3: Starting MinIO and PostgreSQL services..."
docker-compose up -d postgres minio
echo "Waiting for services to be healthy..."
sleep 10
echo "Done"
echo ""

# Start ETL app container
echo "Step 4: Starting ETL application container..."
docker-compose up -d etl-app
echo "Done"
echo ""

if [ "$DEMO_TYPE" == "incremental" ]; then
    # Run Incremental Load Demo
    echo "Step 5: Running incremental load demo..."
    docker-compose run --rm etl-app --run-all
else
    # Run all ETL pipelines (standard demo)
    echo "Step 5: Running full load demo..."
    docker-compose run --rm etl-app --run-all --full-load
fi
echo "Done"
echo ""

# Verify results
echo "Step 6: Verifying results..."
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM gold.gold_order_summary;"
echo ""

echo "Sample data query from Gold layer:"
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT * FROM gold.gold_order_summary LIMIT 5;"
echo ""

echo "=========================================="
echo "Demo completed successfully!"
echo "=========================================="
echo ""
echo "Services running:"
echo "- MinIO UI: http://localhost:9001 (login: minioadmin/minioadmin)"
echo "- PostgreSQL: localhost:5432 (login: postgres/postgres)"
echo ""
echo "To stop all services, run: docker-compose down"
echo ""
echo "To run the incremental load demo: $0 incremental" 