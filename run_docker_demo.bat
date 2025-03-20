@echo off
setlocal enabledelayedexpansion

if "%1"=="incremental" (
    set DEMO_TYPE=incremental
) else (
    set DEMO_TYPE=standard
)

echo ==========================================
echo    Data Ingestion Pipeline Demo
if "!DEMO_TYPE!"=="incremental" (
    echo    [Incremental Load Mode]
) else (
    echo    [Standard Mode]
)
echo ==========================================
echo.

rem Stop and remove existing containers
echo Step 1: Cleaning up any existing containers...
docker-compose down
echo Done!
echo.

rem Build the ETL application
echo Step 2: Building the ETL application...
docker-compose build etl-app
echo Done!
echo.

rem Start all services
echo Step 3: Starting MinIO and PostgreSQL services...
docker-compose up -d postgres minio
echo Waiting for services to be healthy...
timeout /t 10 /nobreak > nul
echo Done!
echo.

rem Start ETL app container
echo Step 4: Starting ETL application container...
docker-compose up -d etl-app
echo Done!
echo.

if "!DEMO_TYPE!"=="incremental" (
    rem Run Incremental Load Demo
    echo Step 5: Running incremental load demo...
    docker-compose run --rm etl-app --run-all
    
) else (
    rem Run all ETL pipelines (standard demo)
    echo Step 5: Running full load demno...
    docker-compose run --rm etl-app --run-all --full-load
)
echo Done!
echo.

rem Verify results
echo Step 6: Verifying results...
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT COUNT(*) FROM gold.gold_order_summary;"
echo.

echo Sample data query from Gold layer:
docker-compose exec postgres psql -U postgres -d datawarehouse -c "SELECT * FROM gold.gold_order_summary LIMIT 5;"
echo.

echo ==========================================

echo Demo completed successfully!

echo ==========================================
echo.
echo Services running:
echo - MinIO UI: http://localhost:9001 (login: minioadmin/minioadmin)
echo - PostgreSQL: localhost:5432 (login: postgres/postgres)
echo.
echo To stop all services, run: docker-compose down

echo.
echo To run the incremental load demo: %0 incremental 