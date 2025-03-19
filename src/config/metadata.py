"""
Metadata-driven ETL pipeline configuration.
This file defines the pipelines, sources, and transformations for the ETL process.
"""

# Sample Data Sources
DATA_SOURCES = {
    "customer_data": {
        "source_type": "csv",
        "source_path": "data/customers.csv",
        "schema": {
            "customer_id": "string",
            "name": "string",
            "email": "string", 
            "registration_date": "date",
            "country": "string"
        },
        "primary_key": "customer_id",
        "incremental_column": "registration_date",
        "bronze_table": "bronze_customers",
        "silver_table": "silver_customers",
    },
    "order_data": {
        "source_type": "csv",
        "source_path": "data/orders.csv",
        "schema": {
            "order_id": "string",
            "customer_id": "string",
            "order_date": "timestamp",
            "total_amount": "double",
            "status": "string"
        },
        "primary_key": "order_id",
        "incremental_column": "order_date",
        "bronze_table": "bronze_orders",
        "silver_table": "silver_orders",
    },
    "product_data": {
        "source_type": "csv",
        "source_path": "data/products.csv",
        "schema": {
            "product_id": "string",
            "name": "string",
            "category": "string",
            "price": "double",
            "in_stock": "boolean"
        },
        "primary_key": "product_id",
        "bronze_table": "bronze_products",
        "silver_table": "silver_products",
    }
}

# Data Quality Rules
DATA_QUALITY_RULES = {
    "customer_data": [
        {"column": "customer_id", "rule_type": "not_null"},
        {"column": "email", "rule_type": "regex", "pattern": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"},
        {"column": "registration_date", "rule_type": "date_format", "format": "yyyy-MM-dd"}
    ],
    "order_data": [
        {"column": "order_id", "rule_type": "not_null"},
        {"column": "customer_id", "rule_type": "not_null"},
        {"column": "total_amount", "rule_type": "greater_than", "value": 0}
    ],
    "product_data": [
        {"column": "product_id", "rule_type": "not_null"},
        {"column": "price", "rule_type": "greater_than_equal", "value": 0}
    ]
}

# Pipeline Definitions
PIPELINES = {
    "customer_pipeline": {
        "source": "customer_data",
        "destination": "silver_customers",
        "schedule": "daily",
        "transformations": [
            {"type": "clean_column", "column": "name"},
            {"type": "clean_column", "column": "email"}
        ],
        "quality_checks": DATA_QUALITY_RULES["customer_data"]
    },
    "order_pipeline": {
        "source": "order_data",
        "destination": "silver_orders",
        "schedule": "hourly",
        "transformations": [
            {"type": "type_conversion", "column": "total_amount", "to_type": "double"}
        ],
        "quality_checks": DATA_QUALITY_RULES["order_data"]
    },
    "product_pipeline": {
        "source": "product_data",
        "destination": "silver_products",
        "schedule": "daily",
        "transformations": [],
        "quality_checks": DATA_QUALITY_RULES["product_data"]
    },
    "order_summary_pipeline": {
        "type": "gold",
        "sources": ["silver_orders", "silver_customers"],
        "destination": "gold_order_summary",
        "schedule": "daily",
        "sql_transform": """
            SELECT 
                c.customer_id,
                c.name as customer_name,
                COUNT(o.order_id) as total_orders,
                SUM(o.total_amount) as total_spent,
                MAX(o.order_date) as last_order_date
            FROM silver_orders o
            JOIN silver_customers c ON o.customer_id = c.customer_id
            GROUP BY c.customer_id, c.name
        """
    }
} 