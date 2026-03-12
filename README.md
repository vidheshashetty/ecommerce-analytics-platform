# End-to-End E-Commerce Data Engineering Pipeline

## Business Problem

Leadership teams often track KPIs like revenue, orders, and conversion rates using spreadsheets.  
Different teams calculate metrics differently, leading to inconsistent reporting.

This project builds a centralized analytics pipeline that:

• Ingests raw ecommerce data  
• Cleans and models the data  
• Calculates trusted business metrics  
• Provides a single source of truth dashboard

## Project Overview
This project demonstrates a complete end-to-end Data Engineering pipeline for an e-commerce platform.  
The pipeline generates synthetic business data, loads it into a cloud data warehouse, transforms it using modern data modeling practices, orchestrates workflows, and visualizes business KPIs in a dashboard.

The goal of this project is to simulate a real-world analytics pipeline used by data teams to track revenue, product performance, customer activity, and user behavior.

---

# Architecture

Python Data Generator → Airflow Orchestration → Snowflake Data Warehouse → dbt Transformations → Power BI Dashboard

The warehouse follows a Medallion Architecture:

Bronze Layer → Raw Data  
Silver Layer → Cleaned/Staging Data  
Gold Layer → Analytics & Business Metrics

![Architecture Diagram](diagrams/architecture_diagram.png)
---

# Technologies Used

- Python (Data Generation)
- SQL
- Apache Airflow (Pipeline Orchestration)
- Snowflake (Cloud Data Warehouse)
- dbt (Data Transformation & Modeling)
- Power BI (Dashboard Visualization)

---

# Phase 1 – Synthetic Data Generation

A Python script generates realistic e-commerce datasets.

Datasets generated:

- Customers – 10,000 records
- Products – 500 records
- Orders – 100,000 records
- Payments – 100,000 records
- Events – 200,000 records

Events simulate user activity such as:

- page_view  
- product_view  
- add_to_cart  
- checkout  

All datasets are exported as CSV files.

---

# Phase 2 – Data Loading to Warehouse (Bronze Layer)

The generated CSV files are uploaded to Snowflake stages and loaded into raw warehouse tables.

Bronze layer tables:

- bronze_customers
- bronze_products
- bronze_orders
- bronze_payments
- bronze_events

These tables store raw ingested data without transformation.

---

# Phase 3 – Data Cleaning (Silver Layer)

Using dbt staging models, raw data is cleaned and standardized.

Staging models:

- stg_customers
- stg_products
- stg_orders
- stg_payments
- stg_events

Transformations performed:

- Deduplication
- Null value handling
- Type casting
- Timestamp formatting
- Column renaming

This prepares the data for analytics modeling.

---

# Phase 4 – Data Modeling (Gold Layer)

The warehouse uses a Star Schema for analytics.

Dimension Tables:

- dim_customers
- dim_products

Fact Tables:

- fact_orders

This structure supports efficient business analytics queries.

---

# Phase 5 – Business Metrics Layer

A business metrics model calculates daily KPIs.

daily_business_metrics.sql generates:

- Order Date
- Total Orders
- Total Revenue
- Average Order Value
- Daily Active Users
- Conversion Rate
- Retention Rate

These metrics are used for monitoring business performance.

---

# Phase 6 – Data Quality Testing

Data validation is implemented using dbt tests.

Tests performed:

- Primary key uniqueness
- Null value checks

Key columns tested include:

- customer_id
- order_id
- product_id

---

# Phase 7 – Pipeline Orchestration

The entire workflow is automated using Apache Airflow.

![DAG FLOW](diagrams/DAG_flow_diagram.png)

Airflow DAG stages:

1. Generate incremental datasets
2. Upload CSV data to Snowflake
3. Run dbt staging models
4. Run dbt fact models and Generate business metrics
5. Run dbt tests

Each DAG run processes incremental data, simulating real production pipelines.

---

# Phase 8 – KPI Dashboard

A Power BI dashboard was built to visualize business metrics.

### Executive Overview

![Executive Overview](dashboard/executive_overview.png)

- Total Revenue
- Total Orders
- Average Order Value
- Daily Active Users
- Revenue Trend

### Conversion Analysis

![Conversion Analysis](dashboard/conversion_analysis.png)

- Conversion Funnel (Product View → Add to Cart → Checkout)
- Conversion Rate Trend

### Customer Behavior

![Customer Behavior](dashboard/customer_behavior.png)

- Customer Retention Rate Trend
- Revenue by Country

These dashboards help business teams track performance and user engagement.

---

# Project Structure

ecommerce-analytics-platform
│
├── dags/
│   └── ecommerce_pipeline_dag.py
│
├── data/
│   └── raw_data/
│       ├── app_events.csv
│       ├── customers.csv
│       ├── orders.csv
│       ├── payments.csv
│       ├── products.csv
│       │
│       └── incremental/
│           └── incremental_data_files.csv
│
├── dashboard/
│   ├── executive_overview.png
│   ├── conversion_analysis.png
│   └── customer_behavior.png
│
├── diagrams/
│   ├── architecture_diagram.png
│   └── dag_flow_diagram.png
│
├── scripts/
│   ├── ecommerce_data.py
│   ├── ecommerce_incremental_data.py
│   ├── load_to_snowflake.py
│   └── upload_to_stage.py
│
├── ecommerce_dbt/
│   ├── dbt_project.yml
│   │
│   └── models/
│       │
│       ├── staging/
│       │   ├── stg_customers.sql
│       │   ├── stg_events.sql
│       │   ├── stg_orders.sql
│       │   ├── stg_payments.sql
│       │   └── stg_products.sql
│       │
│       └── marts/
│           │
│           ├── dimensions/
│           │   ├── dim_customers.sql
│           │   ├── dim_products.sql
│           │   └── schema.yml
│           │
│           ├── facts/
│           │   ├── fact_orders.sql
│           │   └── schema.yml
│           │
│           └── metrics/
│               └── daily_business_metrics.sql
│
└── README.md


# Key Skills Demonstrated

- Data Pipeline Design
- Cloud Data Warehousing
- SQL Data Modeling
- Incremental Data Processing
- Data Quality Testing
- Workflow Orchestration
- Business Intelligence Dashboarding

---

# Conclusion

This project demonstrates how modern data engineering pipelines are built using a cloud data stack.  
It simulates real-world workflows where raw data is transformed into reliable business insights through automated and scalable processes.
