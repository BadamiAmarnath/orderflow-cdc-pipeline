# OrderFlow: CDC-Driven E-Commerce ELT Pipeline
**Product Requirements Document (PRD) — Agent-Buildable Spec**  
*Data Engineering Portfolio Project — September 2026*

---

## 1. Overview

### 1.1 Purpose of this document
This PRD is written to define the scope, architecture, data model, folder structure, build phases with explicit acceptance criteria, and a definition of done for the OrderFlow project.

### 1.2 Business objective
OrderFlow simulates a real e-commerce platform’s operational database (customers, products, orders, inventory) and builds a Change Data Capture (CDC)-driven ELT pipeline that captures only the changes happening in that database — not full re-extraction — and flows them into an analytics warehouse. The warehouse models key dimensions using Slowly Changing Dimension Type 2 (SCD2), preserving full history of how customer and product data changed over time (e.g., every price change, every address update), rather than only showing the current state. Apache Airflow orchestrates the pipeline, and Great Expectations validates data quality at the source before it enters the warehouse, complementing dbt’s business-logic tests downstream.

### 1.3 Why this project exists (relative to a prior streaming project)
| Prior project pattern | OrderFlow pattern |
| :--- | :--- |
| Event-stream simulation (Kafka producer) | Database-level Change Data Capture |
| AWS Step Functions orchestration | Apache Airflow orchestration |
| dbt tests only | Great Expectations (source) + dbt tests (business logic) |
| Fact/aggregate dbt models | SCD Type 2 dimensional modeling (history tracking) |
| Athena/Glue-style warehouse | DuckDB locally, portable to Snowflake/BigQuery |

### 1.4 Success criteria
- PostgreSQL runs as a realistic OLTP source with ongoing simulated activity (inserts, updates, deletes).
- Change events are captured with before/after state and operation type (`c`/`u`/`d`), via Debezium or a schema-faithful fallback simulator.
- Airflow DAGs orchestrate ingestion -> warehouse load -> dbt run, with retries configured and failure alerting demonstrably working.
- `dim_customers_scd2` and `dim_products_scd2` correctly implement SCD Type 2: an update to a tracked attribute closes out the old row (`valid_to` set, `is_current = false`) and inserts a new current row (`valid_from` set, `valid_to = NULL`, `is_current = true`).
- Great Expectations validates source data before warehouse load; dbt tests validate business logic after transformation.
- The entire pipeline runs locally with zero cloud credentials required.
- A validation script independently proves SCD2 correctness, idempotency, and CDC completeness.

---

## 2. Architecture & Data Flow

```
[PostgreSQL OLTP DB]
customers, products, orders, order_items, inventory
  │
  ▼ (simulate_changes.py generates ongoing INSERT/UPDATE/DELETE)
[CDC Capture Layer]
Debezium / WAL Change Capture
  │ (before/after state + op: c/u/d)
  ▼
[Raw Change Log] (JSON/Parquet, partitioned by table + date)
  │
  ▼ (Airflow DAG: cdc_ingestion_dag & warehouse_load_dag)
[DuckDB Warehouse — Staging Layer]
Cleaned, typed views per source table
  │
  ▼ (Airflow DAG: dbt_run_dag)
[dbt Transformation Layer]
dim_customers_scd2, dim_products_scd2 (SCD2 History)
fct_orders (Order Fact Table with Point-in-Time dimension joins)
agg_daily_sales (Daily Business Metrics)
  │
  ▼
[Great Expectations: Source Quality] ─── [dbt tests: Business Logic Quality]
```

---

## 3. Data Model

### 3.1 Source Tables (PostgreSQL)
- **`customers`**: `customer_id` (PK), `name`, `email`, `address`, `created_at`, `updated_at`
- **`products`**: `product_id` (PK), `name`, `category`, `price`, `updated_at`
- **`orders`**: `order_id` (PK), `customer_id` (FK), `order_date`, `status` (`pending` -> `shipped` -> `delivered` / `cancelled`), `total_amount`
- **`order_items`**: `order_item_id` (PK), `order_id` (FK), `product_id` (FK), `quantity`, `unit_price`
- **`inventory`**: `product_id` (PK/FK), `warehouse_id`, `stock_level`, `updated_at`

### 3.2 CDC Event Shape
```json
{
  "table": "products",
  "operation": "update",
  "before": { "product_id": 101, "price": 29.99, "updated_at": "..." },
  "after": { "product_id": 101, "price": 24.99, "updated_at": "..." },
  "captured_at": "ISO-8601 timestamp"
}
```

### 3.3 Warehouse Models (dbt)
- `dim_customers_scd2`: Historical customer tracking across address/email changes.
- `dim_products_scd2`: Historical product tracking across price and category changes.
- `fct_orders`: Grain is 1 row per order, joined point-in-time to customer and product dimensions valid at `order_date`.
- `agg_daily_sales`: Daily revenue, order count, average order value, cancellation rate.
