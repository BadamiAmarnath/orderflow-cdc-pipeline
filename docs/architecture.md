# OrderFlow Architecture & Cloud Production Guide

## 1. System Architecture

OrderFlow implements an enterprise Change Data Capture (CDC)-driven ELT pipeline replicating an e-commerce platform's transactional operations into a local analytics warehouse with Slowly Changing Dimension Type 2 (SCD2) history tracking.

```
[PostgreSQL / OLTP Source DB]
customers, products, orders, order_items, inventory
  │
  ▼ (Continuous transaction simulation via simulate_changes.py)
[CDC Capture Layer]
Debezium WAL Engine / Polling Change Log
  │ (Emits standardized JSON with before/after state + operation)
  ▼
[Bronze Raw Change Store]
data/cdc_events/<table_name>/<YYYY-MM-DD>.jsonl
  │
  ▼ (Airflow: cdc_ingestion_dag & warehouse_load_dag)
[Great Expectations Quality Gate]
(Source-level checks: null PKs, price > 0, valid status enums, foreign keys)
  │
  ▼
[DuckDB Analytics Warehouse - Staging Layer]
stg_customers_cdc, stg_products_cdc, stg_orders_cdc, stg_order_items_cdc, stg_inventory_cdc
  │
  ▼ (Airflow: dbt_run_dag)
[dbt Transformation Layer]
├── dim_customers_scd2  (Historical customer address/email mutations)
├── dim_products_scd2   (Historical product price and category updates)
├── fct_orders          (Point-in-Time dimension key resolution at order_date)
└── agg_daily_sales     (Executive business metrics & cancellation rates)
  │
  ▼
[dbt Business Tests & Custom SCD2 Integrity Verifications]
```

---

## 2. CDC Event Specification

Every change event follows Debezium's canonical envelope specification:

```json
{
  "table": "products",
  "operation": "update",
  "before": {
    "product_id": 13,
    "name": "Italian Barista Espresso & Cappuccino Machine",
    "category": "Home & Kitchen",
    "price": 299.99,
    "updated_at": "2026-08-01 08:00:00"
  },
  "after": {
    "product_id": 13,
    "name": "Italian Barista Espresso & Cappuccino Machine",
    "category": "Home & Kitchen",
    "price": 269.58,
    "updated_at": "2026-09-05 17:09:43"
  },
  "captured_at": "2026-09-05T17:09:43.669243+00:00",
  "change_id": 453
}
```

- **Operations**: `insert` (`c`), `update` (`u`), `delete` (`d`).
- **State Invariants**:
  - `insert`: `before` is `null`, `after` contains initial state.
  - `update`: `before` contains prior state, `after` contains modified state.
  - `delete`: `before` contains state prior to removal, `after` is `null`.

---

## 3. SCD Type 2 Dimensional Modeling Design

### Customer & Product Dimensions
When a tracked attribute (e.g., customer address or product price) is modified:
1. The existing active row is closed:
   - `valid_to = change_timestamp`
   - `is_current = FALSE`
2. A new version row is inserted:
   - `valid_from = change_timestamp`
   - `valid_to = NULL`
   - `is_current = TRUE`
3. A unique surrogate key is calculated:
   - `customer_key = md5(customer_id || '-' || valid_from)`
   - `product_key = md5(product_id || '-' || valid_from)`

### Point-in-Time Fact Table Join
Orders in `fct_orders` resolve the exact customer dimension record valid at the time the order was placed:
```sql
SELECT
    o.order_id,
    c.customer_key,
    o.customer_id,
    o.order_date,
    o.status,
    o.total_amount
FROM stg_orders o
LEFT JOIN dim_customers_scd2 c
    ON o.customer_id = c.customer_id
    AND o.order_date >= c.valid_from
    AND (o.order_date < c.valid_to OR c.valid_to IS NULL)
```

---

## 4. Two-Layer Data Quality Architecture

| Layer | Tool | Scope | Example Checks |
| :--- | :--- | :--- | :--- |
| **Layer 1: Pre-Ingestion** | **Great Expectations** | Source / Staging Bronze Data | Non-null PKs, `price > 0`, `status IN ('pending', 'shipped', 'delivered', 'cancelled')`, valid foreign keys |
| **Layer 2: Post-Transformation** | **dbt Tests** | Business Logic & Dimensional Integrity | `unique(customer_key)`, `assert_one_current_row_per_entity`, `assert_no_overlapping_scd2_windows`, `not_null` |

---

## 5. Cloud Production Migration Mapping

How this local DuckDB + Airflow architecture maps to enterprise cloud data platforms:

| Local Component | AWS Production Target | GCP Production Target | Snowflake / Databricks Target |
| :--- | :--- | :--- | :--- |
| **OLTP Source** | Amazon Aurora PostgreSQL | Cloud SQL for PostgreSQL | PostgreSQL / Operational DB |
| **CDC Capture** | AWS DMS / Debezium on MSK | Datastream / Debezium on Cloud Pub/Sub | Debezium on Kafka / Qlik Replicate |
| **Raw Storage** | Amazon S3 (Bronze Bucket) | Google Cloud Storage (GCS) | Amazon S3 / Snowflake Internal Stage |
| **Orchestrator** | Amazon MWAA (Managed Airflow) | Cloud Composer (Managed Airflow) | Astronomer / MWAA / Airflow |
| **Data Warehouse** | Amazon Redshift / DuckDB on S3 | Google BigQuery | Snowflake Enterprise Warehouse |
| **Transformations** | dbt-redshift / dbt-core | dbt-bigquery | dbt-snowflake |
| **Data Quality** | Great Expectations + dbt test | Great Expectations + dbt test | Great Expectations + dbt test |

### Portability Configuration for Snowflake:
To redirect the transformation pipeline to Snowflake, simply modify `dbt_project/profiles.yml`:
```yaml
orderflow_snowflake:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: "<org>-<account>"
      user: "<username>"
      password: "<password>"
      role: "TRANSFORMER"
      database: "ANALYTICS"
      warehouse: "TRANSFORMING_WH"
      schema: "MARTS"
      threads: 8
```
The dbt SQL models (`dim_customers_scd2.sql`, `fct_orders.sql`, `agg_daily_sales.sql`) require zero code changes due to standard ANSI SQL window function compatibility.
