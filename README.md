# OrderFlow: CDC-Driven E-Commerce ELT Pipeline

OrderFlow is an enterprise Change Data Capture (CDC)-driven ELT pipeline replicating an e-commerce platform's transactional operations into a local analytics warehouse (DuckDB) with Slowly Changing Dimension Type 2 (SCD2) history tracking.

---

## 🌟 Key Features
- **Database-Level Change Data Capture**: Row-level change capture emitting strict Debezium JSON events with before/after state and operations (`insert`/`update`/`delete`).
- **SCD Type 2 Dimensional Modeling**: Full history preservation for customer address/email updates and product price/category changes (`valid_from`, `valid_to`, `is_current`).
- **Point-in-Time Fact Attribution**: `fct_orders` links transactions to the exact historical dimension state valid at `order_date`.
- **Two-Layer Data Quality Gate**:
  - **Layer 1 (Pre-Load)**: *Great Expectations* validates source data (null PKs, positive prices, valid status enums, referential integrity).
  - **Layer 2 (Post-Transform)**: *dbt tests* validate business logic and SCD2 constraints (`assert_one_current_row_per_entity`, `assert_no_overlapping_scd2_windows`).
- **Dual Orchestration Model**: Standalone Python orchestrator with zero external dependencies, plus container-ready Apache Airflow DAGs with exponential backoff retries and structured Slack/log failure alerts.
- **100% Idempotent**: Re-running the pipeline on identical data generates zero duplicate rows.
- **Zero Cloud Credentials Required**: Completely runnable locally on DuckDB, portable to Snowflake/BigQuery.

---

## 🏗️ System Architecture & Data Flow

```
[PostgreSQL / Transactional Source DB]
customers, products, orders, order_items, inventory
  │
  ▼ (Continuous transaction simulation via simulate_changes.py)
[CDC Capture Layer]
Debezium WAL Engine / Outbox Change Log (cdc_event_schema.py)
  │ (Emits standardized JSON with before/after state + op: c/u/d)
  ▼
[Bronze Raw Change Store]
data/cdc_events/<table_name>/<YYYY-MM-DD>.jsonl
  │
  ▼ (Airflow / Pipeline Gate)
[Great Expectations Quality Gate]
(Source-level checks: null PKs, price > 0, valid status enums, foreign keys)
  │
  ▼
[DuckDB Analytics Warehouse - Staging Layer]
stg_customers_cdc, stg_products_cdc, stg_orders_cdc, stg_order_items_cdc, stg_inventory_cdc
  │
  ▼ (dbt Transformation Layer)
├── dim_customers_scd2  (Historical customer address/email mutations)
├── dim_products_scd2   (Historical product price and category updates)
├── fct_orders          (Point-in-Time dimension key resolution at order_date)
└── agg_daily_sales     (Executive business metrics & cancellation rates)
  │
  ▼
[dbt Business Tests & Custom SCD2 Integrity Verifications]
```

---

## ⚖️ Design Decision: Debezium vs. CDC Engine Fallback

The project supports two CDC execution modes:
1. **Containerized Production Pattern (`docker-compose.yml` + `cdc/debezium_connector_config.json`)**:
   Uses Debezium PostgreSQL Connector reading Postgres WAL via `pgoutput` to Kafka topics.
2. **Local Python CDC Engine (`cdc/cdc_event_schema.py`)**:
   Captures transactional deltas and emits the identical canonical Debezium event envelope (`table`, `operation`, `before`, `after`, `captured_at`, `change_id`).

**Why the Local Fallback was Built**:
- Runs in lightweight developer and CI environments without requiring Docker, Kafka, or Zookeeper daemons.
- Produces the identical payload schema, ensuring downstream consumers (Great Expectations, DuckDB staging, dbt) operate identically regardless of whether events originate from live Debezium or the local engine.

---

## 📁 Repository Structure
```
orderflow-cdc-pipeline/
├── README.md                              # Project overview, architecture & quickstart
├── PRD.md                                 # Full PRD specification
├── docker-compose.yml                     # Multi-service stack (Postgres, Kafka, Debezium, Airflow)
├── requirements.txt                       # Python dependencies
├── postgres/
│   ├── init.sql                           # Schema DDL, triggers & seed data
│   └── simulate_changes.py                # OLTP change simulator (inserts/updates/deletes)
├── cdc/
│   ├── debezium_connector_config.json     # Debezium Postgres connector config
│   └── cdc_event_schema.py                # CDC event parser & extraction engine
├── airflow/
│   ├── dags/
│   │   ├── cdc_ingestion_dag.py           # Airflow Ingestion DAG
│   │   ├── warehouse_load_dag.py          # Airflow Staging Load DAG
│   │   └── dbt_run_dag.py                 # Airflow Transformation & Test DAG
│   └── plugins/
│       └── alerts.py                      # Failure alert callbacks & retry handlers
├── great_expectations/
│   ├── great_expectations.yml             # GX project config
│   └── expectations/                      # Expectation suite definitions
├── great_expectations_validator.py        # Great Expectations source validation runner
├── dbt_project/
│   ├── dbt_project.yml                    # dbt project definition
│   ├── profiles.yml                       # DuckDB connection profile
│   ├── models/
│   │   ├── staging/                       # Staging views (stg_customers, stg_orders, etc.)
│   │   └── marts/                         # SCD2 dimensions, fact tables & aggregates
│   │       ├── dim_customers_scd2.sql
│   │       ├── dim_products_scd2.sql
│   │       ├── fct_orders.sql
│   │       └── agg_daily_sales.sql
│   ├── schema.yml                         # Column tests & documentation
│   └── tests/                             # Custom SCD2 integrity tests
│       ├── assert_one_current_row_per_entity.sql
│       └── assert_no_overlapping_scd2_windows.sql
├── scripts/
│   ├── run_full_pipeline.py               # End-to-end orchestrator & idempotency prover
│   ├── validate_scd2.py                   # Independent SCD2 verification suite
│   ├── prove_point_in_time_scd2.py        # Point-in-time resolution verification proof
│   └── inject_bad_data.py                 # Data quality failure test harness
└── docs/
    └── architecture.md                    # Architecture diagram & cloud migration guide
```

---

## 🚀 Quickstart & Execution Runbook

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Full End-to-End Pipeline
```bash
python scripts/run_full_pipeline.py --seed --steps 15
```

### 3. Verify SCD Type 2 History Preservation & Constraints
```bash
python scripts/validate_scd2.py
```

### 4. Prove Point-in-Time Dimension Resolution (`fct_orders`)
```bash
python scripts/prove_point_in_time_scd2.py
```

### 5. Demonstrate Great Expectations Catching Corrupted Source Data
```bash
python scripts/inject_bad_data.py
```

### 6. Prove 100% Pipeline Idempotency
```bash
python scripts/run_full_pipeline.py --test-idempotency
```

### 7. Test Airflow Task Retries and Alerting Callbacks
```bash
python airflow/dags/cdc_ingestion_dag.py --test-retry
```

---

## 📊 Sample Analytical Queries & Outputs

### Executive Daily Sales Metrics (`agg_daily_sales`)
```sql
SELECT sale_date, total_orders, gross_revenue, delivered_revenue, average_order_value, cancellation_rate_pct 
FROM agg_daily_sales 
ORDER BY sale_date DESC LIMIT 5;
```
| sale_date | total_orders | gross_revenue | delivered_revenue | average_order_value | cancellation_rate_pct |
| :---: | :---: | :---: | :---: | :---: | :---: |
| 2026-09-05 | 6 | $2,436.26 | $0.00 | $487.25 | 16.67% |
| 2026-08-17 | 5 | $2,583.93 | $773.95 | $645.98 | 20.00% |
| 2026-08-16 | 6 | $2,166.93 | $1,212.45 | $433.39 | 16.67% |
| 2026-08-15 | 6 | $1,899.95 | $1,123.95 | $474.99 | 33.33% |
| 2026-08-14 | 6 | $2,372.42 | $1,324.94 | $474.48 | 16.67% |

### Point-in-Time Order Resolution Proof
```sql
SELECT 
    f.order_id,
    f.order_date,
    c.address AS address_at_time_of_order,
    c.is_current AS is_currently_active_address_version,
    f.total_amount
FROM fct_orders f
JOIN dim_customers_scd2 c ON f.customer_key = c.customer_key
WHERE f.customer_id = 1 AND f.order_id IN (101, 102);
```
| order_id | order_date | address_at_time_of_order | is_currently_active_address_version | total_amount |
| :---: | :---: | :--- | :---: | :---: |
| **101** | 2026-08-10 12:00:00 | 124 Market St, San Francisco, CA 94105 | **False** (Historical Version) | $199.99 |
| **102** | 2026-08-25 15:00:00 | 777 Broadway, Fl 14, New York, NY 10003 | **True** (Active Version) | $349.50 |

---

## ⚠️ Known Limitations & Deployment Details

- **Local vs Containerized Airflow Execution**:
  The Airflow DAGs (`cdc_ingestion_dag.py`, `warehouse_load_dag.py`, `dbt_run_dag.py`) and failure alert plugin (`alerts.py`) are fully implemented and configured for Apache Airflow with `LocalExecutor` via `docker-compose.yml`. For native Windows local demo runs where Docker is not running, the pipeline utilizes the unified `scripts/run_full_pipeline.py` orchestrator and standalone DAG test runners (`--test-retry`) to simulate task lifecycle and alert callbacks.
- **Local DuckDB Warehouse**:
  Designed for local embedded execution. To point the pipeline to Snowflake, BigQuery, or Amazon Redshift in production, update the connection target in `dbt_project/profiles.yml` (all dbt models use ANSI SQL window functions with zero changes required).
