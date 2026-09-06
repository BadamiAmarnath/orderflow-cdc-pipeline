import duckdb
import os
import sys

WAREHOUSE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "warehouse", "orderflow.duckdb")

def inspect():
    if not os.path.exists(WAREHOUSE_PATH):
        print(f"Error: Warehouse not found at {WAREHOUSE_PATH}")
        return

    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    
    print("=" * 80)
    print("  ORDERFLOW DUCKDB ANALYTICAL WAREHOUSE INSPECTOR")
    print("=" * 80)
    
    # 1. Table Summary
    print("\n[1] WAREHOUSE TABLES & ROW COUNTS:")
    tables = con.execute("SHOW TABLES;").fetchall()
    table_names = [t[0] for t in tables]
    
    for t in table_names:
        count = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"   * {t.ljust(25)} : {count} rows")
        
    # 2. Daily Sales Summary (Mart)
    print("\n" + "=" * 80)
    print("[2] DAILY SALES AGGREGATION (agg_daily_sales - Latest 5 days):")
    print("=" * 80)
    df_sales = con.execute("""
        SELECT sale_date, total_orders, gross_revenue, delivered_revenue, delivered_orders, average_order_value, cancellation_rate_pct 
        FROM agg_daily_sales 
        ORDER BY sale_date DESC 
        LIMIT 5;
    """).fetchdf()
    print(df_sales.to_string(index=False))

    # 3. Fact Orders (Marts)
    print("\n" + "=" * 80)
    print("[3] FACT ORDERS SAMPLE (fct_orders - Latest 5 orders):")
    print("=" * 80)
    df_orders = con.execute("""
        SELECT order_id, customer_key, customer_id, order_date, status, total_amount, total_items, total_quantity
        FROM fct_orders 
        ORDER BY order_date DESC, order_id DESC 
        LIMIT 5;
    """).fetchdf()
    print(df_orders.to_string(index=False))

    # 4. SCD Type 2 Customers
    print("\n" + "=" * 80)
    print("[4] SCD TYPE 2 CUSTOMERS SAMPLE (dim_customers_scd2):")
    print("=" * 80)
    df_cust = con.execute("""
        SELECT customer_key, customer_id, name, address, is_current, valid_from, valid_to
        FROM dim_customers_scd2
        ORDER BY customer_id ASC, customer_key ASC
        LIMIT 5;
    """).fetchdf()
    print(df_cust.to_string(index=False))

    # 5. SCD Type 2 Products
    print("\n" + "=" * 80)
    print("[5] SCD TYPE 2 PRODUCTS WITH MULTIPLE HISTORICAL VERSIONS (dim_products_scd2):")
    print("=" * 80)
    df_prod = con.execute("""
        SELECT product_key, product_id, name, price, is_current, valid_from, valid_to
        FROM dim_products_scd2
        WHERE product_id IN (
            SELECT product_id FROM dim_products_scd2 GROUP BY product_id HAVING COUNT(*) > 1
        )
        ORDER BY product_id ASC, product_key ASC;
    """).fetchdf()
    if not df_prod.empty:
        print(df_prod.to_string(index=False))
    else:
        print("   (All products currently at version 1)")

    print("\n" + "=" * 80)
    print("DATA FILES LOCATION ON DISK:")
    print(f"   * DuckDB Warehouse: {os.path.abspath(WAREHOUSE_PATH)}")
    print(f"   * Raw CDC Events  : {os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'cdc_events'))}")
    print(f"   * SQLite OLTP DB  : {os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'source_oltp.db'))}")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    inspect()
