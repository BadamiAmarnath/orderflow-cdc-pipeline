#!/usr/bin/env python3
"""
OrderFlow: PostgreSQL OLTP Source & Change Simulator
Generates continuous, realistic e-commerce transactions:
- New order placement (inserts into orders and order_items, inventory decrement)
- Order status lifecycle transitions (pending -> shipped -> delivered / cancelled)
- Product price adjustments & promotions (updates to products)
- Customer address & email updates (updates to customers)
- Warehouse inventory restocks (updates to inventory)

Supports both live PostgreSQL connection and embedded transactional engine with identical schema and CDC event capture.
"""

import os
import sys
import time
import random
import argparse
import datetime
import json
import logging
import sqlite3
from typing import Dict, Any, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("simulate_changes")

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "orderflow_oltp")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

LOCAL_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "source_oltp.db")


class OLTPDatabase:
    """Unified interface supporting PostgreSQL with embedded SQLite fallback."""
    def __init__(self, use_postgres: bool = True):
        self.is_postgres = False
        self.conn = None
        os.makedirs(os.path.dirname(LOCAL_DB_PATH), exist_ok=True)

        if use_postgres and DB_PASSWORD:
            try:
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    dbname=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                self.conn.autocommit = True
                self.is_postgres = True
                logger.info(f"Connected to live PostgreSQL database at {DB_HOST}:{DB_PORT}/{DB_NAME}")
            except Exception as e:
                logger.info(f"Live PostgreSQL not reachable ({e}). Using local transactional database ({LOCAL_DB_PATH}).")
                self.conn = sqlite3.connect(LOCAL_DB_PATH)
                self.conn.row_factory = sqlite3.Row
        else:
            self.conn = sqlite3.connect(LOCAL_DB_PATH)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Using local transactional database at {LOCAL_DB_PATH}")

    def execute(self, query: str, params: Tuple = ()) -> Any:
        cur = self.conn.cursor()
        # Adapt parameter placeholders if needed (%s -> ?)
        if not self.is_postgres:
            adapted_query = query.replace("%s", "?")
            cur.execute(adapted_query, params)
            self.conn.commit()
        else:
            cur.execute(query, params)
        return cur

    def fetchone(self, query: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
        cur = self.execute(query, params)
        row = cur.fetchone()
        if row is None:
            return None
        if self.is_postgres:
            return dict(row)
        return dict(row)

    def fetchall(self, query: str, params: Tuple = ()) -> List[Dict[str, Any]]:
        cur = self.execute(query, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    def record_cdc(self, table: str, op: str, before: Optional[Dict], after: Optional[Dict]):
        """Records CDC change event into cdc_change_log."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        b_json = json.dumps(before) if before else None
        a_json = json.dumps(after) if after else None
        self.execute(
            """
            INSERT INTO cdc_change_log (table_name, operation, before_state, after_state, captured_at, processed)
            VALUES (%s, %s, %s, %s, %s, 0)
            """,
            (table, op, b_json, a_json, now)
        )

    def close(self):
        if self.conn:
            self.conn.close()


def seed_database(db: OLTPDatabase):
    """Initializes schema and populates with 50 customers, 30 products, 100 orders, and inventory."""
    logger.info("Initializing schema...")
    
    # 1. Customers
    db.execute("DROP TABLE IF EXISTS cdc_change_log")
    db.execute("DROP TABLE IF EXISTS order_items")
    db.execute("DROP TABLE IF EXISTS orders")
    db.execute("DROP TABLE IF EXISTS inventory")
    db.execute("DROP TABLE IF EXISTS products")
    db.execute("DROP TABLE IF EXISTS customers")

    db.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(150) NOT NULL,
            email VARCHAR(200) NOT NULL UNIQUE,
            address TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(200) NOT NULL,
            category VARCHAR(100) NOT NULL,
            price REAL NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_date TIMESTAMP NOT NULL,
            status VARCHAR(50) NOT NULL,
            total_amount REAL NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE inventory (
            product_id INTEGER PRIMARY KEY,
            warehouse_id VARCHAR(50) NOT NULL,
            stock_level INTEGER NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)

    db.execute("""
        CREATE TABLE cdc_change_log (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name VARCHAR(100) NOT NULL,
            operation VARCHAR(20) NOT NULL,
            before_state TEXT,
            after_state TEXT,
            captured_at TIMESTAMP NOT NULL,
            processed INTEGER DEFAULT 0
        )
    """)

    logger.info("Seeding 50 customers...")
    customers_data = [
        ('Alice Walker', 'alice.walker@example.com', '124 Market St, San Francisco, CA 94105'),
        ('Bob Smith', 'bob.smith@example.com', '789 Elm Ave, Seattle, WA 98101'),
        ('Charlie Brown', 'charlie.brown@example.com', '456 Pine Rd, Austin, TX 78701'),
        ('Diana Prince', 'diana.prince@example.com', '101 Ocean Blvd, Miami, FL 33139'),
        ('Evan Wright', 'evan.wright@example.com', '202 Maple Dr, Denver, CO 80202'),
        ('Fiona Gallagher', 'fiona.g@example.com', '303 Birch Ct, Chicago, IL 60601'),
        ('George Clark', 'george.clark@example.com', '404 Cedar Way, Boston, MA 02108'),
        ('Hannah Abbott', 'hannah.a@example.com', '505 Walnut Ln, New York, NY 10001'),
        ('Ian Malcolm', 'ian.malcolm@example.com', '606 Spruce St, Portland, OR 97201'),
        ('Julia Roberts', 'julia.r@example.com', '707 Ash Blvd, Atlanta, GA 30303'),
        ('Kevin Bacon', 'kevin.bacon@example.com', '808 Willow Way, Nashville, TN 37201'),
        ('Laura Croft', 'laura.croft@example.com', '909 Magnolia Ave, Phoenix, AZ 85001'),
        ('Michael Scott', 'michael.scott@example.com', '1725 Slough Ave, Scranton, PA 18503'),
        ('Nina Simone', 'nina.simone@example.com', '111 Jazz Way, New Orleans, LA 70112'),
        ('Oscar Martinez', 'oscar.m@example.com', '222 Penn Ave, Philadelphia, PA 19104'),
        ('Pam Beesly', 'pam.beesly@example.com', '333 Maple Ave, Scranton, PA 18503'),
        ('Quinn Fabray', 'quinn.f@example.com', '444 High School Rd, Lima, OH 45801'),
        ('Rachel Green', 'rachel.green@example.com', '90 Bedford St, New York, NY 10014'),
        ('Steve Rogers', 'steve.rogers@example.com', '569 56th St, Brooklyn, NY 11220'),
        ('Tony Stark', 'tony.stark@example.com', '10880 Malibu Point, Malibu, CA 90265'),
        ('Uma Thurman', 'uma.thurman@example.com', '555 Sunset Blvd, Los Angeles, CA 90028'),
        ('Victor Stone', 'victor.stone@example.com', '666 Tech Parkway, Detroit, MI 48201'),
        ('Wanda Maximoff', 'wanda.m@example.com', '777 Westview Ct, Newark, NJ 07102'),
        ('Xavier Charles', 'charles.xavier@example.com', '1407 Graymalkin Ln, Salem, NY 10560'),
        ('Yvonne Strahovski', 'yvonne.s@example.com', '888 Bay St, San Diego, CA 92101'),
        ('Zack Snyder', 'zack.s@example.com', '999 Vista Dr, Pasadena, CA 91101'),
        ('Arthur Pendragon', 'arthur.p@example.com', '1 Camelot Way, Las Vegas, NV 89109'),
        ('Bruce Wayne', 'bruce.wayne@example.com', '1007 Mountain Dr, Gotham, NJ 07001'),
        ('Clara Oswald', 'clara.o@example.com', '12 Coal Hill Ln, London, OH 43140'),
        ('David Tennant', 'david.t@example.com', '42 Tardis Way, Seattle, WA 98104'),
        ('Elena Gilbert', 'elena.g@example.com', '2100 Mystic Falls Dr, Richmond, VA 23219'),
        ('Frank Castle', 'frank.castle@example.com', '350 Hells Kitchen Way, New York, NY 10036'),
        ('Gwen Stacy', 'gwen.stacy@example.com', '415 Forest Hills Ave, Queens, NY 11375'),
        ('Harry Potter', 'harry.p@example.com', '4 Privet Dr, Little Whinging, ME 04001'),
        ('Iris West', 'iris.west@example.com', '505 Central City Ave, St. Louis, MO 63101'),
        ('Jack Sparrow', 'jack.s@example.com', '700 Black Pearl Way, Key West, FL 33040'),
        ('Katniss Everdeen', 'katniss.e@example.com', '12 Victor Village, District 12, WV 25001'),
        ('Luke Skywalker', 'luke.s@example.com', '100 Desert Oasis Rd, Tucson, AZ 85701'),
        ('Maya Lin', 'maya.lin@example.com', '880 Monument Way, Washington, DC 20001'),
        ('Nathan Drake', 'nathan.drake@example.com', '404 Uncharted Way, Honolulu, HI 96813'),
        ('Olivia Dunham', 'olivia.d@example.com', '330 Fringe Division Rd, Boston, MA 02115'),
        ('Peter Parker', 'peter.parker@example.com', '20 Ingram St, Queens, NY 11375'),
        ('Quincy Adams', 'quincy.a@example.com', '1825 Presidential Way, Quincy, MA 02169'),
        ('Ron Weasley', 'ron.w@example.com', '1 The Burrow Rd, Ottery, VT 05401'),
        ('Sarah Connor', 'sarah.connor@example.com', '1984 Cybernetic Way, Los Angeles, CA 90012'),
        ('Thomas Anderson', 'neo.anderson@example.com', '101 Matrix Blvd, Chicago, IL 60602'),
        ('Ursula Buffay', 'ursula.b@example.com', '45 Soho Ct, New York, NY 10012'),
        ('Valerie Page', 'valerie.p@example.com', '5 V For Vendetta Rd, London, KY 40741'),
        ('Walter White', 'walter.white@example.com', '308 Negra Arroyo Ln, Albuquerque, NM 87104'),
        ('Zoey Deschanel', 'zoey.d@example.com', '404 Hollywood Hills, Los Angeles, CA 90068')
    ]

    base_time = "2026-08-01 08:00:00"
    for idx, (name, email, addr) in enumerate(customers_data, 1):
        db.execute(
            "INSERT INTO customers (name, email, address, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            (name, email, addr, base_time, base_time)
        )
        db.record_cdc("customers", "insert", None, {
            "customer_id": idx, "name": name, "email": email, "address": addr,
            "created_at": base_time, "updated_at": base_time
        })

    logger.info("Seeding 30 products & inventory...")
    products_data = [
        ('Quantum Pro Wireless Noise-Cancelling Headphones', 'Electronics', 199.99, 'WH-EAST-01', 120),
        ('UltraHD 4K 27-inch IPS Gaming Monitor', 'Electronics', 349.50, 'WH-EAST-01', 45),
        ('Ergonomic Mechanical Keyboard (RGB Cherry MX)', 'Electronics', 129.99, 'WH-WEST-02', 80),
        ('Precision Wireless Laser Mouse 16000 DPI', 'Electronics', 79.99, 'WH-WEST-02', 150),
        ('Thunderbolt 4 Docking Station 12-in-1', 'Electronics', 189.00, 'WH-CENTRAL-03', 60),
        ('Smart Ambient LED Desk Lamp with Qi Charger', 'Electronics', 59.95, 'WH-EAST-01', 95),
        ('Men Classic Merino Wool Crewneck Sweater', 'Apparel', 89.00, 'WH-WEST-02', 110),
        ('Women All-Weather Waterproof Trench Coat', 'Apparel', 175.00, 'WH-EAST-01', 40),
        ('Unisex Organic Cotton Relaxed Fit Hoodie', 'Apparel', 65.00, 'WH-CENTRAL-03', 200),
        ('Performance Stretch Running Shorts', 'Apparel', 42.50, 'WH-WEST-02', 130),
        ('Polarized UV400 Aviator Sunglasses', 'Apparel', 55.00, 'WH-EAST-01', 75),
        ('Genuine Full-Grain Leather Bi-Fold Wallet', 'Apparel', 48.00, 'WH-WEST-02', 85),
        ('Italian Barista Espresso & Cappuccino Machine', 'Home & Kitchen', 299.99, 'WH-CENTRAL-03', 35),
        ('Ceramic Non-Stick 10-Piece Cookware Set', 'Home & Kitchen', 149.95, 'WH-EAST-01', 50),
        ('Smart Wi-Fi Connected Air Purifier HEPA-H13', 'Home & Kitchen', 139.00, 'WH-WEST-02', 65),
        ('Cast Iron Enameled Dutch Oven 6-Quart', 'Home & Kitchen', 99.50, 'WH-CENTRAL-03', 40),
        ('Precision Temperature Digital Pour-Over Kettle', 'Home & Kitchen', 79.00, 'WH-EAST-01', 90),
        ('Heavy-Duty Stainless Steel Chef Knife 8-inch', 'Home & Kitchen', 45.00, 'WH-WEST-02', 115),
        ('Designing Data-Intensive Applications (Hardcover)', 'Books', 49.99, 'WH-CENTRAL-03', 300),
        ('Fundamentals of Data Engineering (Paperback)', 'Books', 44.50, 'WH-CENTRAL-03', 250),
        ('The Pragmatic Programmer: 20th Anniversary Edition', 'Books', 39.95, 'WH-EAST-01', 180),
        ('System Design Interview - Volume 1 & 2 Bundle', 'Books', 69.00, 'WH-WEST-02', 140),
        ('Staff Engineer: Leadership Beyond Management', 'Books', 32.00, 'WH-CENTRAL-03', 90),
        ('Building Microservices (2nd Edition)', 'Books', 46.50, 'WH-EAST-01', 110),
        ('Adjustable Dumbbell Set (5 to 52.5 lbs Pair)', 'Fitness', 299.00, 'WH-WEST-02', 25),
        ('High-Density Non-Slip Yoga & Pilates Mat', 'Fitness', 38.00, 'WH-CENTRAL-03', 160),
        ('Heavy Duty Multi-Grip Pull-Up Bar Doorway', 'Fitness', 49.99, 'WH-EAST-01', 70),
        ('Deep Tissue Percussion Muscle Massage Gun', 'Fitness', 119.00, 'WH-WEST-02', 55),
        ('Smart Fitness Activity Tracker with Heart Monitor', 'Fitness', 89.95, 'WH-CENTRAL-03', 80),
        ('Latex Resistance Exercise Loops Set of 5', 'Fitness', 24.50, 'WH-EAST-01', 220)
    ]

    for idx, (name, cat, price, wh, stock) in enumerate(products_data, 1):
        db.execute(
            "INSERT INTO products (name, category, price, updated_at) VALUES (%s, %s, %s, %s)",
            (name, cat, price, base_time)
        )
        db.record_cdc("products", "insert", None, {
            "product_id": idx, "name": name, "category": cat, "price": price, "updated_at": base_time
        })

        db.execute(
            "INSERT INTO inventory (product_id, warehouse_id, stock_level, updated_at) VALUES (%s, %s, %s, %s)",
            (idx, wh, stock, base_time)
        )
        db.record_cdc("inventory", "insert", None, {
            "product_id": idx, "warehouse_id": wh, "stock_level": stock, "updated_at": base_time
        })

    logger.info("Seeding 100 initial orders and order items...")
    statuses = ['delivered', 'delivered', 'shipped', 'pending', 'cancelled']
    for i in range(1, 101):
        cust_id = ((i * 7) % 50) + 1
        prod_id_1 = ((i * 3) % 30) + 1
        prod_id_2 = ((i * 5 + 2) % 30) + 1
        qty_1 = (i % 3) + 1
        qty_2 = ((i + 1) % 2) + 1
        status = statuses[i % 5]
        order_dt = (datetime.datetime(2026, 8, 1, 0, 0, 0, tzinfo=datetime.timezone.utc) +
                    datetime.timedelta(hours=4 * i, minutes=15 * (i % 4))).isoformat()

        p1 = products_data[prod_id_1 - 1]
        p2 = products_data[prod_id_2 - 1]
        price_1 = p1[2]
        price_2 = p2[2]
        total_amt = round((price_1 * qty_1) + (price_2 * qty_2), 2)

        cur = db.execute(
            "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)",
            (cust_id, order_dt, status, total_amt)
        )
        order_id = cur.lastrowid if not db.is_postgres else i

        db.record_cdc("orders", "insert", None, {
            "order_id": order_id, "customer_id": cust_id, "order_date": order_dt,
            "status": status, "total_amount": total_amt
        })

        # Items
        cur1 = db.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, prod_id_1, qty_1, price_1)
        )
        item1_id = cur1.lastrowid if not db.is_postgres else (i * 2 - 1)
        db.record_cdc("order_items", "insert", None, {
            "order_item_id": item1_id, "order_id": order_id, "product_id": prod_id_1,
            "quantity": qty_1, "unit_price": price_1
        })

        cur2 = db.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, prod_id_2, qty_2, price_2)
        )
        item2_id = cur2.lastrowid if not db.is_postgres else (i * 2)
        db.record_cdc("order_items", "insert", None, {
            "order_item_id": item2_id, "order_id": order_id, "product_id": prod_id_2,
            "quantity": qty_2, "unit_price": price_2
        })

    logger.info("Successfully seeded database: 50 customers, 30 products, 30 inventory rows, 100 orders, 200 items.")


class ChangeSimulator:
    def __init__(self, db: OLTPDatabase):
        self.db = db
        self.streets = ["Pine Rd", "Oak Ave", "Cedar St", "Maple Blvd", "Beacon St", "Mission St", "Broadway", "Lakeview Dr"]
        self.cities = [
            ("San Francisco", "CA", "94107"),
            ("Austin", "TX", "78702"),
            ("Seattle", "WA", "98103"),
            ("New York", "NY", "10002"),
            ("Denver", "CO", "80204"),
            ("Chicago", "IL", "60611"),
            ("Boston", "MA", "02116"),
            ("Miami", "FL", "33101")
        ]

    def simulate_order_creation(self) -> Optional[Dict[str, Any]]:
        """Simulates customer placing a new order with items & stock decrement."""
        cust = self.db.fetchone("SELECT customer_id, name FROM customers ORDER BY RANDOM() LIMIT 1")
        if not cust:
            return None
        cust_id = cust["customer_id"]

        prod_count = random.randint(1, 3)
        prods = self.db.fetchall("SELECT product_id, name, price FROM products ORDER BY RANDOM() LIMIT %s", (prod_count,))
        if not prods:
            return None

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        total_amount = 0.0
        items_data = []

        for p in prods:
            qty = random.randint(1, 3)
            price = float(p["price"])
            total_amount += (price * qty)
            items_data.append((p["product_id"], qty, price))

        total_amount = round(total_amount, 2)
        cur = self.db.execute(
            "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (%s, %s, 'pending', %s)",
            (cust_id, now, total_amount)
        )
        order_id = cur.lastrowid

        self.db.record_cdc("orders", "insert", None, {
            "order_id": order_id, "customer_id": cust_id, "order_date": now,
            "status": "pending", "total_amount": total_amount
        })

        for pid, qty, price in items_data:
            cur_item = self.db.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                (order_id, pid, qty, price)
            )
            item_id = cur_item.lastrowid
            self.db.record_cdc("order_items", "insert", None, {
                "order_item_id": item_id, "order_id": order_id, "product_id": pid,
                "quantity": qty, "unit_price": price
            })

            # Inventory decrement
            inv_before = self.db.fetchone("SELECT product_id, warehouse_id, stock_level, updated_at FROM inventory WHERE product_id = %s", (pid,))
            if inv_before:
                new_stock = max(0, inv_before["stock_level"] - qty)
                self.db.execute(
                    "UPDATE inventory SET stock_level = %s, updated_at = %s WHERE product_id = %s",
                    (new_stock, now, pid)
                )
                inv_after = dict(inv_before)
                inv_after["stock_level"] = new_stock
                inv_after["updated_at"] = now
                self.db.record_cdc("inventory", "update", inv_before, inv_after)

        logger.info(f"[ACTION: NEW ORDER] Order #{order_id} created for Customer #{cust_id} | Total: ${total_amount:.2f} | Items: {len(items_data)}")
        return {"action": "new_order", "order_id": order_id, "customer_id": cust_id, "total": total_amount}

    def simulate_order_status_transition(self) -> Optional[Dict[str, Any]]:
        """Transitions order status: pending -> shipped -> delivered / cancelled."""
        order = self.db.fetchone(
            "SELECT order_id, customer_id, order_date, status, total_amount FROM orders WHERE status IN ('pending', 'shipped') ORDER BY RANDOM() LIMIT 1"
        )
        if not order:
            return None

        order_id = order["order_id"]
        current_status = order["status"]
        if current_status == "pending":
            new_status = random.choice(["shipped", "shipped", "shipped", "cancelled"])
        elif current_status == "shipped":
            new_status = "delivered"
        else:
            return None

        self.db.execute("UPDATE orders SET status = %s WHERE order_id = %s", (new_status, order_id))
        order_after = dict(order)
        order_after["status"] = new_status
        self.db.record_cdc("orders", "update", order, order_after)

        logger.info(f"[ACTION: STATUS UPDATE] Order #{order_id} status changed: {current_status} -> {new_status}")
        return {"action": "status_update", "order_id": order_id, "from": current_status, "to": new_status}

    def simulate_product_price_change(self) -> Optional[Dict[str, Any]]:
        """Simulates product price updates (repricing/promotions) for SCD2 tracking."""
        prod = self.db.fetchone("SELECT product_id, name, category, price, updated_at FROM products ORDER BY RANDOM() LIMIT 1")
        if not prod:
            return None

        old_price = float(prod["price"])
        delta_pct = random.uniform(-0.15, 0.15)
        new_price = round(max(5.0, old_price * (1 + delta_pct)), 2)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        self.db.execute(
            "UPDATE products SET price = %s, updated_at = %s WHERE product_id = %s",
            (new_price, now, prod["product_id"])
        )
        prod_after = dict(prod)
        prod_after["price"] = new_price
        prod_after["updated_at"] = now
        self.db.record_cdc("products", "update", prod, prod_after)

        logger.info(f"[ACTION: PRICE CHANGE (SCD2)] Product #{prod['product_id']} ('{prod['name']}') price: ${old_price:.2f} -> ${new_price:.2f}")
        return {"action": "price_change", "product_id": prod["product_id"], "old_price": old_price, "new_price": new_price}

    def simulate_customer_address_update(self) -> Optional[Dict[str, Any]]:
        """Simulates customer moving/updating address or email for SCD2 tracking."""
        cust = self.db.fetchone("SELECT customer_id, name, email, address, created_at, updated_at FROM customers ORDER BY RANDOM() LIMIT 1")
        if not cust:
            return None

        street_num = random.randint(100, 9999)
        street = random.choice(self.streets)
        city, state, zip_code = random.choice(self.cities)
        new_address = f"{street_num} {street}, {city}, {state} {zip_code}"
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        self.db.execute(
            "UPDATE customers SET address = %s, updated_at = %s WHERE customer_id = %s",
            (new_address, now, cust["customer_id"])
        )
        cust_after = dict(cust)
        cust_after["address"] = new_address
        cust_after["updated_at"] = now
        self.db.record_cdc("customers", "update", cust, cust_after)

        logger.info(f"[ACTION: CUSTOMER UPDATE (SCD2)] Customer #{cust['customer_id']} ('{cust['name']}') new address: '{new_address}'")
        return {"action": "customer_update", "customer_id": cust["customer_id"], "new_address": new_address}

    def simulate_inventory_restock(self) -> Optional[Dict[str, Any]]:
        """Simulates inventory restock arrival."""
        inv = self.db.fetchone("SELECT product_id, warehouse_id, stock_level, updated_at FROM inventory ORDER BY RANDOM() LIMIT 1")
        if not inv:
            return None

        restock_amt = random.randint(20, 100)
        new_level = inv["stock_level"] + restock_amt
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        self.db.execute(
            "UPDATE inventory SET stock_level = %s, updated_at = %s WHERE product_id = %s",
            (new_level, now, inv["product_id"])
        )
        inv_after = dict(inv)
        inv_after["stock_level"] = new_level
        inv_after["updated_at"] = now
        self.db.record_cdc("inventory", "update", inv, inv_after)

        logger.info(f"[ACTION: INVENTORY RESTOCK] Product #{inv['product_id']} restocked +{restock_amt} units (new level: {new_level})")
        return {"action": "restock", "product_id": inv["product_id"], "restock": restock_amt}

    def run_step(self):
        """Executes a single randomized simulation action weighted by realism."""
        action_weights = [
            (self.simulate_order_creation, 0.40),
            (self.simulate_order_status_transition, 0.25),
            (self.simulate_product_price_change, 0.15),
            (self.simulate_customer_address_update, 0.10),
            (self.simulate_inventory_restock, 0.10),
        ]
        func = random.choices(
            [a[0] for a in action_weights],
            weights=[a[1] for a in action_weights]
        )[0]
        return func()

    def get_stats(self) -> Dict[str, Any]:
        """Returns row counts and metrics."""
        cust_count = self.db.fetchone("SELECT COUNT(*) AS count FROM customers")["count"]
        prod_count = self.db.fetchone("SELECT COUNT(*) AS count FROM products")["count"]
        order_count = self.db.fetchone("SELECT COUNT(*) AS count FROM orders")["count"]
        items_count = self.db.fetchone("SELECT COUNT(*) AS count FROM order_items")["count"]
        inv_count = self.db.fetchone("SELECT COUNT(*) AS count FROM inventory")["count"]
        cdc_count = self.db.fetchone("SELECT COUNT(*) AS count FROM cdc_change_log")["count"]
        cdc_unprocessed = self.db.fetchone("SELECT COUNT(*) AS count FROM cdc_change_log WHERE processed = 0")["count"]

        return {
            "customers": cust_count,
            "products": prod_count,
            "inventory": inv_count,
            "orders": order_count,
            "order_items": items_count,
            "total_cdc_events": cdc_count,
            "unprocessed_cdc_events": cdc_unprocessed
        }


def main():
    parser = argparse.ArgumentParser(description="OrderFlow OLTP Change Simulator")
    parser.add_argument("--seed", action="store_true", help="Initialize database schema and seed data")
    parser.add_argument("--seed-only", action="store_true", help="Initialize database and exit")
    parser.add_argument("--duration", type=int, default=0, help="Run continuous simulation for N seconds")
    parser.add_argument("--steps", type=int, default=10, help="Run N discrete simulation steps")
    parser.add_argument("--rate", type=float, default=0.2, help="Delay in seconds between simulation steps")
    args = parser.parse_args()

    db = OLTPDatabase()

    if args.seed or args.seed_only:
        seed_database(db)
        if args.seed_only:
            stats = ChangeSimulator(db).get_stats()
            logger.info(f"Database Stats after seeding:\n{json.dumps(stats, indent=2)}")
            db.close()
            return

    simulator = ChangeSimulator(db)
    initial_stats = simulator.get_stats()
    logger.info(f"Initial Database State:\n{json.dumps(initial_stats, indent=2)}")

    if args.duration > 0:
        logger.info(f"Starting continuous simulation for {args.duration} seconds...")
        end_time = time.time() + args.duration
        step_count = 0
        while time.time() < end_time:
            simulator.run_step()
            step_count += 1
            time.sleep(args.rate)
        logger.info(f"Completed {step_count} simulation events in {args.duration} seconds.")
    else:
        logger.info(f"Running {args.steps} discrete simulation steps...")
        for _ in range(args.steps):
            simulator.run_step()
            time.sleep(args.rate)

    final_stats = simulator.get_stats()
    logger.info(f"Final Database State:\n{json.dumps(final_stats, indent=2)}")
    db.close()


if __name__ == "__main__":
    main()
