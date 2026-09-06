-- ============================================================================
-- OrderFlow: E-Commerce Operational Database (PostgreSQL) Schema & Seed Script
-- ============================================================================

-- Clean up existing tables
DROP TABLE IF EXISTS cdc_change_log CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- 1. Customers Table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(200) NOT NULL UNIQUE,
    address TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Products Table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Orders Table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'shipped', 'delivered', 'cancelled')),
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00
);

-- 4. Order Items Table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0)
);

-- 5. Inventory Table
CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY REFERENCES products(product_id) ON DELETE CASCADE,
    warehouse_id VARCHAR(50) NOT NULL,
    stock_level INTEGER NOT NULL CHECK (stock_level >= 0),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- CDC Change Capture Audit Table & Triggers (Debezium-compatible capture)
-- ============================================================================
CREATE TABLE cdc_change_log (
    change_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(20) NOT NULL, -- 'insert' (c), 'update' (u), 'delete' (d)
    before_state JSONB,
    after_state JSONB,
    captured_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_cdc_processed ON cdc_change_log(processed, captured_at);

-- Generic Trigger Function for CDC Event Capture
CREATE OR REPLACE FUNCTION fn_capture_cdc_change()
RETURNS TRIGGER AS $$
DECLARE
    v_op VARCHAR(20);
    v_before JSONB := NULL;
    v_after JSONB := NULL;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_op := 'insert';
        v_after := to_jsonb(NEW);
    ELSIF (TG_OP = 'UPDATE') THEN
        v_op := 'update';
        v_before := to_jsonb(OLD);
        v_after := to_jsonb(NEW);
    ELSIF (TG_OP = 'DELETE') THEN
        v_op := 'delete';
        v_before := to_jsonb(OLD);
    END IF;

    INSERT INTO cdc_change_log (table_name, operation, before_state, after_state, captured_at)
    VALUES (TG_TABLE_NAME, v_op, v_before, v_after, CURRENT_TIMESTAMP);

    IF (TG_OP = 'DELETE') THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Attach CDC triggers to operational tables
CREATE TRIGGER trg_cdc_customers
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW EXECUTE FUNCTION fn_capture_cdc_change();

CREATE TRIGGER trg_cdc_products
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW EXECUTE FUNCTION fn_capture_cdc_change();

CREATE TRIGGER trg_cdc_orders
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION fn_capture_cdc_change();

CREATE TRIGGER trg_cdc_order_items
AFTER INSERT OR UPDATE OR DELETE ON order_items
FOR EACH ROW EXECUTE FUNCTION fn_capture_cdc_change();

CREATE TRIGGER trg_cdc_inventory
AFTER INSERT OR UPDATE OR DELETE ON inventory
FOR EACH ROW EXECUTE FUNCTION fn_capture_cdc_change();

-- ============================================================================
-- Seed Data: 50 Customers, 30 Products, Inventory, and 100 Initial Orders
-- ============================================================================

-- Customers Seed (50 rows)
INSERT INTO customers (name, email, address, created_at, updated_at) VALUES
('Alice Walker', 'alice.walker@example.com', '124 Market St, San Francisco, CA 94105', '2026-08-01 08:30:00+00', '2026-08-01 08:30:00+00'),
('Bob Smith', 'bob.smith@example.com', '789 Elm Ave, Seattle, WA 98101', '2026-08-01 09:15:00+00', '2026-08-01 09:15:00+00'),
('Charlie Brown', 'charlie.brown@example.com', '456 Pine Rd, Austin, TX 78701', '2026-08-02 10:00:00+00', '2026-08-02 10:00:00+00'),
('Diana Prince', 'diana.prince@example.com', '101 Ocean Blvd, Miami, FL 33139', '2026-08-02 11:20:00+00', '2026-08-02 11:20:00+00'),
('Evan Wright', 'evan.wright@example.com', '202 Maple Dr, Denver, CO 80202', '2026-08-03 12:45:00+00', '2026-08-03 12:45:00+00'),
('Fiona Gallagher', 'fiona.g@example.com', '303 Birch Ct, Chicago, IL 60601', '2026-08-03 14:10:00+00', '2026-08-03 14:10:00+00'),
('George Clark', 'george.clark@example.com', '404 Cedar Way, Boston, MA 02108', '2026-08-04 15:30:00+00', '2026-08-04 15:30:00+00'),
('Hannah Abbott', 'hannah.a@example.com', '505 Walnut Ln, New York, NY 10001', '2026-08-04 16:00:00+00', '2026-08-04 16:00:00+00'),
('Ian Malcolm', 'ian.malcolm@example.com', '606 Spruce St, Portland, OR 97201', '2026-08-05 09:00:00+00', '2026-08-05 09:00:00+00'),
('Julia Roberts', 'julia.r@example.com', '707 Ash Blvd, Atlanta, GA 30303', '2026-08-05 10:30:00+00', '2026-08-05 10:30:00+00'),
('Kevin Bacon', 'kevin.bacon@example.com', '808 Willow Way, Nashville, TN 37201', '2026-08-06 11:15:00+00', '2026-08-06 11:15:00+00'),
('Laura Croft', 'laura.croft@example.com', '909 Magnolia Ave, Phoenix, AZ 85001', '2026-08-06 13:00:00+00', '2026-08-06 13:00:00+00'),
('Michael Scott', 'michael.scott@example.com', '1725 Slough Ave, Scranton, PA 18503', '2026-08-07 09:00:00+00', '2026-08-07 09:00:00+00'),
('Nina Simone', 'nina.simone@example.com', '111 Jazz Way, New Orleans, LA 70112', '2026-08-07 10:45:00+00', '2026-08-07 10:45:00+00'),
('Oscar Martinez', 'oscar.m@example.com', '222 Penn Ave, Philadelphia, PA 19104', '2026-08-08 11:30:00+00', '2026-08-08 11:30:00+00'),
('Pam Beesly', 'pam.beesly@example.com', '333 Maple Ave, Scranton, PA 18503', '2026-08-08 14:00:00+00', '2026-08-08 14:00:00+00'),
('Quinn Fabray', 'quinn.f@example.com', '444 High School Rd, Lima, OH 45801', '2026-08-09 15:20:00+00', '2026-08-09 15:20:00+00'),
('Rachel Green', 'rachel.green@example.com', '90 Bedford St, New York, NY 10014', '2026-08-09 16:50:00+00', '2026-08-09 16:50:00+00'),
('Steve Rogers', 'steve.rogers@example.com', '569 56th St, Brooklyn, NY 11220', '2026-08-10 10:00:00+00', '2026-08-10 10:00:00+00'),
('Tony Stark', 'tony.stark@example.com', '10880 Malibu Point, Malibu, CA 90265', '2026-08-10 11:45:00+00', '2026-08-10 11:45:00+00'),
('Uma Thurman', 'uma.thurman@example.com', '555 Sunset Blvd, Los Angeles, CA 90028', '2026-08-11 13:10:00+00', '2026-08-11 13:10:00+00'),
('Victor Stone', 'victor.stone@example.com', '666 Tech Parkway, Detroit, MI 48201', '2026-08-11 14:30:00+00', '2026-08-11 14:30:00+00'),
('Wanda Maximoff', 'wanda.m@example.com', '777 Westview Ct, Newark, NJ 07102', '2026-08-12 09:15:00+00', '2026-08-12 09:15:00+00'),
('Xavier Charles', 'charles.xavier@example.com', '1407 Graymalkin Ln, Salem, NY 10560', '2026-08-12 11:00:00+00', '2026-08-12 11:00:00+00'),
('Yvonne Strahovski', 'yvonne.s@example.com', '888 Bay St, San Diego, CA 92101', '2026-08-13 12:30:00+00', '2026-08-13 12:30:00+00'),
('Zack Snyder', 'zack.s@example.com', '999 Vista Dr, Pasadena, CA 91101', '2026-08-13 15:45:00+00', '2026-08-13 15:45:00+00'),
('Arthur Pendragon', 'arthur.p@example.com', '1 Camelot Way, Las Vegas, NV 89109', '2026-08-14 08:00:00+00', '2026-08-14 08:00:00+00'),
('Bruce Wayne', 'bruce.wayne@example.com', '1007 Mountain Dr, Gotham, NJ 07001', '2026-08-14 10:20:00+00', '2026-08-14 10:20:00+00'),
('Clara Oswald', 'clara.o@example.com', '12 Coal Hill Ln, London, OH 43140', '2026-08-15 11:35:00+00', '2026-08-15 11:35:00+00'),
('David Tennant', 'david.t@example.com', '42 Tardis Way, Seattle, WA 98104', '2026-08-15 13:50:00+00', '2026-08-15 13:50:00+00'),
('Elena Gilbert', 'elena.g@example.com', '2100 Mystic Falls Dr, Richmond, VA 23219', '2026-08-16 09:10:00+00', '2026-08-16 09:10:00+00'),
('Frank Castle', 'frank.castle@example.com', '350 Hells Kitchen Way, New York, NY 10036', '2026-08-16 11:40:00+00', '2026-08-16 11:40:00+00'),
('Gwen Stacy', 'gwen.stacy@example.com', '415 Forest Hills Ave, Queens, NY 11375', '2026-08-17 12:15:00+00', '2026-08-17 12:15:00+00'),
('Harry Potter', 'harry.p@example.com', '4 Privet Dr, Little Whinging, ME 04001', '2026-08-17 14:00:00+00', '2026-08-17 14:00:00+00'),
('Iris West', 'iris.west@example.com', '505 Central City Ave, St. Louis, MO 63101', '2026-08-18 10:10:00+00', '2026-08-18 10:10:00+00'),
('Jack Sparrow', 'jack.s@example.com', '700 Black Pearl Way, Key West, FL 33040', '2026-08-18 11:45:00+00', '2026-08-18 11:45:00+00'),
('Katniss Everdeen', 'katniss.e@example.com', '12 Victor Village, District 12, WV 25001', '2026-08-19 13:20:00+00', '2026-08-19 13:20:00+00'),
('Luke Skywalker', 'luke.s@example.com', '100 Desert Oasis Rd, Tucson, AZ 85701', '2026-08-19 15:30:00+00', '2026-08-19 15:30:00+00'),
('Maya Lin', 'maya.lin@example.com', '880 Monument Way, Washington, DC 20001', '2026-08-20 09:00:00+00', '2026-08-20 09:00:00+00'),
('Nathan Drake', 'nathan.drake@example.com', '404 Uncharted Way, Honolulu, HI 96813', '2026-08-20 11:15:00+00', '2026-08-20 11:15:00+00'),
('Olivia Dunham', 'olivia.d@example.com', '330 Fringe Division Rd, Boston, MA 02115', '2026-08-21 12:45:00+00', '2026-08-21 12:45:00+00'),
('Peter Parker', 'peter.parker@example.com', '20 Ingram St, Queens, NY 11375', '2026-08-21 14:10:00+00', '2026-08-21 14:10:00+00'),
('Quincy Adams', 'quincy.a@example.com', '1825 Presidential Way, Quincy, MA 02169', '2026-08-22 10:00:00+00', '2026-08-22 10:00:00+00'),
('Ron Weasley', 'ron.w@example.com', '1 The Burrow Rd, Ottery, VT 05401', '2026-08-22 11:30:00+00', '2026-08-22 11:30:00+00'),
('Sarah Connor', 'sarah.connor@example.com', '1984 Cybernetic Way, Los Angeles, CA 90012', '2026-08-23 13:00:00+00', '2026-08-23 13:00:00+00'),
('Thomas Anderson', 'neo.anderson@example.com', '101 Matrix Blvd, Chicago, IL 60602', '2026-08-23 14:45:00+00', '2026-08-23 14:45:00+00'),
('Ursula Buffay', 'ursula.b@example.com', '45 Soho Ct, New York, NY 10012', '2026-08-24 09:30:00+00', '2026-08-24 09:30:00+00'),
('Valerie Page', 'valerie.p@example.com', '5 V For Vendetta Rd, London, KY 40741', '2026-08-24 11:00:00+00', '2026-08-24 11:00:00+00'),
('Walter White', 'walter.white@example.com', '308 Negra Arroyo Ln, Albuquerque, NM 87104', '2026-08-25 12:20:00+00', '2026-08-25 12:20:00+00'),
('Zoey Deschanel', 'zoey.d@example.com', '404 Hollywood Hills, Los Angeles, CA 90068', '2026-08-25 14:00:00+00', '2026-08-25 14:00:00+00');

-- Products Seed (30 rows across 5 categories)
INSERT INTO products (name, category, price, updated_at) VALUES
('Quantum Pro Wireless Noise-Cancelling Headphones', 'Electronics', 199.99, '2026-08-01 08:00:00+00'),
('UltraHD 4K 27-inch IPS Gaming Monitor', 'Electronics', 349.50, '2026-08-01 08:00:00+00'),
('Ergonomic Mechanical Keyboard (RGB Cherry MX)', 'Electronics', 129.99, '2026-08-01 08:00:00+00'),
('Precision Wireless Laser Mouse 16000 DPI', 'Electronics', 79.99, '2026-08-01 08:00:00+00'),
('Thunderbolt 4 Docking Station 12-in-1', 'Electronics', 189.00, '2026-08-01 08:00:00+00'),
('Smart Ambient LED Desk Lamp with Qi Charger', 'Electronics', 59.95, '2026-08-01 08:00:00+00'),
('Men Classic Merino Wool Crewneck Sweater', 'Apparel', 89.00, '2026-08-01 08:00:00+00'),
('Women All-Weather Waterproof Trench Coat', 'Apparel', 175.00, '2026-08-01 08:00:00+00'),
('Unisex Organic Cotton Relaxed Fit Hoodie', 'Apparel', 65.00, '2026-08-01 08:00:00+00'),
('Performance Stretch Running Shorts', 'Apparel', 42.50, '2026-08-01 08:00:00+00'),
('Polarized UV400 Aviator Sunglasses', 'Apparel', 55.00, '2026-08-01 08:00:00+00'),
('Genuine Full-Grain Leather Bi-Fold Wallet', 'Apparel', 48.00, '2026-08-01 08:00:00+00'),
('Italian Barista Espresso & Cappuccino Machine', 'Home & Kitchen', 299.99, '2026-08-01 08:00:00+00'),
('Ceramic Non-Stick 10-Piece Cookware Set', 'Home & Kitchen', 149.95, '2026-08-01 08:00:00+00'),
('Smart Wi-Fi Connected Air Purifier HEPA-H13', 'Home & Kitchen', 139.00, '2026-08-01 08:00:00+00'),
('Cast Iron Enameled Dutch Oven 6-Quart', 'Home & Kitchen', 99.50, '2026-08-01 08:00:00+00'),
('Precision Temperature Digital Pour-Over Kettle', 'Home & Kitchen', 79.00, '2026-08-01 08:00:00+00'),
('Heavy-Duty Stainless Steel Chef Knife 8-inch', 'Home & Kitchen', 45.00, '2026-08-01 08:00:00+00'),
('Designing Data-Intensive Applications (Hardcover)', 'Books', 49.99, '2026-08-01 08:00:00+00'),
('Fundamentals of Data Engineering (Paperback)', 'Books', 44.50, '2026-08-01 08:00:00+00'),
('The Pragmatic Programmer: 20th Anniversary Edition', 'Books', 39.95, '2026-08-01 08:00:00+00'),
('System Design Interview - Volume 1 & 2 Bundle', 'Books', 69.00, '2026-08-01 08:00:00+00'),
('Staff Engineer: Leadership Beyond Management', 'Books', 32.00, '2026-08-01 08:00:00+00'),
('Building Microservices (2nd Edition)', 'Books', 46.50, '2026-08-01 08:00:00+00'),
('Adjustable Dumbbell Set (5 to 52.5 lbs Pair)', 'Fitness', 299.00, '2026-08-01 08:00:00+00'),
('High-Density Non-Slip Yoga & Pilates Mat', 'Fitness', 38.00, '2026-08-01 08:00:00+00'),
('Heavy Duty Multi-Grip Pull-Up Bar Doorway', 'Fitness', 49.99, '2026-08-01 08:00:00+00'),
('Deep Tissue Percussion Muscle Massage Gun', 'Fitness', 119.00, '2026-08-01 08:00:00+00'),
('Smart Fitness Activity Tracker with Heart Monitor', 'Fitness', 89.95, '2026-08-01 08:00:00+00'),
('Latex Resistance Exercise Loops Set of 5', 'Fitness', 24.50, '2026-08-01 08:00:00+00');

-- Inventory Seed (30 rows)
INSERT INTO inventory (product_id, warehouse_id, stock_level, updated_at) VALUES
(1, 'WH-EAST-01', 120, '2026-08-01 08:00:00+00'),
(2, 'WH-EAST-01', 45, '2026-08-01 08:00:00+00'),
(3, 'WH-WEST-02', 80, '2026-08-01 08:00:00+00'),
(4, 'WH-WEST-02', 150, '2026-08-01 08:00:00+00'),
(5, 'WH-CENTRAL-03', 60, '2026-08-01 08:00:00+00'),
(6, 'WH-EAST-01', 95, '2026-08-01 08:00:00+00'),
(7, 'WH-WEST-02', 110, '2026-08-01 08:00:00+00'),
(8, 'WH-EAST-01', 40, '2026-08-01 08:00:00+00'),
(9, 'WH-CENTRAL-03', 200, '2026-08-01 08:00:00+00'),
(10, 'WH-WEST-02', 130, '2026-08-01 08:00:00+00'),
(11, 'WH-EAST-01', 75, '2026-08-01 08:00:00+00'),
(12, 'WH-WEST-02', 85, '2026-08-01 08:00:00+00'),
(13, 'WH-CENTRAL-03', 35, '2026-08-01 08:00:00+00'),
(14, 'WH-EAST-01', 50, '2026-08-01 08:00:00+00'),
(15, 'WH-WEST-02', 65, '2026-08-01 08:00:00+00'),
(16, 'WH-CENTRAL-03', 40, '2026-08-01 08:00:00+00'),
(17, 'WH-EAST-01', 90, '2026-08-01 08:00:00+00'),
(18, 'WH-WEST-02', 115, '2026-08-01 08:00:00+00'),
(19, 'WH-CENTRAL-03', 300, '2026-08-01 08:00:00+00'),
(20, 'WH-CENTRAL-03', 250, '2026-08-01 08:00:00+00'),
(21, 'WH-EAST-01', 180, '2026-08-01 08:00:00+00'),
(22, 'WH-WEST-02', 140, '2026-08-01 08:00:00+00'),
(23, 'WH-CENTRAL-03', 90, '2026-08-01 08:00:00+00'),
(24, 'WH-EAST-01', 110, '2026-08-01 08:00:00+00'),
(25, 'WH-WEST-02', 25, '2026-08-01 08:00:00+00'),
(26, 'WH-CENTRAL-03', 160, '2026-08-01 08:00:00+00'),
(27, 'WH-EAST-01', 70, '2026-08-01 08:00:00+00'),
(28, 'WH-WEST-02', 55, '2026-08-01 08:00:00+00'),
(29, 'WH-CENTRAL-03', 80, '2026-08-01 08:00:00+00'),
(30, 'WH-EAST-01', 220, '2026-08-01 08:00:00+00');

-- 100 Initial Orders & Order Items Seed
DO $$
DECLARE
    v_order_id INT;
    v_cust_id INT;
    v_prod_id_1 INT;
    v_prod_id_2 INT;
    v_price_1 NUMERIC(10,2);
    v_price_2 NUMERIC(10,2);
    v_qty_1 INT;
    v_qty_2 INT;
    v_total NUMERIC(12,2);
    v_status VARCHAR(50);
    v_statuses TEXT[] := ARRAY['delivered', 'delivered', 'shipped', 'pending', 'cancelled'];
    v_timestamp TIMESTAMP WITH TIME ZONE;
    i INT;
BEGIN
    FOR i IN 1..100 LOOP
        v_cust_id := ((i * 7) % 50) + 1;
        v_prod_id_1 := ((i * 3) % 30) + 1;
        v_prod_id_2 := ((i * 5 + 2) % 30) + 1;
        v_qty_1 := (i % 3) + 1;
        v_qty_2 := ((i + 1) % 2) + 1;
        v_status := v_statuses[(i % 5) + 1];
        v_timestamp := TIMESTAMP WITH TIME ZONE '2026-08-01 00:00:00+00' + (i * INTERVAL '4 hours 15 minutes');

        SELECT price INTO v_price_1 FROM products WHERE product_id = v_prod_id_1;
        SELECT price INTO v_price_2 FROM products WHERE product_id = v_prod_id_2;

        v_total := (v_price_1 * v_qty_1) + (v_price_2 * v_qty_2);

        INSERT INTO orders (customer_id, order_date, status, total_amount)
        VALUES (v_cust_id, v_timestamp, v_status, v_total)
        RETURNING order_id INTO v_order_id;

        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (v_order_id, v_prod_id_1, v_qty_1, v_price_1);

        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (v_order_id, v_prod_id_2, v_qty_2, v_price_2);
    END LOOP;
END $$;
