-- Raw table definitions for the Olist customer retention project.
-- The Python build script imports the CSV files into equivalent SQLite tables.

DROP TABLE IF EXISTS raw_customers;
CREATE TABLE raw_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

DROP TABLE IF EXISTS raw_orders;
CREATE TABLE raw_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

DROP TABLE IF EXISTS raw_order_items;
CREATE TABLE raw_order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price REAL,
    freight_value REAL
);

DROP TABLE IF EXISTS raw_order_payments;
CREATE TABLE raw_order_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value REAL
);

DROP TABLE IF EXISTS raw_order_reviews;
CREATE TABLE raw_order_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
);

DROP TABLE IF EXISTS raw_products;
CREATE TABLE raw_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL
);

DROP TABLE IF EXISTS raw_sellers;
CREATE TABLE raw_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

DROP TABLE IF EXISTS raw_category_translation;
CREATE TABLE raw_category_translation (
    product_category_name TEXT PRIMARY KEY,
    product_category_name_english TEXT
);
