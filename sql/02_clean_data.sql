-- Staging layer: standardise dates, order status, revenue fields, and review data.

DROP TABLE IF EXISTS stg_orders;
CREATE TABLE stg_orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    datetime(order_purchase_timestamp) AS order_purchase_timestamp,
    date(order_purchase_timestamp) AS order_date,
    strftime('%Y-%m', order_purchase_timestamp) AS order_month,
    datetime(order_delivered_customer_date) AS delivered_at,
    datetime(order_estimated_delivery_date) AS estimated_delivery_at,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
             AND order_delivered_customer_date != ''
             AND julianday(order_delivered_customer_date) <= julianday(order_estimated_delivery_date)
        THEN 1 ELSE 0
    END AS delivered_on_time
FROM raw_orders
WHERE order_purchase_timestamp IS NOT NULL
  AND order_purchase_timestamp != '';

DROP TABLE IF EXISTS stg_order_revenue;
CREATE TABLE stg_order_revenue AS
SELECT
    order_id,
    COUNT(*) AS item_count,
    COUNT(DISTINCT product_id) AS distinct_products,
    ROUND(SUM(CAST(price AS REAL)), 2) AS merchandise_revenue,
    ROUND(SUM(CAST(freight_value AS REAL)), 2) AS freight_revenue
FROM raw_order_items
GROUP BY order_id;

DROP TABLE IF EXISTS stg_payment_summary;
CREATE TABLE stg_payment_summary AS
SELECT
    order_id,
    ROUND(SUM(CAST(payment_value AS REAL)), 2) AS payment_value,
    MAX(CAST(payment_installments AS INTEGER)) AS max_installments,
    GROUP_CONCAT(DISTINCT payment_type) AS payment_types
FROM raw_order_payments
GROUP BY order_id;

DROP TABLE IF EXISTS stg_review_summary;
CREATE TABLE stg_review_summary AS
SELECT
    order_id,
    ROUND(AVG(CAST(review_score AS REAL)), 2) AS review_score
FROM raw_order_reviews
GROUP BY order_id;
