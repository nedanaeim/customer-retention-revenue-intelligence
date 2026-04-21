-- Analytical layer: create the order, item, and customer-level datasets used for reporting.

DROP TABLE IF EXISTS fact_orders;
CREATE TABLE fact_orders AS
SELECT
    o.order_id,
    c.customer_unique_id,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_date,
    o.order_month,
    o.delivered_on_time,
    COALESCE(r.item_count, 0) AS item_count,
    COALESCE(r.distinct_products, 0) AS distinct_products,
    COALESCE(r.merchandise_revenue, 0) AS merchandise_revenue,
    COALESCE(r.freight_revenue, 0) AS freight_revenue,
    COALESCE(p.payment_value, 0) AS payment_value,
    COALESCE(p.max_installments, 0) AS max_installments,
    COALESCE(p.payment_types, 'unknown') AS payment_types,
    COALESCE(v.review_score, 0) AS review_score
FROM stg_orders o
LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
LEFT JOIN stg_order_revenue r ON o.order_id = r.order_id
LEFT JOIN stg_payment_summary p ON o.order_id = p.order_id
LEFT JOIN stg_review_summary v ON o.order_id = v.order_id
WHERE o.order_status = 'delivered';

DROP TABLE IF EXISTS fact_order_items_enriched;
CREATE TABLE fact_order_items_enriched AS
SELECT
    i.order_id,
    o.order_date,
    o.order_month,
    o.customer_unique_id,
    o.customer_state,
    i.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category,
    i.seller_id,
    s.seller_state,
    CAST(i.price AS REAL) AS price,
    CAST(i.freight_value AS REAL) AS freight_value
FROM raw_order_items i
JOIN fact_orders o ON i.order_id = o.order_id
LEFT JOIN raw_products p ON i.product_id = p.product_id
LEFT JOIN raw_category_translation t ON p.product_category_name = t.product_category_name
LEFT JOIN raw_sellers s ON i.seller_id = s.seller_id;

DROP TABLE IF EXISTS customer_metrics;
CREATE TABLE customer_metrics AS
WITH customer_orders AS (
    SELECT
        customer_unique_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS orders,
        ROUND(SUM(payment_value), 2) AS customer_revenue,
        ROUND(AVG(payment_value), 2) AS avg_order_value,
        MAX(julianday(order_date)) AS last_order_jd
    FROM fact_orders
    GROUP BY customer_unique_id
)
SELECT
    *,
    CAST(julianday((SELECT MAX(order_date) FROM fact_orders)) - last_order_jd AS INTEGER) AS recency_days,
    CASE WHEN orders > 1 THEN 'Repeat buyer' ELSE 'One-time buyer' END AS repeat_segment,
    CASE
        WHEN customer_revenue >= 500 AND orders > 1 THEN 'High-value loyal'
        WHEN customer_revenue >= 250 THEN 'High-value one-time'
        WHEN orders > 1 THEN 'Emerging loyal'
        ELSE 'Low-value one-time'
    END AS value_segment,
    CASE
        WHEN julianday((SELECT MAX(order_date) FROM fact_orders)) - last_order_jd > 180 THEN 'Inactive / churn risk'
        WHEN julianday((SELECT MAX(order_date) FROM fact_orders)) - last_order_jd > 90 THEN 'Cooling down'
        ELSE 'Recently active'
    END AS activity_status
FROM customer_orders;
