-- KPI queries for stakeholder reporting and dashboard validation.

-- Executive KPI scorecard
SELECT
    ROUND(SUM(payment_value), 2) AS total_revenue,
    COUNT(DISTINCT order_id) AS delivered_orders,
    COUNT(DISTINCT customer_unique_id) AS unique_customers,
    ROUND(AVG(payment_value), 2) AS average_order_value,
    ROUND(100.0 * SUM(delivered_on_time) / COUNT(*), 2) AS on_time_delivery_rate_pct
FROM fact_orders;

-- Repeat purchase and activity risk
SELECT
    repeat_segment,
    activity_status,
    COUNT(*) AS customers,
    ROUND(SUM(customer_revenue), 2) AS revenue,
    ROUND(AVG(customer_revenue), 2) AS avg_customer_revenue
FROM customer_metrics
GROUP BY repeat_segment, activity_status
ORDER BY revenue DESC;

-- Monthly revenue trend
SELECT
    order_month,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT customer_unique_id) AS customers,
    ROUND(SUM(payment_value), 2) AS revenue,
    ROUND(AVG(payment_value), 2) AS average_order_value
FROM fact_orders
GROUP BY order_month
ORDER BY order_month;

-- Category performance
SELECT
    product_category,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(*) AS items_sold,
    ROUND(SUM(price), 2) AS merchandise_revenue,
    COUNT(DISTINCT customer_unique_id) AS customers
FROM fact_order_items_enriched
GROUP BY product_category
ORDER BY merchandise_revenue DESC
LIMIT 20;
