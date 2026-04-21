-- Monthly first-purchase cohort retention.

DROP TABLE IF EXISTS cohort_retention;
CREATE TABLE cohort_retention AS
WITH customer_orders AS (
    SELECT
        customer_unique_id,
        order_id,
        order_date,
        strftime('%Y-%m', order_date) AS order_month,
        MIN(strftime('%Y-%m', order_date)) OVER (PARTITION BY customer_unique_id) AS cohort_month
    FROM fact_orders
),
cohort_activity AS (
    SELECT
        cohort_month,
        order_month,
        (CAST(substr(order_month, 1, 4) AS INTEGER) - CAST(substr(cohort_month, 1, 4) AS INTEGER)) * 12
          + (CAST(substr(order_month, 6, 2) AS INTEGER) - CAST(substr(cohort_month, 6, 2) AS INTEGER)) AS months_since_first_purchase,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM customer_orders
    GROUP BY cohort_month, order_month
),
cohort_sizes AS (
    SELECT cohort_month, active_customers AS cohort_customers
    FROM cohort_activity
    WHERE months_since_first_purchase = 0
)
SELECT
    a.cohort_month,
    a.months_since_first_purchase,
    a.active_customers,
    s.cohort_customers,
    ROUND(100.0 * a.active_customers / s.cohort_customers, 2) AS retention_rate_pct
FROM cohort_activity a
JOIN cohort_sizes s ON a.cohort_month = s.cohort_month
WHERE a.months_since_first_purchase BETWEEN 0 AND 12
ORDER BY a.cohort_month, a.months_since_first_purchase;
