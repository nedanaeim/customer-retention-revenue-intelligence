-- Final Tableau-ready exports.
-- The build script writes these outputs into data/processed/.

SELECT * FROM fact_orders ORDER BY order_date;
SELECT * FROM customer_metrics ORDER BY customer_revenue DESC;
SELECT * FROM cohort_retention ORDER BY cohort_month, months_since_first_purchase;
SELECT
    product_category,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(*) AS items_sold,
    ROUND(SUM(price), 2) AS merchandise_revenue,
    ROUND(AVG(price), 2) AS avg_item_price,
    COUNT(DISTINCT customer_unique_id) AS customers
FROM fact_order_items_enriched
GROUP BY product_category
ORDER BY merchandise_revenue DESC;
