# Data Dictionary

## Raw Data

| File | Description |
| --- | --- |
| `olist_customers_dataset.csv` | Customer profile, unique customer identifier, city, and state |
| `olist_orders_dataset.csv` | Order status and purchase/delivery timestamps |
| `olist_order_items_dataset.csv` | Product line items, seller IDs, item prices, and freight values |
| `olist_order_payments_dataset.csv` | Payment type, instalments, and payment value |
| `olist_order_reviews_dataset.csv` | Customer review scores and review text fields |
| `olist_products_dataset.csv` | Product category and physical product attributes |
| `olist_sellers_dataset.csv` | Seller location data |
| `product_category_name_translation.csv` | Portuguese-to-English category mapping |

## Processed Outputs

| Output | Grain | Purpose |
| --- | --- | --- |
| `tableau_dashboard_orders.csv` | One row per delivered order | Main dashboard dataset |
| `customer_segments.csv` | One row per unique customer | Retention, value, and churn-risk segmentation |
| `cohort_retention.csv` | Cohort month and month offset | Retention heatmap input |
| `monthly_revenue.csv` | One row per order month | Revenue trend analysis |
| `category_performance.csv` | One row per product category | Product/category performance |
| `state_performance.csv` | One row per customer state | Regional performance |

## Important Derived Fields

| Field | Meaning |
| --- | --- |
| `payment_value` | Total payment amount per delivered order |
| `merchandise_revenue` | Sum of item prices before freight |
| `freight_revenue` | Sum of freight charges |
| `repeat_segment` | Repeat buyer versus one-time buyer |
| `value_segment` | Customer value classification based on revenue and order frequency |
| `activity_status` | Recent, cooling down, or inactive/churn-risk customer status |
| `retention_rate_pct` | Cohort retention percentage by months since first purchase |
