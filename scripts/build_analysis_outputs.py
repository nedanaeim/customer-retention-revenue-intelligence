#!/usr/bin/env python3
"""Build portfolio-ready analytics outputs from the Olist ecommerce dataset.

The script intentionally uses only Python's standard library plus SQLite so the
project can run on a clean machine without dependency installation.
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "archive 2"
DB_PATH = ROOT / "data" / "olist_retention.db"
PROCESSED_DIR = ROOT / "data" / "processed"
FIGURES_DIR = ROOT / "reports" / "figures"

TABLES = {
    "olist_customers_dataset.csv": "raw_customers",
    "olist_orders_dataset.csv": "raw_orders",
    "olist_order_items_dataset.csv": "raw_order_items",
    "olist_order_payments_dataset.csv": "raw_order_payments",
    "olist_order_reviews_dataset.csv": "raw_order_reviews",
    "olist_products_dataset.csv": "raw_products",
    "olist_sellers_dataset.csv": "raw_sellers",
    "product_category_name_translation.csv": "raw_category_translation",
}


def clean_header(value: str) -> str:
    return value.replace("\ufeff", "").strip().strip('"')


def load_csv(conn: sqlite3.Connection, csv_path: Path, table_name: str) -> None:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = [clean_header(column) for column in reader.fieldnames or []]
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(
            f"CREATE TABLE {table_name} ("
            + ", ".join([f'"{column}" TEXT' for column in columns])
            + ")"
        )
        placeholders = ", ".join(["?"] * len(columns))
        column_sql = ", ".join([f'"{column}"' for column in columns])
        insert_sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"
        rows = ([row.get(column, "") for column in columns] for row in reader)
        conn.executemany(insert_sql, rows)


def execute_model_sql(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS stg_orders;
        CREATE TABLE stg_orders AS
        SELECT
            order_id,
            customer_id,
            order_status,
            datetime(order_purchase_timestamp) AS order_purchase_timestamp,
            date(order_purchase_timestamp) AS order_date,
            strftime('%Y-%m', order_purchase_timestamp) AS order_month,
            strftime('%Y-Q', order_purchase_timestamp)
                || ((CAST(strftime('%m', order_purchase_timestamp) AS INTEGER) + 2) / 3) AS order_quarter,
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
            o.order_quarter,
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
        ),
        scored AS (
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
            FROM customer_orders
        )
        SELECT * FROM scored;

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
        """
    )


def export_query(conn: sqlite3.Connection, query: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cursor = conn.execute(query)
    headers = [description[0] for description in cursor.description]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(cursor.fetchall())


def write_svg_bar_chart(rows: list[tuple[str, float]], title: str, output_path: Path) -> None:
    width, height = 900, 520
    margin_left, margin_right, margin_top, margin_bottom = 170, 40, 70, 70
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom
    max_value = max(value for _, value in rows) if rows else 1
    bar_gap = 10
    bar_height = (chart_height - bar_gap * (len(rows) - 1)) / max(len(rows), 1)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin_left}" y="38" font-family="Arial" font-size="24" font-weight="700" fill="#17324d">{title}</text>',
    ]
    for index, (label, value) in enumerate(rows):
        y = margin_top + index * (bar_height + bar_gap)
        bar_width = chart_width * (value / max_value)
        parts.append(f'<text x="{margin_left - 12}" y="{y + bar_height * 0.65:.1f}" text-anchor="end" font-family="Arial" font-size="13" fill="#243447">{label}</text>')
        parts.append(f'<rect x="{margin_left}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" rx="3" fill="#2f80ed"/>')
        parts.append(f'<text x="{margin_left + bar_width + 8:.1f}" y="{y + bar_height * 0.65:.1f}" font-family="Arial" font-size="13" fill="#243447">{value:,.0f}</text>')
    parts.append("</svg>")
    output_path.write_text("\n".join(parts), encoding="utf-8")


def build_outputs(conn: sqlite3.Connection) -> None:
    export_query(conn, "SELECT * FROM fact_orders ORDER BY order_date", PROCESSED_DIR / "tableau_dashboard_orders.csv")
    export_query(conn, "SELECT * FROM customer_metrics ORDER BY customer_revenue DESC", PROCESSED_DIR / "customer_segments.csv")
    export_query(conn, "SELECT * FROM cohort_retention", PROCESSED_DIR / "cohort_retention.csv")
    export_query(
        conn,
        """
        SELECT
            order_month,
            COUNT(DISTINCT order_id) AS orders,
            COUNT(DISTINCT customer_unique_id) AS customers,
            ROUND(SUM(payment_value), 2) AS revenue,
            ROUND(AVG(payment_value), 2) AS avg_order_value,
            ROUND(AVG(review_score), 2) AS avg_review_score,
            ROUND(100.0 * SUM(delivered_on_time) / COUNT(*), 2) AS on_time_delivery_rate_pct
        FROM fact_orders
        GROUP BY order_month
        ORDER BY order_month
        """,
        PROCESSED_DIR / "monthly_revenue.csv",
    )
    export_query(
        conn,
        """
        SELECT
            customer_state,
            COUNT(DISTINCT order_id) AS orders,
            COUNT(DISTINCT customer_unique_id) AS customers,
            ROUND(SUM(payment_value), 2) AS revenue,
            ROUND(AVG(payment_value), 2) AS avg_order_value,
            ROUND(AVG(review_score), 2) AS avg_review_score
        FROM fact_orders
        GROUP BY customer_state
        ORDER BY revenue DESC
        """,
        PROCESSED_DIR / "state_performance.csv",
    )
    export_query(
        conn,
        """
        SELECT
            product_category,
            COUNT(DISTINCT order_id) AS orders,
            COUNT(*) AS items_sold,
            ROUND(SUM(price), 2) AS merchandise_revenue,
            ROUND(AVG(price), 2) AS avg_item_price,
            COUNT(DISTINCT customer_unique_id) AS customers
        FROM fact_order_items_enriched
        GROUP BY product_category
        ORDER BY merchandise_revenue DESC
        """,
        PROCESSED_DIR / "category_performance.csv",
    )
    export_query(
        conn,
        """
        SELECT
            'Total revenue' AS metric,
            ROUND(SUM(payment_value), 2) AS value
        FROM fact_orders
        UNION ALL
        SELECT 'Delivered orders', COUNT(*) FROM fact_orders
        UNION ALL
        SELECT 'Unique customers', COUNT(DISTINCT customer_unique_id) FROM fact_orders
        UNION ALL
        SELECT 'Average order value', ROUND(AVG(payment_value), 2) FROM fact_orders
        UNION ALL
        SELECT 'Repeat purchase rate pct',
            ROUND(100.0 * SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM customer_metrics
        UNION ALL
        SELECT 'Inactive or churn-risk customers pct',
            ROUND(100.0 * SUM(CASE WHEN activity_status = 'Inactive / churn risk' THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM customer_metrics
        """,
        PROCESSED_DIR / "executive_kpis.csv",
    )

    top_categories = conn.execute(
        """
        SELECT product_category, SUM(price) AS revenue
        FROM fact_order_items_enriched
        GROUP BY product_category
        ORDER BY revenue DESC
        LIMIT 10
        """
    ).fetchall()
    top_states = conn.execute(
        """
        SELECT customer_state, SUM(payment_value) AS revenue
        FROM fact_orders
        GROUP BY customer_state
        ORDER BY revenue DESC
        LIMIT 10
        """
    ).fetchall()
    write_svg_bar_chart(top_categories, "Top Product Categories by Merchandise Revenue", FIGURES_DIR / "top_categories.svg")
    write_svg_bar_chart(top_states, "Top Customer States by Revenue", FIGURES_DIR / "top_states.svg")


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        for filename, table_name in TABLES.items():
            load_csv(conn, RAW_DIR / filename, table_name)
        conn.commit()
        execute_model_sql(conn)
        conn.commit()
        build_outputs(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
