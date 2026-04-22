#!/usr/bin/env python3
"""Build portfolio-ready analytics outputs from the Olist ecommerce dataset.

The script intentionally uses only Python's standard library plus SQLite so the
project can run on a clean machine without dependency installation.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "archive 2"
DB_PATH = ROOT / "data" / "olist_retention.db"
PROCESSED_DIR = ROOT / "data" / "processed"
FIGURES_DIR = ROOT / "reports" / "figures"
PUBLIC_DIR = ROOT / "docs"
REPORTS_DIR = ROOT / "reports"

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


def query_dicts(conn: sqlite3.Connection, query: str) -> list[dict[str, object]]:
    cursor = conn.execute(query)
    headers = [description[0] for description in cursor.description]
    return [dict(zip(headers, row)) for row in cursor.fetchall()]


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


def write_interactive_dashboard(conn: sqlite3.Connection) -> None:
    dashboard_data = {
        "kpis": query_dicts(conn, "SELECT metric, value FROM (SELECT * FROM (SELECT 'Total revenue' AS metric, ROUND(SUM(payment_value), 2) AS value FROM fact_orders UNION ALL SELECT 'Delivered orders', COUNT(*) FROM fact_orders UNION ALL SELECT 'Unique customers', COUNT(DISTINCT customer_unique_id) FROM fact_orders UNION ALL SELECT 'Average order value', ROUND(AVG(payment_value), 2) FROM fact_orders UNION ALL SELECT 'Repeat purchase rate pct', ROUND(100.0 * SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) FROM customer_metrics UNION ALL SELECT 'Inactive or churn-risk customers pct', ROUND(100.0 * SUM(CASE WHEN activity_status = 'Inactive / churn risk' THEN 1 ELSE 0 END) / COUNT(*), 2) FROM customer_metrics))"),
        "monthly": query_dicts(
            conn,
            """
            SELECT
                order_month,
                orders,
                customers,
                revenue,
                avg_order_value,
                avg_review_score,
                on_time_delivery_rate_pct
            FROM (
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
            )
            ORDER BY order_month
            """,
        ),
        "categories": query_dicts(
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
            LIMIT 20
            """,
        ),
        "states": query_dicts(
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
        ),
        "activitySegments": query_dicts(
            conn,
            """
            SELECT
                activity_status,
                COUNT(*) AS customers,
                ROUND(SUM(customer_revenue), 2) AS revenue,
                ROUND(AVG(avg_order_value), 2) AS avg_order_value
            FROM customer_metrics
            GROUP BY activity_status
            ORDER BY customers DESC
            """,
        ),
        "valueSegments": query_dicts(
            conn,
            """
            SELECT
                value_segment,
                COUNT(*) AS customers,
                ROUND(SUM(customer_revenue), 2) AS revenue,
                ROUND(AVG(avg_order_value), 2) AS avg_order_value
            FROM customer_metrics
            GROUP BY value_segment
            ORDER BY revenue DESC
            """,
        ),
        "retention": query_dicts(
            conn,
            """
            SELECT
                months_since_first_purchase,
                ROUND(AVG(retention_rate_pct), 2) AS avg_retention_rate_pct,
                SUM(active_customers) AS active_customers,
                SUM(cohort_customers) AS cohort_customers
            FROM cohort_retention
            GROUP BY months_since_first_purchase
            ORDER BY months_since_first_purchase
            """,
        ),
    }

    html = DASHBOARD_TEMPLATE.replace(
        "__DASHBOARD_DATA__",
        json.dumps(dashboard_data, ensure_ascii=False),
    )
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "dashboard.html").write_text(html, encoding="utf-8")
    (PUBLIC_DIR / "index.html").write_text(html, encoding="utf-8")
    (PUBLIC_DIR / ".nojekyll").write_text("", encoding="utf-8")


DASHBOARD_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Customer Retention & Revenue Intelligence Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --ink: #191020;
      --muted: #6b5c73;
      --deep: #2d143f;
      --purple: #6f35a5;
      --violet: #9b59d0;
      --lavender: #f5effb;
      --line: #e7dff0;
      --card: #ffffff;
      --accent: #00a88f;
      --gold: #c9891d;
      --shadow: 0 18px 42px rgba(45, 20, 63, 0.14);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      color: var(--ink);
      background:
        linear-gradient(180deg, #fbf8ff 0%, #ffffff 34%, #f8f4fc 100%);
    }

    a { color: var(--purple); }

    header {
      background:
        linear-gradient(135deg, #2d143f 0%, #5a267e 58%, #8a45bd 100%);
      color: #fff;
      padding: 44px 20px 36px;
    }

    .wrap {
      width: min(1180px, calc(100% - 32px));
      margin: 0 auto;
    }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.5fr) minmax(280px, 0.8fr);
      gap: 28px;
      align-items: end;
    }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 14px;
      padding: 7px 10px;
      border: 1px solid rgba(255, 255, 255, 0.28);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.12);
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0;
    }

    h1 {
      margin: 0;
      max-width: 850px;
      font-size: clamp(34px, 6vw, 68px);
      line-height: 0.96;
      letter-spacing: 0;
    }

    .hero p {
      max-width: 720px;
      margin: 18px 0 0;
      color: rgba(255, 255, 255, 0.86);
      font-size: 18px;
      line-height: 1.55;
    }

    .hero-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 22px;
    }

    .hero-actions a,
    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 42px;
      padding: 10px 14px;
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.18);
      background: #fff;
      color: var(--deep);
      font-weight: 800;
      text-decoration: none;
    }

    .hero-actions a.secondary {
      background: transparent;
      color: #fff;
    }

    .summary-panel {
      border: 1px solid rgba(255, 255, 255, 0.18);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.12);
      padding: 20px;
      backdrop-filter: blur(10px);
    }

    .summary-panel h2 {
      margin: 0 0 12px;
      font-size: 17px;
      color: #fff;
    }

    .summary-panel ul {
      margin: 0;
      padding-left: 18px;
      color: rgba(255, 255, 255, 0.86);
      line-height: 1.55;
    }

    main {
      padding: 26px 0 48px;
    }

    .toolbar {
      position: sticky;
      top: 0;
      z-index: 3;
      margin: -1px auto 24px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: 0 12px 26px rgba(45, 20, 63, 0.08);
      padding: 12px;
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 12px;
      align-items: center;
      backdrop-filter: blur(8px);
    }

    .metric-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    button,
    select {
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: var(--deep);
      font: inherit;
    }

    button {
      cursor: pointer;
      padding: 8px 12px;
      font-weight: 800;
    }

    button.active {
      border-color: var(--purple);
      background: var(--purple);
      color: #fff;
    }

    select {
      padding: 8px 34px 8px 10px;
      font-weight: 700;
    }

    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }

    .kpi,
    .card,
    .insight {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--card);
      box-shadow: var(--shadow);
    }

    .kpi {
      padding: 16px;
      min-height: 122px;
    }

    .kpi span {
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0;
    }

    .kpi strong {
      display: block;
      margin-top: 10px;
      color: var(--deep);
      font-size: 26px;
      line-height: 1;
    }

    .kpi small {
      display: block;
      margin-top: 8px;
      color: var(--muted);
      line-height: 1.35;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 18px;
    }

    .card {
      padding: 18px;
      min-width: 0;
    }

    .span-8 { grid-column: span 8; }
    .span-7 { grid-column: span 7; }
    .span-6 { grid-column: span 6; }
    .span-5 { grid-column: span 5; }
    .span-4 { grid-column: span 4; }
    .span-12 { grid-column: span 12; }

    .card-head {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: start;
      margin-bottom: 14px;
    }

    h2 {
      margin: 0;
      color: var(--deep);
      font-size: 20px;
      letter-spacing: 0;
    }

    .card-head p {
      margin: 6px 0 0;
      color: var(--muted);
      line-height: 1.45;
      font-size: 14px;
    }

    .chart-box {
      position: relative;
      height: 360px;
    }

    .chart-box.short {
      height: 305px;
    }

    .insight {
      padding: 18px;
      background: #2d143f;
      color: #fff;
      box-shadow: var(--shadow);
    }

    .insight h2 { color: #fff; }

    .insight p {
      color: rgba(255, 255, 255, 0.84);
      line-height: 1.55;
    }

    .lens-tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 16px 0;
    }

    .lens-tabs button {
      background: rgba(255, 255, 255, 0.12);
      color: #fff;
      border-color: rgba(255, 255, 255, 0.24);
    }

    .lens-tabs button.active {
      background: #fff;
      color: var(--deep);
    }

    .recommendations {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
    }

    .recommendation {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      background: #fff;
    }

    .recommendation strong {
      display: block;
      color: var(--deep);
      margin-bottom: 6px;
    }

    .recommendation p {
      margin: 0;
      color: var(--muted);
      line-height: 1.45;
      font-size: 14px;
    }

    .data-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }

    .data-table th,
    .data-table td {
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      text-align: left;
    }

    .data-table th {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0;
    }

    .data-table td:last-child,
    .data-table th:last-child {
      text-align: right;
    }

    footer {
      margin-top: 26px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }

    @media (max-width: 980px) {
      .hero,
      .toolbar {
        grid-template-columns: 1fr;
      }

      .kpi-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .span-8,
      .span-7,
      .span-6,
      .span-5,
      .span-4 {
        grid-column: span 12;
      }

      .recommendations {
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 640px) {
      header {
        padding-top: 30px;
      }

      .wrap {
        width: min(100% - 22px, 1180px);
      }

      .kpi-grid {
        grid-template-columns: 1fr;
      }

      .chart-box,
      .chart-box.short {
        height: 300px;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="wrap hero">
      <section>
        <div class="eyebrow">Interactive portfolio dashboard</div>
        <h1>Customer Retention & Revenue Intelligence</h1>
        <p>Explore revenue, retention, customer activity, product categories, and regional performance from the Brazilian Olist ecommerce dataset.</p>
        <div class="hero-actions">
          <a href="#dashboard">Explore dashboard</a>
          <a class="secondary" href="https://github.com/nedanaeim/customer-retention-revenue-intelligence">View GitHub project</a>
        </div>
      </section>
      <aside class="summary-panel" aria-label="Dashboard summary">
        <h2>Executive readout</h2>
        <ul>
          <li>Repeat purchase rate is only 3.00%.</li>
          <li>59.05% of customers are inactive or churn risk.</li>
          <li>Sao Paulo is the largest revenue market.</li>
        </ul>
      </aside>
    </div>
  </header>

  <main class="wrap" id="dashboard">
    <nav class="toolbar" aria-label="Dashboard controls">
      <div class="metric-tabs" aria-label="Trend metric">
        <button class="metric-tab active" data-metric="revenue">Revenue</button>
        <button class="metric-tab" data-metric="orders">Orders</button>
        <button class="metric-tab" data-metric="customers">Customers</button>
        <button class="metric-tab" data-metric="avg_order_value">AOV</button>
      </div>
      <label>
        Top rows
        <select id="topN">
          <option value="5">Top 5</option>
          <option value="10" selected>Top 10</option>
          <option value="15">Top 15</option>
        </select>
      </label>
      <label>
        Bar metric
        <select id="barMetric">
          <option value="revenue" selected>Revenue</option>
          <option value="orders">Orders</option>
          <option value="customers">Customers</option>
        </select>
      </label>
    </nav>

    <section class="kpi-grid" aria-label="Executive KPIs">
      <article class="kpi"><span>Total revenue</span><strong id="kpiRevenue"></strong><small>Delivered order payment value</small></article>
      <article class="kpi"><span>Delivered orders</span><strong id="kpiOrders"></strong><small>Completed marketplace orders</small></article>
      <article class="kpi"><span>Unique customers</span><strong id="kpiCustomers"></strong><small>Distinct customer identifiers</small></article>
      <article class="kpi"><span>Average order value</span><strong id="kpiAov"></strong><small>Revenue per delivered order</small></article>
      <article class="kpi"><span>Repeat purchase rate</span><strong id="kpiRepeat"></strong><small>Customers with more than one order</small></article>
      <article class="kpi"><span>Churn-risk customers</span><strong id="kpiChurn"></strong><small>Inactive for more than 180 days</small></article>
    </section>

    <section class="grid">
      <article class="card span-8">
        <div class="card-head">
          <div>
            <h2>Monthly performance trend</h2>
            <p>Use the metric buttons to switch between revenue, order volume, customers, and average order value.</p>
          </div>
        </div>
        <div class="chart-box"><canvas id="monthlyChart"></canvas></div>
      </article>

      <article class="insight span-4">
        <h2>Business lens</h2>
        <p id="lensText">Retention is the biggest opportunity: most customers purchase once and do not naturally return.</p>
        <div class="lens-tabs">
          <button class="lens-tab active" data-lens="retention">Retention</button>
          <button class="lens-tab" data-lens="revenue">Revenue</button>
          <button class="lens-tab" data-lens="experience">Experience</button>
        </div>
        <p id="lensDetail">Start lifecycle campaigns in high-volume states, then measure repeat purchase rate and cohort retention each month.</p>
      </article>

      <article class="card span-6">
        <div class="card-head">
          <div>
            <h2>Top product categories</h2>
            <p>Compare category contribution by revenue, orders, or customers.</p>
          </div>
        </div>
        <div class="chart-box short"><canvas id="categoryChart"></canvas></div>
      </article>

      <article class="card span-6">
        <div class="card-head">
          <div>
            <h2>Top customer states</h2>
            <p>Identify where retention and marketing tests can reach the largest customer base.</p>
          </div>
        </div>
        <div class="chart-box short"><canvas id="stateChart"></canvas></div>
      </article>

      <article class="card span-7">
        <div class="card-head">
          <div>
            <h2>Average cohort retention curve</h2>
            <p>Month zero starts at 100%; later months show how sharply customer activity drops.</p>
          </div>
        </div>
        <div class="chart-box short"><canvas id="retentionChart"></canvas></div>
      </article>

      <article class="card span-5">
        <div class="card-head">
          <div>
            <h2>Customer activity mix</h2>
            <p>Inactive customers dominate the base, which points to win-back and post-purchase engagement.</p>
          </div>
        </div>
        <div class="chart-box short"><canvas id="segmentChart"></canvas></div>
      </article>

      <article class="card span-12">
        <div class="card-head">
          <div>
            <h2>Recommended next actions</h2>
            <p>Actions are prioritised for measurable revenue and retention impact.</p>
          </div>
        </div>
        <div class="recommendations">
          <div class="recommendation">
            <strong>1. Launch post-purchase journeys</strong>
            <p>Target Sao Paulo, Rio de Janeiro, and Minas Gerais first because customer density is highest there.</p>
          </div>
          <div class="recommendation">
            <strong>2. Cross-sell top categories</strong>
            <p>Use health and beauty, watches and gifts, bed bath table, and sports leisure as first campaign groups.</p>
          </div>
          <div class="recommendation">
            <strong>3. Track retention monthly</strong>
            <p>Make repeat purchase rate, cohort retention, inactive customer share, and AOV the executive KPI set.</p>
          </div>
        </div>
      </article>

      <article class="card span-12">
        <div class="card-head">
          <div>
            <h2>Top state detail</h2>
            <p>The top ten states by revenue, including order volume, customers, and average review score.</p>
          </div>
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th>State</th>
              <th>Orders</th>
              <th>Customers</th>
              <th>Avg review</th>
              <th>Revenue</th>
            </tr>
          </thead>
          <tbody id="stateRows"></tbody>
        </table>
      </article>
    </section>

    <footer>
      Built from reproducible SQL and Python outputs in this portfolio repository. Data source: Brazilian Olist ecommerce dataset.
    </footer>
  </main>

  <script id="dashboard-data" type="application/json">__DASHBOARD_DATA__</script>
  <script>
    const data = JSON.parse(document.getElementById("dashboard-data").textContent);
    const purple = "#6f35a5";
    const violet = "#9b59d0";
    const deep = "#2d143f";
    const accent = "#00a88f";
    const gold = "#c9891d";
    const grid = "rgba(111, 53, 165, 0.12)";
    const text = "#35213f";

    const money = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
    const money2 = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
    const number = new Intl.NumberFormat("en-US");

    function kpiValue(name) {
      return Number(data.kpis.find((item) => item.metric === name)?.value ?? 0);
    }

    function formatMetric(value, metric) {
      if (metric === "revenue") return "$" + money.format(value);
      if (metric === "avg_order_value") return "$" + money2.format(value);
      if (metric.includes("rate") || metric.includes("pct")) return money2.format(value) + "%";
      return number.format(value);
    }

    function setKpis() {
      document.getElementById("kpiRevenue").textContent = "$" + money2.format(kpiValue("Total revenue") / 1000000) + "M";
      document.getElementById("kpiOrders").textContent = number.format(kpiValue("Delivered orders"));
      document.getElementById("kpiCustomers").textContent = number.format(kpiValue("Unique customers"));
      document.getElementById("kpiAov").textContent = "$" + money2.format(kpiValue("Average order value"));
      document.getElementById("kpiRepeat").textContent = money2.format(kpiValue("Repeat purchase rate pct")) + "%";
      document.getElementById("kpiChurn").textContent = money2.format(kpiValue("Inactive or churn-risk customers pct")) + "%";
    }

    Chart.defaults.font.family = "Inter, system-ui, -apple-system, Segoe UI, Arial, sans-serif";
    Chart.defaults.color = text;
    Chart.defaults.plugins.legend.labels.boxWidth = 12;

    let selectedMetric = "revenue";
    let monthlyChart;
    let categoryChart;
    let stateChart;
    let retentionChart;
    let segmentChart;

    const metricLabels = {
      revenue: "Revenue",
      orders: "Orders",
      customers: "Customers",
      avg_order_value: "Average order value"
    };

    function makeMonthlyChart() {
      monthlyChart = new Chart(document.getElementById("monthlyChart"), {
        type: "line",
        data: {
          labels: data.monthly.map((row) => row.order_month),
          datasets: [{
            label: metricLabels[selectedMetric],
            data: data.monthly.map((row) => Number(row[selectedMetric])),
            borderColor: purple,
            backgroundColor: "rgba(155, 89, 208, 0.16)",
            fill: true,
            tension: 0.35,
            pointRadius: 3,
            pointHoverRadius: 5
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => metricLabels[selectedMetric] + ": " + formatMetric(ctx.parsed.y, selectedMetric)
              }
            }
          },
          scales: {
            x: { grid: { display: false } },
            y: {
              grid: { color: grid },
              ticks: {
                callback: (value) => selectedMetric === "revenue" || selectedMetric === "avg_order_value" ? "$" + money.format(value) : number.format(value)
              }
            }
          }
        }
      });
    }

    function updateMonthly(metric) {
      selectedMetric = metric;
      monthlyChart.data.datasets[0].label = metricLabels[metric];
      monthlyChart.data.datasets[0].data = data.monthly.map((row) => Number(row[metric]));
      monthlyChart.update();
    }

    function metricKeyForBars() {
      const requested = document.getElementById("barMetric").value;
      return requested === "revenue" ? "merchandise_revenue" : requested;
    }

    function stateMetricKey() {
      return document.getElementById("barMetric").value;
    }

    function barLabel() {
      const value = document.getElementById("barMetric").value;
      return value === "revenue" ? "Revenue" : value.charAt(0).toUpperCase() + value.slice(1);
    }

    function sortedRows(rows, key, topN) {
      return [...rows].sort((a, b) => Number(b[key]) - Number(a[key])).slice(0, topN).reverse();
    }

    function makeHorizontalBar(canvasId, rows, labelKey, valueKey, label, color) {
      return new Chart(document.getElementById(canvasId), {
        type: "bar",
        data: {
          labels: rows.map((row) => row[labelKey]),
          datasets: [{
            label,
            data: rows.map((row) => Number(row[valueKey])),
            backgroundColor: color,
            borderRadius: 6
          }]
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: (ctx) => label + ": " + (label === "Revenue" ? "$" + money.format(ctx.parsed.x) : number.format(ctx.parsed.x))
              }
            }
          },
          scales: {
            x: { grid: { color: grid } },
            y: { grid: { display: false } }
          }
        }
      });
    }

    function refreshBars() {
      const topN = Number(document.getElementById("topN").value);
      const categoryKey = metricKeyForBars();
      const stateKey = stateMetricKey();
      const categories = sortedRows(data.categories, categoryKey, topN);
      const states = sortedRows(data.states, stateKey, topN);

      categoryChart.data.labels = categories.map((row) => row.product_category.replaceAll("_", " "));
      categoryChart.data.datasets[0].label = barLabel();
      categoryChart.data.datasets[0].data = categories.map((row) => Number(row[categoryKey]));
      categoryChart.update();

      stateChart.data.labels = states.map((row) => row.customer_state);
      stateChart.data.datasets[0].label = barLabel();
      stateChart.data.datasets[0].data = states.map((row) => Number(row[stateKey]));
      stateChart.update();
    }

    function makeSupportingCharts() {
      const topN = Number(document.getElementById("topN").value);
      categoryChart = makeHorizontalBar("categoryChart", sortedRows(data.categories, "merchandise_revenue", topN), "product_category", "merchandise_revenue", "Revenue", violet);
      categoryChart.data.labels = categoryChart.data.labels.map((label) => label.replaceAll("_", " "));

      stateChart = makeHorizontalBar("stateChart", sortedRows(data.states, "revenue", topN), "customer_state", "revenue", "Revenue", purple);

      retentionChart = new Chart(document.getElementById("retentionChart"), {
        type: "line",
        data: {
          labels: data.retention.map((row) => "M" + row.months_since_first_purchase),
          datasets: [{
            label: "Average retention rate",
            data: data.retention.map((row) => Number(row.avg_retention_rate_pct)),
            borderColor: accent,
            backgroundColor: "rgba(0, 168, 143, 0.14)",
            fill: true,
            tension: 0.32,
            pointRadius: 3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            tooltip: {
              callbacks: { label: (ctx) => "Retention: " + money2.format(ctx.parsed.y) + "%" }
            }
          },
          scales: {
            x: { grid: { display: false } },
            y: {
              min: 0,
              max: 100,
              grid: { color: grid },
              ticks: { callback: (value) => value + "%" }
            }
          }
        }
      });

      segmentChart = new Chart(document.getElementById("segmentChart"), {
        type: "doughnut",
        data: {
          labels: data.activitySegments.map((row) => row.activity_status),
          datasets: [{
            data: data.activitySegments.map((row) => Number(row.customers)),
            backgroundColor: [purple, violet, accent, gold],
            borderWidth: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          cutout: "62%",
          plugins: {
            tooltip: {
              callbacks: { label: (ctx) => ctx.label + ": " + number.format(ctx.parsed) + " customers" }
            }
          }
        }
      });
    }

    function setStateTable() {
      const rows = data.states.slice(0, 10).map((row) => `
        <tr>
          <td><strong>${row.customer_state}</strong></td>
          <td>${number.format(row.orders)}</td>
          <td>${number.format(row.customers)}</td>
          <td>${money2.format(row.avg_review_score)}</td>
          <td>$${money.format(row.revenue)}</td>
        </tr>
      `).join("");
      document.getElementById("stateRows").innerHTML = rows;
    }

    const lensCopy = {
      retention: {
        text: "Retention is the biggest opportunity: most customers purchase once and do not naturally return.",
        detail: "Start lifecycle campaigns in high-volume states, then measure repeat purchase rate and cohort retention each month."
      },
      revenue: {
        text: "Revenue is concentrated in a small set of categories and states, so campaign testing should begin where density is highest.",
        detail: "Prioritise Sao Paulo, Rio de Janeiro, Minas Gerais, and top categories before expanding nationally."
      },
      experience: {
        text: "Delivery and review experience still matter because poor experiences can weaken the case for repeat purchasing.",
        detail: "Track review score and on-time delivery beside revenue metrics so retention campaigns do not mask service issues."
      }
    };

    function setLens(name) {
      document.getElementById("lensText").textContent = lensCopy[name].text;
      document.getElementById("lensDetail").textContent = lensCopy[name].detail;
    }

    document.querySelectorAll(".metric-tab").forEach((button) => {
      button.addEventListener("click", () => {
        document.querySelectorAll(".metric-tab").forEach((item) => item.classList.remove("active"));
        button.classList.add("active");
        updateMonthly(button.dataset.metric);
      });
    });

    document.querySelectorAll(".lens-tab").forEach((button) => {
      button.addEventListener("click", () => {
        document.querySelectorAll(".lens-tab").forEach((item) => item.classList.remove("active"));
        button.classList.add("active");
        setLens(button.dataset.lens);
      });
    });

    document.getElementById("topN").addEventListener("change", refreshBars);
    document.getElementById("barMetric").addEventListener("change", refreshBars);

    setKpis();
    makeMonthlyChart();
    makeSupportingCharts();
    setStateTable();
  </script>
</body>
</html>
"""


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
    write_interactive_dashboard(conn)


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
