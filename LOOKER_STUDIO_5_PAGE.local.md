# Looker Studio Complete 5-Page Build (Local-Only)

This local guide is intentionally excluded from git so you can keep working notes and dashboard build details private.

Estimated effort for first build: 90-150 minutes.

## Commenting Convention (Project-Wide)

Use these high-level header comments at the top of files for consistency.

1. Python files (`.py`)
   - Use a module docstring (`""" ... """`) with 3-5 bullets.
   - Describe pipeline intent, major steps, and output targets.

2. dbt SQL files (`.sql`)
   - Add 3 comment lines before `{{ config(...) }}`.
   - Focus on business intent + transformation purpose (not line-by-line SQL behavior).

3. Terraform files (`.tf`)
   - Add 2-3 comment lines at file top.
   - Explain what resources/inputs/outputs the file governs.

4. YAML files (`.yml`)
   - Add 2-3 comment lines at file top.
   - Summarize what the workflow/config controls.

Header style template:
- Start with `High-level logic:`
- Keep concise and implementation-aware.
- Mention the primary output table/resource when relevant.

## Global Setup (Do Once)

1. Create a new Looker Studio report.
2. Add these BigQuery data sources:
   - grocery_analytics.mart_sales_summary
   - grocery_analytics.mart_customer_behavior
   - grocery_analytics.customer_lifetime_value
   - grocery_analytics.mart_employee_performance
   - grocery_analytics.customer_segments
   - grocery_analytics.customer_recommendations

Recommendation source-of-truth policy:
1. Use Spark recommendations (`grocery_analytics.customer_recommendations`) for dashboarding and business actions.
2. Treat dbt recommendations (`grocery_analytics.mart_recommend`) as optional comparison/debug only.
3. If Spark and dbt outputs disagree, prioritize Spark output for campaign execution.

3. Apply one consistent theme:
   - One font family across all pages
   - Currency and number formatting standardized
4. Add report-level controls (top area):
   - Date range control
   - Drop-down filter: customer_country
   - Drop-down filter: category_name
5. Create 6 pages:
   - Executive Overview
   - Product Performance
   - Customer Insights
   - Sales Team Performance
   - Recommendations
   - Spark ML Insights

## Page 1: Executive Overview

Data source: grocery_analytics.mart_sales_summary

Charts and widgets:
1. Scorecards
   - Total Revenue: SUM(total_revenue)
   - Total Profit: SUM(total_profit)
   - Transactions: SUM(transaction_count)
   - Unique Customers: SUM(unique_customers)
2. Time series
   - Dimension: year_month
   - Metric: SUM(total_revenue)
3. Country performance bar chart
   - Dimension: customer_country
   - Metrics: SUM(total_revenue), SUM(total_profit)
4. Geo map
   - Dimension: customer_country
   - Metric: SUM(total_revenue)
5. Optional donut
   - Dimension: price_category
   - Metric: SUM(total_revenue)

Formatting guidance:
- Show comparison to previous period for revenue and profit.
- Use conditional color cues for trend movement.

## Page 2: Product Performance

Data source: grocery_analytics.mart_sales_summary

Charts and widgets:
1. Top products table
   - Dimensions: product_name, category_name
   - Metrics: SUM(total_units_sold), SUM(total_revenue), SUM(total_profit)
   - Sort: SUM(total_units_sold) descending
2. Top 10 hot-selling products bar chart
   - Dimension: product_name
   - Metric: SUM(total_units_sold)
   - Filter: monthly_hot_selling_product_rank <= 10
3. Category trend line/bar chart
   - Dimension: year_month
   - Breakdown: category_name
   - Metric: SUM(total_revenue)
4. Product portfolio scatter chart
   - Dimension: product_name
   - X axis: SUM(total_units_sold)
   - Y axis: SUM(total_revenue)
   - Bubble size: SUM(total_profit)

## Page 3: Customer Insights

Primary data source: grocery_analytics.mart_customer_behavior

Charts and widgets:
1. Customer segments table
   - Dimensions: rfm_segment, marketing_action
   - Metrics: COUNT_DISTINCT(customer_id), AVG(total_spend)
2. Segment size bar chart
   - Dimension: rfm_segment
   - Metric: COUNT_DISTINCT(customer_id)
3. Engagement pie chart (optional)
   - Dimension: engagement_status
   - Metric: COUNT_DISTINCT(customer_id)
4. Top customers table (data source override)
   - Data source: grocery_analytics.customer_lifetime_value
   - Dimensions: customer_id, customer_value_tier
   - Metrics: total_spent, estimated_clv, customer_spend_rank
   - Sort: customer_spend_rank ascending

Interaction guidance:
- Enable cross-filtering so selecting a segment filters related visuals.

## Page 4: Sales Team Performance

Data source: grocery_analytics.mart_employee_performance

Charts and widgets:
1. Sales team performance table
   - Dimensions: employee_name, employee_country, performance_tier
   - Metrics: total_sales_revenue, total_sales_transactions, unique_customers_served, company_rank
   - Sort: company_rank ascending
2. Top employees bar chart
   - Dimension: employee_name
   - Metric: total_sales_revenue
   - Filter: Top 10
3. Country performance heatmap table
   - Dimension: employee_country
   - Metrics: AVG(total_sales_revenue), SUM(total_sales_revenue)
   - Use conditional formatting for quick comparison

## Page 5: Recommendations

Data source: grocery_analytics.customer_recommendations

Charts and widgets:
1. Recommendations table
   - Dimensions: customer_id, recommendation_rank, product_name, category_id
   - Metrics: score
   - Sort: score descending
2. Customer filter control
   - Input/drop-down: customer_id
3. Rank-1 activation table
   - Filter: recommendation_rank = 1
   - Columns: customer_id, product_name, score, category_id

## Page 6: Spark ML Insights

This page uses Spark-native outputs from the pipeline:
- grocery_analytics.customer_segments
- grocery_analytics.customer_recommendations

### Section A: Segment Distribution

Data source: grocery_analytics.customer_segments

Charts:
1. Segment distribution bar chart
   - Dimension: segment_name
   - Metric: COUNT_DISTINCT(customer_id)
   - Sort: COUNT_DISTINCT(customer_id) descending
2. Segment spend quality table
   - Dimensions: segment_id, segment_name
   - Metrics: AVG(monetary), AVG(avg_order_value), AVG(frequency), AVG(recency_days)
3. Segment mix donut (optional)
   - Dimension: segment_name
   - Metric: COUNT_DISTINCT(customer_id)

### Section B: Spark Recommendation Coverage

Data source: grocery_analytics.customer_recommendations

Charts:
1. Recommendation volume scorecards
   - Recommended customers: COUNT_DISTINCT(customer_id)
   - Total recommendations: COUNT(product_id)
   - Avg recommendation score: AVG(score)
   - ALS Coverage %: SUM(CASE WHEN model_type = "ALS" THEN 1 ELSE 0 END) / COUNT(product_id)
   - Fallback Coverage %: SUM(CASE WHEN model_type = "HEURISTIC_FALLBACK" THEN 1 ELSE 0 END) / COUNT(product_id)
2. Top recommended products table
   - Dimensions: product_name, category_id
   - Metrics: COUNT(product_id), AVG(score)
   - Sort: COUNT(product_id) descending
3. Customer recommendation explorer table
   - Dimensions: customer_id, recommendation_rank, product_name, model_type
   - Metrics: score
   - Sort: customer_id ascending, recommendation_rank ascending

Controls:
- Filter by customer_id
- Filter by recommendation_rank (default 1 to 5)

Model behavior note:
- `model_type = ALS` means recommendations came from Spark ALS collaborative filtering.
- `model_type = HEURISTIC_FALLBACK` means ALS fallback was used due to sparse data or runtime issues.

Use case:
- This page helps business users validate Spark model output quality and prioritize campaign-ready recommendation lists.

## Spark ML Tables Schema Reference

Use this quick reference when configuring Looker Studio field types and default aggregations.

### Table: grocery_analytics.customer_segments

| Column | Expected Type | Suggested Looker Role | Default Aggregation |
|---|---|---|---|
| customer_id | Number (Integer) | Dimension | None |
| frequency | Number | Metric | SUM |
| monetary | Number (Currency) | Metric | SUM |
| avg_quantity | Number | Metric | AVG |
| avg_order_value | Number (Currency) | Metric | AVG |
| recency_days | Number | Metric | AVG |
| recency_score | Number (Integer) | Dimension | None |
| frequency_score | Number (Integer) | Dimension | None |
| monetary_score | Number (Integer) | Dimension | None |
| rfm_score | Text | Dimension | None |
| segment_id | Number (Integer) | Dimension | None |
| segment_name | Text | Dimension | None |
| snapshot_date | Date/DateTime | Dimension | None |
| model_generated_at | Date/DateTime | Dimension | None |

### Table: grocery_analytics.customer_recommendations

| Column | Expected Type | Suggested Looker Role | Default Aggregation |
|---|---|---|---|
| customer_id | Number (Integer) | Dimension | None |
| product_id | Number (Integer) | Dimension | None |
| product_name | Text | Dimension | None |
| category_id | Number (Integer) | Dimension | None |
| score | Number | Metric | AVG |
| recommendation_rank | Number (Integer) | Dimension | None |
| model_type | Text | Dimension | None |
| model_generated_at | Date/DateTime | Dimension | None |

Notes:
1. If Looker detects IDs as metrics, switch them to Dimension manually.
2. For scorecards on customer volume, prefer COUNT_DISTINCT(customer_id).
3. For recommendation quality, track AVG(score) and distribution by recommendation_rank.
4. Add a filter/control on `model_type` so you can monitor ALS vs fallback usage over time.
5. Format ALS/Fallback Coverage KPIs as percentages with 2 decimal places.

## Calculated Fields in Looker Studio

1. year_month

CONCAT(CAST(year AS TEXT), '-', LPAD(CAST(month AS TEXT), 2, '0'))

2. profit_margin_pct

SAFE_DIVIDE(SUM(total_profit), SUM(total_revenue))

## QA Checklist Before Sharing

1. All pages load without chart errors.
2. Date filter applies correctly across each page.
3. Country and category controls behave correctly for all relevant charts.
4. Executive totals match BigQuery validation queries for the same date range.
5. Product and employee rankings sort correctly.
6. Recommendation page (Spark) returns expected results after applying customer filter.
7. Spark ML page shows non-empty segment and recommendation tables after Spark task completes.

## Debug Tips

1. If numbers look inflated, check whether metric aggregation should be SUM vs COUNT_DISTINCT.
2. If a chart is blank, verify page filters are not over-restrictive.
3. Validate critical metrics using SQL from the README semantic layer section.

## Spark Output Validation SQL (Before Building Page 6)

Run these queries in BigQuery after `run_spark` completes.

1. Validate segment table exists and has rows

```sql
select
   count(*) as segment_rows,
   count(distinct customer_id) as customers_segmented
from `your-project-id.grocery_analytics.customer_segments`;
```

2. Validate recommendation table exists and has rows

```sql
select
   count(*) as recommendation_rows,
   count(distinct customer_id) as customers_with_reco,
   avg(score) as avg_reco_score,
   count(distinct model_type) as model_types_present
from `your-project-id.grocery_analytics.customer_recommendations`;
```

3. Verify segment distribution quality

```sql
select
   segment_id,
   segment_name,
   count(distinct customer_id) as customer_count,
   round(avg(monetary), 2) as avg_spend,
   round(avg(avg_order_value), 2) as avg_order_value
from `your-project-id.grocery_analytics.customer_segments`
group by segment_id, segment_name
order by segment_id;
```

4. Verify top recommended products

```sql
select
   product_name,
   category_id,
   model_type,
   count(*) as recommendation_count,
   round(avg(score), 2) as avg_score
from `your-project-id.grocery_analytics.customer_recommendations`
group by product_name, category_id, model_type
order by recommendation_count desc
limit 20;
```

5. Verify per-customer rank quality (max 10 recommendations)

```sql
select
   customer_id,
   max(recommendation_rank) as max_rank,
   count(*) as recommendation_count
from `your-project-id.grocery_analytics.customer_recommendations`
group by customer_id
having max(recommendation_rank) > 10
order by max_rank desc;
```

Expected result for query 5: zero rows.
