
-- ====================================================================

SELECT 
    DATE_TRUNC('month', order_date) AS sales_month,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_line_value,
    COUNT(*) AS total_order_lines
FROM raw_sales
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY sales_month;

-- ====================================================================

SELECT 
    product_code,
    product_line,
    COUNT(DISTINCT order_number) AS times_ordered,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(unit_price), 2) AS avg_unit_price,
    ROUND(SUM(total_amount) / SUM(quantity), 2) AS effective_price_per_unit
FROM raw_sales
GROUP BY product_code, product_line
ORDER BY total_revenue DESC
LIMIT 10;

-- ====================================================================

SELECT 
    customer_name,
    country,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(quantity) AS total_units_purchased,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_line_value,
    ROUND(SUM(total_amount) / COUNT(DISTINCT order_number), 2) AS avg_order_value
FROM raw_sales
GROUP BY customer_name, country
ORDER BY total_revenue DESC
LIMIT 10;

-- ====================================================================

SELECT 
    country,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_name) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_line_value,
    ROUND(
        SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM raw_sales),
        2
    ) AS revenue_percentage
FROM raw_sales
GROUP BY country
ORDER BY total_revenue DESC;

-- ====================================================================

WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS sales_month,
        SUM(total_amount) AS monthly_revenue,
        COUNT(DISTINCT order_number) AS monthly_orders
    FROM raw_sales
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT 
    sales_month,
    monthly_revenue,
    monthly_orders,
    LAG(monthly_revenue) OVER (ORDER BY sales_month) AS previous_month_revenue,
    ROUND(
        monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY sales_month),
        2
    ) AS revenue_change,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY sales_month)) * 100.0 / 
        NULLIF(LAG(monthly_revenue) OVER (ORDER BY sales_month), 0),
        2
    ) AS growth_percentage
FROM monthly_sales
ORDER BY sales_month;

-- ====================================================================

SELECT 
    deal_size,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_name) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_transaction_value,
    ROUND(
        SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM raw_sales),
        2
    ) AS revenue_percentage,
    ROUND(
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw_sales),
        2
    ) AS transaction_percentage
FROM raw_sales
GROUP BY deal_size
ORDER BY total_revenue DESC;

-- ====================================================================

WITH product_line_metrics AS (
    SELECT 
        product_line,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_name) AS unique_customers,
        COUNT(DISTINCT product_code) AS unique_products,
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_transaction_value,
        ROUND(
            SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM raw_sales),
            2
        ) AS revenue_percentage
    FROM raw_sales
    GROUP BY product_line
)
SELECT 
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    product_line,
    total_orders,
    unique_customers,
    unique_products,
    total_units_sold,
    total_revenue,
    avg_transaction_value,
    revenue_percentage,
    ROUND(total_revenue / unique_products, 2) AS revenue_per_product
FROM product_line_metrics
ORDER BY revenue_rank;

===================