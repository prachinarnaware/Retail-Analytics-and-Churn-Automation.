CREATE DATABASE retail_project1;
USE retail_project1;
sales_rawCREATE TABLE sales_raw (
invoice_no VARCHAR(20),
customer_id INT,
product_id INT,
product_name VARCHAR(100),
category VARCHAR(50),
quantity INT,
unit_price DECIMAL(10,2),
invoice_date DATE,
region VARCHAR(50)
);
SELECT * FROM sales_raw LIMIT 10;
SELECT COUNT(*) FROM sales_raw;
CREATE TABLE dim_customer AS
SELECT DISTINCT customer_id
FROM sales_raw;
CREATE TABLE dim_product AS
SELECT DISTINCT product_id,product_name,category
FROM sales_raw;
CREATE TABLE dim_region AS
SELECT DISTINCT region
FROM sales_raw;
SELECT * FROM dim_customer
LIMIT 10;
SELECT * FROM dim_product
LIMIT 10;
SELECT * FROM dim_region
LIMIT 10;
SELECT SUM(quantity*unit_price) AS total_sales
FROM sales_raw;
SELECT region,
SUM(quantity*unit_price) AS total_sales
FROM sales_raw
GROUP BY region;
SELECT product_name,
SUM(quantity) AS total_qty
FROM sales_raw
GROUP BY product_name
ORDER BY total_qty DESC;
CREATE TABLE fact_sales AS
SELECT 
invoice_no,
customer_id,
product_id,
quantity,
quantity * unit_price AS revenue,
invoice_date
FROM sales_raw;
SELECT * FROM fact_sales
LIMIT 10;
CREATE VIEW customer_360 AS
SELECT
c.customer_id,
r.region,
p.product_name,
p.category,
f.invoice_date,
f.quantity,
f.revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_region r ON r.region = r.region;
SELECT * FROM customer_360
LIMIT 100;
SELECT * FROM customer_360 LIMIT 300;
SELECT
  customer_id,
  SUM(revenue) AS total_spent
FROM customer_360
GROUP BY customer_id;
SELECT
  customer_id,
  COUNT(DISTINCT invoice_no) AS total_orders
FROM fact_sales
GROUP BY customer_id;
SELECT
  customer_id,
  MAX(invoice_date) AS last_purchase_date
FROM fact_sales
GROUP BY customer_id;
CREATE TABLE rfm_base AS
SELECT
  customer_id,
  DATEDIFF(CURDATE(), MAX(invoice_date)) AS recency,
  COUNT(DISTINCT invoice_no) AS frequency,
  SUM(revenue) AS monetary
FROM fact_sales
GROUP BY customer_id;
SELECT *,
CASE
  WHEN monetary > 500 THEN 'High Value'
  WHEN monetary BETWEEN 300 AND 1000 THEN 'Medium Value'
  ELSE 'Low Value'
END AS customer_segment
FROM rfm_base;
SELECT customer_id, invoice_date, revenue
FROM customer_360;

SELECT customer_id, invoice_date, product_name
FROM customer_360;
