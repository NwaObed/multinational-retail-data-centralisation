-- Task 6
SELECT
	ROUND(CAST(SUM(product_price * product_quantity) AS numeric), 2) total_sales,
	year,
	month
-- 	(total_sales / SUM(total_sales))*100
FROM 
	dim_date_times
JOIN
	orders_table
ON
	dim_date_times.date_uuid = orders_table.date_uuid
JOIN 
	dim_products
ON
	orders_table.product_code = dim_products.product_code
GROUP BY
	year, month
ORDER BY
	total_sales DESC
LIMIT 10