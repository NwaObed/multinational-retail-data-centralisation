-- Task 3
SELECT ROUND(CAST(SUM(product_price * product_quantity) AS numeric), 2) total_sales,
		month
FROM
	dim_products
JOIN
	orders_table
ON
	dim_products.product_code = orders_table.product_code
JOIN 
	dim_date_times
ON
	orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
	month
ORDER BY
	total_sales DESC
LIMIT 6;