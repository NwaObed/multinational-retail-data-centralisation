-- Task 8
SELECT
	ROUND(CAST(SUM(product_price * product_quantity) AS numeric), 2) total_sales,
	store_type,
	country_code
-- 	(total_sales / SUM(total_sales))*100
FROM 
	dim_store_details
JOIN
	orders_table
ON
	dim_store_details.store_code =  orders_table.store_code
JOIN 
	dim_products
ON
	orders_table.product_code = dim_products.product_code
WHERE
	country_code = 'DE'
GROUP BY
	store_type, country_code
ORDER BY
	total_sales 
LIMIT 10