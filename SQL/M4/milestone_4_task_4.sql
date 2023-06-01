SELECT
	COUNT(store_type) numbers_of_sales,
	SUM(product_quantity) AS product_quantity_count,
    CASE
        WHEN store_type = 'Web Portal' THEN 'Web'
        WHEN store_type IN ('Mall Kiosk', 'Super Store', 'Local', 'Outlet') THEN 'Offline'
    END AS location
FROM dim_store_details
JOIN
	orders_table
ON 
	dim_store_details.store_code = orders_table.store_code
JOIN 
	dim_products
ON
	dim_products.product_code = orders_table.product_code
GROUP BY
	location
ORDER BY 
	product_quantity_count