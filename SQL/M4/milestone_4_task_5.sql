-- -- Task 5 : Get the percentage of sales from each store
WITH total_table AS (
	-- Using cte to 
	SELECT
		store_type,
		ROUND(CAST(SUM(product_price * product_quantity) AS numeric), 2) total_sales
	FROM 
	dim_store_details
	JOIN
		orders_table
	ON
		dim_store_details.store_code = orders_table.store_code
	JOIN 
		dim_products
	ON
		orders_table.product_code = dim_products.product_code
	GROUP BY
		store_type
)
SELECT
	store_type,
	total_sales,
	ROUND(CAST (total_sales/(SELECT SUM(total_sales) FROM total_table)*100 AS numeric), 2) AS "percentage_total (%)"
FROM 
	total_table
ORDER BY
	"percentage_total (%)" DESC