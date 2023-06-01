-- Task 1
SELECT country_code AS country,
		COUNT(country_code) As total_no_stores
FROM 
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_no_stores DESC